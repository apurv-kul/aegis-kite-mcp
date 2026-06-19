"""
Project Aegis — market_data_pipeline.py
=========================================
High-throughput, low-latency market data pipeline built on Redis Streams.

Architecture
------------
                 ┌──────────────────────────────────────────┐
  Groww/NSE ───► │  MarketDataPublisher                     │
  WebSocket      │  xadd ──► aegis.market.ticks (maxlen=10K)│
                 └──────────────────────────────────────────┘
                                    │  Redis Stream
                 ┌──────────────────▼───────────────────────┐
                 │  MarketDataConsumer (Consumer Group)      │
                 │  xreadgroup ──► process ──► xack          │
                 │  LangGraph orchestrator / Risk Engine     │
                 └──────────────────────────────────────────┘

Key design choices
------------------
* Redis Consumer Groups  — crash-safe "at-least-once" delivery. Unacked
  messages stay in the PEL (Pending Entry List) and are re-delivered on
  restart via XAUTOCLAIM.
* Pydantic v2 for strict runtime validation on both publish and consume paths.
* Async consumer loop (asyncio + redis.asyncio) — non-blocking, compatible
  with LangGraph's async orchestrator.
* Sync publisher — fire-and-forget from a synchronous data-feed callback is
  simpler and avoids event-loop hand-off complexity in the ingestion layer.
* MAXLEN ~= 10_000 with APPROXIMATE trimming — O(1) cost on every XADD.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import AsyncIterator, Optional

import redis
import redis.asyncio as aioredis
from pydantic import BaseModel, Field, field_validator, model_validator

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("aegis.pipeline")


# ─────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────
STREAM_NAME: str    = "aegis.market.ticks"
CONSUMER_GROUP: str = "aegis.orchestrator"
STREAM_MAXLEN: int  = 10_000      # approximate cap; trimmed on every XADD
BLOCK_MS: int       = 2_000       # XREADGROUP blocking timeout per iteration
BATCH_SIZE: int     = 50          # messages to fetch per XREADGROUP call
AUTOCLAIM_MIN_IDLE_MS: int = 30_000   # reclaim PEL entries idle > 30 s


# ─────────────────────────────────────────────
# Pydantic models
# ─────────────────────────────────────────────
class IngestionPayload(BaseModel):
    """
    Canonical market tick pushed by every data-feed connector.

    All F&O relevant fields are optional — a spot tick from the NSE cash
    segment will not carry Greeks; an options tick will.
    """
    instrument:   str             = Field(..., description="NSE symbol, e.g. 'NIFTY24JUN23000CE'")
    exchange:     str             = Field(default="NFO", description="'NFO', 'NSE', 'BSE'")
    spot_price:   Decimal         = Field(..., gt=0, description="Last traded price")
    bid:          Optional[Decimal] = Field(default=None, ge=0)
    ask:          Optional[Decimal] = Field(default=None, ge=0)
    volume:       Optional[int]   = Field(default=None, ge=0)
    oi:           Optional[int]   = Field(default=None, ge=0, description="Open interest")
    iv:           Optional[float] = Field(default=None, ge=0, description="Implied volatility (annualised %)")
    delta:        Optional[float] = Field(default=None, ge=-1, le=1)
    gamma:        Optional[float] = Field(default=None)
    theta:        Optional[float] = Field(default=None)
    vega:         Optional[float] = Field(default=None)
    timestamp:    datetime        = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp of the tick",
    )
    source:       str             = Field(default="unknown", description="Feed source identifier")

    @field_validator("instrument")
    @classmethod
    def instrument_nonempty(cls, v: str) -> str:
        v = v.strip().upper()
        if not v:
            raise ValueError("instrument must not be empty")
        return v

    @field_validator("timestamp", mode="before")
    @classmethod
    def coerce_timestamp(cls, v):
        """Accept ISO-string, epoch float, or datetime."""
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v, tz=timezone.utc)
        if isinstance(v, str):
            return datetime.fromisoformat(v)
        return v

    @model_validator(mode="after")
    def bid_ask_sanity(self) -> "IngestionPayload":
        if self.bid is not None and self.ask is not None:
            if self.bid > self.ask:
                raise ValueError(f"bid ({self.bid}) > ask ({self.ask}): invalid spread")
        return self

    # ── Serialisation helpers ──────────────────────────────────────────────

    def to_redis_dict(self) -> dict[str, str]:
        """
        Flatten the model to a {str: str} dict suitable for XADD.
        Redis field values must be strings (or bytes).
        We JSON-encode the entire payload into a single 'data' field to
        avoid type-mapping complexity and keep deserialization trivial.
        """
        return {
            # Top-level index fields — stored flat for fast XREAD filtering
            # (Redis Streams don't support server-side field filtering, but
            # keeping instrument/timestamp flat helps with consumer-side
            # fast-path checks without deserialising the full blob.)
            "instrument": self.instrument,
            "exchange":   self.exchange,
            "ts_epoch":   str(self.timestamp.timestamp()),
            # Full payload JSON blob
            "data": self.model_dump_json(),
        }

    @classmethod
    def from_redis_dict(cls, raw: dict) -> "IngestionPayload":
        """
        Reconstruct an IngestionPayload from the raw dict returned by redis-py.
        redis-py returns bytes keys/values when decode_responses=False, or
        str when decode_responses=True. We handle both.
        """
        def _decode(v) -> str:
            return v.decode() if isinstance(v, bytes) else v

        data_json = _decode(raw.get(b"data") or raw.get("data", "{}"))
        return cls.model_validate_json(data_json)


# ─────────────────────────────────────────────
# Publisher  (synchronous — for feed callbacks)
# ─────────────────────────────────────────────
class MarketDataPublisher:
    """
    Synchronous publisher that writes IngestionPayload ticks to the
    ``aegis.market.ticks`` Redis Stream.

    Designed to be called from synchronous WebSocket/REST feed callbacks.
    Uses a connection pool so it is safe to instantiate once and call from
    multiple threads (e.g. multiple instrument subscriptions).

    Parameters
    ----------
    host : str
        Redis host. Defaults to 'localhost'.
    port : int
        Redis port. Defaults to 6379.
    db : int
        Redis database index. Defaults to 0.
    password : str | None
        Redis AUTH password. Defaults to None.
    stream_name : str
        Target stream key. Defaults to STREAM_NAME constant.
    maxlen : int
        Approximate stream cap. Defaults to STREAM_MAXLEN.
    """

    def __init__(
        self,
        host:        str          = "localhost",
        port:        int          = 6379,
        db:          int          = 0,
        password:    str | None   = None,
        stream_name: str          = STREAM_NAME,
        maxlen:      int          = STREAM_MAXLEN,
    ) -> None:
        self._stream   = stream_name
        self._maxlen   = maxlen
        self._pool     = redis.ConnectionPool(
            host=host, port=port, db=db, password=password,
            decode_responses=True,          # str keys/values throughout
            max_connections=10,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
        )
        self._client   = redis.Redis(connection_pool=self._pool)
        self._published = 0
        log.info(
            "MarketDataPublisher ready → stream='%s' maxlen=%d redis=%s:%d",
            self._stream, self._maxlen, host, port,
        )

    # ── public API ──────────────────────────────────────────────────────────

    def publish_tick(self, payload: IngestionPayload) -> str:
        """
        Serialize and XADD a single market tick to the stream.

        Parameters
        ----------
        payload : IngestionPayload
            A validated Pydantic tick model.

        Returns
        -------
        str
            The Redis Stream message-id assigned by the server
            (format: ``<milliseconds>-<sequence>``, e.g. ``1718000000000-0``).

        Raises
        ------
        redis.exceptions.RedisError
            Propagated on connection or command failure.
        pydantic.ValidationError
            If the caller constructs IngestionPayload incorrectly (caught early).
        """
        fields = payload.to_redis_dict()
        msg_id = self._client.xadd(
            self._stream,
            fields,
            maxlen=self._maxlen,
            approximate=True,   # MAXLEN ~ N  →  O(1) amortised trim cost
        )
        self._published += 1
        log.debug(
            "XADD  id=%-20s  instrument=%-25s  price=%s",
            msg_id, payload.instrument, payload.spot_price,
        )
        return msg_id

    def publish_raw(self, payload_dict: dict) -> str:
        """
        Convenience method: construct an IngestionPayload from a raw dict,
        validate it, then publish. Raises ValidationError on bad input.
        """
        return self.publish_tick(IngestionPayload.model_validate(payload_dict))

    @property
    def published_count(self) -> int:
        """Total ticks published in this session."""
        return self._published

    def stream_info(self) -> dict:
        """Return XINFO STREAM summary for observability / health-checks."""
        try:
            info = self._client.xinfo_stream(self._stream)
            return {
                "length":          info.get("length"),
                "first_entry":     info.get("first-entry"),
                "last_entry":      info.get("last-entry"),
                "radix_tree_keys": info.get("radix-tree-keys"),
            }
        except redis.exceptions.ResponseError:
            return {"error": "stream does not exist yet"}

    def close(self) -> None:
        """Release the connection pool."""
        self._pool.disconnect()
        log.info("MarketDataPublisher closed. Total published: %d", self._published)


# ─────────────────────────────────────────────
# Consumer  (asynchronous — for orchestrator)
# ─────────────────────────────────────────────
class MarketDataConsumer:
    """
    Async consumer that reads from ``aegis.market.ticks`` via Redis
    Consumer Groups, guaranteeing at-least-once delivery.

    Consumer group semantics
    ------------------------
    * Every message delivered to a consumer is tracked in the server-side
      PEL (Pending Entry List) until explicitly ACKed.
    * On crash/restart, ``>`` (next undelivered) resumes from where the group
      left off; unacked PEL entries are reclaimed via XAUTOCLAIM.
    * Multiple consumer instances can read the same group concurrently for
      horizontal scaling (e.g. Research Agent + Risk Engine in parallel).

    Parameters
    ----------
    host / port / db / password
        Standard Redis connection parameters.
    stream_name : str
        Source stream key.
    group_name : str
        Consumer group name. All consumers sharing a group compete for messages.
    consumer_name : str
        Unique identifier for this consumer instance. Use the pod/hostname in K8s.
    batch_size : int
        Number of messages to fetch per XREADGROUP call.
    block_ms : int
        Milliseconds to block per XREADGROUP if the stream is empty.
    """

    def __init__(
        self,
        host:          str        = "localhost",
        port:          int        = 6379,
        db:            int        = 0,
        password:      str | None = None,
        stream_name:   str        = STREAM_NAME,
        group_name:    str        = CONSUMER_GROUP,
        consumer_name: str        = "consumer-1",
        batch_size:    int        = BATCH_SIZE,
        block_ms:      int        = BLOCK_MS,
    ) -> None:
        self._stream        = stream_name
        self._group         = group_name
        self._consumer      = consumer_name
        self._batch         = batch_size
        self._block_ms      = block_ms
        self._processed     = 0
        self._pending_claim = 0

        # Async Redis client — one connection per consumer instance
        self._redis = aioredis.Redis(
            host=host, port=port, db=db, password=password,
            decode_responses=False,     # we handle decoding in from_redis_dict
            socket_connect_timeout=5,
            socket_timeout=10,
            retry_on_timeout=True,
        )
        log.info(
            "MarketDataConsumer ready → stream='%s' group='%s' consumer='%s'",
            self._stream, self._group, self._consumer,
        )

    # ── Setup ──────────────────────────────────────────────────────────────

    async def ensure_group(self) -> None:
        """
        Create the consumer group if it doesn't exist.
        Uses '$' as start ID so a fresh group only reads *new* messages.
        Pass '0' to replay all historical messages in the stream.

        Safe to call on every startup — BUSYGROUP error is swallowed.
        """
        try:
            await self._redis.xgroup_create(
                self._stream,
                self._group,
                id="$",             # start from latest; use '0' for full replay
                mkstream=True,      # create the stream key if missing
            )
            log.info("Consumer group '%s' created on stream '%s'", self._group, self._stream)
        except aioredis.exceptions.ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                log.debug("Consumer group '%s' already exists — skipping create.", self._group)
            else:
                raise

    # ── Core read loop ──────────────────────────────────────────────────────

    async def listen_for_ticks(self) -> AsyncIterator[tuple[str, IngestionPayload]]:
        """
        Async generator that yields ``(message_id, IngestionPayload)`` tuples.

        On each iteration:
        1. First checks for any un-acked PEL entries from a previous crash
           (via XAUTOCLAIM) and yields those before reading new messages.
        2. Then reads new messages with ``>`` (next undelivered).

        The caller is responsible for calling ``acknowledge_tick(message_id)``
        after successful processing to remove the entry from the PEL.

        Yields
        ------
        tuple[str, IngestionPayload]
            (redis_message_id, validated_tick)

        Example
        -------
        ::

            async for msg_id, tick in consumer.listen_for_ticks():
                await process(tick)
                await consumer.acknowledge_tick(msg_id)
        """
        await self.ensure_group()
        log.info("Consumer '%s' starting listen loop on '%s'", self._consumer, self._stream)

        while True:
            # ── Phase 1: reclaim stale PEL entries from dead consumers ──────
            try:
                claimed = await self._redis.xautoclaim(
                    self._stream,
                    self._group,
                    self._consumer,
                    min_idle_time=AUTOCLAIM_MIN_IDLE_MS,
                    start_id="0-0",
                    count=self._batch,
                )
                # xautoclaim returns (next_start_id, [[id, fields], ...], [deleted_ids])
                _, claimed_messages, _ = claimed
                if claimed_messages:
                    log.info(
                        "XAUTOCLAIM reclaimed %d stale message(s) into '%s'",
                        len(claimed_messages), self._consumer,
                    )
                    for msg_id, fields in claimed_messages:
                        tick = self._safe_deserialize(msg_id, fields)
                        if tick:
                            self._pending_claim += 1
                            yield msg_id.decode() if isinstance(msg_id, bytes) else msg_id, tick
            except aioredis.exceptions.ResponseError as exc:
                # XAUTOCLAIM requires Redis 7.0+; gracefully skip on older versions
                log.warning("XAUTOCLAIM not available (%s) — skipping PEL reclaim.", exc)

            # ── Phase 2: read new undelivered messages ──────────────────────
            try:
                response = await self._redis.xreadgroup(
                    groupname=self._group,
                    consumername=self._consumer,
                    streams={self._stream: ">"},    # '>' = next undelivered
                    count=self._batch,
                    block=self._block_ms,
                    noack=False,                    # manual ACK required
                )
            except aioredis.exceptions.ConnectionError as exc:
                log.error("Redis connection error in consumer loop: %s — retrying in 2s", exc)
                await asyncio.sleep(2)
                continue
            except aioredis.exceptions.ResponseError as exc:
                # Group may have been deleted externally
                log.error("XREADGROUP error: %s — retrying in 5s", exc)
                await asyncio.sleep(5)
                continue

            if not response:
                # Block timeout — no new messages; loop immediately
                continue

            # response shape: [[stream_name, [[msg_id, {field: value}], ...]]]
            for _stream_name, messages in response:
                for msg_id, fields in messages:
                    tick = self._safe_deserialize(msg_id, fields)
                    if tick:
                        self._processed += 1
                        log.debug(
                            "XREAD  id=%-20s  instrument=%-25s  price=%s",
                            msg_id.decode() if isinstance(msg_id, bytes) else msg_id,
                            tick.instrument,
                            tick.spot_price,
                        )
                        yield (
                            msg_id.decode() if isinstance(msg_id, bytes) else msg_id,
                            tick,
                        )

    # ── Acknowledge ─────────────────────────────────────────────────────────

    async def acknowledge_tick(self, message_id: str) -> bool:
        """
        ACK a processed message, removing it from the PEL.

        Must be called after successful processing. Un-ACKed messages will
        be re-delivered to this (or another) consumer after AUTOCLAIM_MIN_IDLE_MS.

        Parameters
        ----------
        message_id : str
            The message id returned by listen_for_ticks.

        Returns
        -------
        bool
            True if the message was successfully ACKed, False if it was
            already ACKed or not found (idempotent — safe to call twice).
        """
        result = await self._redis.xack(self._stream, self._group, message_id)
        acked = bool(result)
        if acked:
            log.debug("XACK   id=%s", message_id)
        else:
            log.warning("XACK returned 0 for id=%s — already acked?", message_id)
        return acked

    # ── Pending inspection ──────────────────────────────────────────────────

    async def pending_count(self) -> int:
        """Return the number of messages currently in this consumer's PEL."""
        try:
            summary = await self._redis.xpending(self._stream, self._group)
            return summary.get("pending", 0) if isinstance(summary, dict) else int(summary[0])
        except Exception as exc:
            log.warning("Could not fetch PEL count: %s", exc)
            return -1

    # ── Internals ──────────────────────────────────────────────────────────

    def _safe_deserialize(self, msg_id, fields: dict) -> IngestionPayload | None:
        """
        Deserialise a Redis Stream field dict into an IngestionPayload.
        Returns None and logs a warning on any parse failure so the consumer
        loop never crashes on a single malformed message.
        """
        try:
            return IngestionPayload.from_redis_dict(fields)
        except Exception as exc:
            raw_id = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
            log.warning(
                "Deserialization failed for msg_id=%s: %s — skipping tick.",
                raw_id, exc,
            )
            return None

    @property
    def processed_count(self) -> int:
        """Total new (non-reclaimed) messages processed in this session."""
        return self._processed

    async def close(self) -> None:
        """Close the async Redis connection."""
        await self._redis.aclose()
        log.info(
            "MarketDataConsumer '%s' closed. processed=%d reclaimed=%d",
            self._consumer, self._processed, self._pending_claim,
        )


# ─────────────────────────────────────────────
# Demo  __main__
# ─────────────────────────────────────────────
async def _demo() -> None:
    """
    End-to-end smoke test:
      1. Publisher sends 5 mock NIFTY option ticks.
      2. Consumer reads them via Consumer Group, validates, ACKs, and prints.

    Requires a running Redis instance on localhost:6379.
    """
    log.info("═" * 60)
    log.info("Project Aegis — Redis Streams pipeline demo")
    log.info("═" * 60)

    DEMO_STREAM = "aegis.demo.ticks"   # separate key so we don't pollute production

    # ── Publisher ────────────────────────────────────────────────────────────
    publisher = MarketDataPublisher(stream_name=DEMO_STREAM, maxlen=1_000)

    mock_ticks = [
        {
            "instrument": "NIFTY24JUN23000CE",
            "exchange":   "NFO",
            "spot_price": "150.25",
            "bid":        "149.90",
            "ask":        "150.60",
            "volume":     12_450,
            "oi":         850_000,
            "iv":         14.3,
            "delta":      0.42,
            "gamma":      0.0031,
            "theta":      -18.5,
            "vega":       28.7,
            "source":     "groww-ws",
        },
        {
            "instrument": "NIFTY24JUN23000PE",
            "exchange":   "NFO",
            "spot_price": "95.50",
            "bid":        "95.20",
            "ask":        "95.80",
            "volume":     9_800,
            "oi":         1_200_000,
            "iv":         16.1,
            "delta":      -0.58,
            "gamma":      0.0029,
            "theta":      -16.2,
            "vega":       27.3,
            "source":     "groww-ws",
        },
        {
            "instrument": "BANKNIFTY24JUN50000CE",
            "exchange":   "NFO",
            "spot_price": "320.00",
            "volume":     5_600,
            "oi":         320_000,
            "iv":         18.9,
            "delta":      0.38,
            "source":     "nse-direct",
        },
        {
            "instrument": "RELIANCE",
            "exchange":   "NSE",
            "spot_price": "2945.30",
            "bid":        "2944.85",
            "ask":        "2945.75",
            "volume":     1_234_567,
            "source":     "nse-direct",
        },
        {
            "instrument": "NIFTY50",
            "exchange":   "NSE",
            "spot_price": "23150.40",
            "volume":     9_999_999,
            "source":     "nse-websocket",
        },
    ]

    log.info("\n── Publishing %d mock ticks ──", len(mock_ticks))
    published_ids = []
    for raw in mock_ticks:
        msg_id = publisher.publish_raw(raw)
        published_ids.append(msg_id)
        log.info("  ✓ Published  %-30s  id=%s", raw["instrument"], msg_id)
        time.sleep(0.05)    # slight delay to get distinct stream IDs

    log.info("Stream info: %s", publisher.stream_info())
    publisher.close()

    # ── Consumer ─────────────────────────────────────────────────────────────
    log.info("\n── Starting consumer (will read %d messages then exit) ──", len(mock_ticks))

    consumer = MarketDataConsumer(
        stream_name=DEMO_STREAM,
        group_name="aegis.demo.group",
        consumer_name="demo-consumer-1",
    )

    received = 0
    async for msg_id, tick in consumer.listen_for_ticks():
        received += 1
        log.info(
            "  ✓ Consumed  [%d/%d]  id=%-20s  instrument=%-25s  price=%s  iv=%s  delta=%s",
            received, len(mock_ticks),
            msg_id, tick.instrument, tick.spot_price,
            tick.iv, tick.delta,
        )

        # Validate a specific field to prove Pydantic round-trip works
        assert isinstance(tick.spot_price, Decimal), "spot_price should be Decimal"
        assert isinstance(tick.timestamp, datetime),  "timestamp should be datetime"

        await consumer.acknowledge_tick(msg_id)

        if received >= len(mock_ticks):
            break   # exit after consuming all demo messages

    pending = await consumer.pending_count()
    log.info("\n── Demo complete ──")
    log.info("  Messages published : %d", len(published_ids))
    log.info("  Messages consumed  : %d", received)
    log.info("  PEL after ACK      : %d  (should be 0)", pending)
    assert pending == 0, "All messages should be ACKed"
    assert received == len(mock_ticks), "Consumer should have received all published ticks"
    log.info("  ✅ All assertions passed — pipeline round-trip verified.")

    await consumer.close()


if __name__ == "__main__":
    asyncio.run(_demo())