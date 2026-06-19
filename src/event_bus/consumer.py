# ─────────────────────────────────────────────
# Consumer  (asynchronous — for orchestrator)
# ─────────────────────────────────────────────

import asyncio
import logging
from typing import AsyncIterator
import redis.asyncio as aioredis
from src.contracts.ingestion import IngestionPayload

log = logging.getLogger(__name__)

STREAM_NAME: str    = "aegis.market.ticks"
CONSUMER_GROUP: str = "aegis.orchestrator"
BATCH_SIZE: int     = 50
BLOCK_MS: int       = 2_000
AUTOCLAIM_MIN_IDLE_MS: int = 30_000

from src.contracts.ingestion import IngestionPayload

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
                id="0",             # start from latest; use '0' for full replay
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
