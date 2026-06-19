# ─────────────────────────────────────────────
# Publisher  (synchronous — for feed callbacks)
# ─────────────────────────────────────────────

from src.contracts.ingestion import IngestionPayload
import logging
import redis
from src.contracts.ingestion import IngestionPayload

log = logging.getLogger(__name__)

STREAM_NAME: str    = "aegis.market.ticks"
STREAM_MAXLEN: int  = 10_000

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