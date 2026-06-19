# ─────────────────────────────────────────────
# Pydantic models
# ─────────────────────────────────────────────

from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, PositiveFloat, Field, field_validator, model_validator
from src.contracts.primitives import TradeInstrument

class IngestionPayload(BaseModel):
    """
    Canonical market tick pushed by every data-feed connector.

    All F&O relevant fields are optional — a spot tick from the NSE cash
    segment will not carry Greeks; an options tick will.
    """
    
    timestamp:    datetime        = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp of the tick",
    )
    instrument: TradeInstrument = Field(..., description="Rich nested contract details")
    spot_price: PositiveFloat = Field(..., description="Live spot price for the underlying")

    # Options Chain Data
    bid_price: Optional[PositiveFloat] = Field(default=None)
    ask_price: Optional[PositiveFloat] = Field(default=None)
    last_traded_price: Optional[PositiveFloat] = Field(default=None)
    volume: Optional[int] = Field(default=None, ge=0)
    open_interest: Optional[int] = Field(default=None, ge=0)

    # Quantitative Metrics
    implied_volatility: Optional[float] = Field(default=None, ge=0.0, le=200.0)
    iv_percentile: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    delta: Optional[float] = Field(default=None, ge=-1.0, le=1.0)
    gamma: Optional[float] = Field(default=None)
    theta: Optional[float] = Field(default=None)
    vega: Optional[float] = Field(default=None)

    source: str = Field(default="unknown", description="Feed source identifier")

    model_config = {"frozen": True, "extra": "forbid"}

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
        if self.bid_price is not None and self.ask_price is not None:
            if self.bid_price > self.ask_price:
                raise ValueError(f"bid ({self.bid_price}) > ask ({self.ask_price}): invalid spread")
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
            "symbol": self.instrument.symbol,           # Top-level for fast Redis XREAD filtering
            "underlying": self.instrument.underlying_symbol,
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