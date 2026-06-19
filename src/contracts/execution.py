from typing import Optional
import uuid
from pydantic import BaseModel, Field, PositiveFloat, field_validator
from src.contracts.primitives import TradeInstrument, TransactionType, OrderType, ExitReason

class ExecutionOrder(BaseModel):
    # ── Identity ──────────────────────────────────────────────────────────────
    order_id: str = Field(
        default_factory=lambda: f"ORD-{uuid.uuid4().hex[:8].upper()}",
        description=(
            "Unique Aegis order identifier. Auto-generated at construction. "
            "Used as the trading_journal primary key and for Kite order tracking. "
            "Distinct from kite_order_id (Kite's own ID, set after submission)."
        ),
    )

    # ── Order specification ───────────────────────────────────────────────────
    instrument: TradeInstrument = Field(...)
    transaction_type: TransactionType = Field(...)
    order_type: OrderType = Field(...)
    quantity: int = Field(..., ge=1)
    lot_size: int = Field(25, ge=1)
    price: Optional[PositiveFloat] = Field(None)
    algo_id: str = Field(..., pattern=r"^[A-Z0-9_-]{6,20}$")

    # ── Lifecycle state (mutable — updated by Execution Monitor) ──────────────
    is_complete: bool = Field(
        default=False,
        description=(
            "Set to True by the Execution Monitor when the order reaches a "
            "terminal state: fully filled, cancelled, or rejected. "
            "PostTradeNode waits for is_complete before writing the journal entry."
        ),
    )
    exit_reason: Optional[ExitReason] = Field(
        default=None,
        description=(
            "Why the position was closed. Set by the Execution Monitor when "
            "an exit trigger fires (stop, target, IV spike, etc.). "
            "None while the position is open. "
            "Persisted to trading_journal for strategy attribution and "
            "learning-loop outcome labelling in Neo4j."
        ),
    )
    realised_pnl_inr: Optional[float] = Field(
        default=None,
        description=(
            "Realised P&L in ₹ after all legs are closed, net of brokerage, "
            "STT, and exchange charges. Set by PostTradeNode from fill receipts. "
            "None while the position is open or fills are pending. "
            "Positive = profit, negative = loss."
        ),
    )

    # frozen=False: ExecutionOrder is a mutable lifecycle object.
    # is_complete, exit_reason, realised_pnl_inr are all written AFTER
    # construction by the monitor loop. Freezing would require rebuilding
    # the entire object on every status update — unnecessary overhead.
    # TradeThesis and RiskAssessment remain frozen (they are immutable verdicts).
    model_config = {"frozen": False, "extra": "forbid"}

    @field_validator("price")
    @classmethod
    def validate_price_for_order_type(cls, value, info):
        order_type = info.data.get("order_type")
        if order_type == OrderType.LIMIT and value is None:
            raise ValueError("price is required for LIMIT orders")
        if order_type == OrderType.MARKET and value is not None:
            raise ValueError("price must be omitted for MARKET orders")
        return value

    from pydantic import model_validator

    @model_validator(mode="after")
    def validate_lot_multiple(self) -> "ExecutionOrder":
        """
        Validates quantity is a multiple of lot_size.
        Must be a model_validator (not field_validator) because Pydantic v2
        field_validators only see fields declared before the current field
        in source order — lot_size would be absent during quantity validation.
        """
        if self.quantity % self.lot_size != 0:
            raise ValueError(
                f"quantity ({self.quantity}) must be a multiple of "
                f"lot_size ({self.lot_size})"
            )
        return self