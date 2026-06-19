from typing import List, Optional
import uuid
from pydantic import BaseModel, Field, PositiveFloat
from src.contracts.primitives import TradeInstrument, TransactionType, StrategyType

class StrikeLeg(BaseModel):
    leg_name: str = Field(..., min_length=1, max_length=40)
    instrument: TradeInstrument = Field(...)
    direction: TransactionType = Field(...)
    expected_premium: PositiveFloat = Field(...)
    model_config = {"frozen": True, "extra": "forbid"}

class TradeThesis(BaseModel):
    thesis_id: str = Field(
        default_factory=lambda: f"THESIS-{uuid.uuid4().hex[:8].upper()}",
        description=(
            "Unique identifier for this trade thesis. "
            "Auto-generated. Used as the primary correlation key in the "
            "trading_journal, Neo4j knowledge graph, and Qdrant vector store."
        ),
    )
    strategy_type: StrategyType = Field(...)
    primary_instrument: TradeInstrument = Field(...)
    strike_legs: List[StrikeLeg] = Field(..., min_length=1)
    expected_entry_premium: PositiveFloat = Field(...)
    target_profit_pct: float = Field(..., ge=0.1, le=200.0)
    stop_loss_pct: float = Field(..., ge=0.1, le=200.0)
    confidence: int = Field(..., ge=0, le=100)
    rationale: Optional[str] = Field(None, max_length=1024)
    model_config = {"frozen": True, "extra": "forbid"}