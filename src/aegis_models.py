from __future__ import annotations

import os
from datetime import date, datetime
from enum import Enum
from typing import List, Optional

from dotenv import load_dotenv
from pydantic import BaseModel, Field, PositiveFloat, SecretStr, field_validator

load_dotenv()


class OptionType(str, Enum):
    CE = "CE"
    PE = "PE"


class TransactionType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"


class StrategyType(str, Enum):
    BEAR_CALL_SPREAD = "Bear Call Spread"
    BULL_PUT_SPREAD = "Bull Put Spread"
    IRON_CONDOR = "Iron Condor"
    STRADDLE = "Straddle"
    STRANGLE = "Strangle"


class TradeInstrument(BaseModel):
    symbol: str = Field(
        ...,
        regex=r"^[A-Z0-9_\-]{3,40}$",
        description="Broker symbol for the contract, e.g. NIFTY23JUN17500CE",
    )
    underlying_symbol: str = Field(
        ...,
        regex=r"^[A-Z0-9_\-]{3,20}$",
        description="Underlying instrument symbol, e.g. NIFTY",
    )
    expiry_date: date = Field(..., description="Options expiry date")
    strike_price: PositiveFloat = Field(..., description="Option strike price")
    option_type: OptionType = Field(..., description="Call or Put leg")

    model_config = {"frozen": True, "extra": "forbid"}


class IngestionPayload(BaseModel):
    market_timestamp: datetime = Field(
        ..., description="Timezone-aware timestamp for the market snapshot"
    )
    underlying_spot_price: PositiveFloat = Field(
        ..., description="Live spot price for the underlying index or stock"
    )
    option_contract: TradeInstrument = Field(..., description="Specific option chain contract")
    implied_volatility: float = Field(
        ..., ge=0.0, le=200.0, description="Implied volatility percentage"
    )
    iv_percentile: float = Field(
        ..., ge=0.0, le=100.0, description="IV percentile relative to historical range"
    )
    bid_price: PositiveFloat = Field(..., description="Best bid price for the option contract")
    ask_price: PositiveFloat = Field(..., description="Best ask price for the option contract")
    last_traded_price: PositiveFloat = Field(..., description="Last traded price for the option contract")
    open_interest: int = Field(
        ..., ge=0, description="Open interest for the option contract"
    )
    volume: int = Field(..., ge=0, description="Volume traded for the option contract")

    model_config = {"frozen": True, "extra": "forbid"}


class StrikeLeg(BaseModel):
    leg_name: str = Field(..., min_length=1, max_length=40)
    instrument: TradeInstrument = Field(...)
    direction: TransactionType = Field(...)
    expected_premium: PositiveFloat = Field(...)

    model_config = {"frozen": True, "extra": "forbid"}


class TradeThesis(BaseModel):
    strategy_type: StrategyType = Field(...)
    primary_instrument: TradeInstrument = Field(...)
    strike_legs: List[StrikeLeg] = Field(
        ..., min_length=1, description="Ordered legs for the proposed spread strategy"
    )
    expected_entry_premium: PositiveFloat = Field(
        ..., description="Total expected entry premium for the strategy"
    )
    target_profit_pct: float = Field(
        ..., ge=0.1, le=200.0, description="Target profit as a percentage of premium paid"
    )
    stop_loss_pct: float = Field(
        ..., ge=0.1, le=200.0, description="Stop-loss as a percentage of premium paid"
    )
    confidence: int = Field(
        ..., ge=0, le=100, description="LLM confidence score for the trade thesis"
    )
    rationale: Optional[str] = Field(
        None,
        max_length=1024,
        description="Optional structured explanation for audit and risk review",
    )

    model_config = {"frozen": True, "extra": "forbid"}


class RiskAssessment(BaseModel):
    is_approved: bool = Field(...)
    max_calculated_loss: PositiveFloat = Field(...)
    margin_required: PositiveFloat = Field(...)
    tripped_risk_rules: List[str] = Field(
        default_factory=list,
        description="List of risk rules that were triggered during assessment",
    )
    risk_engine_version: str = Field(
        ..., min_length=5, max_length=32, description="Deterministic risk engine version"
    )

    model_config = {"frozen": True, "extra": "forbid"}

    @field_validator("tripped_risk_rules")
    @classmethod
    def ensure_rules_on_reject(cls, value, info):
        if not info.data.get("is_approved") and len(value) == 0:
            raise ValueError("Rejected risk assessments must provide at least one tripped risk rule")
        return value


class ExecutionOrder(BaseModel):
    instrument: TradeInstrument = Field(...)
    transaction_type: TransactionType = Field(...)
    order_type: OrderType = Field(...)
    quantity: int = Field(
        ..., ge=1, description="Executed order quantity must be a whole number"
    )
    lot_size: int = Field(
        25,
        ge=1,
        description="Exchange-defined lot size for the instrument",
    )
    price: Optional[PositiveFloat] = Field(
        None,
        description="Limit price. Required for LIMIT orders. Not supplied for MARKET orders.",
    )
    algo_id: str = Field(
        ..., regex=r"^[A-Z0-9_-]{6,20}$",
        description="SEBI algo identifier for the execution strategy",
    )

    model_config = {"frozen": True, "extra": "forbid"}

    @field_validator("quantity")
    @classmethod
    def validate_lot_multiple(cls, value, info):
        lot_size = info.data.get("lot_size") or 1
        if value % lot_size != 0:
            raise ValueError(f"quantity must be a multiple of lot_size ({lot_size})")
        return value

    @field_validator("price")
    @classmethod
    def validate_price_for_order_type(cls, value, info):
        order_type = info.data.get("order_type")
        if order_type == OrderType.LIMIT and value is None:
            raise ValueError("price is required for LIMIT orders")
        if order_type == OrderType.MARKET and value is not None:
            raise ValueError("price must be omitted for MARKET orders")
        return value


class GlobalState(BaseModel):
    broker_session_token: SecretStr = Field(..., description="Persisted broker session token loaded from env")
    market_snapshot: Optional[IngestionPayload] = None
    active_trade_thesis: Optional[TradeThesis] = None
    latest_risk_assessment: Optional[RiskAssessment] = None
    pending_execution_order: Optional[ExecutionOrder] = None

    model_config = {"frozen": False, "extra": "forbid"}

    @classmethod
    def load_from_env(cls) -> GlobalState:
        env_token = os.getenv("BROKER_SESSION_TOKEN")
        if not env_token:
            raise EnvironmentError("BROKER_SESSION_TOKEN must be set in environment")
        return cls(broker_session_token=SecretStr(env_token))

    def persist_session_token(self, token: str) -> None:
        self.broker_session_token = SecretStr(token)


# Example LangGraph integration pattern:
#
# state = GlobalState.load_from_env()
# thesis = TradeThesis(...)
# assessment = RiskAssessment(...)
# if assessment.is_approved:
#     order = ExecutionOrder(...)
#     state.pending_execution_order = order
#     # The broker session token remains secure in GlobalState and is never hardcoded.
