from datetime import date
from enum import Enum
from pydantic import BaseModel, Field, PositiveFloat

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

class ExitReason(str, Enum):
    """
    Canonical vocabulary for why a position was closed.
    Stored on ExecutionOrder and persisted to trading_journal.
    The learning loop uses this to score strategy behaviour per exit type.
    """
    TARGET_HIT         = "TARGET_HIT"          # closed at profit target
    STOP_LOSS_HIT      = "STOP_LOSS_HIT"       # hard stop triggered
    EARLY_EXIT         = "EARLY_EXIT"           # partial profit lock-in
    BREAKEVEN_STOP     = "BREAKEVEN_STOP"       # stop moved to breakeven, then hit
    IV_SPIKE           = "IV_SPIKE"             # emergency exit on IV surge
    OI_COLLAPSE        = "OI_COLLAPSE"          # liquidity warning exit
    TIME_DECAY         = "TIME_DECAY"           # theta budget exhausted
    MAX_HOLDING_DAYS   = "MAX_HOLDING_DAYS"     # time-based forced exit
    RISK_CIRCUIT_BREAK = "RISK_CIRCUIT_BREAK"   # portfolio daily loss limit hit
    EXPIRY_ROLL        = "EXPIRY_ROLL"          # rolled to next expiry
    MANUAL_OVERRIDE    = "MANUAL_OVERRIDE"      # operator-initiated close


class TradeInstrument(BaseModel):
    symbol: str = Field(..., pattern=r"^[A-Z0-9_\-]{3,40}$", description="e.g. NIFTY24JUN23000CE")
    underlying_symbol: str = Field(..., pattern=r"^[A-Z0-9_\-]{3,20}$", description="e.g. NIFTY")
    expiry_date: date = Field(...)
    strike_price: PositiveFloat = Field(...)
    option_type: OptionType = Field(...)

    model_config = {"frozen": True, "extra": "forbid"}