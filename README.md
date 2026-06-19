# Project Aegis — Kite MCP Server

A production-ready MCP server that automates Zerodha Kite Connect daily login via headless Playwright and exposes authenticated broker tools through the Model Context Protocol (MCP).

## Overview

`src/kite_mcp.py` performs the following:

- Loads Kite Connect credentials securely from a `.env` file using `python-dotenv`
- Automates Zerodha login on `https://kite.zerodha.com/connect/login?v=3&api_key=...` with Playwright
- Generates a live 6-digit TOTP PIN with `pyotp`
- Intercepts the final redirect to extract `request_token` without loading the redirect page
- Exchanges the `request_token` for an `access_token` via the `kiteconnect` SDK
- Starts a `FastMCP` server and exposes MCP tools to validate the authenticated session

## Files

- `src/kite_mcp.py` — main MCP server script
- `requirements.txt` — pinned Python dependencies
- `.env.example` — example Kite credential environment variables

## Requirements

- Python 3.13+
- `pip`
- `playwright` browsers installed

## Setup

1. Create a virtual environment and activate it.

```powershell
python -m venv .venv
.\.venv\Scripts\activate.bat
```

2. Install dependencies.

```powershell
python -m pip install -r requirements.txt
```

3. Install Playwright browser binaries.

```powershell
python -m playwright install chromium
```

4. Copy `.env.example` to `.env` and fill in your credentials.

```powershell
copy .env.example .env
```

## Required `.env` variables

- `KITE_API_KEY`
- `KITE_API_SECRET`
- `KITE_USER_ID`
- `KITE_PASSWORD`
- `KITE_TOTP_SECRET`

## Usage

Run the MCP server:

```powershell
python src\kite_mcp.py
```

Run login-only mode to verify authentication and print the access token suffix:

```powershell
python src\kite_mcp.py --login-only
```

## MCP Tools

The MCP server exposes tools including:

- `verify_connection` — validates the Kite session and returns account/profile details
- `refresh_session` — refreshes the Kite Connect session via the automated login flow

## Notes

- This implementation uses Playwright exclusively, not Selenium.
- Logging is configured to output structured, timestamped messages to stderr so MCP stdio transport remains clean.
- The headless browser flow is designed for reliable daily TOTP login automation.

## LangGraph Integration Example

The `src/aegis_models.py` module defines strict Pydantic v2 contracts for the Aegis workflow:

- `IngestionPayload` — live F&O market snapshot
- `TradeThesis` — research agent output
- `RiskAssessment` — risk engine approval/rejection
- `ExecutionOrder` — broker-ready execution payload
- `GlobalState` — secure state container for persistent session tokens

### Example Usage

```python
from aegis_models import (
    GlobalState,
    IngestionPayload,
    TradeThesis,
    RiskAssessment,
    ExecutionOrder,
    OptionType,
    OrderType,
    StrategyType,
    TransactionType,
    TradeInstrument,
)

# Load secure broker session token from environment
state = GlobalState.load_from_env()

# Ingestion stage creates a market snapshot contract
market_snapshot = IngestionPayload(
    market_timestamp=datetime.now(tz=timezone.utc),
    underlying_spot_price=19750.25,
    option_contract=TradeInstrument(
        symbol="NIFTY24JUN19750CE",
        underlying_symbol="NIFTY",
        expiry_date=date(2024, 6, 27),
        strike_price=19750.0,
        option_type=OptionType.CE,
    ),
    implied_volatility=18.4,
    iv_percentile=42.0,
    bid_price=120.5,
    ask_price=122.0,
    last_traded_price=121.2,
    open_interest=54000,
    volume=10234,
)
state.market_snapshot = market_snapshot

# Research stage emits a structured trade thesis
thesis = TradeThesis(
    strategy_type=StrategyType.BEAR_CALL_SPREAD,
    primary_instrument=market_snapshot.option_contract,
    strike_legs=[
        {
            "leg_name": "short_call",
            "instrument": market_snapshot.option_contract,
            "direction": TransactionType.SELL,
            "expected_premium": 120.5,
        }
    ],
    expected_entry_premium=120.5,
    target_profit_pct=25.0,
    stop_loss_pct=10.0,
    confidence=88,
)
state.active_trade_thesis = thesis

# Risk stage deterministically approves or rejects
assessment = RiskAssessment(
    is_approved=True,
    max_calculated_loss=1500.0,
    margin_required=32000.0,
    tripped_risk_rules=[],
    risk_engine_version="v1.0.0",
)
state.latest_risk_assessment = assessment

# Execution stage builds the broker order payload
order = ExecutionOrder(
    instrument=market_snapshot.option_contract,
    transaction_type=TransactionType.SELL,
    order_type=OrderType.LIMIT,
    quantity=50,
    lot_size=50,
    price=120.5,
    algo_id="AegisAlgo01",
)
state.pending_execution_order = order
```

### Secure Secrets Handling

- `GlobalState.load_from_env()` reads `BROKER_SESSION_TOKEN` from environment variables.
- Session tokens are stored as `SecretStr` in the global state and are never hardcoded.
- Use `.env` and `python-dotenv` for local development only; production secrets should remain in secure environment config.
