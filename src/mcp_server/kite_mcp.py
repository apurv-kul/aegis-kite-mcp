"""
Project Aegis — kite_mcp.py
============================
A production-ready MCP server (FastMCP) that:
  1. Automates Zerodha Kite Connect daily TOTP login via Playwright (headless Chromium).
  2. Exchanges the captured request_token for a live access_token via kiteconnect SDK.
  3. Exposes MCP tools to the LLM reasoning layer over stdio transport.

Usage:
    python src/kite_mcp.py                    # runs as MCP server (stdio)
    python src/kite_mcp.py --login-only       # runs the login flow once and prints the access_token

Architecture note (Project Aegis):
    LLM Agent (LangGraph) ──stdio──► kite_mcp.py (MCP Server)
                                           │
                                     kiteconnect SDK
                                           │
                                     Zerodha REST API
"""

import argparse
import asyncio
import json
import logging
import sys
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from typing import Any, TypedDict
from pathlib import Path

# Add the project root to the Python path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from src.mcp_server.kite_session import KiteSession, bootstrap_kite_session
from mcp.server.fastmcp import FastMCP



# ─────────────────────────────────────────────
# 0. Logging — structured, timestamped
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stderr,
)
log = logging.getLogger("aegis.kite_mcp")


# ─────────────────────────────────────────────
# 1. FastMCP server definition
# ─────────────────────────────────────────────
@asynccontextmanager
async def _lifespan(server: FastMCP):
    try:
        result = await bootstrap_kite_session()
        log.info("Startup login succeeded. Server ready.")
    except Exception as exc:
        log.error("Startup login FAILED: %s — call refresh_session to retry.", exc)
    yield
    log.info("MCP server shutting down.")


mcp = FastMCP(
    name="aegis-kite-mcp",
    instructions=(
        "MCP server for Project Aegis. Provides authenticated access to the "
        "Zerodha Kite Connect API for the F&O trading agent. "
        "Always call verify_connection before placing orders to confirm the session is live."
    ),
    lifespan=_lifespan,
)


# ─────────────────────────────────────────────
# 2. MCP Tools
# ─────────────────────────────────────────────

@mcp.tool()
async def verify_connection() -> dict[str, Any]:
    if not KiteSession.is_live():
        return {
            "status": "error",
            "error": "Kite session not initialised. Call refresh_session first.",
        }

    log.info("[tool] verify_connection — calling kite.profile()")
    try:
        profile = KiteSession.kite.profile()
        return {
            "status": "ok",
            "user_name": profile.get("user_name"),
            "user_id": profile.get("user_id"),
            "email": profile.get("email"),
            "broker": profile.get("broker"),
            "exchanges": profile.get("exchanges", []),
            "products": profile.get("products", []),
            "authenticated_at": (
                KiteSession.authenticated_at.isoformat()
                if KiteSession.authenticated_at else None
            ),
        }
    except Exception as exc:
        log.error("[tool] verify_connection failed: %s", exc)
        return {"status": "error", "error": str(exc)}


@mcp.tool()
async def get_margins(segment: str = "equity") -> dict[str, Any]:
    if not KiteSession.is_live():
        return {
            "status": "error",
            "error": "Kite session not initialised. Call refresh_session first.",
        }

    valid_segments = {"equity", "commodity"}
    if segment not in valid_segments:
        return {
            "status": "error",
            "error": f"Invalid segment '{segment}'. Must be one of {valid_segments}.",
        }

    log.info("[tool] get_margins — segment=%s", segment)
    try:
        margins = KiteSession.kite.margins(segment=segment)
        seg_data = margins.get(segment, margins)
        return {
            "status": "ok",
            "segment": segment,
            "net": seg_data.get("net"),
            "available": seg_data.get("available", {}).get("cash"),
            "utilised": seg_data.get("utilised", {}).get("debits"),
            "collateral": seg_data.get("available", {}).get("collateral"),
            "intraday_payin": seg_data.get("available", {}).get("intraday_payin"),
        }
    except Exception as exc:
        log.error("[tool] get_margins failed: %s", exc)
        return {"status": "error", "error": str(exc)}


@mcp.tool()
async def get_positions() -> dict[str, Any]:
    if not KiteSession.is_live():
        return {
            "status": "error",
            "error": "Kite session not initialised. Call refresh_session first.",
        }

    log.info("[tool] get_positions")
    try:
        positions = KiteSession.kite.positions()
        return {
            "status": "ok",
            "net": positions.get("net", []),
            "day": positions.get("day", []),
        }
    except Exception as exc:
        log.error("[tool] get_positions failed: %s", exc)
        return {"status": "error", "error": str(exc)}


@mcp.tool()
async def get_orders() -> dict[str, Any]:
    if not KiteSession.is_live():
        return {
            "status": "error",
            "error": "Kite session not initialised. Call refresh_session first.",
        }

    log.info("[tool] get_orders")
    try:
        orders = KiteSession.kite.orders()
        return {"status": "ok", "orders": orders}
    except Exception as exc:
        log.error("[tool] get_orders failed: %s", exc)
        return {"status": "error", "error": str(exc)}


@mcp.tool()
async def place_order(
    tradingsymbol: str,
    exchange: str,
    transaction_type: str,
    quantity: int,
    order_type: str,
    product: str,
    price: float = 0.0,
    trigger_price: float = 0.0,
    validity: str = "DAY",
    tag: str = "aegis",
) -> dict[str, Any]:
    if not KiteSession.is_live():
        return {
            "status": "error",
            "error": "Kite session not initialised. Call refresh_session first.",
        }

    transaction_type = transaction_type.upper()
    order_type = order_type.upper()
    product = product.upper()
    exchange = exchange.upper()

    if transaction_type not in {"BUY", "SELL"}:
        return {"status": "error", "error": f"Invalid transaction_type: {transaction_type}"}
    if order_type not in {"MARKET", "LIMIT", "SL", "SL-M"}:
        return {"status": "error", "error": f"Invalid order_type: {order_type}"}
    if quantity <= 0:
        return {"status": "error", "error": "quantity must be > 0"}

    log.info(
        "[tool] place_order: %s %s %s qty=%d type=%s product=%s price=%.2f",
        transaction_type,
        tradingsymbol,
        exchange,
        quantity,
        order_type,
        product,
        price,
    )

    try:
        kite = KiteSession.kite
        order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=exchange,
            tradingsymbol=tradingsymbol,
            transaction_type=transaction_type,
            quantity=quantity,
            product=product,
            order_type=order_type,
            price=price if price else None,
            trigger_price=trigger_price if trigger_price else None,
            validity=validity,
            tag=tag[:20],
        )
        log.info("[tool] place_order SUCCESS — order_id=%s", order_id)
        return {"status": "ok", "order_id": order_id}
    except Exception as exc:
        log.error("[tool] place_order FAILED: %s", exc)
        return {"status": "error", "error": str(exc)}


@mcp.tool()
async def cancel_order(order_id: str, variety: str = "regular") -> dict[str, Any]:
    if not KiteSession.is_live():
        return {
            "status": "error",
            "error": "Kite session not initialised. Call refresh_session first.",
        }

    log.info("[tool] cancel_order: order_id=%s variety=%s", order_id, variety)
    try:
        result = KiteSession.kite.cancel_order(variety=variety, order_id=order_id)
        log.info("[tool] cancel_order SUCCESS — order_id=%s", result)
        return {"status": "ok", "cancelled_order_id": result}
    except Exception as exc:
        log.error("[tool] cancel_order FAILED: %s", exc)
        return {"status": "error", "error": str(exc)}


@mcp.tool()
async def get_quote(instruments: list[str]) -> dict[str, Any]:
    if not KiteSession.is_live():
        return {
            "status": "error",
            "error": "Kite session not initialised. Call refresh_session first.",
        }

    if not instruments:
        return {"status": "error", "error": "instruments list cannot be empty"}
    if len(instruments) > 500:
        return {"status": "error", "error": "Maximum 500 instruments per quote call"}

    log.info("[tool] get_quote: %d instrument(s)", len(instruments))
    try:
        quotes = KiteSession.kite.quote(instruments)
        return {"status": "ok", "quotes": quotes}
    except Exception as exc:
        log.error("[tool] get_quote FAILED: %s", exc)
        return {"status": "error", "error": str(exc)}


@mcp.tool()
async def refresh_session() -> dict[str, Any]:
    log.info("[tool] refresh_session — re-authenticating …")
    result = await bootstrap_kite_session()
    return result

# ─────────────────────────────────────────────────────────────────────────────
# get_options_chain — private helpers
#
# Design overview
# ───────────────
# Zerodha does NOT expose a single "get_options_chain" endpoint. We build the
# chain in three phases:
#
#   Phase 1 — INSTRUMENT SCAN
#     Pull the full NFO instrument dump (cached in-process for the trading
#     session; re-fetched if the cache is older than 6 hours). Filter by
#     underlying name + expiry date + option type to get the universe of
#     tradable strikes.
#
#   Phase 2 — ATM ANCHORING
#     Fetch the spot price for the underlying from NSE or NFO futures.
#     Sort the filtered strikes by |strike − spot| to find the ATM strike,
#     then window by strike_count on each side.
#
#   Phase 3 — LIVE QUOTE ENRICHMENT
#     Batch-fetch live quotes for the windowed CE + PE symbols via
#     kite.quote(), always inside asyncio.to_thread() to keep the MCP event
#     loop unblocked. Kite quote() returns OI, volume, LTP, OHLC, and
#     depth — everything the Research Agent needs.
#
# All three phases are wrapped in asyncio.to_thread() because kiteconnect
# uses the requests library (blocking sync I/O) internally.
# ─────────────────────────────────────────────────────────────────────────────
 
# ── In-process instrument cache ──────────────────────────────────────────────
# Avoids re-downloading the ~8 MB NFO dump on every tool call.
# Structure: { "instruments": [...], "cached_at": datetime }
_NFO_INSTRUMENT_CACHE: dict[str, Any] = {}
_CACHE_TTL_HOURS = 6        # refresh once per half-session
 
 
class _OptionContract(TypedDict):
    """Internal intermediate representation for a single option leg."""
    tradingsymbol:    str
    instrument_token: int
    strike:           float
    option_type:      str   # "CE" or "PE"
    expiry:           str   # "YYYY-MM-DD"
    lot_size:         int
 
 
def _get_cached_nfo_instruments(kite) -> list[dict]:
    """
    Return the NFO instrument list from the in-process cache.
    Re-fetches from Kite if the cache is empty or stale.
    This function is SYNCHRONOUS — always call via asyncio.to_thread().
    """
    global _NFO_INSTRUMENT_CACHE
    now = datetime.now(timezone.utc)
    cached_at = _NFO_INSTRUMENT_CACHE.get("cached_at")
 
    if cached_at is not None:
        age_hours = (now - cached_at).total_seconds() / 3600
        if age_hours < _CACHE_TTL_HOURS:
            log.debug("NFO instrument cache HIT (age=%.1fh)", age_hours)
            return _NFO_INSTRUMENT_CACHE["instruments"]
 
    log.info("NFO instrument cache MISS — fetching from Kite API …")
    instruments: list[dict] = kite.instruments("NFO")
    _NFO_INSTRUMENT_CACHE = {
        "instruments": instruments,
        "cached_at":   now,
    }
    log.info("NFO instrument cache populated: %d instruments", len(instruments))
    return instruments
 
 
def _filter_option_contracts(
    instruments:    list[dict],
    underlying:     str,
    expiry_date_obj: date,
) -> tuple[list[_OptionContract], list[_OptionContract]]:
    """
    Filter the NFO dump to CE and PE contracts for the given underlying + expiry.
 
    Returns (calls, puts) as separate lists, each sorted by strike ascending.
 
    Kite instrument record shape (relevant fields):
        tradingsymbol, instrument_token, strike, expiry (datetime.date),
        instrument_type ("CE"|"PE"|"FUT"), name (underlying name), lot_size
 
    Normalisation note
    ------------------
    Kite stores the underlying name as e.g. "NIFTY", "BANKNIFTY", "HDFCBANK".
    Callers may pass "NIFTY 50", "HDFC BANK", "BankNifty" — we normalise both
    sides to uppercase with spaces stripped before comparison.
    """
    underlying_norm = underlying.upper().replace(" ", "").replace("-", "")
 
    calls: list[_OptionContract] = []
    puts:  list[_OptionContract] = []
 
    for inst in instruments:
        # Kite returns expiry as a datetime.date object
        inst_expiry = inst.get("expiry")
        if inst_expiry is None:
            continue
        # Guard: sometimes Kite returns datetime, sometimes date
        if isinstance(inst_expiry, datetime):
            inst_expiry = inst_expiry.date()
 
        if inst_expiry != expiry_date_obj:
            continue
 
        inst_name = (inst.get("name") or "").upper().replace(" ", "").replace("-", "")
        if inst_name != underlying_norm:
            continue
 
        itype = (inst.get("instrument_type") or "").upper()
        if itype not in ("CE", "PE"):
            continue
 
        contract: _OptionContract = {
            "tradingsymbol":    inst["tradingsymbol"],
            "instrument_token": int(inst["instrument_token"]),
            "strike":           float(inst["strike"]),
            "option_type":      itype,
            "expiry":           str(inst_expiry),
            "lot_size":         int(inst.get("lot_size") or 0),
        }
        if itype == "CE":
            calls.append(contract)
        else:
            puts.append(contract)
 
    calls.sort(key=lambda x: x["strike"])
    puts.sort(key=lambda x: x["strike"])
    return calls, puts
 
 
def _get_spot_price(kite, underlying: str) -> float:
    """
    Fetch the current spot price for the underlying.
 
    Strategy (in priority order):
      1. Try NSE quote directly (e.g. "NSE:NIFTY 50", "NSE:HDFCBANK").
      2. Fall back to the nearest NFO FUT quote (avoids NSE premium data issues).
      3. If both fail, raise so the caller can return a structured error.
 
    This function is SYNCHRONOUS — always call via asyncio.to_thread().
    """
    # Kite's NSE symbol for indices uses a space, e.g. "NIFTY 50", "NIFTY BANK"
    # For equities it's the plain symbol, e.g. "HDFCBANK"
    _NSE_INDEX_MAP = {
        "NIFTY":     "NIFTY 50",
        "BANKNIFTY": "NIFTY BANK",
        "MIDCPNIFTY":"NIFTY MID SELECT",
        "FINNIFTY":  "NIFTY FIN SERVICE",
        "SENSEX":    "SENSEX",
    }
    underlying_upper = underlying.upper().replace(" ", "").replace("-", "")
    nse_symbol = _NSE_INDEX_MAP.get(underlying_upper, underlying.upper())
 
    # ── Attempt 1: NSE cash / index quote ────────────────────────────────────
    try:
        key = f"NSE:{nse_symbol}"
        q = kite.quote([key])
        ltp = q[key]["last_price"]
        if ltp and ltp > 0:
            log.debug("Spot via NSE quote: %s = %.2f", key, ltp)
            return float(ltp)
    except Exception as exc:
        log.debug("NSE quote failed for '%s': %s — trying NFO FUT", nse_symbol, exc)
 
    # ── Attempt 2: Nearest NFO FUT ───────────────────────────────────────────
    # Get all NFO instruments to find the nearest-expiry future
    nfo_instruments = _get_cached_nfo_instruments(kite)
    futures = [
        i for i in nfo_instruments
        if (i.get("name") or "").upper().replace(" ", "").replace("-", "") == underlying_upper
        and (i.get("instrument_type") or "").upper() == "FUT"
    ]
    if not futures:
        raise ValueError(
            f"Could not find any NFO futures for underlying '{underlying}'. "
            "Check the underlying name matches Kite's instrument master."
        )
 
    today = date.today()
    futures.sort(key=lambda x: abs((x["expiry"].date() if isinstance(x["expiry"], datetime) else x["expiry"]) - today))
    nearest_fut = futures[0]["tradingsymbol"]
    key = f"NFO:{nearest_fut}"
    q = kite.quote([key])
    ltp = q[key]["last_price"]
    if not ltp or ltp <= 0:
        raise ValueError(f"NFO FUT quote returned invalid LTP for '{nearest_fut}'")
 
    log.debug("Spot via NFO FUT quote: %s = %.2f", key, ltp)
    return float(ltp)
 
 
def _window_strikes(
    calls:       list[_OptionContract],
    puts:        list[_OptionContract],
    spot:        float,
    strike_count: int,
) -> tuple[list[_OptionContract], list[_OptionContract]]:
    """
    Return the `strike_count` strikes on each side of ATM for both CE and PE.
 
    strike_count is interpreted as "N strikes on EACH SIDE of ATM", so the
    total chain depth returned is up to (2 * strike_count + 1) strikes.
 
    ATM is defined as the strike with minimum |strike − spot|.
    """
    all_strikes = sorted(set(c["strike"] for c in calls) | set(p["strike"] for p in puts))
    if not all_strikes:
        return [], []
 
    atm_strike = min(all_strikes, key=lambda s: abs(s - spot))
    atm_idx = all_strikes.index(atm_strike)
 
    lo = max(0, atm_idx - strike_count)
    hi = min(len(all_strikes) - 1, atm_idx + strike_count)
    selected_strikes = set(all_strikes[lo: hi + 1])
 
    windowed_calls = [c for c in calls if c["strike"] in selected_strikes]
    windowed_puts  = [p for p in puts  if p["strike"] in selected_strikes]
    return windowed_calls, windowed_puts
 
 
def _build_quote_keys(
    calls: list[_OptionContract],
    puts:  list[_OptionContract],
) -> list[str]:
    """Build 'NFO:TRADINGSYMBOL' quote keys for all windowed contracts."""
    return [f"NFO:{c['tradingsymbol']}" for c in calls] + \
           [f"NFO:{p['tradingsymbol']}" for p in puts]
 
 
def _fetch_quotes_sync(kite, quote_keys: list[str]) -> dict[str, Any]:
    """
    Fetch live quotes for all contracts in a single Kite API call.
    Kite quote() accepts up to 500 instruments.
 
    Returns the raw quotes dict keyed by 'NFO:TRADINGSYMBOL'.
    This function is SYNCHRONOUS — always call via asyncio.to_thread().
    """
    if not quote_keys:
        return {}
        
    results = {}
    for i in range(0, len(quote_keys), 500):
        batch = quote_keys[i:i + 500]
        results.update(kite.quote(batch))
        
    return results
 
 
def _calculate_max_pain(chain_rows: list[dict[str, Any]]) -> float | None:
    """
    O(N^2) exact calculation of the minimum intrinsic payout strike.
    """
    if not chain_rows:
        return None
        
    all_strikes = [r["strike"] for r in chain_rows]
    min_total_payout = float('inf')
    max_pain_strike = None

    for test_strike in all_strikes:
        total_payout = 0.0
        
        for row in chain_rows:
            target_strike = row["strike"]
            
            # Call payout: test_strike must be higher than the call strike to have intrinsic value
            if test_strike > target_strike and row.get("call"):
                call_oi = row["call"].get("oi", 0)
                total_payout += (test_strike - target_strike) * call_oi
                
            # Put payout: test_strike must be lower than the put strike to have intrinsic value
            if test_strike < target_strike and row.get("put"):
                put_oi = row["put"].get("oi", 0)
                total_payout += (target_strike - test_strike) * put_oi

        if total_payout < min_total_payout:
            min_total_payout = total_payout
            max_pain_strike = test_strike
            
    return max_pain_strike

def _assemble_chain(
    calls:      list[_OptionContract],
    puts:       list[_OptionContract],
    quotes:     dict[str, Any],
    spot_price: float,
    expiry_date: str,
    underlying:  str,
    is_futures_anchor: bool,
    strike_count: int
) -> dict[str, Any]:
    """
    Merge instrument metadata with live quote data into the canonical
    options chain output format consumed by the LangGraph Research Agent.
 
    Output schema per strike
    ─────────────────────────
    {
      "strike": 23000.0,
      "call": {
          "tradingsymbol":    "NIFTY24JUN23000CE",
          "instrument_token": 12345678,
          "lot_size":         75,
          "last_price":       152.30,
          "oi":               3200000,
          "oi_day_high":      3450000,
          "oi_day_low":       3100000,
          "volume":           145000,
          "buy_quantity":     750,
          "sell_quantity":    1500,
          "ohlc":             {"open": 148.0, "high": 165.0, "low": 140.0, "close": 149.0},
          "net_change":       3.30,
          "oi_change":        50000          # derived: oi − previous_oi (day_low used as proxy)
      },
      "put": { … same structure … }
    }
    """
    # Index contracts by strike for O(1) lookup
    call_by_strike = {c["strike"]: c for c in calls}
    put_by_strike  = {p["strike"]: p for p in puts}
    all_strikes = sorted(call_by_strike.keys() | put_by_strike.keys())

    # 1. Build the FULL chain first to calculate accurate macro statistics
    full_chain_rows = []
    for strike in all_strikes:
        row: dict[str, Any] = {"strike": strike}
        for leg_type, by_strike in (("call", call_by_strike), ("put", put_by_strike)):
            contract = by_strike.get(strike)
            if contract:
                qkey  = f"NFO:{contract['tradingsymbol']}"
                qdata = quotes.get(qkey, {})
                row[leg_type] = {
                    "tradingsymbol":    contract["tradingsymbol"],
                    "last_price":       float(qdata.get("last_price") or 0.0),
                    "oi":               int(qdata.get("oi") or 0),
                    "volume":           int(qdata.get("volume") or 0),
                }
        full_chain_rows.append(row)

    # 2. Calculate True Macro Statistics across the ENTIRE chain
    total_ce_oi = sum((r.get("call") or {}).get("oi", 0) for r in full_chain_rows)
    total_pe_oi = sum((r.get("put")  or {}).get("oi", 0) for r in full_chain_rows)
    pcr_oi = round(total_pe_oi / total_ce_oi, 4) if total_ce_oi else None
    max_pain = _calculate_max_pain(full_chain_rows)
    atm_strike = min(all_strikes, key=lambda s: abs(s - spot_price)) if all_strikes else None

    # 3. Truncate the chain strictly for LLM Context Window limits
    if atm_strike:
        atm_idx = all_strikes.index(atm_strike)
        lo = max(0, atm_idx - strike_count)
        hi = min(len(all_strikes) - 1, atm_idx + strike_count)
        windowed_strikes = set(all_strikes[lo: hi + 1])
        final_chain_rows = [r for r in full_chain_rows if r["strike"] in windowed_strikes]
    else:
        final_chain_rows = []

    return {
        "status":             "ok",
        "underlying":         underlying.upper(),
        "expiry":             expiry_date,
        "spot_price":         spot_price,
        "is_futures_anchor":  is_futures_anchor, # CRITICAL: Tells the AI if spot_price includes premium/discount
        "atm_strike":         atm_strike,
        "max_pain":           max_pain,          # CRITICAL: Now mathematically correct
        "pcr_oi":             pcr_oi,            # CRITICAL: Now statistically correct
        "total_ce_oi":        total_ce_oi,
        "total_pe_oi":        total_pe_oi,
        "chain_depth_returned": len(final_chain_rows),
        "chain":              final_chain_rows,  # Stripped of L2 depth to save LLM tokens
    }
 
 
# ─────────────────────────────────────────────────────────────────────────────
# get_options_chain — MCP tool
# ─────────────────────────────────────────────────────────────────────────────
 
@mcp.tool()
async def get_options_chain(
    underlying:   str,
    expiry_date:  str,
    strike_count: int = 10,
) -> str:
    """
    Fetch the live options chain for a given underlying and expiry date.
 
    Returns a JSON string containing strike-by-strike CE and PE data
    (LTP, OI, volume) plus chain-level
    aggregates (spot_price, is_futures_anchor, atm_strike, max_pain, pcr_oi).
 
    This is the primary data source for the Research Agent's options
    chain analysis step in the Project Aegis trading pipeline.
 
    Parameters
    ----------
    underlying : str
        Underlying asset name as it appears in the Kite instrument master.
        Examples: "NIFTY", "BANKNIFTY", "HDFCBANK", "RELIANCE".
        Spaces and case are normalised — "HDFC BANK", "hdfcbank", "HDFC bank"
        all resolve correctly.
 
    expiry_date : str
        Option expiry date in ISO format "YYYY-MM-DD".
        Use the nearest weekly/monthly expiry. Invalid or past dates return
        a structured error — no exception is raised to the LLM.
 
    strike_count : int, optional
        Number of strikes to include on EACH SIDE of the ATM strike.
        Default 10 → up to 21 strikes total (10 ITM + ATM + 10 OTM).
        Reduce to 5 for a tighter context window budget.
        Maximum enforced: 25 per side.
 
    Returns
    -------
    str
        A compact JSON string. On success, top-level keys include:
          status, underlying, expiry, spot_price, atm_strike, max_pain,
          pcr_oi, pcr_volume, total_ce_oi, total_pe_oi, chain_depth,
          fetched_at, chain (list of strike rows).
 
        On failure, returns {"status": "error", "error": "<message>"}.
 
    Raises
    ------
    Does not raise. All exceptions are caught and returned as structured
    JSON error responses so the LLM agent can self-correct.
 
    Example LangGraph usage
    -----------------------
    ::
 
        chain_json = await mcp_client.call_tool(
            "get_options_chain",
            {"underlying": "NIFTY", "expiry_date": "2024-06-27", "strike_count": 5}
        )
        chain = json.loads(chain_json)
        atm   = chain["atm_strike"]
        pcr   = chain["pcr_oi"]
    """
    log.info(
        "[tool] get_options_chain: underlying=%s expiry=%s strike_count=%d",
        underlying, expiry_date, strike_count,
    )
 
    # ── Guard 1: parameter validation ────────────────────────────────────────
    if not underlying or not underlying.strip():
        return json.dumps({"status": "error", "error": "underlying must not be empty."})
 
    strike_count = max(1, min(strike_count, 25))    # clamp [1, 25]
 
    try:
        expiry_date_obj = date.fromisoformat(expiry_date)
    except ValueError:
        return json.dumps({
            "status": "error",
            "error": f"Invalid expiry_date format '{expiry_date}'. Expected ISO format YYYY-MM-DD.",
        })
 
    if expiry_date_obj < date.today():
        return json.dumps({
            "status": "error",
            "error": (
                f"expiry_date '{expiry_date}' is in the past. "
                "Provide a current or future expiry date."
            ),
        })
 
    # ── Guard 2: session liveness — attempt silent re-auth if stale ──────────
    if not KiteSession.is_live():
        log.warning("[tool] get_options_chain — session not live, attempting re-auth …")
        reauth = await bootstrap_kite_session()
        if reauth["status"] != "ok":
            return json.dumps({
                "status": "error",
                "error": (
                    "Kite session is not active and re-authentication failed. "
                    f"Call refresh_session to restore the session. Detail: {reauth.get('error')}"
                ),
            })
        log.info("[tool] get_options_chain — re-auth succeeded, continuing.")
 
    kite = KiteSession.kite
 
    try:
        instruments = await asyncio.to_thread(_get_cached_nfo_instruments, kite)
        calls, puts = _filter_option_contracts(instruments, underlying, expiry_date_obj)
        
        if not calls and not puts:
            return json.dumps({"status": "error", "error": f"No option contracts found."})

        # 1. Fetch spot price and set the anchor flag
        try:
            # We attempt NSE Spot first
            _NSE_INDEX_MAP = {"NIFTY": "NIFTY 50", "BANKNIFTY": "NIFTY BANK", "FINNIFTY": "NIFTY FIN SERVICE"}
            nse_symbol = _NSE_INDEX_MAP.get(underlying.upper(), underlying.upper())
            spot_q = await asyncio.to_thread(kite.quote, [f"NSE:{nse_symbol}"])
            spot_price = float(spot_q[f"NSE:{nse_symbol}"]["last_price"])
            is_futures_anchor = False
        except Exception:
            # Fallback to NFO Futures
            spot_price = await asyncio.to_thread(_get_spot_price, kite, underlying)
            is_futures_anchor = True

        # 2. Fetch quotes for the ENTIRE chain, not just the window
        # (Kite allows up to 500 instruments per quote call. A standard Nifty weekly chain is ~150 strikes = 300 instruments. This is safe.)
        all_quote_keys = _build_quote_keys(calls, puts)
        quotes = await asyncio.to_thread(_fetch_quotes_sync, kite, all_quote_keys)
        
        # 3. Assemble and calculate
        chain_output = _assemble_chain(
            calls, puts, quotes, spot_price, expiry_date, underlying, is_futures_anchor, strike_count
        )
        return json.dumps(chain_output, default=str)

    except Exception as exc:
        log.error("[tool] get_options_chain failed: %s", exc)
        return json.dumps({"status": "error", "error": str(exc)})

# ─────────────────────────────────────────────
# 3. Entrypoint
# ─────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Project Aegis — Kite MCP Server")
    parser.add_argument(
        "--login-only",
        action="store_true",
        help="Run the Playwright login flow once, print the access_token, and exit.",
    )
    return parser.parse_args()


async def _login_only_mode() -> None:
    result = await bootstrap_kite_session()
    if result["status"] == "ok":
        print(f"\n✅ Authentication successful!")
        print(f"   access_token suffix : ...{result['access_token_suffix']}")
        print(f"   authenticated_at    : {result['authenticated_at']}")
        if KiteSession.kite:
            profile = KiteSession.kite.profile()
            print(f"   account name        : {profile.get('user_name')}")
    else:
        print(f"\n❌ Authentication failed at stage '{result.get('stage')}'")
        print(f"   Error: {result.get('error')}")
        sys.exit(1)


if __name__ == "__main__":
    args = _parse_args()

    if args.login_only:
        asyncio.run(_login_only_mode())
    else:
        try:
            log.info("Launching aegis-kite-mcp server over stdio …")
            mcp.run(transport="stdio")
        except KeyboardInterrupt:
            log.info("\n[Aegis] SIGINT received. Shutting down MCP server gracefully...")
        except Exception as exc:
            logging.error(f"[Aegis] Server crashed unexpectedly: {exc}")