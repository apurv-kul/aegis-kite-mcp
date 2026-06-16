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
import logging
import sys
from contextlib import asynccontextmanager
from typing import Any

from kite_session import KiteSession, bootstrap_kite_session
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
    log.info("MCP server starting — initiating Kite Connect session …")
    result = await bootstrap_kite_session()
    if result["status"] == "ok":
        log.info("Startup login succeeded. Server ready.")
    else:
        log.error(
            "Startup login FAILED (stage=%s). Call the 'refresh_session' tool to retry. Error: %s",
            result.get("stage"),
            result.get("error"),
        )
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
        log.info("Launching aegis-kite-mcp server over stdio …")
        mcp.run(transport="stdio")
