"""
Project Aegis — kite_mcp.py
============================
A production-ready MCP server (FastMCP) that:
  1. Automates Zerodha Kite Connect daily TOTP login via Playwright (headless Chromium).
  2. Exchanges the captured request_token for a live access_token via kiteconnect SDK.
  3. Exposes MCP tools to the LLM reasoning layer over stdio transport.

Usage:
    python kite_mcp.py                    # runs as MCP server (stdio)
    python kite_mcp.py --login-only       # runs the login flow once and prints the access_token

Architecture note (Project Aegis):
    LLM Agent (LangGraph) ──stdio──► kite_mcp.py (MCP Server)
                                           │
                                     kiteconnect SDK
                                           │
                                     Zerodha REST API
"""

import asyncio
import logging
import os
import sys
import argparse
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse, parse_qs

import pyotp
from dotenv import load_dotenv
from kiteconnect import KiteConnect
from mcp.server.fastmcp import FastMCP
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ─────────────────────────────────────────────
# 0. Logging — structured, timestamped
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stderr,          # MCP uses stdout; keep all logs on stderr
)
log = logging.getLogger("aegis.kite_mcp")


# ─────────────────────────────────────────────
# 1. Load environment
# ─────────────────────────────────────────────
load_dotenv()

def _require_env(key: str) -> str:
    """Fetch a required environment variable or raise a clear error."""
    value = os.getenv(key, "").strip()
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{key}' is missing or empty. "
            "Check your .env file."
        )
    return value


# ─────────────────────────────────────────────
# 2. Global Kite session state
#    (shared across all MCP tool calls)
# ─────────────────────────────────────────────
class KiteSession:
    """
    Lightweight singleton that holds the authenticated KiteConnect instance.
    Thread-safety note: MCP stdio transport is single-threaded by design;
    no lock is needed for this use-case.
    """
    kite: KiteConnect | None = None
    access_token: str | None = None
    authenticated_at: datetime | None = None

    @classmethod
    def is_live(cls) -> bool:
        return cls.kite is not None and cls.access_token is not None

    @classmethod
    def reset(cls) -> None:
        cls.kite = None
        cls.access_token = None
        cls.authenticated_at = None


# ─────────────────────────────────────────────
# 3. Core: Playwright TOTP login flow
# ─────────────────────────────────────────────
# Kite Connect DOM selectors (as of June 2026).
# If Zerodha updates their UI, these are the only lines to change.
_SEL_USER_ID   = "input#userid"
_SEL_PASSWORD  = "input#password"
_SEL_LOGIN_BTN = "button[type='submit']"
_SEL_TOTP      = "input[type='number'][maxlength='6'], input[placeholder*='PIN'], input[placeholder*='TOTP']"

_LOGIN_TIMEOUT_MS   = 15_000   # 15 s for page navigation / element appearance
_REDIRECT_TIMEOUT_MS = 10_000  # 10 s to capture the post-auth redirect


async def _playwright_kite_login(
    api_key: str,
    user_id: str,
    password: str,
    totp_secret: str,
) -> str:
    """
    Drives a headless Chromium session through the Kite Connect OAuth flow.

    Returns
    -------
    str
        The raw `request_token` extracted from the redirect URL.

    Raises
    ------
    RuntimeError
        If any step of the login flow fails (DOM timeout, missing token, etc.)
    PlaywrightTimeoutError
        Re-raised with context if a specific step times out.
    """
    login_url = f"https://kite.zerodha.com/connect/login?v=3&api_key={api_key}"
    captured_token: str | None = None

    log.info("Starting headless Chromium login flow")
    log.info("Login URL: %s", login_url)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",   # important inside Docker/EKS pods
                "--disable-gpu",
            ],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )
        page = await context.new_page()

        # ── Step A: intercept redirect BEFORE the browser tries to load it ──
        # Zerodha redirects to your registered redirect_url with ?request_token=XXX
        # We capture it here and abort the navigation immediately.
        redirect_future: asyncio.Future[str] = asyncio.get_event_loop().create_future()

        async def _on_request(request):
            nonlocal captured_token
            url = request.url
            if "request_token=" in url and not redirect_future.done():
                log.info("Redirect intercepted: %s", url[:120])
                qs = parse_qs(urlparse(url).query)
                tokens = qs.get("request_token", [])
                if tokens:
                    redirect_future.set_result(tokens[0])
                    await request.abort()   # stop browser loading the redirect page
                else:
                    redirect_future.set_exception(
                        RuntimeError(f"request_token absent in redirect URL: {url}")
                    )
            elif "error" in url.lower() and "request_token" not in url and not redirect_future.done():
                redirect_future.set_exception(
                    RuntimeError(f"Login redirect contains error: {url}")
                )

        page.on("request", _on_request)

        try:
            # ── Step B: navigate to login page ──
            log.info("Navigating to Kite login page …")
            await page.goto(login_url, wait_until="domcontentloaded", timeout=_LOGIN_TIMEOUT_MS)

            # ── Step C: fill User ID ──
            log.info("Filling User ID: %s", user_id)
            try:
                await page.wait_for_selector(_SEL_USER_ID, timeout=_LOGIN_TIMEOUT_MS)
            except PlaywrightTimeoutError as exc:
                raise RuntimeError(
                    "Timed out waiting for User ID input. "
                    "Kite login page may have changed its DOM structure."
                ) from exc
            await page.fill(_SEL_USER_ID, user_id)

            # ── Step D: fill Password ──
            log.info("Filling password …")
            try:
                await page.wait_for_selector(_SEL_PASSWORD, timeout=_LOGIN_TIMEOUT_MS)
            except PlaywrightTimeoutError as exc:
                raise RuntimeError(
                    "Timed out waiting for Password input. "
                    "Verify _SEL_PASSWORD selector is still valid."
                ) from exc
            await page.fill(_SEL_PASSWORD, password)

            # ── Step E: submit login ──
            log.info("Submitting login form …")
            try:
                await page.click(_SEL_LOGIN_BTN, timeout=_LOGIN_TIMEOUT_MS)
            except PlaywrightTimeoutError as exc:
                raise RuntimeError(
                    "Timed out clicking the Login button."
                ) from exc

            # ── Step F: generate fresh TOTP pin ──
            totp_pin = pyotp.TOTP(totp_secret).now()
            log.info("Generated TOTP pin: %s (valid for ~%ds)", totp_pin,
                     30 - (datetime.now(timezone.utc).second % 30))

            # ── Step G: fill TOTP / 2FA field ──
            log.info("Waiting for 2FA prompt …")
            try:
                await page.wait_for_selector(_SEL_TOTP, timeout=_LOGIN_TIMEOUT_MS)
            except PlaywrightTimeoutError as exc:
                # If 2FA is skipped (trusted device), the redirect may already be happening
                log.warning(
                    "2FA field not found within timeout — checking if redirect already captured."
                )
                if redirect_future.done():
                    pass    # redirect already captured in interceptor; continue below
                else:
                    raise RuntimeError(
                        "Timed out waiting for 2FA/TOTP input. "
                        "Check _SEL_TOTP selector or TOTP secret configuration."
                    ) from exc
            else:
                await page.fill(_SEL_TOTP, totp_pin)
                # Some Kite UI versions auto-submit on 6-digit fill;
                # others require an explicit submit. Try submit; ignore if selector missing.
                try:
                    await page.click(_SEL_LOGIN_BTN, timeout=3_000)
                except PlaywrightTimeoutError:
                    log.debug("No submit button after TOTP — likely auto-submitted.")

            # ── Step H: wait for redirect capture ──
            log.info("Waiting for post-auth redirect to be intercepted …")
            try:
                request_token = await asyncio.wait_for(
                    asyncio.shield(redirect_future),
                    timeout=_REDIRECT_TIMEOUT_MS / 1000,
                )
            except asyncio.TimeoutError as exc:
                raise RuntimeError(
                    "Timed out waiting for the post-login redirect. "
                    "Login may have failed, or the redirect URL is not matching "
                    "'request_token='. Check credentials and Kite app redirect URL config."
                ) from exc

            log.info("request_token captured successfully (length=%d)", len(request_token))
            return request_token

        finally:
            await context.close()
            await browser.close()
            log.debug("Playwright browser closed.")


# ─────────────────────────────────────────────
# 4. Session bootstrap — called on startup or on-demand
# ─────────────────────────────────────────────
async def bootstrap_kite_session() -> dict[str, Any]:
    """
    Runs the full Playwright login flow and stores the authenticated KiteConnect
    instance in KiteSession. Returns a status dict.

    This function is safe to call multiple times — it will re-authenticate
    and overwrite the existing session.
    """
    log.info("=== Bootstrapping Kite Connect session ===")

    # Load credentials fresh each time (allows hot-.env-reload)
    api_key    = _require_env("KITE_API_KEY")
    api_secret = _require_env("KITE_API_SECRET")
    user_id    = _require_env("KITE_USER_ID")
    password   = _require_env("KITE_PASSWORD")
    totp_secret = _require_env("KITE_TOTP_SECRET")

    # ── Run Playwright login ──
    try:
        request_token = await _playwright_kite_login(
            api_key=api_key,
            user_id=user_id,
            password=password,
            totp_secret=totp_secret,
        )
    except Exception as exc:
        log.error("Playwright login flow failed: %s", exc, exc_info=True)
        KiteSession.reset()
        return {
            "status": "error",
            "stage": "playwright_login",
            "error": str(exc),
        }

    # ── Exchange request_token for access_token ──
    log.info("Exchanging request_token for access_token via kiteconnect SDK …")
    try:
        kite = KiteConnect(api_key=api_key)
        data = kite.generate_session(request_token, api_secret=api_secret)
        access_token = data["access_token"]
        kite.set_access_token(access_token)
    except Exception as exc:
        log.error("generate_session() failed: %s", exc, exc_info=True)
        KiteSession.reset()
        return {
            "status": "error",
            "stage": "session_exchange",
            "error": str(exc),
        }

    # ── Store in global session ──
    KiteSession.kite = kite
    KiteSession.access_token = access_token
    KiteSession.authenticated_at = datetime.now(timezone.utc)

    log.info(
        "Session active. access_token=...%s authenticated_at=%s",
        access_token[-6:],
        KiteSession.authenticated_at.isoformat(),
    )

    return {
        "status": "ok",
        "access_token_suffix": access_token[-6:],   # never log full token
        "authenticated_at": KiteSession.authenticated_at.isoformat(),
    }


# ─────────────────────────────────────────────
# 5. FastMCP server definition
# ─────────────────────────────────────────────
@asynccontextmanager
async def _lifespan(server: FastMCP):
    """
    MCP server lifespan hook.
    Runs the Kite login flow automatically on startup so every tool call
    has a live session ready. Gracefully logs any startup failure without
    crashing the MCP server process — tools will return a clear error if
    the session is not initialised.
    """
    log.info("MCP server starting — initiating Kite Connect session …")
    result = await bootstrap_kite_session()
    if result["status"] == "ok":
        log.info("Startup login succeeded. Server ready.")
    else:
        log.error(
            "Startup login FAILED (stage=%s). "
            "Call the 'refresh_session' tool to retry. Error: %s",
            result.get("stage"), result.get("error"),
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
# 6. MCP Tools
# ─────────────────────────────────────────────

@mcp.tool()
async def verify_connection() -> dict[str, Any]:
    """
    Verify the Kite Connect session is active and authenticated.

    Calls kite.profile() to confirm the access_token is valid.
    Returns the account name, broker, email, and session metadata.
    Use this as a health-check before any trading operation.
    """
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
    """
    Retrieve available margin balance for a segment.

    Parameters
    ----------
    segment : str
        One of 'equity' or 'commodity'. Defaults to 'equity'.
        For F&O margin check, use 'equity'.

    Returns
    -------
    dict
        Net margin, available cash, utilised margin, and collateral breakdown.
    """
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
        seg_data = margins.get(segment, margins)  # SDK may nest or not
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
    """
    Retrieve current open positions (net + day positions).

    Returns both net positions (carry-forward) and day positions (intraday).
    The F&O risk engine should poll this after every order to track Greeks exposure.
    """
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
    """
    Retrieve all orders placed in the current trading session.

    Returns the full order list with status, fills, and timestamps.
    Used by the execution agent to monitor open orders and partial fills.
    """
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
    """
    Place a single-leg order on Kite Connect.

    Parameters
    ----------
    tradingsymbol : str
        E.g. "NIFTY24JUN23000CE" or "BANKNIFTY2460645000PE".
    exchange : str
        "NFO" for NSE F&O options/futures. "NSE" for cash.
    transaction_type : str
        "BUY" or "SELL".
    quantity : int
        Number of units (must be a multiple of lot size).
    order_type : str
        "MARKET", "LIMIT", "SL", or "SL-M".
    product : str
        "MIS" (intraday margin) or "NRML" (normal/carry-forward).
    price : float
        Limit price. Set to 0 for MARKET orders.
    trigger_price : float
        SL trigger price. Set to 0 for non-SL orders.
    validity : str
        "DAY" or "IOC". Defaults to "DAY".
    tag : str
        Optional alphanumeric tag for order tracking. Defaults to "aegis".

    Returns
    -------
    dict
        order_id on success, or error details.

    Safety note
    -----------
    This tool places a LIVE order with real money. The Risk Engine MCP server
    must validate all parameters before calling this tool.
    """
    if not KiteSession.is_live():
        return {
            "status": "error",
            "error": "Kite session not initialised. Call refresh_session first.",
        }

    transaction_type = transaction_type.upper()
    order_type       = order_type.upper()
    product          = product.upper()
    exchange         = exchange.upper()

    # Basic guard-rails (the Risk Engine MCP enforces the full 12-rule set)
    if transaction_type not in {"BUY", "SELL"}:
        return {"status": "error", "error": f"Invalid transaction_type: {transaction_type}"}
    if order_type not in {"MARKET", "LIMIT", "SL", "SL-M"}:
        return {"status": "error", "error": f"Invalid order_type: {order_type}"}
    if quantity <= 0:
        return {"status": "error", "error": "quantity must be > 0"}

    log.info(
        "[tool] place_order: %s %s %s qty=%d type=%s product=%s price=%.2f",
        transaction_type, tradingsymbol, exchange, quantity, order_type, product, price,
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
            tag=tag[:20],   # Kite enforces max 20-char tag
        )
        log.info("[tool] place_order SUCCESS — order_id=%s", order_id)
        return {"status": "ok", "order_id": order_id}
    except Exception as exc:
        log.error("[tool] place_order FAILED: %s", exc)
        return {"status": "error", "error": str(exc)}


@mcp.tool()
async def cancel_order(order_id: str, variety: str = "regular") -> dict[str, Any]:
    """
    Cancel an open order by order_id.

    Parameters
    ----------
    order_id : str
        The order_id returned by place_order.
    variety : str
        Order variety: "regular", "amo", "co", "iceberg". Defaults to "regular".

    Returns
    -------
    dict
        Confirmation of cancellation or error.
    """
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
    """
    Fetch live quotes for one or more instruments.

    Parameters
    ----------
    instruments : list[str]
        List of instrument keys in 'EXCHANGE:TRADINGSYMBOL' format.
        E.g. ["NFO:NIFTY24JUN23000CE", "NSE:NIFTY 50"].
        Maximum 500 instruments per call (Kite API limit).

    Returns
    -------
    dict
        Quote data including LTP, OHLC, volume, OI, and bid/ask.
    """
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
    """
    Re-run the full Playwright TOTP login flow and refresh the Kite session.

    Use this tool if verify_connection returns an auth error, or at the start
    of each trading day (Kite access_tokens expire at midnight IST).
    This tool blocks for ~10–20 seconds while the browser automation runs.

    Returns
    -------
    dict
        Status of the new session, including authenticated_at timestamp.
    """
    log.info("[tool] refresh_session — re-authenticating …")
    result = await bootstrap_kite_session()
    return result


# ─────────────────────────────────────────────
# 7. Entrypoint
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
    """Utility mode: authenticate and print the access_token (for debugging)."""
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
        # Run as MCP server over stdio (standard MCP transport)
        log.info("Launching aegis-kite-mcp server over stdio …")
        mcp.run(transport="stdio")