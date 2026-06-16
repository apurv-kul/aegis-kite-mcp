import asyncio
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

import pyotp
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

from config import (
    LOGIN_TIMEOUT_MS,
    LOGIN_URL_TEMPLATE,
    REDIRECT_TIMEOUT_MS,
    SEL_LOGIN_BTN,
    SEL_PASSWORD,
    SEL_TOTP,
    SEL_USER_ID,
)

log = logging.getLogger("aegis.kite_mcp.kite_login")


def generate_totp_pin(totp_secret: str) -> str:
    return pyotp.TOTP(totp_secret).now()


async def get_request_token(
    api_key: str,
    user_id: str,
    password: str,
    totp_secret: str,
) -> str:
    """Run the headless Playwright login flow and capture request_token."""
    login_url = LOGIN_URL_TEMPLATE.format(api_key=api_key)

    log.info("Starting headless Chromium login flow")
    log.info("Login URL: %s", login_url)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
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

        redirect_future: asyncio.Future[str] = asyncio.get_event_loop().create_future()

        async def _on_request(request):
            url = request.url
            if "request_token=" in url and not redirect_future.done():
                log.info("Redirect intercepted: %s", url[:120])
                qs = parse_qs(urlparse(url).query)
                tokens = qs.get("request_token", [])
                if tokens:
                    redirect_future.set_result(tokens[0])
                    await request.abort()
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
            log.info("Navigating to Kite login page …")
            await page.goto(login_url, wait_until="domcontentloaded", timeout=LOGIN_TIMEOUT_MS)

            log.info("Filling User ID: %s", user_id)
            try:
                await page.wait_for_selector(SEL_USER_ID, timeout=LOGIN_TIMEOUT_MS)
            except PlaywrightTimeoutError as exc:
                raise RuntimeError(
                    "Timed out waiting for User ID input. "
                    "Kite login page may have changed its DOM structure."
                ) from exc
            await page.fill(SEL_USER_ID, user_id)

            log.info("Filling password …")
            try:
                await page.wait_for_selector(SEL_PASSWORD, timeout=LOGIN_TIMEOUT_MS)
            except PlaywrightTimeoutError as exc:
                raise RuntimeError(
                    "Timed out waiting for Password input. "
                    "Verify SEL_PASSWORD selector is still valid."
                ) from exc
            await page.fill(SEL_PASSWORD, password)

            log.info("Submitting login form …")
            try:
                await page.click(SEL_LOGIN_BTN, timeout=LOGIN_TIMEOUT_MS)
            except PlaywrightTimeoutError as exc:
                raise RuntimeError(
                    "Timed out clicking the Login button."
                ) from exc

            totp_pin = generate_totp_pin(totp_secret)
            log.info(
                "Generated TOTP pin: %s (valid for ~%ds)",
                totp_pin,
                30 - (datetime.now(timezone.utc).second % 30),
            )

            log.info("Waiting for 2FA prompt …")
            try:
                await page.wait_for_selector(SEL_TOTP, timeout=LOGIN_TIMEOUT_MS)
            except PlaywrightTimeoutError as exc:
                log.warning(
                    "2FA field not found within timeout — checking if redirect already captured."
                )
                if redirect_future.done():
                    pass
                else:
                    raise RuntimeError(
                        "Timed out waiting for 2FA/TOTP input. "
                        "Check SEL_TOTP selector or TOTP secret configuration."
                    ) from exc
            else:
                await page.fill(SEL_TOTP, totp_pin)
                try:
                    await page.click(SEL_LOGIN_BTN, timeout=3_000)
                except PlaywrightTimeoutError:
                    log.debug("No submit button after TOTP — likely auto-submitted.")

            log.info("Waiting for post-auth redirect to be intercepted …")
            try:
                request_token = await asyncio.wait_for(
                    asyncio.shield(redirect_future),
                    timeout=REDIRECT_TIMEOUT_MS / 1000,
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
