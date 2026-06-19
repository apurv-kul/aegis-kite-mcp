import asyncio
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs
import pyotp
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

from src.configs.config import (
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
    login_url = LOGIN_URL_TEMPLATE.format(api_key=api_key)
    log.info("Starting headless Chromium login flow")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
        )
        page = await context.new_page()
        redirect_future: asyncio.Future[str] = asyncio.get_running_loop().create_future()

        # Passive observer instead of active interceptor
        async def _on_request(request):
            url = request.url
            if "request_token=" in url and not redirect_future.done():
                log.info("Redirect detected: %s", url[:120])
                qs = parse_qs(urlparse(url).query)
                tokens = qs.get("request_token", [])
                if tokens:
                    redirect_future.set_result(tokens[0])
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
            log.info("Navigating to Kite login page ...")
            await page.goto(login_url, wait_until="domcontentloaded", timeout=LOGIN_TIMEOUT_MS)

            await page.wait_for_selector(SEL_USER_ID, timeout=LOGIN_TIMEOUT_MS)
            await page.fill(SEL_USER_ID, user_id)

            await page.wait_for_selector(SEL_PASSWORD, timeout=LOGIN_TIMEOUT_MS)
            await page.fill(SEL_PASSWORD, password)
            await page.click(SEL_LOGIN_BTN, timeout=LOGIN_TIMEOUT_MS)

            totp_pin = generate_totp_pin(totp_secret)
            log.info("Generated TOTP pin: %s", totp_pin)

            try:
                await page.wait_for_selector(SEL_TOTP, timeout=LOGIN_TIMEOUT_MS)
                await page.fill(SEL_TOTP, totp_pin)
                await page.click(SEL_LOGIN_BTN, timeout=3000)
            except PlaywrightTimeoutError:
                if redirect_future.done():
                    pass
                else:
                    raise

            log.info("Waiting for post-auth redirect ...")
            
            # Wait for authorization button and click it if it appears
            try:
                auth_btn_selectors = [
                    "button:has-text('Authorize')",
                    "button:has-text('Continue')",
                    "//button[contains(text(), 'Authorize')]",
                    "//button[contains(text(), 'Continue')]",
                ]
                for selector in auth_btn_selectors:
                    try:
                        await page.wait_for_selector(selector, timeout=5000)
                        log.info("Found authorization button, clicking: %s", selector)
                        await page.click(selector, timeout=3000)
                        break
                    except PlaywrightTimeoutError:
                        continue
            except Exception as e:
                log.debug("No authorization button found or error clicking: %s", e)
            
            # Monitor page URL changes directly
            start_time = datetime.now(timezone.utc)
            while (datetime.now(timezone.utc) - start_time).total_seconds() < (REDIRECT_TIMEOUT_MS / 1000):
                current_url = page.url
                if "request_token=" in current_url and not redirect_future.done():
                    log.info("URL changed to: %s", current_url[:120])
                    qs = parse_qs(urlparse(current_url).query)
                    tokens = qs.get("request_token", [])
                    if tokens:
                        redirect_future.set_result(tokens[0])
                        break
                await asyncio.sleep(0.5)
            
            try:
                request_token = await asyncio.wait_for(
                    asyncio.shield(redirect_future),
                    timeout=2.0,
                )
            except asyncio.TimeoutError:
                # Take a screenshot for debugging
                try:
                    await page.screenshot(path="login_debug.png")
                    log.warning("Screenshot saved to login_debug.png for debugging")
                    log.warning("Current page URL: %s", page.url)
                except Exception as sc_err:
                    log.warning("Could not save screenshot: %s", sc_err)
                raise RuntimeError(
                    "Timed out waiting for the post-login redirect. "
                    "Current URL: %s. Check Kite app redirect URL config and ensure it matches your broker settings." % page.url
                )
            return request_token

        finally:
            await context.close()
            await browser.close()