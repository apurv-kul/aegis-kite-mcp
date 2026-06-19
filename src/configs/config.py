import os

from dotenv import load_dotenv

load_dotenv()

# Kite Connect DOM selectors (as of June 2026).
# If Zerodha updates their UI, these are the only lines to change.
SEL_USER_ID = "input#userid"
SEL_PASSWORD = "input#password"
SEL_LOGIN_BTN = "button[type='submit']"
SEL_TOTP = "input[type='number'][maxlength='6'], input[placeholder*='PIN'], input[placeholder*='TOTP']"

LOGIN_TIMEOUT_MS = 15_000   # 15 s for page navigation / element appearance
REDIRECT_TIMEOUT_MS = 10_000  # 10 s to capture the post-auth redirect
LOGIN_URL_TEMPLATE = "https://kite.zerodha.com/connect/login?v=3&api_key={api_key}"


def _require_env(key: str) -> str:
    """Fetch a required environment variable or raise a clear error."""
    value = os.getenv(key, "").strip()
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{key}' is missing or empty. "
            "Check your .env file."
        )
    return value
