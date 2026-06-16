from datetime import datetime, timezone
from typing import Any

from kiteconnect import KiteConnect

from config import _require_env
from kite_login import get_request_token


class KiteSession:
    """Lightweight singleton that holds the authenticated KiteConnect instance."""

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


async def bootstrap_kite_session() -> dict[str, Any]:
    """Authenticate via Playwright, exchange request_token, and store session."""
    api_key = _require_env("KITE_API_KEY")
    api_secret = _require_env("KITE_API_SECRET")
    user_id = _require_env("KITE_USER_ID")
    password = _require_env("KITE_PASSWORD")
    totp_secret = _require_env("KITE_TOTP_SECRET")

    request_token = await get_request_token(
        api_key=api_key,
        user_id=user_id,
        password=password,
        totp_secret=totp_secret,
    )

    kite = KiteConnect(api_key=api_key)
    data = kite.generate_session(request_token, api_secret=api_secret)
    access_token = data["access_token"]
    kite.set_access_token(access_token)

    KiteSession.kite = kite
    KiteSession.access_token = access_token
    KiteSession.authenticated_at = datetime.now(timezone.utc)

    return {
        "status": "ok",
        "access_token_suffix": access_token[-6:],
        "authenticated_at": KiteSession.authenticated_at.isoformat(),
    }
