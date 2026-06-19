import asyncio
from dotenv import load_dotenv
from kiteconnect import KiteConnect
from config.config import _require_env

load_dotenv()

async def test_session_exchange():
    api_key = _require_env("KITE_API_KEY")
    api_secret = _require_env("KITE_API_SECRET")
    request_token = "LshqwVGpQvqTTG5MgJTn3a6D5o6LIyH0"
    
    kite = KiteConnect(api_key=api_key)
    try:
        data = await asyncio.to_thread(
            kite.generate_session, request_token, api_secret=api_secret
        )
        print("✅ Session exchange successful!")
        print(f"Access Token suffix: ...{data['access_token'][-10:]}")
        kite.set_access_token(data["access_token"])
        profile = await asyncio.to_thread(kite.profile)
        print(f"Account: {profile.get('user_name')}")
        print(f"Broker: {profile.get('broker')}")
        return True
    except Exception as e:
        print(f"❌ Session exchange failed: {e}")
        return False

if __name__ == "__main__":
    asyncio.run(test_session_exchange())
