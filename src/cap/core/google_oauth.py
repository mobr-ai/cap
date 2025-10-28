# cap/core/google_oauth.py
import requests
from google.oauth2 import id_token
from google.auth.transport import requests as grequests
import os

GOOGLE_USERINFO = "https://openidconnect.googleapis.com/v1/userinfo"
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")

def get_userinfo_from_access_token_or_idtoken(token: str, token_type: str = None):
    """
    Verifies either an access_token (via /userinfo) or an id_token (JWT)
    from Google OAuth or One Tap.
    """
    # Case 1: ID Token (JWT from One Tap)
    if token_type == "id_token" or token.count(".") == 2:
        try:
            claims = id_token.verify_oauth2_token(
                token,
                grequests.Request(),
                GOOGLE_CLIENT_ID,
            )
            return {
                "sub": claims["sub"],
                "email": claims.get("email"),
                "name": claims.get("name"),
                "picture": claims.get("picture", ""),
            }
        except Exception as e:
            raise Exception(f"invalid_id_token: {e}")

    # Case 2: Access Token (OAuth implicit flow)
    try:
        resp = requests.get(
            GOOGLE_USERINFO,
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        if resp.status_code != 200:
            raise Exception(f"userinfo_failed: {resp.status_code}")
        data = resp.json()
        return {
            "sub": data.get("sub"),
            "email": data.get("email"),
            "name": data.get("name"),
            "picture": data.get("picture", ""),
        }
    except Exception as e:
        raise Exception(f"userinfo_error: {e}")
