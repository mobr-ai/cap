import os

import requests
from google.auth.transport import requests as grequests
from google.oauth2 import id_token


GOOGLE_USERINFO = "https://openidconnect.googleapis.com/v1/userinfo"
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")


def _verified_google_identity(data: dict) -> dict:
    """
    Normalize a Google OIDC identity and require Google to explicitly
    report ownership of the returned email as verified.
    """
    subject = str(
        data.get("sub") or ""
    ).strip()

    email = str(
        data.get("email") or ""
    ).strip().lower()

    if not subject:
        raise ValueError(
            "missing_google_subject"
        )

    if not email:
        raise ValueError(
            "missing_google_email"
        )

    if data.get("email_verified") is not True:
        raise ValueError(
            "google_email_not_verified"
        )

    return {
        "sub": subject,
        "email": email,
        "email_verified": True,
        "name": data.get("name") or "",
        "picture": data.get("picture", ""),
    }


def get_userinfo_from_access_token_or_idtoken(
    token: str,
    token_type: str | None = None,
):
    """
    Verify either a Google access token or ID token and return only an
    identity whose email Google explicitly reports as verified.
    """
    # ID Token / Google One Tap.
    if (
        token_type == "id_token"
        or token.count(".") == 2
    ):
        try:
            claims = (
                id_token.verify_oauth2_token(
                    token,
                    grequests.Request(),
                    GOOGLE_CLIENT_ID,
                )
            )

            return _verified_google_identity(
                claims
            )

        except Exception as exc:
            raise Exception(
                f"invalid_id_token: {exc}"
            ) from exc

    # OAuth access token / OIDC userinfo.
    try:
        response = requests.get(
            GOOGLE_USERINFO,
            headers={
                "Authorization": (
                    f"Bearer {token}"
                ),
            },
            timeout=10,
        )

        if response.status_code != 200:
            raise Exception(
                "userinfo_failed: "
                f"{response.status_code}"
            )

        return _verified_google_identity(
            response.json()
        )

    except Exception as exc:
        raise Exception(
            f"userinfo_error: {exc}"
        ) from exc
