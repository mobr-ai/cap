import os
import re
import secrets
import unicodedata
from datetime import UTC, datetime, timedelta
from typing import Any

import bcrypt
import jwt  # PyJWT
from dotenv import load_dotenv
from fastapi import HTTPException

load_dotenv()

JWT_SECRET = os.getenv("JWT_SECRET")
JWT_ALG = os.getenv("JWT_ALG", "HS256")

if not JWT_SECRET:
    raise RuntimeError("JWT_SECRET is not set. Define it in .env or the deployment environment.")

if JWT_ALG.startswith("HS") and len(JWT_SECRET.encode("utf-8")) < 32:
    raise RuntimeError("JWT_SECRET must be at least 32 bytes for HMAC JWT algorithms.")

def hash_password(pw: str) -> str:
    return bcrypt.hashpw(pw.encode(), bcrypt.gensalt()).decode()

def verify_password(pw: str, hashed: str) -> bool:
    return bcrypt.checkpw(pw.encode(), hashed.encode())

def make_access_token(sub: str, remember: bool) -> str:
    exp = datetime.now(UTC) + timedelta(days=7 if remember else 1)
    return jwt.encode({"sub": sub, "exp": exp}, JWT_SECRET, algorithm=JWT_ALG)

USERNAME_REGEX = re.compile(r'^[a-zA-Z][a-zA-Z0-9._]{5,29}$')


def generate_unique_username(db, User, preferred: str | None = None, base_fallback: str = "user"):
    """
    Create a unique, sanitized username:
    - Strip accents (ã -> a, ç -> c, ü -> u, etc.)
    - Lowercase everything
    - Remove characters outside [a-z0-9._]
    - Must start with a letter; if not, prefix with base_fallback
    - Max length 30
    - If taken/invalid, append an incrementing numeric suffix
    """

    def strip_accents(s: str) -> str:
        # NFKD splits letters+accents; then drop combining marks
        return "".join(
            ch for ch in unicodedata.normalize("NFKD", s)
            if not unicodedata.combining(ch)
        )

    def sanitize(name: str) -> str:
        name = (name or "").strip()
        name = strip_accents(name).lower()

        # keep letters, digits, dot, underscore only
        name = re.sub(r"[^a-z0-9._]+", "", name)

        # collapse repeated separators (optional—keeps things tidy)
        name = re.sub(r"[._]{2,}", ".", name)

        # avoid leading/trailing dots/underscores
        name = name.strip("._")

        # must start with a letter
        if not name or not name[0].isalpha():
            name = f"{base_fallback}{name}"

        # enforce max length
        return name[:30]

    base = sanitize(preferred or base_fallback)
    username = base
    counter = 1

    # ensure matches policy and is unique in DB
    while (
        not USERNAME_REGEX.match(username)
        or db.query(User).filter(User.username == username).first()
    ):
        suffix = str(counter)
        # re-trim base to leave room for suffix
        username = f"{base[:30 - len(suffix)]}{suffix}"
        counter += 1

    return username


def new_confirmation_token() -> str:
    return secrets.token_urlsafe(32)


def make_email_attachment_token(
    user_id: int,
    email: str,
    *,
    previous_email: str | None,
    lifetime_hours: int = 24,
) -> str:
    """
    Short-lived proof that an authenticated CAP user requested
    verification of a specific email address.

    previous_email binds the token to the account state that existed
    when the request was created. This prevents stale links from later
    overwriting a newer verified email.
    """
    normalized_email = (email or "").strip().lower()

    if not normalized_email:
        raise ValueError("email is required")

    normalized_previous = (
        (previous_email or "").strip().lower()
        or None
    )

    exp = datetime.now(UTC) + timedelta(
        hours=lifetime_hours
    )

    return jwt.encode(
        {
            "sub": str(user_id),
            "email": normalized_email,
            "previous_email": normalized_previous,
            "purpose": "attach_email",
            "exp": exp,
        },
        JWT_SECRET,
        algorithm=JWT_ALG,
    )


def decode_email_attachment_token(
    token: str,
) -> dict[str, Any]:
    payload = decode_access_token(token)

    if payload.get("purpose") != "attach_email":
        raise HTTPException(
            status_code=400,
            detail="invalidEmailVerificationToken",
        )

    if "previous_email" not in payload:
        raise HTTPException(
            status_code=400,
            detail="invalidEmailVerificationToken",
        )

    email = str(
        payload.get("email") or ""
    ).strip().lower()

    if not email:
        raise HTTPException(
            status_code=400,
            detail="invalidEmailVerificationToken",
        )

    return payload


def decode_access_token(token: str) -> dict[str, Any]:
    """
    Decode & validate JWT; raise HTTPException on invalid token.
    """
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
        # Optional: basic exp/nbf/iat checks handled by PyJWT if present
        return payload

    except jwt.ExpiredSignatureError as e:
        raise HTTPException(
            status_code=401, detail="tokenExpired", headers={"WWW-Authenticate": "Bearer"}
        ) from e

    except jwt.InvalidTokenError as e:
        raise HTTPException(
            status_code=401, detail="invalidToken", headers={"WWW-Authenticate": "Bearer"}
        ) from e
