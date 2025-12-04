# cap/core/security.py
import os
from fastapi import HTTPException
import jwt  # PyJWT
import os, re, bcrypt, jwt, secrets, unicodedata
from datetime import datetime, timedelta, timezone
from typing import Dict, Any

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret")
JWT_ALG = os.getenv("JWT_ALG", "HS256")

def hash_password(pw: str) -> str:
    return bcrypt.hashpw(pw.encode(), bcrypt.gensalt()).decode()

def verify_password(pw: str, hashed: str) -> bool:
    return bcrypt.checkpw(pw.encode(), hashed.encode())

def make_access_token(sub: str, remember: bool) -> str:
    exp = datetime.now(timezone.utc) + timedelta(days=7 if remember else 1)
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


def decode_access_token(token: str) -> Dict[str, Any]:
    """
    Decode & validate JWT; raise HTTPException on invalid token.
    """
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
        # Optional: basic exp/nbf/iat checks handled by PyJWT if present
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="tokenExpired", headers={"WWW-Authenticate": "Bearer"})
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="invalidToken", headers={"WWW-Authenticate": "Bearer"})
