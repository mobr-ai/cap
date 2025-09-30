# cap/src/cap/core/security.py
import os, re, bcrypt, jwt, secrets
from datetime import datetime, timedelta, timezone

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret")
JWT_ALG = "HS256"

def hash_password(pw: str) -> str:
    return bcrypt.hashpw(pw.encode(), bcrypt.gensalt()).decode()

def verify_password(pw: str, hashed: str) -> bool:
    return bcrypt.checkpw(pw.encode(), hashed.encode())

def make_access_token(sub: str, remember: bool) -> str:
    exp = datetime.now(timezone.utc) + timedelta(days=7 if remember else 1)
    return jwt.encode({"sub": sub, "exp": exp}, JWT_SECRET, algorithm=JWT_ALG)

USERNAME_REGEX = re.compile(r'^[a-zA-Z][a-zA-Z0-9._]{5,29}$')

def generate_unique_username(db, User, preferred: str | None = None, base_fallback="user"):
    def sanitize(name: str) -> str:
        name = re.sub(r'[^a-zA-Z0-9._]', '', name or "")
        if not name or not name[0].isalpha():
            name = base_fallback + name
        return name[:30]

    base = sanitize(preferred or base_fallback)
    username = base
    counter = 1
    while not USERNAME_REGEX.match(username) or db.query(User).filter(User.username == username).first():
        suffix = f"{counter}"
        username = f"{base[:30 - len(suffix)]}{suffix}"
        counter += 1
    return username

def new_confirmation_token() -> str:
    return secrets.token_urlsafe(32)
