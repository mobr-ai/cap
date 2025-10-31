# cap/src/cap/api/auth.py
import hashlib
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session

from cap.database.session import get_db
from cap.database.model import User
from cap.core.security import (
    hash_password, verify_password, make_access_token,
    generate_unique_username, new_confirmation_token
)
from cap.core.google_oauth import get_userinfo_from_access_token_or_idtoken

# --- Event triggers (mailer) ---
try:
    from cap.mailing.event_triggers import (
        on_user_registered,        # existing in CAP (confirm-your-email)
        on_waiting_list_joined,    # notify user joined waiting list (not used here)
        on_confirmation_resent,    # notify user that a new confirmation email was sent
        on_user_confirmed,         # notify / log that user confirmed their email
        on_oauth_login,            # notify / log OAuth login
        on_wallet_login,           # notify / log Cardano wallet login
    )
except Exception:
    # Fallbacks to avoid breaking imports if optional triggers aren't defined yet.
    def on_user_registered(*args, **kwargs): pass
    def on_confirmation_resent(*args, **kwargs): pass
    def on_user_confirmed(*args, **kwargs): pass
    def on_oauth_login(*args, **kwargs): pass
    def on_wallet_login(*args, **kwargs): pass

route_prefix = "/api/v1"
router = APIRouter(prefix=route_prefix, tags=["auth"])

# ---- Pydantic shapes ----
class RegisterIn(BaseModel):
    email: EmailStr
    password: str
    language: str | None = "en"

class LoginIn(BaseModel):
    email: EmailStr
    password: str
    remember_me: bool = False

class ResendIn(BaseModel):
    email: EmailStr
    language: str | None = "en"

class GoogleIn(BaseModel):
    token: str
    token_type: str | None = None
    remember_me: bool = False

class CardanoIn(BaseModel):
    address: str
    remember_me: bool = True

# ---- Auth: Register (unconfirmed) ----
@router.post("/register")
def register(data: RegisterIn, request: Request, db: Session = Depends(get_db)):
    if not data.email or not data.password:
        raise HTTPException(400, detail="registerError")

    user = db.query(User).filter(User.email == data.email).first()
    if user:
        if user.google_id:
            raise HTTPException(400, detail="oauthExistsError")
        raise HTTPException(400, detail="userExistsError")

    token = new_confirmation_token()
    new_user = User(
        email=data.email,
        username=(data.email.split("@")[0])[:30],
        password_hash=hash_password(data.password),
        confirmation_token=token,
        is_confirmed=False,
    )
    db.add(new_user)
    db.commit()

    # Build confirmation link
    base = str(request.base_url).rstrip("/")
    activation_link = f"{base}/{route_prefix}/confirm/{token}"

    # Send confirmation email
    # Uses CAP mailing service (Resend + Jinja templates + i18n). :contentReference[oaicite:2]{index=2}
    on_user_registered(
        to=[data.email],
        language=(data.language or "en"),
        username=new_user.username or data.email.split('@')[0],
        activation_link=activation_link,
    )

    return {"redirect": "/login?confirmed=false"}

# ---- Confirm email ----
@router.get("/confirm/{token}")
def confirm_email(token: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.confirmation_token == token).first()
    if not user:
        raise HTTPException(400, detail="confirmationError")

    user.is_confirmed = True
    user.confirmation_token = None
    db.commit()

    # Optional: fire a "user confirmed" trigger (logging/notification)
    on_user_confirmed(to=[user.email] if user.email else [], language="en")

    return RedirectResponse(url="/login?confirmed=true")

# ---- Resend confirmation ----
@router.post("/resend_confirmation")
def resend_confirmation(data: ResendIn, request: Request, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == data.email).first()
    if not user:
        raise HTTPException(404, detail="userNotFound")

    if user.is_confirmed:
        raise HTTPException(400, detail="alreadyConfirmed")

    token = new_confirmation_token()
    user.confirmation_token = token
    db.commit()

    base = str(request.base_url).rstrip("/")
    activation_link = f"{base}/{route_prefix}/confirm/{token}"

    # You may choose to send the full "confirm your email" again here:
    # on_user_registered([...]) — or use a lighter "confirmation resent" notice:
    on_confirmation_resent(to=[data.email], language=(data.language or "en"))

    return {"message": "resent"}

# ---- Login (email/password) ----
@router.post("/login")
def login(data: LoginIn, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == data.email).first()
    if user and user.google_id:
        # prevent password login if Google account exists for this email
        raise HTTPException(400, detail="oauthExistsError")

    if not user or not user.password_hash or not verify_password(data.password, user.password_hash):
        raise HTTPException(401, detail="loginError")

    if not user.is_confirmed:
        raise HTTPException(403, detail="confirmationError")

    token = make_access_token(str(user.user_id), remember=data.remember_me)

    return {
        "id": user.user_id,
        "username": user.username,
        "wallet_address": user.wallet_address,
        "display_name": user.display_name,
        "email": user.email,
        "avatar": user.avatar,
        "settings": user.settings,
        "access_token": token,
    }

# ---- Google OAuth (access_token from client) ----
@router.post("/auth/google")
def auth_google(data: GoogleIn, db: Session = Depends(get_db)):
    try:
        # Exchange access_token → People API profile. :contentReference[oaicite:3]{index=3}
        info = get_userinfo_from_access_token_or_idtoken(data.token, getattr(data, "token_type", None))

        google_id = info["sub"]
        email = info["email"]
        display_name = info["name"]
        avatar = info.get("picture", "")

        user = db.query(User).filter(User.google_id == google_id).first()
        if not user:
            username = generate_unique_username(db, User, preferred=(display_name or email.split("@")[0]))
            user = User(
                google_id=google_id,
                email=email,
                username=username,
                display_name=display_name,
                avatar=avatar,
                is_confirmed=True,
            )
            db.add(user)

            # Fire an OAuth login trigger (analytics/notice) once
            on_oauth_login(to=[email], language="en", provider="Google")
        elif not user.avatar:
            # keep avatar if already defined
            user.avatar = avatar
        db.commit()

        token = make_access_token(str(user.user_id), remember=data.remember_me)

        return {
            "id": user.user_id,
            "username": user.username,
            "wallet_address": user.wallet_address,
            "display_name": user.display_name,
            "email": user.email,
            "avatar": user.avatar,
            "settings": user.settings,
            "access_token": token,
        }
    except Exception as e:
        raise HTTPException(400, detail=str(e))

# ---- Cardano wallet auth (simplified flow) ----
@router.post("/auth/cardano")
def cardano_auth(data: CardanoIn, db: Session = Depends(get_db)):
    if not data.address:
        raise HTTPException(400, detail="Missing address")

    user = db.query(User).filter(User.wallet_address == data.address).first()
    if not user:
        # make a stable username derived from address
        suffix = int(hashlib.sha256(data.address.encode()).hexdigest(), 16) % 1_000_000
        username = f"cardano_user{suffix}"
        # ensure uniqueness
        if db.query(User).filter(User.username == username).first():
            username = generate_unique_username(db, User, preferred=username)

        display_name = f"{data.address[:8]}...{data.address[-5:]}"
        user = User(
            username=username,
            wallet_address=data.address,
            display_name=display_name,
            is_confirmed=True,
        )
        db.add(user)
        db.commit()

    token = make_access_token(str(user.user_id), remember=data.remember_me)

    # Optional: fire a wallet login trigger (analytics/notice)
    if user.email:
        on_wallet_login(to=[user.email] if user.email else [], language="en", wallet_address=data.address)

    return {
        "id": user.user_id,
        "username": user.username,
        "wallet_address": user.wallet_address,
        "display_name": user.display_name,
        "email": user.email,
        "avatar": user.avatar,
        "settings": user.settings,
        "access_token": token,
    }
