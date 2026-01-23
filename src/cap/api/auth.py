# cap/src/cap/api/auth.py
import hashlib
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session
from sqlalchemy import text

from cap.database.session import get_db
from cap.database.model import User
from cap.services.admin_alerts_service import maybe_notify_admins_new_user
from cap.core.security import (
    hash_password,
    verify_password,
    make_access_token,
    generate_unique_username,
    new_confirmation_token,
)
from cap.core.google_oauth import get_userinfo_from_access_token_or_idtoken

# --- Event triggers (mailer) ---
try:
    from cap.mailing.event_triggers import (
        on_user_registered,        # existing in CAP (confirm-your-email)
        on_waiting_list_joined,    # notify user joined waiting list
        on_confirmation_resent,    # notify user that a new confirmation email was sent
        on_user_confirmed,         # notify / log that user confirmed their email
        on_oauth_login,            # notify / log OAuth login
        on_wallet_login,           # notify / log Cardano wallet login
    )
except Exception:
    # Fallbacks to avoid breaking imports if optional triggers aren't defined yet.
    def on_user_registered(*args, **kwargs): pass
    def on_waiting_list_joined(*args, **kwargs): pass
    def on_confirmation_resent(*args, **kwargs): pass
    def on_user_confirmed(*args, **kwargs): pass
    def on_oauth_login(*args, **kwargs): pass
    def on_wallet_login(*args, **kwargs): pass


route_prefix = "/api/v1"
router = APIRouter(prefix=route_prefix, tags=["auth"])


# ---- Pydantic shapes ----
class WalletClaimEmailIn(BaseModel):
    user_id: int
    wallet_address: str
    email: EmailStr
    ref: str | None = ""
    language: str | None = "en"


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
    language: str | None = "en"
    ref: str | None = ""


class CardanoIn(BaseModel):
    address: str
    remember_me: bool = True
    language: str | None = "en"
    ref: str | None = ""


def _ensure_waitlist_row(
    db: Session, email: str, ref: str = "", language: str = "en"
) -> bool:
    """
    Insert into waiting_list if not exists.
    Returns True if inserted, False if already existed / skipped.
    """
    e = (email or "").strip().lower()
    if not e:
        return False

    exists = db.execute(
        text("SELECT 1 FROM waiting_list WHERE email = :e"),
        {"e": e},
    ).first()
    if exists:
        return False

    db.execute(
        text("INSERT INTO waiting_list (email, ref, language) VALUES (:e, :r, :l)"),
        {"e": e, "r": ref or "", "l": (language or "en").strip().lower()},
    )
    db.commit()
    return True


# ---- Auth: Claim e-mail (for wallet) ----
@router.post("/auth/wallet_claim_email")
def wallet_claim_email(
    data: WalletClaimEmailIn,
    request: Request,
    db: Session = Depends(get_db),
):
    user = db.query(User).filter(User.user_id == data.user_id).first()
    if not user:
        raise HTTPException(404, detail="userNotFound")

    if not user.wallet_address or user.wallet_address != data.wallet_address:
        raise HTTPException(400, detail="walletMismatch")

    email_norm = (str(data.email or "").strip().lower()) if data.email else ""
    if not email_norm:
        raise HTTPException(400, detail="invalidEmailFormat")

    # prevent stealing an email already owned by another user
    existing = db.query(User).filter(User.email == email_norm).first()
    if existing and existing.user_id != user.user_id:
        raise HTTPException(400, detail="userExistsError")

    # Attach email
    user.email = email_norm
    db.commit()

    inserted = _ensure_waitlist_row(
        db,
        email=email_norm,
        ref=(data.ref or ""),
        language=(data.language or "en"),
    )

    # Trigger waitlist email ONLY when this is a new waitlist entry
    if inserted:
        try:
            on_waiting_list_joined(
                to=[email_norm],
                language=(data.language or "en"),
                referral_link="",
            )
        except Exception as mail_err:
            print(f"[WAITLIST] Mail trigger failed for {email_norm}: {mail_err}")

    # Return status that lets frontend show success vs already
    if not inserted:
        raise HTTPException(418, detail="alreadyOnList")

    return {"status": "waitlisted", "id": user.user_id, "email": user.email}


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
        is_admin=False,
    )
    db.add(new_user)
    db.commit()

    # Notify admins (if configured)
    maybe_notify_admins_new_user(db, new_user, source="password")

    # Build confirmation link
    base = str(request.base_url).rstrip("/")
    activation_link = f"{base}/{route_prefix}/confirm/{token}"

    # Send confirmation email
    on_user_registered(
        to=[data.email],
        language=(data.language or "en"),
        username=new_user.username or data.email.split("@")[0],
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
        "is_admin": getattr(user, "is_admin", False),
        "access_token": token,
    }


# ---- Google OAuth (access_token from client) ----
@router.post("/auth/google")
def auth_google(data: GoogleIn, request: Request, db: Session = Depends(get_db)):
    try:
        info = get_userinfo_from_access_token_or_idtoken(
            data.token, getattr(data, "token_type", None)
        )

        google_id = info["sub"]
        email = info.get("email")
        display_name = info.get("name") or ""
        avatar = info.get("picture", "")

        if not email:
            raise HTTPException(400, detail="missingGoogleEmail")

        user = db.query(User).filter(User.google_id == google_id).first()

        if not user:
            username = generate_unique_username(
                db, User, preferred=(display_name or email.split("@")[0])
            )
            user = User(
                google_id=google_id,
                email=email,
                username=username,
                display_name=display_name,
                avatar=avatar,
                is_confirmed=False,  # do not auto-confirm
                is_admin=False,
            )
            db.add(user)
            db.commit()

            maybe_notify_admins_new_user(db, user, source="google")
        else:
            # Keep profile fields fresh
            if email and not user.email:
                user.email = email
            if display_name and not user.display_name:
                user.display_name = display_name
            if avatar and not user.avatar:
                user.avatar = avatar
            db.commit()

        # If not confirmed, put on waitlist and do not issue token
        if not bool(user.is_confirmed):
            inserted = _ensure_waitlist_row(
                db,
                email=email,
                ref=(data.ref or ""),
                language=(data.language or "en"),
            )

            if inserted:
                try:
                    on_waiting_list_joined(
                        to=[email],
                        language=(data.language or "en"),
                        referral_link="",
                    )
                except Exception as mail_err:
                    print(f"[WAITLIST] Mail trigger failed for {email}: {mail_err}")

            return {
                "status": "pending_confirmation",
                "id": user.user_id,
                "email": user.email,
            }

        # Confirmed users can login normally
        token = make_access_token(str(user.user_id), remember=data.remember_me)
        on_oauth_login(to=[email], language=(data.language or "en"), provider="Google")

        return {
            "id": user.user_id,
            "username": user.username,
            "wallet_address": user.wallet_address,
            "display_name": user.display_name,
            "email": user.email,
            "avatar": user.avatar,
            "settings": user.settings,
            "is_admin": getattr(user, "is_admin", False),
            "access_token": token,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, detail=str(e))


# ---- Cardano wallet auth (simplified flow) ----
@router.post("/auth/cardano")
def cardano_auth(data: CardanoIn, db: Session = Depends(get_db)):
    if not data.address:
        raise HTTPException(400, detail="missingWalletAddress")

    user = db.query(User).filter(User.wallet_address == data.address).first()

    if not user:
        suffix = int(hashlib.sha256(data.address.encode()).hexdigest(), 16) % 1_000_000
        username = f"cardano_user{suffix}"
        if db.query(User).filter(User.username == username).first():
            username = generate_unique_username(db, User, preferred=username)

        display_name = f"{data.address[:8]}...{data.address[-5:]}"
        user = User(
            username=username,
            wallet_address=data.address,
            display_name=display_name,
            is_confirmed=False,  # do not auto-confirm
            is_admin=False,
        )
        db.add(user)
        db.commit()

        maybe_notify_admins_new_user(db, user, source="cardano")

    # If not confirmed, do not issue token
    if not bool(user.is_confirmed):
        # Note: waiting_list table is email-based today; wallet users still become "pending"
        return {
            "status": "pending_confirmation",
            "id": user.user_id,
            "wallet_address": user.wallet_address,
        }

    token = make_access_token(str(user.user_id), remember=data.remember_me)

    if user.email:
        on_wallet_login(
            to=[user.email],
            language=(data.language or "en"),
            wallet_address=data.address,
        )

    return {
        "id": user.user_id,
        "username": user.username,
        "wallet_address": user.wallet_address,
        "display_name": user.display_name,
        "email": user.email,
        "avatar": user.avatar,
        "settings": user.settings,
        "is_admin": getattr(user, "is_admin", False),
        "access_token": token,
    }
