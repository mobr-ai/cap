import os

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, EmailStr
from sqlalchemy import text
from sqlalchemy.orm import Session

from cap.core.google_oauth import get_userinfo_from_access_token_or_idtoken
from cap.core.security import (
    generate_unique_username,
    hash_password,
    make_access_token,
    new_confirmation_token,
    verify_password,
)
from cap.database.model import User
from cap.database.session import get_db
from cap.mailing.event_triggers import on_user_access_granted
from cap.services.admin_alerts_service import maybe_notify_admins_new_user

# --- Event triggers (mailer) ---
try:
    from cap.mailing.event_triggers import (
        on_oauth_login,  # notify / log OAuth login
        on_user_confirmed,  # notify / log that user confirmed their email
        on_user_registered,  # existing user (confirm-your-email)
        on_wallet_login,  # notify / log Cardano wallet login
    )
except Exception:
    # Fallbacks to avoid breaking imports if optional triggers aren't defined yet.
    def on_user_registered(*args, **kwargs): pass
    def on_user_confirmed(*args, **kwargs): pass
    def on_oauth_login(*args, **kwargs): pass
    def on_wallet_login(*args, **kwargs): pass


route_prefix = "/api/v1"
router = APIRouter(prefix=route_prefix, tags=["auth"])

PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL")

def _stable_base_url(request: Request) -> str:
    """Prefer PUBLIC_BASE_URL to avoid localhost/0.0.0.0 links; fallback to request base_url."""
    if PUBLIC_BASE_URL:
        return PUBLIC_BASE_URL.rstrip("/")
    return str(request.base_url).rstrip("/")

def _make_referral_link(base_url: str, user_id: int | None) -> str:
    """Build /signup?ref=u<user_id> or plain /signup if absent."""
    if user_id:
        return f"{base_url}/signup?ref=u{user_id}"
    return f"{base_url}/signup"


# ---- Pydantic shapes ----
class ResendSetupLinkIn(BaseModel):
    email: EmailStr
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


class SetPasswordIn(BaseModel):
    token: str
    password: str
    remember_me: bool = False




# ---- Auth: Claim e-mail (for wallet) ----

@router.post("/auth/wallet_claim_email")
def wallet_claim_email():
    """
    Retired with the end of CAP's closed-beta admission flow.

    Cardano wallet control is now verified directly through the signed
    CIP-8 authentication challenge.
    """
    raise HTTPException(
        status_code=410,
        detail="walletEmailClaimRetired",
    )


@router.post("/register")
def register(
    data: RegisterIn,
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Public email/password account creation.

    New accounts verify email ownership before they can log in. Historical
    unconfirmed passwordless placeholder accounts created by the old waitlist
    may be claimed through the same verification flow.
    """
    email_norm = (
        str(data.email or "").strip().lower()
        if data.email
        else ""
    )

    if not email_norm:
        raise HTTPException(400, detail="registerError")

    password = data.password or ""
    if len(password) < 8:
        raise HTTPException(400, detail="weakPassword")

    user = db.query(User).filter(User.email == email_norm).first()

    if user:
        if user.google_id:
            raise HTTPException(400, detail="oauthExistsError")

        # Historical waitlist/public placeholder compatibility:
        # only an UNCONFIRMED passwordless account may be claimed.
        #
        # A confirmed passwordless account may have been deliberately
        # provisioned by an administrator and must use the existing setup
        # flow instead of allowing password takeover by email alone.
        if not user.password_hash and not bool(user.is_confirmed):
            token = new_confirmation_token()
            email_local = email_norm.split("@")[0]

            user.password_hash = hash_password(password)
            user.confirmation_token = token
            user.is_confirmed = False

            if not user.username:
                user.username = generate_unique_username(
                    db,
                    User,
                    preferred=email_local,
                )

            db.commit()
            db.refresh(user)

            base_url = _stable_base_url(request)
            activation_link = (
                f"{base_url}{route_prefix}/confirm/{token}"
            )

            on_user_registered(
                to=[email_norm],
                language=(data.language or "en"),
                username=user.username or email_local,
                activation_link=activation_link,
            )

            return {
                "redirect": "/login?confirmed=false",
                "claimed_existing_account": True,
            }

        raise HTTPException(400, detail="userExistsError")

    token = new_confirmation_token()
    email_local = email_norm.split("@")[0]

    new_user = User(
        email=email_norm,
        username=generate_unique_username(
            db,
            User,
            preferred=email_local,
        ),
        password_hash=hash_password(password),
        confirmation_token=token,
        is_confirmed=False,
        is_admin=False,
    )

    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    maybe_notify_admins_new_user(
        db,
        new_user,
        source="password",
    )

    base_url = _stable_base_url(request)
    activation_link = (
        f"{base_url}{route_prefix}/confirm/{token}"
    )

    on_user_registered(
        to=[email_norm],
        language=(data.language or "en"),
        username=new_user.username or email_local,
        activation_link=activation_link,
    )

    return {
        "redirect": "/login?confirmed=false",
    }

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


# ---- Re-send setup link in case it expires ----
@router.post("/auth/resend_setup_link")
def resend_setup_link(data: ResendSetupLinkIn, request: Request, db: Session = Depends(get_db)):
    email_norm = (str(data.email or "").strip().lower()) if data.email else ""
    if not email_norm:
        raise HTTPException(400, detail="invalidEmailFormat")

    user = db.query(User).filter(User.email == email_norm).first()
    if not user:
        raise HTTPException(404, detail="userNotFound")

    # Must already be approved
    if not bool(user.is_confirmed):
        raise HTTPException(403, detail="accessNotGranted")

    # If Google user, they should use Google login
    if user.google_id:
        raise HTTPException(400, detail="oauthExistsError")

    # If password is already set, they can just login
    if user.password_hash:
        raise HTTPException(400, detail="passwordAlreadySet")

    # Create a fresh token (reusing existing generator)
    token = new_confirmation_token()
    user.confirmation_token = token
    db.commit()
    db.refresh(user)

    base_url = _stable_base_url(request)
    setup_url = f"{base_url}/login?state=setpass&token={token}"

    # Send the same "access granted" email but with setup_url CTA
    on_user_access_granted(
        to=[email_norm],
        language=(data.language or "en"),
        app_url=base_url,
        setup_url=setup_url,
    )

    return {"status": "sent"}

# ---- Define user password for approved accounts ----
@router.post("/auth/set_password")
def set_password(data: SetPasswordIn, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.confirmation_token == data.token).first()
    if not user:
        raise HTTPException(400, detail="invalidOrExpiredToken")

    # Must be approved already (pre-alpha gate)
    if not user.is_confirmed:
        raise HTTPException(403, detail="accessNotGranted")

    # Block if this account is Google-based
    if user.google_id:
        raise HTTPException(400, detail="oauthExistsError")

    # Keep the same password policy as public registration.
    pw = data.password or ""
    if len(pw) < 8:
        raise HTTPException(400, detail="weakPassword")

    user.password_hash = hash_password(pw)
    user.confirmation_token = None
    db.commit()
    db.refresh(user)

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
    db.refresh(user)

    base_url = _stable_base_url(request)
    activation_link = (
        f"{base_url}{route_prefix}/confirm/{token}"
    )

    email_norm = str(user.email or data.email).strip().lower()
    email_local = email_norm.split("@")[0]

    # Re-send the actual confirmation CTA. The legacy
    # confirmation-resent notification does not carry an activation link.
    on_user_registered(
        to=[email_norm],
        language=(data.language or "en"),
        username=user.username or email_local,
        activation_link=activation_link,
    )

    return {"message": "resent"}


# ---- Login (email/password) ----
@router.post("/login")
def login(data: LoginIn, db: Session = Depends(get_db)):
    email_norm = (str(data.email or "").strip().lower()) if data.email else ""
    if not email_norm:
        raise HTTPException(401, detail="loginError")

    user = db.query(User).filter(User.email == email_norm).first()
    if not user:
        raise HTTPException(401, detail="loginError")

    if not user.is_confirmed:
        raise HTTPException(403, detail="confirmationError")

    # If user has no password set, they cannot use password login.
    # This also covers google-only accounts (unless you later add "set password" for them).
    if not user.password_hash:
        if user.google_id:
            raise HTTPException(400, detail="oauthExistsError")
        raise HTTPException(403, detail="passwordNotSet")

    if not verify_password(data.password, user.password_hash):
        raise HTTPException(401, detail="loginError")

    token = make_access_token(str(user.user_id), remember=data.remember_me)

    resp = {
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

    # If linked with Google, include a friendly notice for the frontend
    if user.google_id:
        resp["notice"] = "googleLinked"

    return resp


# ---- Google OAuth (access_token from client) ----

@router.post("/auth/google")
def auth_google(
    data: GoogleIn,
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Public Google authentication.

    Google must explicitly verify ownership of the returned email.
    Google verification bypasses the retired beta/waitlist admission
    gate, but first-time or otherwise-unconfirmed CAP accounts must
    still complete CAP's own email activation step.
    """
    try:
        token_type = getattr(
            data,
            "token_type",
            None,
        )

        info = (
            get_userinfo_from_access_token_or_idtoken(
                data.token,
                (
                    token_type
                    if isinstance(
                        token_type,
                        str,
                    )
                    else None
                ),
            )
        )

        google_id = str(
            info.get("sub") or ""
        ).strip()

        email = str(
            info.get("email") or ""
        ).strip().lower()

        email_verified = (
            info.get("email_verified")
            is True
        )

        display_name = (
            info.get("name") or ""
        )

        avatar = (
            info.get("picture") or ""
        )

        if not google_id:
            raise HTTPException(
                400,
                detail="missingGoogleSubject",
            )

        if not email:
            raise HTTPException(
                400,
                detail="missingGoogleEmail",
            )

        if not email_verified:
            raise HTTPException(
                400,
                detail="unverifiedGoogleEmail",
            )

        # Prefer an already-linked Google identity.
        user = (
            db.query(User)
            .filter(
                User.google_id
                == google_id
            )
            .first()
        )

        created = False

        if not user:
            # A verified Google email may link to an existing CAP account.
            user = (
                db.query(User)
                .filter(
                    User.email == email
                )
                .first()
            )

            if user:
                if (
                    user.google_id
                    and user.google_id
                    != google_id
                ):
                    raise HTTPException(
                        400,
                        detail="oauthExistsError",
                    )

                user.google_id = google_id

            else:
                username = (
                    generate_unique_username(
                        db,
                        User,
                        preferred=(
                            email.split("@")[0]
                            or display_name
                        ),
                    )
                )

                user = User(
                    google_id=google_id,
                    email=email,
                    username=username,
                    display_name=display_name,
                    avatar=avatar,
                    is_confirmed=False,
                    confirmation_token=(
                        new_confirmation_token()
                    ),
                    is_admin=False,
                )

                db.add(user)
                db.commit()
                db.refresh(user)

                created = True

                maybe_notify_admins_new_user(
                    db,
                    user,
                    source="google",
                )

        # Keep safe profile metadata current.
        if (
            display_name
            and not user.display_name
        ):
            user.display_name = display_name

        if avatar and not user.avatar:
            user.avatar = avatar

        if not user.username:
            user.username = (
                generate_unique_username(
                    db,
                    User,
                    preferred=(
                        email.split("@")[0]
                        or display_name
                    ),
                )
            )

        # If this is an already-linked but still-unconfirmed Google user,
        # a current verified Google email may refresh its pending address.
        # Never silently replace the canonical email of an already
        # confirmed CAP account.
        if not bool(user.is_confirmed):
            if (
                not user.email
                or (
                    user.google_id
                    == google_id
                    and user.email.strip().lower()
                    != email
                )
            ):
                email_owner = (
                    db.query(User)
                    .filter(
                        User.email == email
                    )
                    .first()
                )

                if (
                    email_owner
                    and email_owner.user_id
                    != user.user_id
                ):
                    raise HTTPException(
                        400,
                        detail="oauthExistsError",
                    )

                user.email = email

                # The old token may have been delivered to the previous
                # address. Invalidate it when the pending Google email
                # identity changes.
                user.confirmation_token = None

            # Keep an existing pending activation token stable. Automatic
            # repeated OAuth callbacks must not invalidate an email that was
            # already sent. Explicit /resend_confirmation is responsible for
            # rotating tokens.
            confirmation_token = (
                user.confirmation_token
            )

            send_activation_email = bool(created)

            if not confirmation_token:
                confirmation_token = (
                    new_confirmation_token()
                )

                user.confirmation_token = (
                    confirmation_token
                )

                send_activation_email = True

            db.add(user)
            db.commit()
            db.refresh(user)

            base_url = (
                _stable_base_url(request)
            )

            activation_link = (
                f"{base_url}"
                f"{route_prefix}"
                f"/confirm/"
                f"{confirmation_token}"
            )

            activation_email = (
                str(
                    user.email or email
                )
                .strip()
                .lower()
            )

            email_local = (
                activation_email
                .split("@")[0]
            )

            if send_activation_email:
                on_user_registered(
                    to=[activation_email],
                    language=(
                        data.language or "en"
                    ),
                    username=(
                        user.username
                        or email_local
                    ),
                    activation_link=(
                        activation_link
                    ),
                )

            return {
                "status": (
                    "pending_confirmation"
                ),
                "redirect": (
                    "/login?confirmed=false"
                ),
                "id": user.user_id,
                "email": activation_email,
                "created": created,
            }

        # Already activated CAP account: Google login is now complete.
        user.confirmation_token = None

        db.add(user)
        db.commit()
        db.refresh(user)

        token = make_access_token(
            str(user.user_id),
            remember=data.remember_me,
        )

        notification_email = (
            str(
                user.email or email
            )
            .strip()
            .lower()
        )

        if notification_email:
            on_oauth_login(
                to=[notification_email],
                language=(
                    data.language or "en"
                ),
                provider="Google",
            )

        return {
            "id": user.user_id,
            "username": user.username,
            "wallet_address": (
                user.wallet_address
            ),
            "display_name": (
                user.display_name
            ),
            "email": user.email,
            "avatar": user.avatar,
            "settings": user.settings,
            "is_admin": getattr(
                user,
                "is_admin",
                False,
            ),
            "access_token": token,
        }

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            400,
            detail=str(exc),
        ) from exc
