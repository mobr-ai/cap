import hashlib
import os
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, File, Header, HTTPException, Request, Response, UploadFile
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, EmailStr
from sqlalchemy import select
from sqlalchemy.orm import Session
from starlette.responses import StreamingResponse

from cap.core.auth_dependencies import get_current_user
from cap.core.security import (
    decode_email_attachment_token,
    generate_unique_username,
    make_email_attachment_token,
)
from cap.database.model import User
from cap.database.session import get_db
from cap.mailing.event_triggers import (
    on_user_confirmed,
    on_user_registered,
)

router = APIRouter(prefix="/api/v1/user", tags=["user"])

# -----------------------------
# User endpoints
# -----------------------------
ALLOWED_MIME = {"image/png", "image/jpeg", "image/webp"}
MAX_BYTES = 2 * 1024 * 1024  # 2 MB


# -----------------------------
# Request models
# -----------------------------
class UsernameIn(BaseModel):
    username: str


class DisplayNameIn(BaseModel):
    display_name: str


class EmailVerificationIn(BaseModel):
    email: EmailStr
    language: str | None = "en"


# -----------------------------
# Username / Display name
# -----------------------------
@router.post("/validate_username")
def validate_username(
    data: UsernameIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Returns whether the provided username is available *as typed*.
    We do not silently accept a different normalized value here; instead
    we return a suggested value when normalization changes it.
    """
    raw = (data.username or "").strip()
    if not raw:
        return {"available": False, "reason": "empty"}

    # Normalize using canonical rules + uniqueness checks
    suggested = generate_unique_username(db, User, preferred=raw)

    # If canonical normalization changed it, it's not valid "as typed"
    # (could be invalid chars/format OR collision causing suffix)
    if suggested != raw.lower():
        # Check if suggested is already taken by someone else (rare but possible
        # if raw.lower() differs and suggested is also occupied)
        existing = db.scalar(select(User).where(User.username == suggested))
        if existing and existing.user_id != current_user.user_id:
            return {"available": False, "reason": "taken", "suggested": suggested}

        return {"available": False, "reason": "normalized", "suggested": suggested}

    # Check availability for the exact value
    existing = db.scalar(select(User).where(User.username == raw.lower()))
    if existing and existing.user_id != current_user.user_id:
        return {"available": False, "reason": "taken"}

    return {"available": True}


@router.post("/username")
def update_username(
    data: UsernameIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    raw = (data.username or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="invalidUsername")

    # Canonical normalization + uniqueness
    new_username = generate_unique_username(db, User, preferred=raw)

    # If user asked for X but canonical becomes Y (due to collisions/format),
    # we still accept and return Y (frontend should reflect the saved value).
    current_user.username = new_username

    # Keep display_name precedence behavior as requested:
    # display_name remains the primary label. We only set it if it's missing.
    if not current_user.display_name:
        current_user.display_name = new_username

    db.add(current_user)
    db.commit()

    return {"username": current_user.username, "display_name": current_user.display_name}


@router.post("/display_name")
def update_display_name(
    data: DisplayNameIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    name = (data.display_name or "").strip()

    # Allow clearing display name (so UI falls back to username/email)
    if not name:
        current_user.display_name = None
        db.add(current_user)
        db.commit()
        return {"display_name": None}

    # Minimal safety: length bound to DB column (30)
    if len(name) > 30:
        raise HTTPException(status_code=400, detail="displayNameTooLong")

    # Optional: block control chars
    for ch in name:
        if ord(ch) < 32 or ord(ch) == 127:
            raise HTTPException(status_code=400, detail="invalidDisplayName")

    current_user.display_name = name
    db.add(current_user)
    db.commit()

    return {"display_name": current_user.display_name}


# -----------------------------
# Optional verified account email
# -----------------------------
@router.get("/email/status")
def email_status(
    current_user: User = Depends(get_current_user),
):
    return {
        "email": current_user.email,
        "has_email": bool(current_user.email),
    }


@router.post("/email/verification")
def request_email_verification(
    data: EmailVerificationIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Request verification of an optional account email.

    CAP access has already been established independently. For a wallet
    account that means successful signed Cardano authentication.

    The current canonical email is not changed until the verification
    link is opened.
    """
    email = str(
        data.email or ""
    ).strip().lower()

    if not email:
        raise HTTPException(
            status_code=400,
            detail="invalidEmailFormat",
        )

    existing = db.scalar(
        select(User).where(
            User.email == email
        )
    )

    if (
        existing
        and existing.user_id
        != current_user.user_id
    ):
        raise HTTPException(
            status_code=409,
            detail="emailAlreadyInUse",
        )

    current_email = (
        (current_user.email or "")
        .strip()
        .lower()
        or None
    )

    if current_email == email:
        return {
            "status": "already_verified",
            "email": current_user.email,
        }

    token = make_email_attachment_token(
        current_user.user_id,
        email,
        previous_email=current_email,
    )

    public_base_url = (
        os.getenv("PUBLIC_BASE_URL")
        or str(request.base_url).rstrip("/")
    ).rstrip("/")

    verification_link = (
        f"{public_base_url}"
        f"/api/v1/user/email/confirm/{token}"
    )

    username = (
        current_user.username
        or current_user.display_name
        or email.split("@")[0]
    )

    on_user_registered(
        to=[email],
        language=(data.language or "en"),
        username=username,
        activation_link=verification_link,
    )

    return {
        "status": "verification_sent",
        "email": email,
    }


@router.get("/email/confirm/{token}")
def confirm_account_email(
    token: str,
    db: Session = Depends(get_db),
):
    """
    Verify an optional account email without changing CAP access state.
    """
    payload = decode_email_attachment_token(
        token
    )

    try:
        user_id = int(payload.get("sub"))
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail="invalidEmailVerificationToken",
        ) from exc

    email = str(
        payload.get("email") or ""
    ).strip().lower()

    previous_email_raw = payload.get(
        "previous_email"
    )

    expected_previous_email = (
        str(previous_email_raw or "")
        .strip()
        .lower()
        or None
    )

    user = db.scalar(
        select(User).where(
            User.user_id == user_id
        )
    )

    if not user:
        raise HTTPException(
            status_code=404,
            detail="userNotFound",
        )

    current_email = (
        (user.email or "")
        .strip()
        .lower()
        or None
    )

    # A newer verification/change already modified the account.
    # Do not allow this older signed link to overwrite it.
    if current_email != expected_previous_email:
        raise HTTPException(
            status_code=409,
            detail="emailVerificationSuperseded",
        )

    existing = db.scalar(
        select(User).where(
            User.email == email
        )
    )

    if (
        existing
        and existing.user_id
        != user.user_id
    ):
        raise HTTPException(
            status_code=409,
            detail="emailAlreadyInUse",
        )

    user.email = email

    db.add(user)
    db.commit()
    db.refresh(user)

    try:
        on_user_confirmed(
            to=[email],
            language="en",
        )
    except Exception:
        # Ownership is already verified. Failure of the follow-up
        # notification must not roll the verified email back.
        pass

    return RedirectResponse(
        url="/settings?emailConfirmed=true",
    )


# -----------------------------
# Avatar
# -----------------------------
@router.post("/{user_id}/avatar")
async def upload_avatar(
    user_id: int,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if current_user.user_id != user_id:
        raise HTTPException(status_code=403, detail="Not allowed")

    if file.content_type not in ALLOWED_MIME:
        raise HTTPException(status_code=415, detail="Unsupported media type")

    data = await file.read()
    if len(data) > MAX_BYTES:
        raise HTTPException(status_code=413, detail="File too large")

    etag = hashlib.md5(data).hexdigest()

    user = db.scalar(select(User).where(User.user_id == user_id))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.avatar_blob = data
    user.avatar_mime = file.content_type
    user.avatar_etag = etag
    # Canonical URL used by the frontend
    user.avatar = f"/user/{user_id}/avatar"

    db.add(user)
    db.commit()

    return {"url": f"/user/{user_id}/avatar?v={etag}"}


@router.get("/{user_id}/avatar")
def get_avatar(
    user_id: int,
    db: Session = Depends(get_db),
    if_none_match: str | None = Header(default=None),
):
    user = db.scalar(select(User).where(User.user_id == user_id))
    if not user or not getattr(user, "avatar_blob", None) or not getattr(user, "avatar_mime", None):
        raise HTTPException(status_code=404, detail="Avatar not found")

    etag = user.avatar_etag or hashlib.md5(user.avatar_blob).hexdigest()
    headers = {
        "Cache-Control": "public, max-age=86400, immutable",
        "ETag": etag,
    }

    if if_none_match and if_none_match.strip('"') == etag:
        return Response(status_code=304, headers=headers)

    return StreamingResponse(
        iter([user.avatar_blob]),
        media_type=user.avatar_mime,
        headers=headers,
    )


@router.delete("/{user_id}/avatar")
def delete_avatar(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if current_user.user_id != user_id:
        raise HTTPException(status_code=403, detail="Not allowed")

    user = db.scalar(select(User).where(User.user_id == user_id))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.avatar_blob = None
    user.avatar_mime = None
    user.avatar_etag = None
    user.avatar = None
    db.add(user)
    db.commit()
    return {"ok": True}


# -----------------------------
# Account delete (anonymize)
# -----------------------------
def _generate_anonymous_username(user_id: int) -> str:
    # Unique, stable-ish placeholder that satisfies USERNAME_REGEX
    # e.g. deleted_12345_20251031
    ts = datetime.now(UTC).strftime("%Y%m%d")
    return f"deleted_{user_id}_{ts}"


@router.delete("/{user_id}")
def delete_user_account(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Anonymize a user while preserving their content references.
    - Only the user themself may invoke this.
    - Clears PII and authentication fields.
    - Clears avatar blob & URL.
    - Leaves the row to preserve FK integrity.
    """
    if current_user.user_id != user_id:
        raise HTTPException(status_code=403, detail="Not allowed")

    user = db.scalar(select(User).where(User.user_id == user_id))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    anon_username = _generate_anonymous_username(user_id)

    try:
        # PII / credentials
        user.email = None
        user.password_hash = None
        user.google_id = None
        user.wallet_address = None
        user.display_name = None
        user.is_confirmed = False
        user.confirmation_token = None

        # Public profile / settings
        user.username = anon_username
        user.settings = "{}"
        user.refer_id = None

        # Avatar data + URL
        user.avatar = None
        user.avatar_blob = None
        user.avatar_mime = None
        user.avatar_etag = None

        db.add(user)
        db.commit()

        return {"message": "User deleted, content preserved", "username": anon_username}
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500, detail=str(e)
        ) from e
