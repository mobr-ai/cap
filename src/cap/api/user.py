# cap/src/cap/api/user.py
import hashlib
import os
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Response, Header
from starlette.responses import StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import select
from cap.database.session import get_db
from cap.database.model import User
from cap.api.auth_dependencies import get_current_user

router = APIRouter(prefix="/user", tags=["user"])

# -----------------------------
# User endpoints 
# -----------------------------
ALLOWED_MIME = {"image/png", "image/jpeg", "image/webp"}
MAX_BYTES = 2 * 1024 * 1024  # 2 MB

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
    if_none_match: Optional[str] = Header(default=None),
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

def _generate_anonymous_username(user_id: int) -> str:
    # Unique, stable-ish placeholder that satisfies your USERNAME_REGEX
    # e.g. deleted_12345_20251031
    ts = datetime.utcnow().strftime("%Y%m%d")
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

    # If other tables have non-nullable FKs pointing to user, handle them here
    # (e.g., set to None or a "system user"). Since only User model is provided,
    # we restrict to anonymizing this row.

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

        # Note: JWT invalidation is handled client-side by clearing storage.
        # If we ever maintain a server-side token denylist, add the current JTI there.

        return {"message": "User deleted, content preserved", "username": anon_username}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
