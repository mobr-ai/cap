import hashlib
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Response, Header
from starlette.responses import StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import select
from cap.database.session import get_db
from cap.database.model import User
from cap.api.auth_dependencies import get_current_user

router = APIRouter(prefix="/user", tags=["user-avatar"])

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
    # Optional: set a canonical API URL so the frontend can just load it
    user.avatar = f"/user/{user_id}/avatar"

    db.add(user)
    db.commit()

    # Respond with a public URL to the avatar
    return {"url": f"/user/{user_id}/avatar?v={etag}"}

@router.get("/{user_id}/avatar")
def get_avatar(
    user_id: int,
    db: Session = Depends(get_db),
    if_none_match: str | None = Header(default=None),
):
    user = db.scalar(select(User).where(User.user_id == user_id))
    if not user or not user.avatar_blob or not user.avatar_mime:
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
