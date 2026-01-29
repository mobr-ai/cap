# cap/src/cap/api/user_admin.py
import logging
import secrets
import re
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import select, or_, func, delete
from sqlalchemy.exc import IntegrityError

from cap.core.security import generate_unique_username
from cap.database.model import (
    User,
    Dashboard,
    DashboardMetrics,
    QueryMetrics,
    Conversation,
    SharedImage,
)
from cap.database.session import get_db
from cap.core.auth_dependencies import get_current_admin_user
from cap.mailing.event_triggers import (
    on_user_access_granted,
    on_user_access_revoked,
)
from cap.services.admin_alerts_service import (
    maybe_notify_admins_user_confirmed,
)

router = APIRouter(prefix="/api/v1/admin/users", tags=["user_admin"])

APP_URL = "https://cap.mobr.ai"


# ---------- Helpers ----------

def _looks_like_placeholder_username(u: User) -> bool:
    uname = (u.username or "").strip().lower()
    if not uname:
        return True
    # Your observed pattern: "CAP User39"
    if uname.startswith("cap user"):
        return True
    return False

def _preferred_username_from_email(email: str) -> str:
    local = (email.split("@")[0] if email else "").strip().lower()
    local = re.sub(r"[^a-z0-9_]+", "_", local)
    local = re.sub(r"_+", "_", local).strip("_")
    return (local or "user")[:30]

# ---------- Schemas ----------

class AdminFlagUpdate(BaseModel):
    is_admin: bool


class ConfirmedFlagUpdate(BaseModel):
    is_confirmed: bool


class AdminFlagsUpdate(BaseModel):
    is_admin: Optional[bool] = None
    is_confirmed: Optional[bool] = None


def _needs_password_setup(u: User) -> bool:
    return bool(
        getattr(u, "email", None)
        and not getattr(u, "google_id", None)
        and not getattr(u, "password_hash", None)
    )


def _ensure_setup_token(db: Session, u: User) -> str:
    token = (getattr(u, "confirmation_token", None) or "").strip()
    if token:
        return token
    u.confirmation_token = secrets.token_urlsafe(32)
    db.add(u)
    db.commit()
    db.refresh(u)
    return u.confirmation_token


def _user_to_dict(u: User) -> dict:
    return {
        "user_id": u.user_id,
        "email": u.email,
        "username": u.username,
        "wallet_address": u.wallet_address,
        "display_name": u.display_name,
        "is_confirmed": u.is_confirmed,
        "is_admin": getattr(u, "is_admin", False),
        "refer_id": u.refer_id,
        "settings": u.settings,
        "avatar": getattr(u, "avatar", None),
    }


def _generate_anonymous_username(user_id: int) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"deleted_{user_id}_{ts}"


def _is_anonymized(user: User) -> bool:
    return (
        user.email is None
        and user.username is not None
        and user.username.startswith("deleted_")
    )


def _anonymize_user(user: User) -> None:
    anon_username = _generate_anonymous_username(user.user_id)

    user.email = None
    user.password_hash = None
    user.google_id = None
    user.wallet_address = None
    user.display_name = None
    user.is_confirmed = False
    user.confirmation_token = None
    user.is_admin = False

    user.username = anon_username
    user.settings = "{}"
    user.refer_id = None

    user.avatar = None
    user.avatar_blob = None
    user.avatar_mime = None
    user.avatar_etag = None


@router.get("/")
def list_users(
    search: Optional[str] = Query(None, description="Search by email/username/wallet"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin_user),
):
    stmt = select(User)

    if search:
        term = f"%{search.lower()}%"
        stmt = stmt.where(
            or_(
                func.lower(User.email).like(term),
                func.lower(User.username).like(term),
                func.lower(User.wallet_address).like(term),
            )
        )

    count_stmt = stmt.with_only_columns(func.count()).order_by(None)
    total = db.scalar(count_stmt) or 0

    total_users = db.scalar(select(func.count()).select_from(User)) or 0
    total_admins = db.scalar(
        select(func.count()).select_from(User).where(User.is_admin.is_(True))
    ) or 0
    total_confirmed = db.scalar(
        select(func.count()).select_from(User).where(User.is_confirmed.is_(True))
    ) or 0

    stmt = stmt.order_by(User.user_id).limit(limit).offset(offset)
    users = db.scalars(stmt).all()

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": [_user_to_dict(u) for u in users],
        "stats": {
            "total_users": total_users,
            "total_admins": total_admins,
            "total_confirmed": total_confirmed,
            "filtered_total": total,
        },
    }


@router.get("/{user_id}")
def get_user_detail(
    user_id: int,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin_user),
):
    user = db.scalar(select(User).where(User.user_id == user_id))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return _user_to_dict(user)


@router.patch("/{user_id}")
def update_user_admin_flags(
    user_id: int,
    payload: AdminFlagsUpdate,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin_user),
):
    user = db.scalar(select(User).where(User.user_id == user_id))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if user.user_id == admin.user_id and payload.is_admin is False:
        raise HTTPException(status_code=400, detail="You cannot remove your own admin privileges")

    if payload.is_admin is not None:
        user.is_admin = payload.is_admin

    if payload.is_confirmed is not None:
        user.is_confirmed = payload.is_confirmed

    db.add(user)
    db.commit()
    db.refresh(user)

    return _user_to_dict(user)


@router.post("/{user_id}/admin")
def set_user_admin_flag(
    user_id: int,
    payload: AdminFlagUpdate,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin_user),
):
    user = db.scalar(select(User).where(User.user_id == user_id))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if user.user_id == admin.user_id and payload.is_admin is False:
        raise HTTPException(status_code=400, detail="You cannot remove your own admin privileges")

    user.is_admin = payload.is_admin

    db.add(user)
    db.commit()
    db.refresh(user)

    return _user_to_dict(user)


@router.post("/{user_id}/confirmed")
def set_user_confirmed_flag(
    user_id: int,
    payload: ConfirmedFlagUpdate,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin_user),
):
    user = db.scalar(select(User).where(User.user_id == user_id))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    was_confirmed = bool(user.is_confirmed)
    now_confirmed = bool(payload.is_confirmed)

    if was_confirmed == now_confirmed:
        return _user_to_dict(user)

    user.is_confirmed = now_confirmed

    # Normalize username when approving waitlist users
    if now_confirmed and user.email:
        email_local = user.email.split("@")[0]

        # Normalize if missing or placeholder
        if not user.username or user.username.lower().startswith("cap user"):
            user.username = generate_unique_username(
                db,
                User,
                preferred=email_local,
            )

        # Optional: also set display_name if empty
        if not user.display_name:
            user.display_name = email_local

    db.add(user)
    db.commit()
    db.refresh(user)

    setup_url: Optional[str] = None
    if (not was_confirmed) and now_confirmed and _needs_password_setup(user):
        token = _ensure_setup_token(db, user)
        setup_url = f"{APP_URL}/login?state=setpass&token={token}"

    # ----------------------------
    # User-facing notifications
    # ----------------------------
    if user.email:
        if not was_confirmed and now_confirmed:
            on_user_access_granted(
                to=[user.email],
                language="en",
                app_url=APP_URL,
                setup_url=setup_url,
            )
        elif was_confirmed and not now_confirmed:
            on_user_access_revoked(
                to=[user.email],
                language="en",
                app_url=APP_URL,
            )

    # ----------------------------
    # Admin-facing notification
    # ----------------------------
    if not was_confirmed and now_confirmed:
        try:
            maybe_notify_admins_user_confirmed(
                db=db,
                user=user,
                source="user_admin",
            )
        except Exception:
            logging.exception("[admin_alerts] user_confirmed notification failed")

    out = _user_to_dict(user)
    if setup_url:
        out["setup_url"] = setup_url
    return out


@router.delete("/{user_id}")
def admin_delete_user_account(
    user_id: int,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin_user),
):
    if admin.user_id == user_id:
        raise HTTPException(status_code=400, detail="Admins may not delete themselves via this endpoint.")

    user = db.scalar(select(User).where(User.user_id == user_id))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if user.is_admin:
        remaining_admins = db.scalar(
            select(func.count())
            .select_from(User)
            .where(User.is_admin.is_(True))
            .where(User.user_id != user.user_id)
        ) or 0
        if remaining_admins <= 0:
            raise HTTPException(status_code=400, detail="Cannot delete the last remaining admin.")

    def _is_anonymized(u: User) -> bool:
        uname = (u.username or "").strip()
        return (u.email is None) and uname.startswith("deleted_user_")

    try:
        if not _is_anonymized(user):
            ts = int(datetime.now(timezone.utc).timestamp())

            user.email = None
            user.password_hash = None
            user.google_id = None
            user.wallet_address = None

            user.display_name = None
            user.settings = None

            user.avatar_blob = None
            user.avatar_mime = None
            user.avatar_etag = None
            user.avatar = None

            user.is_admin = False
            user.is_confirmed = False
            user.confirmation_token = None

            user.username = f"deleted_user_{user.user_id}_{ts}"

            db.add(user)
            db.commit()
            db.refresh(user)

            return {"status": "anonymized", "user_id": user.user_id}

        db.execute(delete(Conversation).where(Conversation.user_id == user.user_id))
        db.execute(delete(SharedImage).where(SharedImage.user_id == user.user_id))
        db.execute(delete(DashboardMetrics).where(DashboardMetrics.user_id == user.user_id))
        db.execute(delete(Dashboard).where(Dashboard.user_id == user.user_id))
        db.execute(delete(QueryMetrics).where(QueryMetrics.user_id == user.user_id))

        db.delete(user)
        db.commit()

        return {"status": "deleted", "user_id": user_id}

    except IntegrityError as exc:
        db.rollback()
        detail = "Cannot fully delete user because other records still reference this account."
        orig = getattr(exc, "orig", None)
        diag = getattr(orig, "diag", None)
        if diag:
            parts = []
            if getattr(diag, "table_name", None):
                parts.append(f"table={diag.table_name}")
            if getattr(diag, "constraint_name", None):
                parts.append(f"constraint={diag.constraint_name}")
            if parts:
                detail = f"{detail} ({', '.join(parts)})"
        raise HTTPException(status_code=400, detail=detail) from exc

    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(exc)) from exc
