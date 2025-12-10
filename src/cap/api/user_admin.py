# cap/src/cap/api/user_admin.py
from datetime import datetime, timezone
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import select, or_, func, delete
from sqlalchemy.exc import IntegrityError
from cap.database.model import User, Dashboard, DashboardMetrics, QueryMetrics
from cap.database.session import get_db
from cap.core.auth_dependencies import get_current_admin_user

router = APIRouter(prefix="/api/v1/admin/users", tags=["user_admin"])


# ---------- Schemas ----------

class AdminFlagUpdate(BaseModel):
  # Used by POST /{user_id}/admin
  is_admin: bool


class ConfirmedFlagUpdate(BaseModel):
  # Used by POST /{user_id}/confirmed
  is_confirmed: bool


class AdminFlagsUpdate(BaseModel):
  # Used by PATCH /{user_id} for combined updates
  is_admin: Optional[bool] = None
  is_confirmed: Optional[bool] = None


def _user_to_dict(u: User) -> dict:
    # Keep this minimal & consistent with what you expose elsewhere
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
    # Use timezone-aware UTC timestamp (recommended replacement for utcnow())
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"deleted_{user_id}_{ts}"


def _is_anonymized(user: User) -> bool:
    """
    Heuristic to detect anonymized users.
    """
    return (
        user.email is None
        and user.username is not None
        and user.username.startswith("deleted_")
    )


def _anonymize_user(user: User) -> None:
    """
    Clear PII and auth data but keep the row for FK integrity.
    Mirrors the behavior of delete_user_account in cap/api/user.py.
    """
    anon_username = _generate_anonymous_username(user.user_id)

    # PII / credentials
    user.email = None
    user.password_hash = None
    user.google_id = None
    user.wallet_address = None
    user.display_name = None
    user.is_confirmed = False
    user.confirmation_token = None
    user.is_admin = False  # deleted users must not remain admins

    # Public profile / settings
    user.username = anon_username
    user.settings = "{}"
    user.refer_id = None

    # Avatar data + URL
    user.avatar = None
    user.avatar_blob = None
    user.avatar_mime = None
    user.avatar_etag = None


# ---------- Endpoints: Users ----------

@router.get("/")
def list_users(
    search: Optional[str] = Query(None, description="Search by email/username/wallet"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin_user),
):
    """
    List users with basic pagination and search.

    Only accessible to admins.
    """
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

    # total count for pagination (respecting search)
    count_stmt = stmt.with_only_columns(func.count()).order_by(None)
    total = db.scalar(count_stmt) or 0

    # global stats (ignore search, look at all users)
    total_users = db.scalar(
        select(func.count()).select_from(User)
    ) or 0

    total_admins = db.scalar(
        select(func.count())
        .select_from(User)
        .where(User.is_admin.is_(True))
    ) or 0

    total_confirmed = db.scalar(
        select(func.count())
        .select_from(User)
        .where(User.is_confirmed.is_(True))
    ) or 0

    stmt = stmt.order_by(User.user_id).limit(limit).offset(offset)
    users = db.scalars(stmt).all()

    return {
        "total": total,  # total matching the search (for pagination)
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
    """
    Get a single user's details.

    Only accessible to admins.
    """
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
    """
    Update admin-related flags for a user (is_admin, is_confirmed).

    Guardrails:
    - Prevent an admin from removing their own admin flag.
    """
    user = db.scalar(select(User).where(User.user_id == user_id))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Prevent self-demotion
    if user.user_id == admin.user_id and payload.is_admin is False:
        raise HTTPException(
            status_code=400,
            detail="You cannot remove your own admin privileges",
        )

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
    """
    Set or unset the is_admin flag for a user.

    Guardrails:
    - Prevent an admin from removing their own admin flag.
    """
    user = db.scalar(select(User).where(User.user_id == user_id))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Prevent self-demotion
    if user.user_id == admin.user_id and payload.is_admin is False:
        raise HTTPException(
            status_code=400,
            detail="You cannot remove your own admin privileges",
        )

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
    """
    Set or unset the is_confirmed flag for a user.
    """
    user = db.scalar(select(User).where(User.user_id == user_id))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.is_confirmed = payload.is_confirmed

    db.add(user)
    db.commit()
    db.refresh(user)

    return _user_to_dict(user)


@router.delete("/{user_id}")
def admin_delete_user_account(
    user_id: int,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin_user),
):
    """
    Admin-triggered deletion flow for a user.

    Two-stage behavior:
    1) First deletion: anonymize user but keep row & content.
    2) Second deletion (on already anonymized user): hard delete the user row.

    Guardrails:
    - Admins may not delete themselves here (use self-delete flow instead).
    - Admins may not delete the last remaining admin.
    """
    user = db.scalar(select(User).where(User.user_id == user_id))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if user.user_id == admin.user_id:
        raise HTTPException(
            status_code=400,
            detail="Use your own account deletion flow instead",
        )

    # Guard: do not delete (or anonymize) the last remaining admin
    total_admins = db.scalar(
        select(func.count())
        .select_from(User)
        .where(User.is_admin.is_(True))
    ) or 0

    if user.is_admin and total_admins <= 1:
        raise HTTPException(
            status_code=400,
            detail="Cannot delete the last remaining admin",
        )

    try:
        if not _is_anonymized(user):
            # First stage: anonymize but keep the row & content
            _anonymize_user(user)
            db.add(user)
            db.commit()
            db.refresh(user)
            return {
                "status": "anonymized",
                "user": _user_to_dict(user),
            }

        # Second stage: already anonymized -> hard delete related data and user

        # 1) Delete dashboard-related data (dashboard_items and dashboard_metrics
        #     will cascade from these FKs)
        db.execute(
            delete(DashboardMetrics).where(
                DashboardMetrics.user_id == user.user_id
            )
        )
        db.execute(
            delete(Dashboard).where(
                Dashboard.user_id == user.user_id
            )
        )

        # 2) Delete query metrics tied to this user
        db.execute(
            delete(QueryMetrics).where(
                QueryMetrics.user_id == user.user_id
            )
        )

        # 3) Finally delete the user row
        db.delete(user)
        db.commit()

        return {
            "status": "deleted",
        }

    except IntegrityError as exc:
        db.rollback()
        # If any other FK we didn't cover exists, return a clean error instead of raw SQL
        raise HTTPException(
            status_code=400,
            detail="Cannot fully delete user because other records still reference this account.",
        ) from exc

    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(exc)) from exc


    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(exc)) from exc

