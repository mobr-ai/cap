# cap/src/cap/api/waitlist_admin.py

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, EmailStr
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from cap.core.auth_dependencies import get_current_admin_user
from cap.database.model import User
from cap.database.session import get_db

# Mailing triggers
from cap.mailing.event_triggers import (
  on_waitlist_promoted,
  # If you prefer to also send a generic "access granted" email,
  # you can enable it below (but beware of double emails).
  # on_user_access_granted,
)

# Admin alerts (config-driven)
from cap.services.admin_alerts_service import (
  maybe_notify_admins_user_confirmed,
)

router = APIRouter(prefix="/api/v1/admin/wait_list", tags=["waitlist_admin"])


# -----------------------------
# Schemas
# -----------------------------

class CreateUserFromWaitlistIn(BaseModel):
  confirm: bool = False


class WaitlistItemOut(BaseModel):
  email: EmailStr
  ref: Optional[str] = ""
  language: Optional[str] = "en"
  created_at: Optional[str] = None  # stringified for safety across drivers


class WaitlistStatsOut(BaseModel):
  total_waiting: int = 0
  filtered_total: int = 0


class WaitlistListOut(BaseModel):
  items: List[WaitlistItemOut]
  stats: WaitlistStatsOut
  limit: int
  offset: int


class CreateUserFromWaitlistOut(BaseModel):
  status: str  # "created" | "exists" | "updated"
  removed_from_waitlist: bool
  user: Dict[str, Any]


# -----------------------------
# Helpers
# -----------------------------

EMAIL_REGEX = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")
USERNAME_ALLOWED_RE = re.compile(r"[^a-z0-9_]+")


def _normalize_email(raw: str) -> str:
  email = (raw or "").strip().lower()
  if not email or not EMAIL_REGEX.match(email):
    raise HTTPException(status_code=400, detail="Invalid email format")
  return email


def _parse_ref_to_user_id(ref: Optional[str]) -> Optional[int]:
  """
  Accepts 'u123', '123', or URLs with '?ref=...'
  Returns integer user_id if parseable; else None.
  Mirrors cap/api/waitlist.py behavior.
  """
  if not ref:
    return None

  if "ref=" in ref:
    try:
      ref = ref.split("ref=", 1)[1].split("&", 1)[0]
    except Exception:
      pass

  ref = ref.strip()
  if ref.startswith("u") and ref[1:].isdigit():
    return int(ref[1:])
  if ref.isdigit():
    return int(ref)
  return None


def _user_to_dict(u: User) -> Dict[str, Any]:
  return {
    "user_id": u.user_id,
    "email": u.email,
    "username": u.username,
    "wallet_address": u.wallet_address,
    "display_name": u.display_name,
    "is_confirmed": u.is_confirmed,
    "is_admin": getattr(u, "is_admin", False),
    "refer_id": u.refer_id,
  }


def _email_to_username_base(email: str) -> str:
  """
  Derive a stable username base from email local-part:
  - lower
  - replace invalid chars with underscore
  - collapse underscores
  - trim to a reasonable size
  """
  local = (email.split("@", 1)[0] if email else "").strip().lower()
  local = USERNAME_ALLOWED_RE.sub("_", local)
  local = re.sub(r"_+", "_", local).strip("_")
  if not local:
    local = "user"
  # keep reasonably short (avoid UI overflow / DB limits)
  return local[:24]


def _ensure_username(db: Session, user: User) -> bool:
  """
  Ensure `user.username` is set and unique.
  Returns True if it changed the user.
  """
  current = (getattr(user, "username", None) or "").strip()
  if current:
    return False

  base = _email_to_username_base(getattr(user, "email", "") or "")
  candidate = base

  # Try a few suffixes to avoid collisions.
  # We purposely avoid random here for determinism in dev/test.
  for i in range(0, 200):
    if i > 0:
      candidate = f"{base}{i}"
      candidate = candidate[:30]  # keep within typical limits

    exists = db.scalar(select(User.user_id).where(User.username == candidate))
    if not exists:
      user.username = candidate
      return True

  # Fallback if something pathological happens
  user.username = f"{base}{user.user_id or ''}"[:30]
  return True


def _get_or_create_user(db: Session, email: str, refer_user_id: Optional[int]) -> tuple[User, str]:
  """
  Returns (user, status): status in {"exists","created","updated"}.
  """
  user = db.scalar(select(User).where(User.email == email))
  if user:
    changed = False
    if refer_user_id and not getattr(user, "refer_id", None):
      user.refer_id = refer_user_id
      changed = True

    # backfill username if missing
    if _ensure_username(db, user):
      changed = True

    if changed:
      db.add(user)
      db.commit()
      db.refresh(user)
      return user, "updated"
    return user, "exists"

  try:
    user = User(email=email, refer_id=refer_user_id)

    # assign username for newly created waitlist users
    _ensure_username(db, user)

    db.add(user)
    db.commit()
    db.refresh(user)

    # In case username fallback relies on user_id, ensure it’s set
    if (getattr(user, "username", None) or "").strip() == "user":
      if _ensure_username(db, user):
        db.add(user)
        db.commit()
        db.refresh(user)

    return user, "created"
  except IntegrityError:
    db.rollback()
    # race condition
    user = db.scalar(select(User).where(User.email == email))
    if not user:
      raise

    # even in race, ensure username is not blank
    changed = _ensure_username(db, user)
    if changed:
      db.add(user)
      db.commit()
      db.refresh(user)

    return user, "exists"
  except SQLAlchemyError:
    db.rollback()
    raise


def _stringify_dt(dt_val: Any) -> Optional[str]:
  if dt_val is None:
    return None
  try:
    return dt_val.isoformat()
  except Exception:
    return str(dt_val)


# -----------------------------
# Endpoints
# -----------------------------

@router.get("/", response_model=WaitlistListOut)
def list_waitlist(
  search: Optional[str] = Query(None, description="Search by email/ref"),
  limit: int = Query(50, ge=1, le=200),
  offset: int = Query(0, ge=0),
  db: Session = Depends(get_db),
  admin: User = Depends(get_current_admin_user),
):
  """
  List waitlist entries with pagination + search.

  Uses raw SQL because waiting_list is not mapped as an ORM model.
  """
  # total (no filter)
  total_waiting = db.execute(text("SELECT COUNT(*) FROM waiting_list")).scalar() or 0

  where_sql = ""
  params: Dict[str, Any] = {"limit": limit, "offset": offset}

  if search and search.strip():
    s = f"%{search.strip().lower()}%"
    where_sql = "WHERE LOWER(email) LIKE :s OR LOWER(ref) LIKE :s"
    params["s"] = s

  filtered_total = (
    db.execute(text(f"SELECT COUNT(*) FROM waiting_list {where_sql}"), params).scalar()
    or 0
  )

  # Try to read created_at if it exists; fallback if it doesn't.
  try:
    rows = db.execute(
      text(
        f"""
        SELECT email, ref, language, created_at
        FROM waiting_list
        {where_sql}
        ORDER BY created_at DESC NULLS LAST, email ASC
        LIMIT :limit OFFSET :offset
        """
      ),
      params,
    ).mappings().all()
    has_created_at = True
  except Exception:
    rows = db.execute(
      text(
        f"""
        SELECT email, ref, language
        FROM waiting_list
        {where_sql}
        ORDER BY email ASC
        LIMIT :limit OFFSET :offset
        """
      ),
      params,
    ).mappings().all()
    has_created_at = False

  items: List[WaitlistItemOut] = []
  for r in rows:
    items.append(
      WaitlistItemOut(
        email=r.get("email"),
        ref=r.get("ref") or "",
        language=r.get("language") or "en",
        created_at=_stringify_dt(r.get("created_at")) if has_created_at else None,
      )
    )

  return WaitlistListOut(
    items=items,
    stats=WaitlistStatsOut(
      total_waiting=int(total_waiting),
      filtered_total=int(filtered_total),
    ),
    limit=limit,
    offset=offset,
  )


@router.delete("/{email}", status_code=status.HTTP_200_OK)
def delete_waitlist_entry(
  email: str,
  db: Session = Depends(get_db),
  admin: User = Depends(get_current_admin_user),
):
  normalized = _normalize_email(email)

  try:
    res = db.execute(
      text("DELETE FROM waiting_list WHERE email = :e"),
      {"e": normalized},
    )
    db.commit()
  except SQLAlchemyError as exc:
    db.rollback()
    raise HTTPException(status_code=500, detail=str(exc)) from exc

  if res.rowcount == 0:
    raise HTTPException(status_code=404, detail="Waitlist entry not found")

  return {"status": "deleted", "email": normalized}


@router.post("/{email}/create_user", response_model=CreateUserFromWaitlistOut)
def create_user_from_waitlist(
  email: str,
  payload: CreateUserFromWaitlistIn,
  db: Session = Depends(get_db),
  admin: User = Depends(get_current_admin_user),
):
  normalized = _normalize_email(email)

  row = db.execute(
    text("SELECT email, ref, language FROM waiting_list WHERE email = :e"),
    {"e": normalized},
  ).mappings().first()

  if not row:
    raise HTTPException(status_code=404, detail="Waitlist entry not found")

  ref = (row.get("ref") or "").strip()
  lang = (row.get("language") or "en").strip() or "en"
  refer_user_id = _parse_ref_to_user_id(ref)

  try:
    user, status_str = _get_or_create_user(db, normalized, refer_user_id=refer_user_id)

    # Track transition for notifications (avoid duplicates)
    was_confirmed = bool(user.is_confirmed)
    will_confirm = bool(payload.confirm)

    # optionally confirm
    if will_confirm and not was_confirmed:
      user.is_confirmed = True
      db.add(user)
      db.commit()
      db.refresh(user)

      # ----------------------------
      # User-facing: waitlist approved
      # ----------------------------
      if user.email:
        on_waitlist_promoted(
          to=[user.email],
          language=lang,
          app_url=None,
        )

      # ----------------------------
      # Admin-facing: "user confirmed" (config bucket)
      # ----------------------------
      maybe_notify_admins_user_confirmed(
        db=db,
        user=user,
        source="waitlist_admin",
      )

    # remove from waitlist (always attempt, regardless of confirm)
    res = db.execute(
      text("DELETE FROM waiting_list WHERE email = :e"),
      {"e": normalized},
    )
    db.commit()

    removed = res.rowcount > 0

  except SQLAlchemyError as exc:
    db.rollback()
    raise HTTPException(status_code=500, detail=str(exc)) from exc

  return CreateUserFromWaitlistOut(
    status=status_str,
    removed_from_waitlist=removed,
    user=_user_to_dict(user),
  )
