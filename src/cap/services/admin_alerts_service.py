from __future__ import annotations

from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import select

from cap.database.model import AdminSetting, User
from cap.mailing.event_triggers import (
    on_admin_user_created,
    on_admin_waitlist_created,
)

NEW_USER_CONFIG_KEY = "new_user_notifications"
WAITLIST_CONFIG_KEY = "waitlist_notifications"


def _get_config(db: Session, key: str) -> dict:
    row = db.scalar(select(AdminSetting).where(AdminSetting.key == key))
    if not row or not row.value:
        # Default: disabled, no recipients
        return {"enabled": False, "recipients": []}
    cfg = row.value or {}
    cfg.setdefault("enabled", False)
    cfg.setdefault("recipients", [])
    return cfg


def _set_config(db: Session, key: str, cfg: dict) -> dict:
    row = db.scalar(select(AdminSetting).where(AdminSetting.key == key))
    if not row:
        row = AdminSetting(key=key, value=cfg)
    else:
        row.value = cfg
    db.add(row)
    db.commit()
    db.refresh(row)
    return row.value


def _normalize_recipients(recipients: List[str]) -> List[str]:
    # Normalize recipients: strip, dedupe, drop empties
    norm: List[str] = []
    seen = set()
    for r in recipients:
        r = (r or "").strip()
        if not r or r in seen:
            continue
        seen.add(r)
        norm.append(r)
    return norm


# ---------------------------
# New user notifications
# ---------------------------

def get_new_user_notification_config(db: Session) -> dict:
    return _get_config(db, NEW_USER_CONFIG_KEY)


def update_new_user_notification_config(db: Session, enabled: bool, recipients: List[str]) -> dict:
    cfg = {"enabled": bool(enabled), "recipients": _normalize_recipients(recipients)}
    return _set_config(db, NEW_USER_CONFIG_KEY, cfg)


def maybe_notify_admins_new_user(
    db: Session,
    user: User,
    source: str,
) -> None:
    """
    Call this right after a new User is committed.
    Reads config from admin_setting and, if enabled, fires the mail trigger.
    """
    cfg = _get_config(db, NEW_USER_CONFIG_KEY)
    if not cfg.get("enabled") or not cfg.get("recipients"):
        return

    to_list = cfg["recipients"]
    username = getattr(user, "username", "") or ""
    email = getattr(user, "email", "") or ""

    on_admin_user_created(
        to=to_list,
        language="en",  # keep consistent with existing behavior for now
        new_user_email=email,
        new_user_username=username,
        source=source,
    )


# ---------------------------
# Waitlist notifications (NEW)
# ---------------------------

def get_waitlist_notification_config(db: Session) -> dict:
    return _get_config(db, WAITLIST_CONFIG_KEY)


def update_waitlist_notification_config(db: Session, enabled: bool, recipients: List[str]) -> dict:
    cfg = {"enabled": bool(enabled), "recipients": _normalize_recipients(recipients)}
    return _set_config(db, WAITLIST_CONFIG_KEY, cfg)


def maybe_notify_admins_waitlist(
    db: Session,
    email: str,
    ref: Optional[str] = None,
    language: Optional[str] = None,
    source: str = "waitlist",
) -> None:
    """
    Call this right after a new waitlist entry is committed.

    Reads config from admin_setting and, if enabled, fires the mail trigger.
    """
    cfg = _get_config(db, WAITLIST_CONFIG_KEY)
    if not cfg.get("enabled") or not cfg.get("recipients"):
        return

    to_list = cfg["recipients"]
    email = (email or "").strip()
    ref = (ref or "").strip()
    lang = (language or "en").strip() or "en"

    on_admin_waitlist_created(
        to=to_list,
        language=lang,
        waitlist_email=email,
        waitlist_ref=ref,
        source=source,
    )
