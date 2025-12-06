# cap/src/cap/core/admin_alerts_service.py
from __future__ import annotations

from typing import List
from sqlalchemy.orm import Session
from sqlalchemy import select

from cap.database.model import AdminSetting, User
from cap.mailing.event_triggers import on_admin_user_created


CONFIG_KEY = "new_user_notifications"


def _get_config(db: Session) -> dict:
    row = db.scalar(select(AdminSetting).where(AdminSetting.key == CONFIG_KEY))
    if not row or not row.value:
        # Default: disabled, no recipients
        return {"enabled": False, "recipients": []}
    cfg = row.value or {}
    cfg.setdefault("enabled", False)
    cfg.setdefault("recipients", [])
    return cfg


def _set_config(db: Session, cfg: dict) -> dict:
    row = db.scalar(select(AdminSetting).where(AdminSetting.key == CONFIG_KEY))
    if not row:
        row = AdminSetting(key=CONFIG_KEY, value=cfg)
    else:
        row.value = cfg
    db.add(row)
    db.commit()
    db.refresh(row)
    return row.value


def get_new_user_notification_config(db: Session) -> dict:
    return _get_config(db)


def update_new_user_notification_config(db: Session, enabled: bool, recipients: List[str]) -> dict:
    # Normalize recipients: strip, dedupe, drop empties
    norm = []
    seen = set()
    for r in recipients:
        r = (r or "").strip()
        if not r or r in seen:
            continue
        seen.add(r)
        norm.append(r)

    cfg = {"enabled": bool(enabled), "recipients": norm}
    return _set_config(db, cfg)


def maybe_notify_admins_new_user(
    db: Session,
    user: User,
    source: str,
) -> None:
    """
    Call this right after a new User is committed.
    Reads config from admin_setting and, if enabled, fires the mail trigger.
    """
    cfg = _get_config(db)
    if not cfg.get("enabled") or not cfg.get("recipients"):
        return

    to_list = cfg["recipients"]
    username = getattr(user, "username", "") or ""
    email = getattr(user, "email", "") or ""

    on_admin_user_created(
        to=to_list,
        language="en",  # or take from config later
        new_user_email=email,
        new_user_username=username,
        source=source,
    )
