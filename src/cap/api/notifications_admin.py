# cap/src/cap/api/notifications_admin.py
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session

from cap.database.model import User
from cap.database.session import get_db
from cap.core.auth_dependencies import get_current_admin_user
from cap.services.admin_alerts_service import (
    # existing
    get_new_user_notification_config,
    update_new_user_notification_config,
    maybe_notify_admins_new_user,
    # NEW (waitlist)
    get_waitlist_notification_config,
    update_waitlist_notification_config,
    maybe_notify_admins_waitlist,
)

router = APIRouter(prefix="/api/v1/admin/notifications", tags=["notifications_admin"])


# ---------- Schemas ----------

class NotificationConfigIn(BaseModel):
    enabled: bool
    recipients: List[EmailStr]


class NotificationConfigOut(BaseModel):
    enabled: bool
    recipients: List[EmailStr]


# ---------- New user notifications (existing) ----------

@router.get("/new_user", response_model=NotificationConfigOut)
def get_new_user_notifications(
    db: Session = Depends(get_db),
    admin=Depends(get_current_admin_user),
):
    cfg = get_new_user_notification_config(db)
    return NotificationConfigOut(**cfg)


@router.put("/new_user", response_model=NotificationConfigOut)
def set_new_user_notifications(
    payload: NotificationConfigIn,
    db: Session = Depends(get_db),
    admin=Depends(get_current_admin_user),
):
    cfg = update_new_user_notification_config(
        db,
        enabled=payload.enabled,
        recipients=list(payload.recipients),
    )
    return NotificationConfigOut(**cfg)


@router.post("/new_user/test")
def send_test_new_user_notification(
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin_user),
):
    """
    Trigger a test 'new user' notification.
    """
    try:
        maybe_notify_admins_new_user(
            db=db,
            user=admin,
            source="admin-test",
        )
    except Exception as exc:  # pragma: no cover
        raise HTTPException(
            status_code=500,
            detail="Error sending test notification.",
        ) from exc

    return {"ok": True}


# ---------- Waitlist notifications (NEW) ----------

@router.get("/waitlist", response_model=NotificationConfigOut)
def get_waitlist_notifications(
    db: Session = Depends(get_db),
    admin=Depends(get_current_admin_user),
):
    cfg = get_waitlist_notification_config(db)
    return NotificationConfigOut(**cfg)


@router.put("/waitlist", response_model=NotificationConfigOut)
def set_waitlist_notifications(
    payload: NotificationConfigIn,
    db: Session = Depends(get_db),
    admin=Depends(get_current_admin_user),
):
    cfg = update_waitlist_notification_config(
        db,
        enabled=payload.enabled,
        recipients=list(payload.recipients),
    )
    return NotificationConfigOut(**cfg)


@router.post("/waitlist/test")
def send_test_waitlist_notification(
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin_user),
):
    """
    Trigger a test 'waitlist registration' notification.

    Uses the currently logged-in admin as a fake waitlist entry.
    """
    try:
        maybe_notify_admins_waitlist(
            db=db,
            email=admin.email,
            ref="admin-test",
            language="en",
            source="admin-test",
        )
    except Exception as exc:  # pragma: no cover
        raise HTTPException(
            status_code=500,
            detail="Error sending waitlist test notification.",
        ) from exc

    return {"ok": True}
