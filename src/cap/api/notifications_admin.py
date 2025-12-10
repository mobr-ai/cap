# cap/src/cap/api/notifications_admin.py
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session

from cap.database.model import User
from cap.database.session import get_db
from cap.core.auth_dependencies import get_current_admin_user
from cap.services.admin_alerts_service import (
    get_new_user_notification_config,
    update_new_user_notification_config,
    maybe_notify_admins_new_user
)

router = APIRouter(prefix="/api/v1/admin/notifications", tags=["notifications_admin"])


class NewUserNotificationConfigIn(BaseModel):
    enabled: bool
    recipients: List[EmailStr]


class NewUserNotificationConfigOut(BaseModel):
    enabled: bool
    recipients: List[EmailStr]


@router.get("/new_user", response_model=NewUserNotificationConfigOut)
def get_new_user_notifications(
    db: Session = Depends(get_db),
    admin=Depends(get_current_admin_user),
):
    cfg = get_new_user_notification_config(db)
    return NewUserNotificationConfigOut(**cfg)


@router.put("/new_user", response_model=NewUserNotificationConfigOut)
def set_new_user_notifications(
    payload: NewUserNotificationConfigIn,
    db: Session = Depends(get_db),
    admin=Depends(get_current_admin_user),
):
    cfg = update_new_user_notification_config(
        db,
        enabled=payload.enabled,
        recipients=list(payload.recipients),
    )
    return NewUserNotificationConfigOut(**cfg)


@router.post("/test")
def send_test_new_user_notification(
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin_user),
):
    """
    Trigger a test 'new user' notification to the configured recipients.

    It reuses the same pipeline as a real user registration:
    - Reads admin notification settings (enabled + recipient list)
    - Uses the currently logged-in admin as the "fake" new user

    If notifications are disabled or misconfigured, the helper should
    simply no-op; we still return 200 so the frontend can say
    "test triggered, check your inbox".
    """
    try:
        # Use the admin as the fake "new user" and mark the source as a manual test
        maybe_notify_admins_new_user(
            db=db,
            user=admin,
            source="admin-test",
        )
    except Exception as exc:  # pragma: no cover (defensive)
        raise HTTPException(
            status_code=500,
            detail="Error sending test notification.",
        ) from exc

    return {"ok": True}
