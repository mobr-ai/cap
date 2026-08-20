import os
import re

from dotenv import load_dotenv
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, EmailStr
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from cap.database.model import User
from cap.database.session import get_db
from cap.mailing.event_triggers import on_waiting_list_joined
from cap.services.admin_alerts_service import maybe_notify_admins_waitlist

router = APIRouter(prefix="/api/v1", tags=["waitlist"])

load_dotenv()

PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL")


class WaitIn(BaseModel):
    email: EmailStr
    ref: str | None = None
    language: str | None = "en"
    uid: int | None = None
    wallet: str | None = None


# Keep a simple injection guard
INJECTION_CHARS = set('<>"\';&(){}\\')
EMAIL_REGEX = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")


def _make_referral_link(base_url: str, user_id: int | None) -> str:
    """Build /signup?ref=u<user_id> or plain /signup if absent."""
    if user_id:
        return f"{base_url}/signup?ref=u{user_id}"
    return f"{base_url}/signup"


def _stable_base_url(request: Request) -> str:
    """Prefer PUBLIC_BASE_URL to avoid 0.0.0.0 links; fallback to request base_url."""
    if PUBLIC_BASE_URL:
        return PUBLIC_BASE_URL.rstrip("/")
    return str(request.base_url).rstrip("/")


def _parse_ref(ref: str | None) -> int | None:
    """
    Accepts 'u123', '123', or URLs with '?ref=...'.
    Returns the integer user_id if parseable; else None.
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


def _get_or_create_user(db: Session, email: str, refer_user_id: int | None) -> User:
    """
    Get the user by email, or create it. Since 'refer_id' in db model
    is a plain Integer (no FK), we can set it directly when creating.
    """
    user = db.query(User).filter(User.email == email).first()
    if user:
        return user

    try:
        user = User(email=email, refer_id=refer_user_id)
        db.add(user)
        db.commit()
        db.refresh(user)
        return user
    except IntegrityError:
        db.rollback()
        # If a race created the user in the meantime, fetch and return it
        user = db.query(User).filter(User.email == email).first()
        if user:
            return user
        raise
    except SQLAlchemyError:
        db.rollback()
        raise



@router.post("/wait_list", status_code=status.HTTP_201_CREATED)
def wait_list(
    data: WaitIn,
    request: Request,
    db: Session = Depends(get_db),
):
    """
    CAP public registration is open; the legacy waitlist is retired.

    Existing waitlist rows remain available to administrators as historical
    data, but this endpoint never creates new rows.
    """
    raise HTTPException(
        status_code=status.HTTP_410_GONE,
        detail="waitlistClosed",
    )
