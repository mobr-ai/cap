from __future__ import annotations

import os
import re
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, EmailStr
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.orm import Session

from cap.database.session import get_db
from cap.database.model import User
from cap.mailing.event_triggers import on_waiting_list_joined

router = APIRouter(prefix="/api/v1", tags=["waitlist"])

from dotenv import load_dotenv
load_dotenv()

PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL")  # i.e. "https://cap.mobr.ai"

class WaitIn(BaseModel):
    email: EmailStr
    ref: Optional[str] = None
    language: Optional[str] = "en"

# Keep a simple injection guard
INJECTION_CHARS = set('<>"\';&(){}\\')
EMAIL_REGEX = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")

def _stable_base_url(request: Request) -> str:
    """Prefer PUBLIC_BASE_URL to avoid 0.0.0.0 links; fallback to request base_url."""
    if PUBLIC_BASE_URL:
        return PUBLIC_BASE_URL.rstrip("/")
    return str(request.base_url).rstrip("/")

def _parse_ref(ref: Optional[str]) -> Optional[int]:
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

def _get_or_create_user(db: Session, email: str, refer_user_id: Optional[int]) -> User:
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

def _make_referral_link(base_url: str, user_id: Optional[int]) -> str:
    """Build /signup?ref=u<user_id> or plain /signup if absent."""
    if user_id:
        return f"{base_url}/signup?ref=u{user_id}"
    return f"{base_url}/signup"

@router.post("/wait_list", status_code=status.HTTP_201_CREATED)
def wait_list(data: WaitIn, request: Request, db: Session = Depends(get_db)):
    email = data.email.strip().lower()
    language = (data.language or "en").strip().lower()

    # Basic sanity checks (redundant to EmailStr, kept intentionally)
    if any(c in INJECTION_CHARS for c in email):
        raise HTTPException(status_code=400, detail="Invalid email format")
    if not EMAIL_REGEX.match(email):
        raise HTTPException(status_code=400, detail="Invalid email format")

    # Already on the waitlist?
    existing = db.execute(
        text("SELECT 1 FROM waiting_list WHERE email = :e"),
        {"e": email},
    ).first()
    if existing:
        # Frontend treats 418 as "already on list"
        raise HTTPException(status_code=418, detail="alreadyOnList")

    # Ensure we have a User and attach optional referrer
    refer_user_id = _parse_ref(data.ref)
    try:
        user = _get_or_create_user(db, email=email, refer_user_id=refer_user_id)
    except SQLAlchemyError as e:
        # Don't block waitlist on user-create errors
        print(f"[WAITLIST] user create failed for {email}: {e}")
        user = None

    # Insert on waitlist
    db.execute(
        text("INSERT INTO waiting_list (email, ref, language) VALUES (:e, :r, :l)"),
        {"e": email, "r": data.ref or "", "l": language},
    )
    db.commit()

    # Build referral link and fire-and-forget email
    base_url = _stable_base_url(request)
    referral_link = _make_referral_link(base_url, getattr(user, "user_id", None))

    try:
        on_waiting_list_joined(
            to=[email],
            language=language,
            referral_link=referral_link,
        )
    except Exception as mail_err:
        # Do not fail the API if mailing fails; just log
        print(f"[WAITLIST] Mail trigger failed for {email}: {mail_err}")

    return {"message": "ok"}
