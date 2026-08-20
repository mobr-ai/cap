import hashlib
import os
import secrets
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session

from cap.chains.cardano.auth import verify_cardano_data_signature
from cap.core.security import generate_unique_username, make_access_token
from cap.database.model import CardanoAuthChallenge, User
from cap.database.session import get_db
from cap.services.admin_alerts_service import maybe_notify_admins_new_user

try:
    from cap.mailing.event_triggers import on_wallet_login
except Exception:
    def on_wallet_login(*args, **kwargs): pass


router = APIRouter(prefix="/api/v1", tags=["auth"])


class CardanoIn(BaseModel):
    address: str
    remember_me: bool = True
    language: str | None = "en"
    ref: str | None = ""


class CardanoChallengeIn(BaseModel):
    address: str
    address_hex: str | None = None
    wallet_name: str | None = None
    network_id: int | None = None
    language: str | None = "en"
    ref: str | None = ""


class CardanoVerifyIn(BaseModel):
    address: str
    challenge_id: str
    message: str
    signature: str
    key: str
    remember_me: bool = True
    language: str | None = "en"


def _hex_encode_utf8(value: str) -> str:
    return (value or "").encode("utf-8").hex()


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _format_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat()


def _to_db_naive_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).replace(tzinfo=None)


def _create_or_get_cardano_user(db: Session, address: str) -> User:
    user = db.query(User).filter(User.wallet_address == address).first()
    if user:
        return user

    suffix = int(hashlib.sha256(address.encode()).hexdigest(), 16) % 1_000_000
    username = f"cardano_user{suffix}"

    if db.query(User).filter(User.username == username).first():
        username = generate_unique_username(db, User, preferred=username)

    user = User(
        username=username,
        wallet_address=address,
        display_name=f"{address[:8]}...{address[-5:]}",
        is_confirmed=False,
        is_admin=False,
    )

    db.add(user)
    db.commit()
    db.refresh(user)

    maybe_notify_admins_new_user(db, user, source="cardano")
    return user


def _cardano_auth_response(user: User, *, remember_me: bool, language: str = "en"):
    if not bool(user.is_confirmed):
        return {
            "status": "pending_confirmation",
            "id": user.user_id,
            "wallet_address": user.wallet_address,
        }

    token = make_access_token(str(user.user_id), remember=remember_me)

    if user.email:
        on_wallet_login(
            to=[user.email],
            language=(language or "en"),
            wallet_address=user.wallet_address,
        )

    return {
        "id": user.user_id,
        "username": user.username,
        "wallet_address": user.wallet_address,
        "display_name": user.display_name,
        "email": user.email,
        "avatar": user.avatar,
        "settings": user.settings,
        "is_admin": getattr(user, "is_admin", False),
        "access_token": token,
    }


def _build_cardano_login_message(
    *,
    base_url: str,
    address: str,
    challenge_id: str,
    nonce: str,
    issued_at: datetime,
    expires_at: datetime,
) -> str:
    return "\n".join(
        [
            "CAP Cardano Login",
            "",
            f"Domain: {base_url}",
            f"Address: {address}",
            f"Challenge ID: {challenge_id}",
            f"Nonce: {nonce}",
            f"Issued At: {_format_utc(issued_at)}",
            f"Expires At: {_format_utc(expires_at)}",
            "",
            "Only sign this message if you are logging into CAP.",
        ]
    )


@router.post("/auth/cardano/challenge")
def cardano_auth_challenge(
    data: CardanoChallengeIn,
    request: Request,
    db: Session = Depends(get_db),
):
    address = (data.address or "").strip()
    if not address:
        raise HTTPException(400, detail="missingWalletAddress")

    now = _utcnow()
    expires_at = now + timedelta(minutes=10)
    challenge_id = f"cardano_chal_{secrets.token_urlsafe(24)}"
    nonce = secrets.token_urlsafe(32)

    base_url = os.getenv("PUBLIC_BASE_URL") or str(request.base_url).rstrip("/")
    message = _build_cardano_login_message(
        base_url=base_url.rstrip("/"),
        address=address,
        challenge_id=challenge_id,
        nonce=nonce,
        issued_at=now,
        expires_at=_to_db_naive_utc(expires_at),
    )

    challenge = CardanoAuthChallenge(
        challenge_id=challenge_id,
        wallet_address=address,
        wallet_name=(data.wallet_name or None),
        nonce=nonce,
        message=message,
        message_hex=_hex_encode_utf8(message),
        status="pending",
        created_at=now.replace(tzinfo=None),
        expires_at=_to_db_naive_utc(expires_at),
    )

    db.add(challenge)
    db.commit()

    return {
        "challenge_id": challenge_id,
        "message": message,
        "message_hex": challenge.message_hex,
        "expires_at": _format_utc(expires_at),
    }


@router.post("/auth/cardano/verify")
def cardano_auth_verify(
    data: CardanoVerifyIn,
    db: Session = Depends(get_db),
):
    address = (data.address or "").strip()
    challenge_id = (data.challenge_id or "").strip()

    if not address:
        raise HTTPException(400, detail="missingWalletAddress")
    if not challenge_id:
        raise HTTPException(400, detail="missingChallengeId")

    challenge = (
        db.query(CardanoAuthChallenge)
        .filter(CardanoAuthChallenge.challenge_id == challenge_id)
        .first()
    )

    if not challenge:
        raise HTTPException(404, detail="challengeNotFound")
    if challenge.status != "pending":
        raise HTTPException(400, detail="challengeAlreadyUsed")

    now = _utcnow()
    expires_at = challenge.expires_at
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=UTC)

    if expires_at <= now:
        challenge.status = "expired"
        db.add(challenge)
        db.commit()
        raise HTTPException(400, detail="challengeExpired")

    if challenge.wallet_address != address:
        raise HTTPException(400, detail="walletMismatch")
    if challenge.message != data.message:
        raise HTTPException(400, detail="messageMismatch")

    verification = verify_cardano_data_signature(
        address=address,
        message=challenge.message,
        signature=data.signature,
        key=data.key,
    )

    if not verification.ok:
        raise HTTPException(401, detail=verification.error or "invalidSignature")

    challenge.status = "used"
    challenge.used_at = now.replace(tzinfo=None)
    db.add(challenge)
    db.commit()

    user = _create_or_get_cardano_user(db, address)

    # A successfully verified CIP-8 signature proves control of the
    # Cardano wallet. Public CAP access no longer requires beta approval
    # or an additional email-admission step.
    if not bool(user.is_confirmed):
        user.is_confirmed = True
        user.confirmation_token = None
        db.add(user)
        db.commit()
        db.refresh(user)

    return _cardano_auth_response(
        user,
        remember_me=data.remember_me,
        language=(data.language or "en"),
    )


@router.post("/auth/cardano")
def cardano_auth(data: CardanoIn, db: Session = Depends(get_db)):
    if os.getenv("ALLOW_LEGACY_CARDANO_AUTH", "false").strip().lower() != "true":
        raise HTTPException(410, detail="cardanoSignatureRequired")

    address = (data.address or "").strip()
    if not address:
        raise HTTPException(400, detail="missingWalletAddress")

    user = _create_or_get_cardano_user(db, address)

    return _cardano_auth_response(
        user,
        remember_me=data.remember_me,
        language=(data.language or "en"),
    )
