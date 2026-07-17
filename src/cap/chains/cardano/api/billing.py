import logging
import os
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from cap.chains.cardano.payment import get_cardano_payment_verifier
from cap.core.auth_dependencies import get_current_user
from cap.database.model import (
    BillingPaymentAddress,
    BillingPlan,
    BillingPrice,
    PaymentSession,
    User,
    UserBillingPreference,
    UserCreditBalance,
    UserCreditLedger,
    UserEntitlement,
)
from cap.database.session import get_db
from cap.services.billing_access import get_billing_access_state
from cap.services.billing_notifications import is_billing_notification_enabled

router = APIRouter(prefix="/api/v1/billing", tags=["billing"])
logger = logging.getLogger(__name__)

PAYMENT_KIND_PLAN_PURCHASE = "plan_purchase"
PAYMENT_KIND_CREDIT_DEPOSIT = "credit_deposit"
PAYMENT_KIND_SUPPORT_CONTRIBUTION = "support_contribution"

MIN_CREDIT_DEPOSIT_LOVELACE = 1_000_000
MAX_CREDIT_DEPOSIT_LOVELACE = 10_000_000_000
MIN_SUPPORT_CONTRIBUTION_LOVELACE = 1_000_000
MAX_SUPPORT_CONTRIBUTION_LOVELACE = 10_000_000_000
AUTO_RENEWAL_WINDOW_SECONDS = int(os.getenv("BILLING_AUTO_RENEWAL_WINDOW_SECONDS", "3600"))


class CreateCardanoPaymentSessionIn(BaseModel):
    kind: str = PAYMENT_KIND_PLAN_PURCHASE
    plan_code: str | None = "cap_premium_access"
    amount_lovelace: int | None = None


class VerifyCardanoPaymentIn(BaseModel):
    session_id: str
    tx_hash: str


class ActivatePlanFromBalanceIn(BaseModel):
    plan_code: str = "cap_premium_access"


class UpdateBillingPreferencesIn(BaseModel):
    auto_renew_premium_enabled: bool | None = None
    auto_renew_plan_code: str | None = None
    payg_enabled: bool | None = None



def _send_billing_email_safely(trigger_name: str, **kwargs) -> None:
    try:
        from cap.mailing import event_triggers as mailing_events

        trigger = getattr(mailing_events, trigger_name)
        trigger(**kwargs)
    except Exception as exc:
        logger.warning("Billing email trigger %s failed: %s", trigger_name, exc)


def _send_billing_email_if_enabled(
    db: Session,
    event_code: str,
    trigger_name: str,
    *,
    default: bool | None = None,
    **kwargs,
) -> None:
    if not is_billing_notification_enabled(db, event_code, default=default):
        return
    _send_billing_email_safely(trigger_name, **kwargs)

def _utcnow() -> datetime:
    return datetime.now(UTC)


def _to_db_naive_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).replace(tzinfo=None)


def _from_db_naive_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _format_utc(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    fixed = _from_db_naive_utc(dt)
    return fixed.isoformat() if fixed else None


def _network() -> str:
    return os.getenv("CARDANO_NETWORK", "mainnet").strip().lower()


def _active_payment_address(db: Session, network: str) -> BillingPaymentAddress:
    payment_address = db.scalar(
        select(BillingPaymentAddress)
        .where(
            BillingPaymentAddress.network == network,
            BillingPaymentAddress.is_active.is_(True),
        )
        .order_by(BillingPaymentAddress.id.desc())
    )
    if not payment_address:
        raise HTTPException(status_code=404, detail="billingPaymentAddressNotConfigured")
    return payment_address


def _active_plan_bundle(db: Session, plan_code: str, network: str):
    now = _to_db_naive_utc(_utcnow())

    plan = db.scalar(
        select(BillingPlan).where(
            BillingPlan.code == plan_code,
            BillingPlan.is_active.is_(True),
        )
    )
    if not plan:
        raise HTTPException(status_code=404, detail="billingPlanNotFound")

    price = db.scalar(
        select(BillingPrice)
        .where(
            BillingPrice.plan_id == plan.id,
            BillingPrice.network == network,
            BillingPrice.is_active.is_(True),
            BillingPrice.starts_at <= now,
            (BillingPrice.ends_at.is_(None)) | (BillingPrice.ends_at > now),
        )
        .order_by(BillingPrice.starts_at.desc(), BillingPrice.id.desc())
    )
    if not price:
        raise HTTPException(status_code=404, detail="billingPriceNotConfigured")

    payment_address = _active_payment_address(db, network)
    return plan, price, payment_address


def _session_response(session: PaymentSession) -> dict[str, Any]:
    return {
        "session_id": session.session_id,
        "kind": getattr(session, "kind", PAYMENT_KIND_PLAN_PURCHASE),
        "plan_code": session.plan_code_snapshot,
        "entitlement_code": session.entitlement_code_snapshot,
        "network": session.network_snapshot,
        "currency": session.currency_snapshot,
        "amount": session.amount_snapshot,
        "payment_address": session.payment_address_snapshot,
        "duration_days": session.duration_days_snapshot,
        "status": session.status,
        "tx_hash": session.tx_hash,
        "provider": session.provider,
        "expires_at": _format_utc(session.expires_at),
        "paid_at": _format_utc(session.paid_at),
        "created_at": _format_utc(session.created_at),
    }


def _reconcile_pending_payment_sessions(
    db: Session,
    *,
    user: User,
    limit: int = 25,
) -> int:
    sessions = (
        db.query(PaymentSession)
        .filter(
            PaymentSession.user_id == user.user_id,
            PaymentSession.status == "pending",
            PaymentSession.tx_hash.isnot(None),
        )
        .order_by(PaymentSession.created_at.desc(), PaymentSession.id.desc())
        .with_for_update(skip_locked=True)
        .limit(max(1, min(int(limit or 25), 100)))
        .all()
    )

    if not sessions:
        return 0

    verifier = get_cardano_payment_verifier()
    updated = 0

    for session in sessions:
        tx_hash = (session.tx_hash or "").strip()
        if not tx_hash:
            continue

        duplicate_paid = db.scalar(
            select(PaymentSession).where(
                PaymentSession.tx_hash == tx_hash,
                PaymentSession.status == "paid",
                PaymentSession.id != session.id,
            )
        )
        if duplicate_paid:
            session.provider_response = {
                "ok": False,
                "error": "txHashAlreadyUsed",
                "duplicate_payment_session_id": duplicate_paid.session_id,
                "reconciled_at": _format_utc(_utcnow()),
            }
            db.add(session)
            updated += 1
            continue

        try:
            result = verifier.verify_payment_tx(
                tx_hash=tx_hash,
                expected_address=session.payment_address_snapshot,
                expected_lovelace=session.amount_snapshot,
                network=session.network_snapshot,
            )
        except Exception as err:
            session.provider_response = {
                "ok": False,
                "error": "reconciliationException",
                "message": str(err),
                "reconciled_at": _format_utc(_utcnow()),
            }
            db.add(session)
            updated += 1
            continue

        now = _utcnow()
        session.provider = result.provider
        session.provider_response = {
            "ok": result.ok,
            "expected_address": result.expected_address,
            "expected_lovelace": result.expected_lovelace,
            "received_lovelace": result.received_lovelace,
            "error": result.error,
            "reconciled_at": _format_utc(now),
        }

        if result.ok:
            finalized = _finalize_reconciled_payment_session(
                db,
                user=user,
                session=session,
                paid_at=now,
            )
            db.commit()
            db.refresh(session)
            _send_reconciled_payment_email(
                db,
                user=user,
                session=session,
                finalized=finalized,
            )
            updated += 1
            continue

        db.add(session)
        updated += 1

    if updated:
        db.commit()

    return updated


def _grant_entitlement(
    db: Session,
    *,
    user: User,
    session: PaymentSession,
    duration_days: int,
) -> UserEntitlement:
    now = _utcnow()
    starts_at = now

    existing = db.scalar(
        select(UserEntitlement)
        .where(
            UserEntitlement.user_id == user.user_id,
            UserEntitlement.entitlement_code == session.entitlement_code_snapshot,
            UserEntitlement.status == "active",
        )
        .order_by(UserEntitlement.expires_at.desc(), UserEntitlement.id.desc())
    )

    if existing:
        existing_expires = _from_db_naive_utc(existing.expires_at)
        base = existing_expires if existing_expires and existing_expires > now else now
        existing.expires_at = _to_db_naive_utc(base + timedelta(days=int(duration_days)))
        existing.source = "cardano_payment"
        existing.payment_session_id = session.id
        db.add(existing)
        return existing

    entitlement = UserEntitlement(
        user_id=user.user_id,
        entitlement_code=session.entitlement_code_snapshot,
        source="cardano_payment",
        payment_session_id=session.id,
        starts_at=_to_db_naive_utc(starts_at),
        expires_at=_to_db_naive_utc(starts_at + timedelta(days=int(duration_days))),
        status="active",
    )
    db.add(entitlement)
    return entitlement


def _grant_entitlement_from_balance(
    db: Session,
    *,
    user: User,
    entitlement_code: str,
    duration_days: int,
    source: str = "balance_purchase",
) -> UserEntitlement:
    now = _utcnow()
    starts_at = now

    existing = db.scalar(
        select(UserEntitlement)
        .where(
            UserEntitlement.user_id == user.user_id,
            UserEntitlement.entitlement_code == entitlement_code,
            UserEntitlement.status == "active",
        )
        .order_by(UserEntitlement.expires_at.desc(), UserEntitlement.id.desc())
    )

    if existing:
        existing_expires = _from_db_naive_utc(existing.expires_at)
        base = existing_expires if existing_expires and existing_expires > now else now
        existing.expires_at = _to_db_naive_utc(base + timedelta(days=int(duration_days)))
        existing.source = source
        existing.payment_session_id = None
        db.add(existing)
        db.flush()
        return existing

    entitlement = UserEntitlement(
        user_id=user.user_id,
        entitlement_code=entitlement_code,
        source=source,
        payment_session_id=None,
        starts_at=_to_db_naive_utc(starts_at),
        expires_at=_to_db_naive_utc(starts_at + timedelta(days=int(duration_days))),
        status="active",
    )
    db.add(entitlement)
    db.flush()
    return entitlement


def _debit_user_balance_for_entitlement(
    db: Session,
    *,
    user: User,
    amount_lovelace: int,
    reason: str,
    entitlement: UserEntitlement,
    metadata: dict[str, Any] | None = None,
) -> UserCreditBalance:
    balance = db.scalar(
        select(UserCreditBalance)
        .where(
            UserCreditBalance.user_id == user.user_id,
            UserCreditBalance.currency == "lovelace",
        )
        .with_for_update()
    )

    current_balance = int(balance.balance or 0) if balance else 0

    if current_balance < int(amount_lovelace):
        raise HTTPException(
            status_code=402,
            detail={
                "code": "insufficientBalance",
                "balance_lovelace": current_balance,
                "required_lovelace": int(amount_lovelace),
                "missing_lovelace": int(amount_lovelace) - current_balance,
            },
        )

    if balance is None:
        raise HTTPException(
            status_code=402,
            detail={
                "code": "insufficientBalance",
                "balance_lovelace": 0,
                "required_lovelace": int(amount_lovelace),
                "missing_lovelace": int(amount_lovelace),
            },
        )

    balance.balance = current_balance - int(amount_lovelace)
    balance.updated_at = _to_db_naive_utc(_utcnow())
    db.add(balance)
    db.flush()

    ledger = UserCreditLedger(
        user_id=user.user_id,
        currency="lovelace",
        amount=-int(amount_lovelace),
        balance_after=int(balance.balance),
        reason=reason,
        related_entitlement_id=entitlement.id,
        metadata_json=metadata or {},
    )
    db.add(ledger)
    db.flush()

    return balance



def _get_or_create_credit_balance(
    db: Session,
    *,
    user_id: int,
    currency: str = "lovelace",
) -> UserCreditBalance:
    row = db.scalar(
        select(UserCreditBalance).where(
            UserCreditBalance.user_id == user_id,
            UserCreditBalance.currency == currency,
        )
    )
    if row:
        return row

    row = UserCreditBalance(
        user_id=user_id,
        currency=currency,
        balance=0,
        updated_at=_to_db_naive_utc(_utcnow()),
    )
    db.add(row)
    db.flush()
    return row


def _credit_user_balance(
    db: Session,
    *,
    user: User,
    session: PaymentSession,
    amount_lovelace: int,
    reason: str = "deposit",
) -> UserCreditBalance:
    existing_ledger = db.scalar(
        select(UserCreditLedger)
        .where(
            UserCreditLedger.user_id == user.user_id,
            UserCreditLedger.payment_session_id == session.id,
            UserCreditLedger.reason == reason,
        )
        .with_for_update()
    )

    if existing_ledger:
        balance = _get_or_create_credit_balance(
            db,
            user_id=user.user_id,
            currency=session.currency_snapshot,
        )
        logger.warning(
            "Skipping duplicate credit ledger for payment_session_id=%s user_id=%s reason=%s",
            session.id,
            user.user_id,
            reason,
        )
        return balance

    balance = _get_or_create_credit_balance(
        db,
        user_id=user.user_id,
        currency=session.currency_snapshot,
    )

    balance.balance = int(balance.balance or 0) + int(amount_lovelace)
    balance.updated_at = _to_db_naive_utc(_utcnow())
    db.add(balance)
    db.flush()

    ledger = UserCreditLedger(
        user_id=user.user_id,
        currency=session.currency_snapshot,
        amount=int(amount_lovelace),
        balance_after=int(balance.balance),
        reason=reason,
        payment_session_id=session.id,
        metadata_json={
            "session_id": session.session_id,
            "tx_hash": session.tx_hash,
            "network": session.network_snapshot,
        },
    )
    db.add(ledger)
    return balance


def _finalize_reconciled_payment_session(
    db: Session,
    *,
    user: User,
    session: PaymentSession,
    paid_at: datetime,
) -> dict[str, Any]:
    kind = getattr(session, "kind", PAYMENT_KIND_PLAN_PURCHASE)

    session.status = "paid"
    session.paid_at = _to_db_naive_utc(paid_at)
    db.add(session)
    db.flush()

    if kind == PAYMENT_KIND_CREDIT_DEPOSIT:
        balance = _credit_user_balance(
            db,
            user=user,
            session=session,
            amount_lovelace=int(session.amount_snapshot or 0),
            reason="deposit",
        )
        db.flush()
        return {
            "kind": kind,
            "balance": balance,
        }

    if kind == PAYMENT_KIND_SUPPORT_CONTRIBUTION:
        return {
            "kind": kind,
        }

    had_active_entitlement = _latest_premium_entitlement(
        db,
        user=user,
        entitlement_code=session.entitlement_code_snapshot,
    ) is not None

    entitlement = _grant_entitlement(
        db,
        user=user,
        session=session,
        duration_days=int(session.duration_days_snapshot or 0),
    )
    db.flush()

    return {
        "kind": kind,
        "entitlement": entitlement,
        "had_active_entitlement": had_active_entitlement,
    }


def _send_reconciled_payment_email(
    db: Session,
    *,
    user: User,
    session: PaymentSession,
    finalized: dict[str, Any],
) -> None:
    kind = finalized.get("kind")

    if kind == PAYMENT_KIND_CREDIT_DEPOSIT:
        _send_billing_email_if_enabled(
            db,
            "balance_credited",
            "on_billing_balance_credited",
            user=user,
            session=session,
            balance=finalized.get("balance"),
        )
        return

    if kind == PAYMENT_KIND_SUPPORT_CONTRIBUTION:
        _send_billing_email_if_enabled(
            db,
            "support_contribution_confirmed",
            "on_billing_payment_confirmed",
            user=user,
            session=session,
        )
        return

    had_active_entitlement = bool(finalized.get("had_active_entitlement"))
    _send_billing_email_if_enabled(
        db,
        "premium_extended" if had_active_entitlement else "premium_activated",
        "on_billing_premium_extended" if had_active_entitlement else "on_billing_premium_activated",
        user=user,
        entitlement=finalized.get("entitlement"),
        session=session,
    )


def _credit_balance_payload(row: UserCreditBalance | None) -> dict[str, Any]:
    balance = int(row.balance or 0) if row else 0
    return {
        "currency": row.currency if row else "lovelace",
        "balance": balance,
        "balance_lovelace": balance,
        "updated_at": _format_utc(row.updated_at) if row else None,
    }


def _get_or_create_billing_preferences(
    db: Session,
    *,
    user: User,
) -> UserBillingPreference:
    row = db.scalar(
        select(UserBillingPreference).where(
            UserBillingPreference.user_id == user.user_id,
        )
    )

    if row:
        return row

    row = UserBillingPreference(
        user_id=user.user_id,
        auto_renew_premium_enabled=False,
        auto_renew_plan_code="cap_premium_access",
        payg_enabled=False,
        created_at=_to_db_naive_utc(_utcnow()),
        updated_at=_to_db_naive_utc(_utcnow()),
    )
    db.add(row)
    db.flush()
    return row


def _billing_preferences_payload(row: UserBillingPreference) -> dict[str, Any]:
    return {
        "auto_renew_premium_enabled": bool(row.auto_renew_premium_enabled),
        "auto_renew_plan_code": row.auto_renew_plan_code or "cap_premium_access",
        "payg_enabled": bool(row.payg_enabled),
        "created_at": _format_utc(row.created_at),
        "updated_at": _format_utc(row.updated_at),
    }


def _latest_premium_entitlement(
    db: Session,
    *,
    user: User,
    entitlement_code: str,
) -> UserEntitlement | None:
    return db.scalar(
        select(UserEntitlement)
        .where(
            UserEntitlement.user_id == user.user_id,
            UserEntitlement.entitlement_code == entitlement_code,
            UserEntitlement.status == "active",
        )
        .order_by(UserEntitlement.expires_at.desc(), UserEntitlement.id.desc())
    )


def _maybe_auto_renew_premium_from_balance(
    db: Session,
    *,
    user: User,
) -> dict[str, Any]:
    preferences = db.scalar(
        select(UserBillingPreference).where(
            UserBillingPreference.user_id == user.user_id,
        )
    )

    if not preferences or not preferences.auto_renew_premium_enabled:
        return {"attempted": False, "renewed": False, "reason": "disabled"}

    plan_code = (preferences.auto_renew_plan_code or "cap_premium_access").strip() or "cap_premium_access"
    network = _network()
    plan, price, _payment_address = _active_plan_bundle(db, plan_code, network)

    if price.currency != "lovelace":
        return {
            "attempted": False,
            "renewed": False,
            "reason": "unsupported_currency",
            "currency": price.currency,
        }

    now = _utcnow()
    renewal_threshold = now + timedelta(seconds=max(0, AUTO_RENEWAL_WINDOW_SECONDS))

    entitlement = _latest_premium_entitlement(
        db,
        user=user,
        entitlement_code=plan.entitlement_code,
    )

    if entitlement is None:
        return {"attempted": False, "renewed": False, "reason": "no_existing_entitlement"}

    expires_at = _from_db_naive_utc(entitlement.expires_at)

    if expires_at and expires_at > renewal_threshold:
        return {
            "attempted": False,
            "renewed": False,
            "reason": "not_due",
            "expires_at": _format_utc(expires_at),
        }

    balance = db.scalar(
        select(UserCreditBalance)
        .where(
            UserCreditBalance.user_id == user.user_id,
            UserCreditBalance.currency == "lovelace",
        )
        .with_for_update()
    )
    current_balance = int(balance.balance or 0) if balance else 0
    required = int(price.amount or 0)

    if current_balance < required:
        return {
            "attempted": True,
            "renewed": False,
            "reason": "insufficient_balance",
            "balance_lovelace": current_balance,
            "required_lovelace": required,
            "missing_lovelace": required - current_balance,
        }

    renewed_entitlement = _grant_entitlement_from_balance(
        db,
        user=user,
        entitlement_code=plan.entitlement_code,
        duration_days=price.duration_days,
        source="auto_renewal",
    )

    renewed_balance = _debit_user_balance_for_entitlement(
        db,
        user=user,
        amount_lovelace=required,
        reason="auto_renewal",
        entitlement=renewed_entitlement,
        metadata={
            "plan_code": plan.code,
            "entitlement_code": plan.entitlement_code,
            "duration_days": price.duration_days,
            "network": price.network,
            "currency": price.currency,
            "amount_lovelace": required,
            "source": "auto_renewal",
        },
    )

    db.commit()
    db.refresh(renewed_entitlement)
    db.refresh(renewed_balance)

    _send_billing_email_if_enabled(
        db,
        "auto_renew_succeeded",
        "on_billing_auto_renew_succeeded",
        user=user,
        entitlement=renewed_entitlement,
        balance=renewed_balance,
        amount_lovelace=required,
        plan_code=plan.code,
    )

    return {
        "attempted": True,
        "renewed": True,
        "reason": "renewed",
        "entitlement_code": renewed_entitlement.entitlement_code,
        "expires_at": _format_utc(renewed_entitlement.expires_at),
        "balance_lovelace": int(renewed_balance.balance or 0),
    }


@router.get("/plans")
def list_billing_plans(db: Session = Depends(get_db)):
    network = _network()
    now = _to_db_naive_utc(_utcnow())

    rows = (
        db.query(BillingPlan, BillingPrice)
        .join(BillingPrice, BillingPrice.plan_id == BillingPlan.id)
        .filter(
            BillingPlan.is_active.is_(True),
            BillingPrice.is_active.is_(True),
            BillingPrice.network == network,
            BillingPrice.starts_at <= now,
            (BillingPrice.ends_at.is_(None)) | (BillingPrice.ends_at > now),
        )
        .order_by(BillingPlan.id.asc(), BillingPrice.id.desc())
        .all()
    )

    seen: set[str] = set()
    plans = []

    for plan, price in rows:
        if plan.code in seen:
            continue
        seen.add(plan.code)
        plans.append(
            {
                "code": plan.code,
                "name": plan.name,
                "description": plan.description,
                "entitlement_code": plan.entitlement_code,
                "network": price.network,
                "currency": price.currency,
                "amount": price.amount,
                "duration_days": price.duration_days,
            }
        )

    return {"network": network, "plans": plans}



@router.get("/access/me")
def get_my_billing_access(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    auto_renewal = _maybe_auto_renew_premium_from_balance(db, user=current_user)

    state = get_billing_access_state(db, current_user)
    preferences = _get_or_create_billing_preferences(db, user=current_user)

    state["billing_preferences"] = _billing_preferences_payload(preferences)
    state["auto_renewal"] = auto_renewal

    return state


@router.get("/preferences/me")
def get_my_billing_preferences(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    row = _get_or_create_billing_preferences(db, user=current_user)
    db.commit()
    db.refresh(row)
    return {"preferences": _billing_preferences_payload(row)}


@router.put("/preferences/me")
def update_my_billing_preferences(
    data: UpdateBillingPreferencesIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    row = _get_or_create_billing_preferences(db, user=current_user)
    was_auto_renew_enabled = bool(row.auto_renew_premium_enabled)

    if data.auto_renew_premium_enabled is not None:
        row.auto_renew_premium_enabled = bool(data.auto_renew_premium_enabled)

    if data.auto_renew_plan_code is not None:
        plan_code = (data.auto_renew_plan_code or "").strip() or "cap_premium_access"
        row.auto_renew_plan_code = plan_code

    if data.payg_enabled is not None:
        row.payg_enabled = bool(data.payg_enabled)

    row.updated_at = _to_db_naive_utc(_utcnow())
    db.add(row)
    db.commit()
    db.refresh(row)

    is_auto_renew_enabled = bool(row.auto_renew_premium_enabled)
    if is_auto_renew_enabled != was_auto_renew_enabled:
        _send_billing_email_if_enabled(
            db,
            "auto_renew_enabled" if is_auto_renew_enabled else "auto_renew_disabled",
            "on_billing_auto_renew_enabled" if is_auto_renew_enabled else "on_billing_auto_renew_disabled",
            user=current_user,
            plan_code=row.auto_renew_plan_code or "cap_premium_access",
        )

    return {"preferences": _billing_preferences_payload(row)}


@router.get("/balance/me")
def get_my_credit_balance(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    row = db.scalar(
        select(UserCreditBalance).where(
            UserCreditBalance.user_id == current_user.user_id,
            UserCreditBalance.currency == "lovelace",
        )
    )
    return {"balance": _credit_balance_payload(row)}


@router.get("/transactions/me")
def get_my_billing_transactions(
    limit: int = 20,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    safe_limit = max(1, min(int(limit or 20), 100))

    _reconcile_pending_payment_sessions(
        db,
        user=current_user,
        limit=safe_limit,
    )

    ledger_rows = (
        db.query(UserCreditLedger)
        .filter(UserCreditLedger.user_id == current_user.user_id)
        .order_by(UserCreditLedger.created_at.desc(), UserCreditLedger.id.desc())
        .limit(safe_limit)
        .all()
    )

    payment_session_rows = (
        db.query(PaymentSession)
        .filter(
            PaymentSession.user_id == current_user.user_id,
            (
                (PaymentSession.kind == PAYMENT_KIND_SUPPORT_CONTRIBUTION)
                | (PaymentSession.status != "paid")
            ),
        )
        .order_by(PaymentSession.created_at.desc(), PaymentSession.id.desc())
        .limit(safe_limit)
        .all()
    )

    balance = db.scalar(
        select(UserCreditBalance).where(
            UserCreditBalance.user_id == current_user.user_id,
            UserCreditBalance.currency == "lovelace",
        )
    )
    balance_after = int(balance.balance or 0) if balance else 0

    ledger_payment_session_ids = [
        int(row.payment_session_id)
        for row in ledger_rows
        if row.payment_session_id is not None
    ]

    ledger_payment_sessions = {}
    if ledger_payment_session_ids:
        payment_rows = (
            db.query(PaymentSession)
            .filter(
                PaymentSession.user_id == current_user.user_id,
                PaymentSession.id.in_(ledger_payment_session_ids),
            )
            .all()
        )
        ledger_payment_sessions = {int(row.id): row for row in payment_rows}

    transactions = []
    for row in ledger_rows:
        linked_session = (
            ledger_payment_sessions.get(int(row.payment_session_id))
            if row.payment_session_id is not None
            else None
        )

        metadata = dict(row.metadata_json or {})
        if linked_session:
            metadata.update({
                "kind": getattr(linked_session, "kind", None),
                "session_id": linked_session.session_id,
                "tx_hash": linked_session.tx_hash,
                "network": linked_session.network_snapshot,
                "provider": linked_session.provider,
                "paid_at": _format_utc(linked_session.paid_at),
                "expires_at": _format_utc(linked_session.expires_at),
            })

        # For payment-linked ledger entries, the payment reconciliation time
        # is authoritative and is already stored as UTC. Non-payment ledger
        # entries use their explicitly UTC-normalized created_at value.
        event_at = (
            linked_session.paid_at
            if linked_session is not None and linked_session.paid_at is not None
            else row.created_at
        )

        transactions.append({
            "id": f"ledger:{row.id}",
            "currency": row.currency,
            "amount": row.amount,
            "balance_after": row.balance_after,
            "reason": getattr(linked_session, "kind", None) or row.reason,
            "status": linked_session.status if linked_session else "posted",
            "payment_session_id": row.payment_session_id,
            "metadata": metadata,
            "created_at": _format_utc(event_at),
            "_sort_at": _from_db_naive_utc(event_at),
        })

    transactions.extend(
        {
            "id": f"payment:{row.id}",
            "currency": row.currency_snapshot,
            "amount": int(row.amount_snapshot or 0),
            "balance_after": balance_after,
            "reason": getattr(row, "kind", None) or PAYMENT_KIND_SUPPORT_CONTRIBUTION,
            "status": row.status,
            "payment_session_id": row.id,
            "metadata": {
                "kind": getattr(row, "kind", None) or PAYMENT_KIND_SUPPORT_CONTRIBUTION,
                "session_id": row.session_id,
                "tx_hash": row.tx_hash,
                "network": row.network_snapshot,
                "provider": row.provider,
                "paid_at": _format_utc(row.paid_at),
                "expires_at": _format_utc(row.expires_at),
            },
            "created_at": _format_utc(row.paid_at or row.created_at),
            "_sort_at": _from_db_naive_utc(row.paid_at or row.created_at),
        }
        for row in payment_session_rows
    )

    transactions.sort(key=lambda item: item.get("_sort_at") or datetime.min.replace(tzinfo=UTC), reverse=True)

    return {
        "transactions": [
            {key: value for key, value in item.items() if key != "_sort_at"}
            for item in transactions[:safe_limit]
        ]
    }


@router.post("/cardano/session")
def create_cardano_payment_session(
    data: CreateCardanoPaymentSessionIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    network = _network()
    kind = (data.kind or PAYMENT_KIND_PLAN_PURCHASE).strip()

    now = _utcnow()
    provider = os.getenv("CARDANO_PAYMENT_VERIFIER", "blockfrost").strip().lower()

    if kind == PAYMENT_KIND_PLAN_PURCHASE:
        plan_code = (data.plan_code or "cap_premium_access").strip()
        if not plan_code:
            raise HTTPException(status_code=400, detail="missingPlanCode")

        plan, price, payment_address = _active_plan_bundle(db, plan_code, network)

        session = PaymentSession(
            session_id=f"pay_{secrets.token_urlsafe(24)}",
            user_id=current_user.user_id,
            kind=PAYMENT_KIND_PLAN_PURCHASE,
            plan_id=plan.id,
            price_id=price.id,
            payment_address_id=payment_address.id,
            plan_code_snapshot=plan.code,
            entitlement_code_snapshot=plan.entitlement_code,
            network_snapshot=network,
            currency_snapshot=price.currency,
            amount_snapshot=price.amount,
            payment_address_snapshot=payment_address.address,
            duration_days_snapshot=price.duration_days,
            status="pending",
            provider=provider,
            expires_at=_to_db_naive_utc(now + timedelta(minutes=30)),
            created_at=_to_db_naive_utc(now),
        )

    elif kind == PAYMENT_KIND_CREDIT_DEPOSIT:
        amount = int(data.amount_lovelace or 0)

        if amount < MIN_CREDIT_DEPOSIT_LOVELACE:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "creditDepositTooSmall",
                    "minimum_lovelace": MIN_CREDIT_DEPOSIT_LOVELACE,
                },
            )

        if amount > MAX_CREDIT_DEPOSIT_LOVELACE:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "creditDepositTooLarge",
                    "maximum_lovelace": MAX_CREDIT_DEPOSIT_LOVELACE,
                },
            )

        payment_address = _active_payment_address(db, network)

        session = PaymentSession(
            session_id=f"pay_{secrets.token_urlsafe(24)}",
            user_id=current_user.user_id,
            kind=PAYMENT_KIND_CREDIT_DEPOSIT,
            plan_id=None,
            price_id=None,
            payment_address_id=payment_address.id,
            plan_code_snapshot="credit_deposit",
            entitlement_code_snapshot="prepaid_balance",
            network_snapshot=network,
            currency_snapshot="lovelace",
            amount_snapshot=amount,
            payment_address_snapshot=payment_address.address,
            duration_days_snapshot=0,
            status="pending",
            provider=provider,
            expires_at=_to_db_naive_utc(now + timedelta(minutes=30)),
            created_at=_to_db_naive_utc(now),
        )

    elif kind == PAYMENT_KIND_SUPPORT_CONTRIBUTION:
        amount = int(data.amount_lovelace or 0)

        if amount < MIN_SUPPORT_CONTRIBUTION_LOVELACE:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "supportContributionTooSmall",
                    "minimum_lovelace": MIN_SUPPORT_CONTRIBUTION_LOVELACE,
                },
            )

        if amount > MAX_SUPPORT_CONTRIBUTION_LOVELACE:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "supportContributionTooLarge",
                    "maximum_lovelace": MAX_SUPPORT_CONTRIBUTION_LOVELACE,
                },
            )

        payment_address = _active_payment_address(db, network)

        session = PaymentSession(
            session_id=f"pay_{secrets.token_urlsafe(24)}",
            user_id=current_user.user_id,
            kind=PAYMENT_KIND_SUPPORT_CONTRIBUTION,
            plan_id=None,
            price_id=None,
            payment_address_id=payment_address.id,
            plan_code_snapshot="support_contribution",
            entitlement_code_snapshot="support_contribution",
            network_snapshot=network,
            currency_snapshot="lovelace",
            amount_snapshot=amount,
            payment_address_snapshot=payment_address.address,
            duration_days_snapshot=0,
            status="pending",
            provider=provider,
            expires_at=_to_db_naive_utc(now + timedelta(minutes=30)),
            created_at=_to_db_naive_utc(now),
        )

    else:
        raise HTTPException(status_code=400, detail="unsupportedPaymentSessionKind")

    db.add(session)
    db.commit()
    db.refresh(session)

    # Disabled by default in DB settings to avoid noisy multi-email payment flows.
    _send_billing_email_if_enabled(
        db,
        "payment_session_created",
        "on_billing_payment_created",
        user=current_user,
        session=session,
    )

    return _session_response(session)


@router.post("/cardano/verify")
def verify_cardano_payment(
    data: VerifyCardanoPaymentIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    session_id = (data.session_id or "").strip()
    tx_hash = (data.tx_hash or "").strip()

    if not session_id:
        raise HTTPException(status_code=400, detail="missingPaymentSessionId")

    if not tx_hash:
        raise HTTPException(status_code=400, detail="missingTxHash")

    session = db.scalar(
        select(PaymentSession).where(
            PaymentSession.session_id == session_id,
            PaymentSession.user_id == current_user.user_id,
        )
    )
    if not session:
        raise HTTPException(status_code=404, detail="paymentSessionNotFound")

    if session.status == "paid":
        payload = {"payment": _session_response(session), "already_paid": True}
        if getattr(session, "kind", PAYMENT_KIND_PLAN_PURCHASE) == PAYMENT_KIND_CREDIT_DEPOSIT:
            balance = db.scalar(
                select(UserCreditBalance).where(
                    UserCreditBalance.user_id == current_user.user_id,
                    UserCreditBalance.currency == session.currency_snapshot,
                )
            )
            payload["credit_balance"] = _credit_balance_payload(balance)
        return payload

    if session.status != "pending":
        raise HTTPException(status_code=400, detail="paymentSessionNotPending")

    now = _utcnow()
    expires_at = _from_db_naive_utc(session.expires_at)

    if expires_at and expires_at <= now:
        session.status = "expired"
        db.add(session)
        db.commit()
        raise HTTPException(status_code=400, detail="paymentSessionExpired")

    duplicate = db.scalar(
        select(PaymentSession).where(
            PaymentSession.tx_hash == tx_hash,
            PaymentSession.status == "paid",
        )
    )
    if duplicate:
        raise HTTPException(status_code=400, detail="txHashAlreadyUsed")

    verifier = get_cardano_payment_verifier()
    result = verifier.verify_payment_tx(
        tx_hash=tx_hash,
        expected_address=session.payment_address_snapshot,
        expected_lovelace=session.amount_snapshot,
        network=session.network_snapshot,
    )

    session.tx_hash = tx_hash
    session.provider = result.provider
    session.provider_response = {
        "ok": result.ok,
        "expected_address": result.expected_address,
        "expected_lovelace": result.expected_lovelace,
        "received_lovelace": result.received_lovelace,
        "error": result.error,
    }

    if not result.ok:
        db.add(session)
        db.commit()

        if result.error == "txNotFound":
            return JSONResponse(
                status_code=202,
                content={
                    "status": "pending_verification",
                    "code": "txNotFound",
                    "session_id": session.session_id,
                    "tx_hash": tx_hash,
                    "received_lovelace": result.received_lovelace,
                    "expected_lovelace": result.expected_lovelace,
                },
            )

        _send_billing_email_if_enabled(
            db,
            "payment_failed",
            "on_billing_payment_failed",
            user=current_user,
            session=session,
            error_code=result.error or "paymentVerificationFailed",
            received_lovelace=result.received_lovelace,
            expected_lovelace=result.expected_lovelace,
        )

        raise HTTPException(
            status_code=400,
            detail={
                "code": result.error or "paymentVerificationFailed",
                "received_lovelace": result.received_lovelace,
                "expected_lovelace": result.expected_lovelace,
            },
        )

    session.status = "paid"
    session.paid_at = _to_db_naive_utc(now)
    db.add(session)
    db.flush()

    kind = getattr(session, "kind", PAYMENT_KIND_PLAN_PURCHASE)

    if kind == PAYMENT_KIND_CREDIT_DEPOSIT:
        balance = _credit_user_balance(
            db,
            user=current_user,
            session=session,
            amount_lovelace=session.amount_snapshot,
            reason="deposit",
        )

        db.commit()
        db.refresh(session)
        db.refresh(balance)

        # Final/specific email only: do not also send generic payment_confirmed.
        _send_billing_email_if_enabled(
            db,
            "balance_credited",
            "on_billing_balance_credited",
            user=current_user,
            session=session,
            balance=balance,
        )

        return {
            "payment": _session_response(session),
            "credit_balance": _credit_balance_payload(balance),
        }

    if kind == PAYMENT_KIND_SUPPORT_CONTRIBUTION:
        db.commit()
        db.refresh(session)

        _send_billing_email_if_enabled(
            db,
            "support_contribution_confirmed",
            "on_billing_payment_confirmed",
            user=current_user,
            session=session,
        )

        return {
            "payment": _session_response(session),
            "support_contribution": {
                "amount": int(session.amount_snapshot or 0),
                "currency": session.currency_snapshot,
                "tx_hash": session.tx_hash,
            },
        }

    had_active_entitlement = _latest_premium_entitlement(
        db,
        user=current_user,
        entitlement_code=session.entitlement_code_snapshot,
    ) is not None

    entitlement = _grant_entitlement(
        db,
        user=current_user,
        session=session,
        duration_days=session.duration_days_snapshot,
    )

    db.commit()
    db.refresh(session)
    db.refresh(entitlement)

    # Final/specific email only: premium activated/extended already implies payment confirmation.
    _send_billing_email_if_enabled(
        db,
        "premium_extended" if had_active_entitlement else "premium_activated",
        "on_billing_premium_extended" if had_active_entitlement else "on_billing_premium_activated",
        user=current_user,
        entitlement=entitlement,
        session=session,
    )

    return {
        "payment": _session_response(session),
        "entitlement": {
            "entitlement_code": entitlement.entitlement_code,
            "status": entitlement.status,
            "starts_at": _format_utc(entitlement.starts_at),
            "expires_at": _format_utc(entitlement.expires_at),
        },
    }



@router.post("/balance/activate-plan")
def activate_plan_from_balance(
    data: ActivatePlanFromBalanceIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    plan_code = data.plan_code or "cap_premium_access"
    plan, price, _payment_address = _active_plan_bundle(db, plan_code, _network())

    if price.currency != "lovelace":
        raise HTTPException(
            status_code=400,
            detail={
                "code": "unsupportedBalanceCurrency",
                "currency": price.currency,
            },
        )

    had_active_entitlement = _latest_premium_entitlement(
        db,
        user=current_user,
        entitlement_code=plan.entitlement_code,
    ) is not None

    entitlement = _grant_entitlement_from_balance(
        db,
        user=current_user,
        entitlement_code=plan.entitlement_code,
        duration_days=price.duration_days,
    )

    balance = _debit_user_balance_for_entitlement(
        db,
        user=current_user,
        amount_lovelace=price.amount,
        reason="premium_activation",
        entitlement=entitlement,
        metadata={
            "plan_code": plan.code,
            "entitlement_code": plan.entitlement_code,
            "duration_days": price.duration_days,
            "network": price.network,
            "currency": price.currency,
            "amount_lovelace": price.amount,
            "source": "balance",
        },
    )

    db.commit()
    db.refresh(entitlement)
    db.refresh(balance)

    _send_billing_email_if_enabled(
        db,
        "premium_extended" if had_active_entitlement else "premium_activated",
        "on_billing_premium_extended" if had_active_entitlement else "on_billing_premium_activated",
        user=current_user,
        entitlement=entitlement,
        balance=balance,
    )

    return {
        "entitlement": {
            "entitlement_code": entitlement.entitlement_code,
            "source": entitlement.source,
            "status": entitlement.status,
            "starts_at": _format_utc(entitlement.starts_at),
            "expires_at": _format_utc(entitlement.expires_at),
        },
        "credit_balance": _credit_balance_payload(balance),
        "access": get_billing_access_state(db, current_user),
    }


@router.get("/entitlements/me")
def get_my_entitlements(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    now = _to_db_naive_utc(_utcnow())

    rows = (
        db.query(UserEntitlement)
        .filter(
            UserEntitlement.user_id == current_user.user_id,
            UserEntitlement.status == "active",
            UserEntitlement.starts_at <= now,
            UserEntitlement.expires_at > now,
        )
        .order_by(UserEntitlement.expires_at.desc())
        .all()
    )

    return {
        "entitlements": [
            {
                "entitlement_code": row.entitlement_code,
                "source": row.source,
                "status": row.status,
                "starts_at": _format_utc(row.starts_at),
                "expires_at": _format_utc(row.expires_at),
            }
            for row in rows
        ]
    }
