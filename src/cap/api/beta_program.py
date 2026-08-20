import logging
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from cap.core.auth_dependencies import get_current_admin_user
from cap.core.beta_program_config import beta_admin_enabled, beta_program_enabled
from cap.database.model import BetaProgramRegistration, QueryMetrics, User
from cap.database.session import get_db
from cap.mailing.event_triggers import on_beta_program_confirmation, on_beta_program_invitation
from cap.services.admin_alerts_service import maybe_notify_admins_beta_registration

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["beta_program"])

VALID_STATUSES = {
    "new",
    "reviewing",
    "invited",
    "accepted",
    "rejected",
    "archived",
}


class BetaRegistrationIn(BaseModel):
    email: EmailStr
    full_name: str | None = Field(default=None, max_length=120)
    role: str | None = Field(default=None, max_length=80)
    organization: str | None = Field(default=None, max_length=160)
    use_case: str | None = Field(default=None, max_length=2000)
    language: str | None = Field(default="en", max_length=12)
    source: str | None = Field(default="welcome_beta_cta", max_length=80)
    company_url: str | None = Field(default=None, max_length=255)  # honeypot


class BetaQueryOut(BaseModel):
    id: int
    user_id: int | None = None
    beta_registration_id: int | None = None
    beta_email: str | None = None
    beta_name: str | None = None
    beta_status: str | None = None
    user_email: str | None = None
    username: str | None = None
    nl_query: str
    normalized_query: str | None = None
    language: str | None = None
    succeeded: bool | None = None
    complexity_score: int | None = None
    total_latency_ms: int | None = None
    created_at: str | None = None
    conversation_id: int | None = None
    conversation_message_id: int | None = None


class BetaRegistrationOut(BaseModel):
    id: int
    email: EmailStr
    full_name: str | None = None
    role: str | None = None
    organization: str | None = None
    use_case: str | None = None
    language: str = "en"
    source: str = "welcome_beta_cta"
    status: str = "new"
    admin_notes: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    user_id: int | None = None
    username: str | None = None
    display_name: str | None = None
    user_email: str | None = None
    is_confirmed: bool | None = None
    query_count: int = 0
    last_query_at: str | None = None


class BetaRegistrationListOut(BaseModel):
    items: list[BetaRegistrationOut]
    stats: dict[str, Any]
    limit: int
    offset: int


class BetaLatestQueriesOut(BaseModel):
    queries: list[BetaQueryOut]
    limit: int


class BetaRegistrationUpdateIn(BaseModel):
    status: str | None = None
    admin_notes: str | None = Field(default=None, max_length=4000)


class BetaRegistrationPublicOut(BaseModel):
    message: str = "ok"
    status: str = "registered"


class BetaProgramConfigOut(BaseModel):
    program_enabled: bool
    admin_enabled: bool


def _ensure_beta_admin_enabled() -> None:
    if not beta_admin_enabled():
        raise HTTPException(status_code=404, detail="betaProgramAdminDisabled")


def _clean(value: str | None, max_len: int | None = None) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(str(value).strip().split())
    if not cleaned:
        return None
    if max_len and len(cleaned) > max_len:
        cleaned = cleaned[:max_len]
    return cleaned


def _clean_long(value: str | None, max_len: int = 2000) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    if not cleaned:
        return None
    if len(cleaned) > max_len:
        cleaned = cleaned[:max_len]
    return cleaned


def _dt(value) -> str | None:
    if value is None:
        return None
    try:
        return value.isoformat()
    except Exception:
        return str(value)


def _user_maps_for_emails(db: Session, emails: list[str]) -> tuple[dict[str, User], dict[int, dict[str, Any]]]:
    normalized = sorted({(email or "").strip().lower() for email in emails if email})
    if not normalized:
        return {}, {}

    users = db.scalars(select(User).where(func.lower(User.email).in_(normalized))).all()
    by_email = {str(u.email or "").strip().lower(): u for u in users if u.email}
    ids = [int(u.user_id) for u in users if u.user_id is not None]

    if not ids:
        return by_email, {}

    rows = db.execute(
        select(
            QueryMetrics.user_id,
            func.count(QueryMetrics.id),
            func.max(QueryMetrics.created_at),
        )
        .where(QueryMetrics.user_id.in_(ids))
        .group_by(QueryMetrics.user_id)
    ).all()

    activity = {
        int(user_id): {
            "query_count": int(count or 0),
            "last_query_at": _dt(last_query_at),
        }
        for user_id, count, last_query_at in rows
        if user_id is not None
    }
    return by_email, activity


def _to_out(
    item: BetaProgramRegistration,
    *,
    user_by_email: dict[str, User] | None = None,
    activity_by_user_id: dict[int, dict[str, Any]] | None = None,
) -> BetaRegistrationOut:
    email_key = str(item.email or "").strip().lower()
    user = (user_by_email or {}).get(email_key)
    activity = (activity_by_user_id or {}).get(int(user.user_id)) if user else None

    return BetaRegistrationOut(
        id=item.id,
        email=item.email,
        full_name=item.full_name,
        role=item.role,
        organization=item.organization,
        use_case=item.use_case,
        language=item.language or "en",
        source=item.source or "welcome_beta_cta",
        status=item.status or "new",
        admin_notes=item.admin_notes,
        created_at=_dt(item.created_at),
        updated_at=_dt(item.updated_at),
        user_id=int(user.user_id) if user and user.user_id is not None else None,
        username=getattr(user, "username", None) if user else None,
        display_name=getattr(user, "display_name", None) if user else None,
        user_email=getattr(user, "email", None) if user else None,
        is_confirmed=bool(getattr(user, "is_confirmed", False)) if user else None,
        query_count=int((activity or {}).get("query_count", 0)),
        last_query_at=(activity or {}).get("last_query_at"),
    )


def _query_out(metric: QueryMetrics, reg: BetaProgramRegistration | None, user: User | None = None) -> BetaQueryOut:
    return BetaQueryOut(
        id=int(metric.id),
        user_id=int(metric.user_id) if metric.user_id is not None else None,
        beta_registration_id=int(reg.id) if reg and reg.id is not None else None,
        beta_email=getattr(reg, "email", None) if reg else None,
        beta_name=getattr(reg, "full_name", None) if reg else None,
        beta_status=getattr(reg, "status", None) if reg else None,
        user_email=getattr(user, "email", None) if user else None,
        username=getattr(user, "username", None) if user else None,
        nl_query=metric.nl_query,
        normalized_query=getattr(metric, "normalized_query", None),
        language=getattr(metric, "detected_language", None),
        succeeded=getattr(metric, "query_succeeded", None),
        complexity_score=getattr(metric, "complexity_score", None),
        total_latency_ms=getattr(metric, "total_latency_ms", None),
        created_at=_dt(getattr(metric, "created_at", None)),
        conversation_id=None,
        conversation_message_id=None,
    )


def _notify_beta_invitation(item: BetaProgramRegistration) -> None:
    try:
        on_beta_program_invitation(
            to=item.email,
            language=item.language or "en",
            full_name=item.full_name or "",
        )
    except Exception:
        logger.exception("Failed to queue beta invitation email")


def _notify_new_registration(item: BetaProgramRegistration) -> None:
    try:
        on_beta_program_confirmation(
            to=item.email,
            language=item.language or "en",
            full_name=item.full_name or "",
            role=item.role or "",
            organization=item.organization or "",
            use_case=item.use_case or "",
        )
    except Exception:
        logger.exception("Failed to queue beta confirmation email")


@router.get("/admin/beta/config", response_model=BetaProgramConfigOut)
def get_beta_program_config(
    admin: User = Depends(get_current_admin_user),
):
    return BetaProgramConfigOut(
        program_enabled=beta_program_enabled(),
        admin_enabled=beta_admin_enabled(),
    )



@router.post(
    "/beta/register",
    response_model=BetaRegistrationPublicOut,
    status_code=status.HTTP_201_CREATED,
)
def register_beta_interest(
    payload: BetaRegistrationIn,
    db: Session = Depends(get_db),
):
    """
    The CAP closed-beta intake is retired.

    Historical beta registrations remain available through the admin APIs,
    but no new beta applications are accepted.
    """
    raise HTTPException(
        status_code=status.HTTP_410_GONE,
        detail="betaProgramClosed",
    )

@router.get("/admin/beta/registrations", response_model=BetaRegistrationListOut)
def list_beta_registrations(
    search: str | None = Query(None, description="Search by email, name, org, role, or use case"),
    status_filter: str | None = Query(None, alias="status"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin_user),
):
    _ensure_beta_admin_enabled()

    query = select(BetaProgramRegistration)
    count_query = select(func.count()).select_from(BetaProgramRegistration)

    filters = []
    if status_filter:
        normalized_status = status_filter.strip().lower()
        if normalized_status not in VALID_STATUSES:
            raise HTTPException(status_code=400, detail="invalidBetaStatus")
        filters.append(BetaProgramRegistration.status == normalized_status)

    if search and search.strip():
        pattern = f"%{search.strip().lower()}%"
        filters.append(
            or_(
                func.lower(BetaProgramRegistration.email).like(pattern),
                func.lower(BetaProgramRegistration.full_name).like(pattern),
                func.lower(BetaProgramRegistration.organization).like(pattern),
                func.lower(BetaProgramRegistration.role).like(pattern),
                func.lower(BetaProgramRegistration.use_case).like(pattern),
            )
        )

    for f in filters:
        query = query.where(f)
        count_query = count_query.where(f)

    total = db.scalar(select(func.count()).select_from(BetaProgramRegistration)) or 0
    filtered_total = db.scalar(count_query) or 0
    rows = db.scalars(
        query.order_by(BetaProgramRegistration.created_at.desc(), BetaProgramRegistration.id.desc())
        .limit(limit)
        .offset(offset)
    ).all()

    status_rows = db.execute(
        select(BetaProgramRegistration.status, func.count())
        .group_by(BetaProgramRegistration.status)
        .order_by(BetaProgramRegistration.status.asc())
    ).all()

    all_reg_emails = db.scalars(
        select(BetaProgramRegistration.email)
    ).all()

    all_user_by_email, all_activity_by_user_id = _user_maps_for_emails(db, all_reg_emails)

    user_by_email, activity_by_user_id = _user_maps_for_emails(db, [row.email for row in rows])

    linked_user_ids = [int(u.user_id) for u in all_user_by_email.values() if u.user_id is not None]
    total_beta_queries = 0
    if linked_user_ids:
        total_beta_queries = int(
            db.scalar(
                select(func.count(QueryMetrics.id)).where(QueryMetrics.user_id.in_(linked_user_ids))
            )
            or 0
        )

    with_activity = sum(
        1
        for u in all_user_by_email.values()
        if u.user_id is not None and int((all_activity_by_user_id.get(int(u.user_id)) or {}).get("query_count", 0)) > 0
    )

    return BetaRegistrationListOut(
        items=[_to_out(row, user_by_email=user_by_email, activity_by_user_id=activity_by_user_id) for row in rows],
        stats={
            "total": int(total),
            "filtered_total": int(filtered_total),
            "by_status": {str(status): int(count) for status, count in status_rows},
            "linked_users": int(len(linked_user_ids)),
            "with_activity": int(with_activity),
            "query_total": int(total_beta_queries),
        },
        limit=limit,
        offset=offset,
    )


@router.get("/admin/beta/queries", response_model=BetaLatestQueriesOut)
def list_beta_latest_queries(
    limit: int = Query(25, ge=1, le=200),
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin_user),
):
    _ensure_beta_admin_enabled()

    registrations = db.scalars(select(BetaProgramRegistration)).all()
    reg_by_email = {str(r.email or "").strip().lower(): r for r in registrations if r.email}
    if not reg_by_email:
        return BetaLatestQueriesOut(queries=[], limit=limit)

    users = db.scalars(select(User).where(func.lower(User.email).in_(list(reg_by_email.keys())))).all()
    user_by_id = {int(u.user_id): u for u in users if u.user_id is not None}
    reg_by_user_id = {
        int(u.user_id): reg_by_email.get(str(u.email or "").strip().lower())
        for u in users
        if u.user_id is not None
    }
    ids = list(user_by_id.keys())
    if not ids:
        return BetaLatestQueriesOut(queries=[], limit=limit)

    rows = db.scalars(
        select(QueryMetrics)
        .where(QueryMetrics.user_id.in_(ids))
        .order_by(QueryMetrics.created_at.desc(), QueryMetrics.id.desc())
        .limit(limit)
    ).all()

    return BetaLatestQueriesOut(
        queries=[_query_out(q, reg_by_user_id.get(int(q.user_id)), user_by_id.get(int(q.user_id))) for q in rows],
        limit=limit,
    )


@router.patch("/admin/beta/registrations/{registration_id}", response_model=BetaRegistrationOut)
def update_beta_registration(
    registration_id: int,
    payload: BetaRegistrationUpdateIn,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin_user),
):
    _ensure_beta_admin_enabled()

    item = db.get(BetaProgramRegistration, registration_id)
    if not item:
        raise HTTPException(status_code=404, detail="betaRegistrationNotFound")

    if payload.status is not None:
        normalized_status = payload.status.strip().lower()
        if normalized_status not in VALID_STATUSES:
            raise HTTPException(status_code=400, detail="invalidBetaStatus")
        item.status = normalized_status

    if payload.admin_notes is not None:
        item.admin_notes = _clean_long(payload.admin_notes, 4000)

    item.updated_at = datetime.utcnow()

    try:
        db.add(item)
        db.commit()
        db.refresh(item)
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail="betaRegistrationError") from exc

    user_by_email, activity_by_user_id = _user_maps_for_emails(db, [item.email])
    return _to_out(item, user_by_email=user_by_email, activity_by_user_id=activity_by_user_id)

@router.post("/admin/beta/registrations/{registration_id}/invite", response_model=BetaRegistrationOut)
def invite_beta_registration(
    registration_id: int,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin_user),
):
    _ensure_beta_admin_enabled()

    item = db.get(BetaProgramRegistration, registration_id)
    if not item:
        raise HTTPException(status_code=404, detail="betaRegistrationNotFound")

    item.status = "invited"
    item.updated_at = datetime.utcnow()

    try:
        db.add(item)
        db.commit()
        db.refresh(item)
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail="betaRegistrationError") from exc

    _notify_beta_invitation(item)

    user_by_email, activity_by_user_id = _user_maps_for_emails(db, [item.email])
    return _to_out(item, user_by_email=user_by_email, activity_by_user_id=activity_by_user_id)


@router.delete("/admin/beta/registrations/{registration_id}")
def delete_beta_registration(
    registration_id: int,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin_user),
):
    _ensure_beta_admin_enabled()

    item = db.get(BetaProgramRegistration, registration_id)
    if not item:
        raise HTTPException(status_code=404, detail="betaRegistrationNotFound")

    try:
        db.delete(item)
        db.commit()
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail="betaRegistrationError") from exc

    return {"ok": True}

