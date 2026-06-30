import os
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from cap.database.model import TelegramGuestUsagePeriod

TELEGRAM_GUEST_FEATURE_CODE = "telegram_guest_nl_query"
DEFAULT_TELEGRAM_GUEST_DAILY_LIMIT = 3


class TelegramGuestLimitDenied(Exception):
    def __init__(self, payload: dict[str, Any]):
        super().__init__(payload.get("code", "telegramGuestDailyLimitReached"))
        self.payload = payload


def _guest_daily_limit() -> int:
    return max(
        0,
        int(os.getenv("TELEGRAM_GUEST_DAILY_QUERY_LIMIT", str(DEFAULT_TELEGRAM_GUEST_DAILY_LIMIT))),
    )


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
    fixed = _from_db_naive_utc(dt)
    return fixed.isoformat() if fixed else None


def _daily_window(now: datetime) -> tuple[datetime, datetime]:
    start = now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end


def _format_wait(seconds: int | None) -> str:
    if seconds is None:
        return "later"

    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, _ = divmod(remainder, 60)

    if hours > 0:
        return f"{hours}h {minutes}m"
    if minutes > 0:
        return f"{minutes}m"
    return "less than 1m"


def _usage_row(
    db: Session,
    *,
    telegram_user_id: int,
    period_start: datetime,
    period_end: datetime,
    limit_count: int,
    create: bool,
    lock: bool = False,
) -> TelegramGuestUsagePeriod | None:
    stmt = select(TelegramGuestUsagePeriod).where(
        TelegramGuestUsagePeriod.telegram_user_id == telegram_user_id,
        TelegramGuestUsagePeriod.feature_code == TELEGRAM_GUEST_FEATURE_CODE,
        TelegramGuestUsagePeriod.period_start == _to_db_naive_utc(period_start),
        TelegramGuestUsagePeriod.period_end == _to_db_naive_utc(period_end),
    )

    if lock:
        stmt = stmt.with_for_update()

    row = db.scalar(stmt)

    if row or not create:
        return row

    now = _utcnow()
    row = TelegramGuestUsagePeriod(
        telegram_user_id=telegram_user_id,
        feature_code=TELEGRAM_GUEST_FEATURE_CODE,
        period_start=_to_db_naive_utc(period_start),
        period_end=_to_db_naive_utc(period_end),
        used_count=0,
        limit_count=limit_count,
        created_at=_to_db_naive_utc(now),
        updated_at=_to_db_naive_utc(now),
    )
    db.add(row)
    db.flush()
    return row


def get_telegram_guest_access_state(
    db: Session,
    *,
    telegram_user_id: int,
    create_usage_period: bool = False,
) -> dict[str, Any]:
    now = _utcnow()
    limit_count = _guest_daily_limit()
    period_start, period_end = _daily_window(now)

    usage = _usage_row(
        db,
        telegram_user_id=telegram_user_id,
        period_start=period_start,
        period_end=period_end,
        limit_count=limit_count,
        create=create_usage_period,
    )

    used_count = int(usage.used_count or 0) if usage else 0
    remaining = max(0, limit_count - used_count)

    blocked = remaining <= 0
    seconds_until_next_query = None
    next_query_at = None

    if blocked:
        seconds_until_next_query = int(max(1, round((period_end - now).total_seconds())))
        next_query_at = period_end

    return {
        "feature_code": TELEGRAM_GUEST_FEATURE_CODE,
        "can_query": not blocked,
        "access_mode": "telegram_guest" if not blocked else "blocked",
        "blocked_reason": "telegramGuestDailyLimitReached" if blocked else None,
        "telegram_user_id": telegram_user_id,
        "daily_query_limit": limit_count,
        "daily_query_used": used_count,
        "daily_query_remaining": remaining,
        "period_start": _format_utc(period_start),
        "period_end": _format_utc(period_end),
        "next_query_at": _format_utc(next_query_at),
        "seconds_until_next_query": seconds_until_next_query,
        "wait_text": _format_wait(seconds_until_next_query),
    }


def check_telegram_guest_query_access(
    db: Session,
    *,
    telegram_user_id: int,
) -> dict[str, Any]:
    state = get_telegram_guest_access_state(
        db,
        telegram_user_id=telegram_user_id,
        create_usage_period=False,
    )

    if not state["can_query"]:
        raise TelegramGuestLimitDenied(
            {
                "code": "telegramGuestDailyLimitReached",
                "message": f"Daily guest limit reached. Try again in {state['wait_text']}. Ready to go beyond guest access? Apply for CAP’s closed beta and help shape the platform: https://cap.mobr.ai/beta",
                "access": state,
            }
        )

    return state


def consume_telegram_guest_query_success(
    db: Session,
    *,
    telegram_user_id: int,
) -> dict[str, Any]:
    now = _utcnow()
    limit_count = _guest_daily_limit()
    period_start, period_end = _daily_window(now)

    usage = _usage_row(
        db,
        telegram_user_id=telegram_user_id,
        period_start=period_start,
        period_end=period_end,
        limit_count=limit_count,
        create=True,
        lock=True,
    )

    if usage is None:
        raise RuntimeError("Failed to create or load Telegram guest usage period.")

    used_count = int(usage.used_count or 0)

    if used_count >= limit_count:
        state = get_telegram_guest_access_state(
            db,
            telegram_user_id=telegram_user_id,
            create_usage_period=False,
        )
        raise TelegramGuestLimitDenied(
            {
                "code": "telegramGuestDailyLimitReached",
                "message": f"Daily guest limit reached. Try again in {state['wait_text']}. Ready to go beyond guest access? Apply for CAP’s closed beta and help shape the platform: https://cap.mobr.ai/beta",
                "access": state,
            }
        )

    usage.limit_count = limit_count
    usage.used_count = used_count + 1
    usage.updated_at = _to_db_naive_utc(now)
    db.add(usage)
    db.commit()

    return get_telegram_guest_access_state(
        db,
        telegram_user_id=telegram_user_id,
        create_usage_period=False,
    )
