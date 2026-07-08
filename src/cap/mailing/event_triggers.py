"""
Mailing triggers.

Thin wrappers around send_async_email so API/business code stays tidy.
"""
import os
from collections.abc import Iterable, Mapping
from typing import Any

from .email_service import send_async_email

# -------------------------
# Helpers / defaults
# -------------------------

def _lang_or_default(lang: str | None) -> str:
    """Normalize language to 'en' or 'pt' (extend as needed)."""
    if not lang:
        return "en"
    lang = lang.lower()
    if lang.startswith("pt"):
        return "pt"
    return "en"


def _public_base_url() -> str:
    """Read PUBLIC_BASE_URL once, fallback to production hostname."""
    return os.getenv("PUBLIC_BASE_URL").rstrip("/")


def _app_url() -> str:
    """
    Canonical app entry point (used in templates).
    Keep it stable; templates may link to app_url even in pre-alpha.
    """
    return f"{_public_base_url()}"


def _send(
    template: str,
    to: Iterable[str] | str,
    language: str | None,
    ctx: Mapping[str, Any],
    template_type: str | None = None,
) -> None:
    payload = dict(ctx)
    if template_type:
        payload["template_type"] = template_type

    send_async_email(
        to_email=to,
        language=_lang_or_default(language),
        template_name=template,
        context=payload,
    )


# -------------------------
# Auth & Waitlist triggers
# -------------------------

def on_waiting_list_joined(
    to: Iterable[str] | str,
    language: str | None,
    referral_link: str,
    app_url: str | None = None,
) -> None:
    """
    'Thanks for joining the waitlist' + personal referral link.

    Template: waiting_list_confirmation
    Vars:
      - referral_link (str)
      - app_url (str)
    """
    _send(
        template="waiting_list_confirmation",
        to=to,
        language=language,
        ctx={
            "referral_link": referral_link,
            "app_url": app_url or _app_url(),
        },
    )


def on_user_registered(
    to: Iterable[str] | str,
    language: str | None,
    username: str,
    activation_link: str,
) -> None:
    """
    'Confirm your email' for new sign-ups.

    Template: user_registration
    Vars:
      - username (str)
      - activation_link (str)
    """
    _send(
        template="user_registration",
        to=to,
        language=language,
        ctx={"username": username, "activation_link": activation_link},
    )


def on_confirmation_resent(
    to: Iterable[str] | str,
    language: str | None = "en",
) -> None:
    """
    Optional: 'We re-sent your confirmation' notice.

    Template: user_confirmation_resent
    """
    _send("user_confirmation_resent", to, language, ctx={})


def on_user_confirmed(
    to: Iterable[str] | str,
    language: str | None = "en",
) -> None:
    """
    Optional: 'Your email is confirmed' notice.

    Template: user_confirmed
    """
    _send("user_confirmed", to, language, ctx={})


def on_oauth_login(
    to: Iterable[str] | str,
    language: str | None = "en",
    provider: str = "Google",
) -> None:
    """
    Optional: OAuth login notification.

    Template: oauth_login
    Vars:
      - provider (str)
    """
    _send("oauth_login", to, language, ctx={"provider": provider}, template_type="auth")


def on_wallet_login(
    to: Iterable[str] | str,
    language: str | None = "en",
    wallet_address: str = "",
) -> None:
    """
    Optional: wallet login notification.

    Template: wallet_login
    Vars:
      - wallet_address (str)
    """
    _send("wallet_login", to, language, ctx={"wallet_address": wallet_address}, template_type="security")


def on_admin_user_created(
    to: Iterable[str] | str,
    language: str | None = "en",
    new_user_email: str | None = "",
    new_user_username: str | None = "",
    source: str | None = "password",
) -> None:
    """
    Notify admins that a new user account has been created.

    Template: admin_user_created
    Vars:
      - new_user_email (str)
      - new_user_username (str)
      - source (str)
      - app_url (str)
    """
    _send(
        template="admin_user_created",
        to=to,
        language=language,
        ctx={
            "new_user_email": new_user_email or "",
            "new_user_username": new_user_username or "",
            "source": source or "password",
            "app_url": _app_url(),
        },
        template_type="admin",
    )


def on_admin_waitlist_created(
    to: Iterable[str] | str,
    language: str | None = "en",
    waitlist_email: str | None = "",
    waitlist_ref: str | None = "",
    source: str | None = "waitlist",
) -> None:
    """
    Notify admins that a new waitlist entry has been created.

    Template: admin_waitlist_created
    Vars:
      - waitlist_email (str)
      - waitlist_ref (str)
      - source (str)
      - app_url (str)
    """
    _send(
        template="admin_waitlist_created",
        to=to,
        language=language,
        ctx={
            "waitlist_email": waitlist_email or "",
            "waitlist_ref": waitlist_ref or "",
            "source": source or "waitlist",
            "app_url": _app_url(),
        },
        template_type="admin",
    )


def on_user_email_verified(
    to: Iterable[str] | str,
    language: str | None = "en",
    app_url: str | None = None,
) -> None:
    """
    User clicked the email verification link successfully.
    Meaning: email ownership verified (NOT necessarily access granted).

    Template: user_email_verified
    Vars:
      - app_url (str)
    """
    _send(
        template="user_email_verified",
        to=to,
        language=language,
        ctx={"app_url": app_url or _app_url()},
    )


def on_user_access_granted(
    to: Iterable[str] | str,
    language: str | None = "en",
    app_url: str | None = None,
    setup_url: str | None = None,
) -> None:
    """
    Admin approved the user (or system granted access).

    Template: user_access_granted
    Vars:
      - app_url (str)
      - setup_url (str|None)  # if present, user must set password first
    """
    _send(
        template="user_access_granted",
        to=to,
        language=language,
        ctx={
            "app_url": app_url or _app_url(),
            "setup_url": (setup_url or "").strip() or None,
        },
    )


def on_waitlist_promoted(
    to: Iterable[str] | str,
    language: str | None = "en",
    app_url: str | None = None,
    setup_url: str | None = None,
) -> None:
    """
    User moved from waitlist to active access.

    Template: waitlist_promoted
    Vars:
      - app_url (str)
      - setup_url (str|None)  # if present, user must set password first
    """
    _send(
        template="waitlist_promoted",
        to=to,
        language=language,
        ctx={
            "app_url": app_url or _app_url(),
            "setup_url": (setup_url or "").strip() or None,
        },
    )


def on_admin_user_confirmed(
    *,
    to: list[str],
    language: str,
    confirmed_user_email: str = "",
    confirmed_user_username: str = "",
    confirmed_user_id: int | None = None,
    source: str = "admin",
    **_ignored,
) -> None:
    """
    Notify admins that an admin confirmed/approved a user.

    Template: admin_user_confirmed
    Vars:
      - confirmed_user_email (str)
      - confirmed_user_username (str)
      - confirmed_user_id (int|None)
      - source (str)
    """
    _send(
        template="admin_user_confirmed",
        to=to,
        language=language,
        ctx={
            "confirmed_user_email": confirmed_user_email,
            "confirmed_user_username": confirmed_user_username,
            "confirmed_user_id": confirmed_user_id,
            "source": source,
        },
        template_type="admin",
    )


def on_user_access_revoked(
    to: Iterable[str] | str,
    language: str | None = "en",
    support_email: str | None = "",
    app_url: str | None = None,
) -> None:
    """
    Optional future: access disabled/revoked.

    Template: user_access_revoked
    Vars:
      - support_email (str)
      - app_url (str)
    """
    _send(
        template="user_access_revoked",
        to=to,
        language=language,
        ctx={
            "support_email": support_email or "",
            "app_url": app_url or _app_url(),
        },
        template_type="security",
    )




# -------------------------
# Beta program & admin activity triggers
# -------------------------

def _admin_app_url(tab: str = "beta-program") -> str:
    return f"{_app_url()}/admin?tab={tab}"


def on_beta_program_confirmation(
    *,
    to: Iterable[str] | str,
    language: str | None = "en",
    full_name: str | None = "",
    role: str | None = "",
    organization: str | None = "",
    use_case: str | None = "",
) -> None:
    """Confirm a public closed beta registration."""
    _send(
        template="beta_program_confirmation",
        to=to,
        language=language,
        ctx={
            "full_name": full_name or "",
            "role": role or "",
            "organization": organization or "",
            "use_case": use_case or "",
            "app_url": _app_url(),
        },
        template_type="beta",
    )


def on_admin_beta_registration_created(
    *,
    to: Iterable[str] | str,
    language: str | None = "en",
    beta_email: str | None = "",
    beta_name: str | None = "",
    beta_role: str | None = "",
    beta_organization: str | None = "",
    beta_use_case: str | None = "",
    source: str | None = "beta_program",
) -> None:
    """Notify admins about a new beta registration."""
    _send(
        template="admin_beta_registration_created",
        to=to,
        language=language,
        ctx={
            "beta_email": beta_email or "",
            "beta_name": beta_name or "",
            "beta_role": beta_role or "",
            "beta_organization": beta_organization or "",
            "beta_use_case": beta_use_case or "",
            "source": source or "beta_program",
            "app_url": _admin_app_url("beta-program"),
        },
        template_type="admin",
    )


def on_admin_query_created(
    *,
    to: Iterable[str] | str,
    language: str | None = "en",
    query_id: int | None = None,
    user_id: int | None = None,
    user_email: str | None = "",
    username: str | None = "",
    nl_query: str | None = "",
    detected_language: str | None = "",
    succeeded: bool | None = None,
    total_latency_ms: int | None = None,
    complexity_score: int | None = None,
) -> None:
    """Notify admins about a new user query."""
    _send(
        template="admin_query_created",
        to=to,
        language=language,
        ctx={
            "query_id": query_id,
            "user_id": user_id,
            "user_email": user_email or "",
            "username": username or "",
            "nl_query": nl_query or "",
            "detected_language": detected_language or "",
            "succeeded": bool(succeeded),
            "total_latency_ms": total_latency_ms,
            "complexity_score": complexity_score,
            "app_url": _admin_app_url("metrics"),
        },
        template_type="admin",
    )


# -------------------------
# Billing triggers
# -------------------------

def _user_to_email(user: Any) -> str | None:
    value = getattr(user, "email", None)
    return (value or "").strip() or None


def _user_language(user: Any, fallback: str | None = "en") -> str:
    settings = getattr(user, "settings", None)
    if isinstance(settings, str) and settings.strip():
        try:
            import json

            settings = json.loads(settings)
        except Exception:
            settings = None

    if isinstance(settings, Mapping):
        for key in ("language", "lang", "locale", "i18nextLng"):
            value = settings.get(key)
            if isinstance(value, str) and value.strip():
                return _lang_or_default(value)

    return _lang_or_default(fallback)


def _lovelace_to_ada(value: Any) -> str:
    try:
        ada = int(value or 0) / 1_000_000
    except Exception:
        ada = 0
    return f"{ada:,.6f}".rstrip("0").rstrip(".")


def _short(value: Any, prefix: int = 12, suffix: int = 8) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if len(raw) <= prefix + suffix + 3:
        return raw
    return f"{raw[:prefix]}...{raw[-suffix:]}"


def _billing_app_url(path: str = "/settings?section=billing") -> str:
    base = _app_url()
    return f"{base}{path if path.startswith('/') else '/' + path}"


def _session_billing_context(session: Any | None = None) -> dict[str, Any]:
    if session is None:
        return {}

    amount = getattr(session, "amount_snapshot", None)
    return {
        "amount_ada": _lovelace_to_ada(amount) if amount is not None else "",
        "plan_code": getattr(session, "plan_code_snapshot", "") or "",
        "duration_days": getattr(session, "duration_days_snapshot", "") or "",
        "network": getattr(session, "network_snapshot", "") or "",
        "tx_hash_short": _short(getattr(session, "tx_hash", "")),
        "payment_address_short": _short(getattr(session, "payment_address_snapshot", ""), 14, 10),
        "expires_at": str(getattr(session, "expires_at", "") or ""),
    }


def _entitlement_billing_context(entitlement: Any | None = None) -> dict[str, Any]:
    if entitlement is None:
        return {}

    return {
        "plan_code": getattr(entitlement, "entitlement_code", "") or "",
        "expires_at": str(getattr(entitlement, "expires_at", "") or ""),
    }


def _balance_billing_context(balance: Any | None = None) -> dict[str, Any]:
    if balance is None:
        return {}

    return {
        "balance_ada": _lovelace_to_ada(getattr(balance, "balance", 0)),
    }


def _send_billing_user_event(
    *,
    user: Any,
    template: str,
    ctx: Mapping[str, Any] | None = None,
    language: str | None = None,
) -> None:
    to = _user_to_email(user)
    if not to:
        return

    payload = {
        "cta_url": _billing_app_url(),
        **dict(ctx or {}),
    }
    _send(
        template=template,
        to=to,
        language=language or _user_language(user),
        ctx=payload,
        template_type="billing",
    )


def on_billing_payment_created(
    *,
    user: Any,
    session: Any,
    language: str | None = None,
) -> None:
    _send_billing_user_event(
        user=user,
        template="billing_payment_created",
        language=language,
        ctx=_session_billing_context(session),
    )


def on_billing_payment_confirmed(
    *,
    user: Any,
    session: Any,
    language: str | None = None,
) -> None:
    _send_billing_user_event(
        user=user,
        template="billing_payment_confirmed",
        language=language,
        ctx=_session_billing_context(session),
    )



def on_billing_payment_failed(
    *,
    user: Any,
    session: Any,
    error_code: str | None = None,
    received_lovelace: int | None = None,
    expected_lovelace: int | None = None,
    language: str | None = None,
) -> None:
    detail = error_code or "paymentVerificationFailed"
    if expected_lovelace is not None:
        detail = f"{detail} · Expected {_lovelace_to_ada(expected_lovelace)} ADA"
        if received_lovelace is not None:
            detail = f"{detail} · Received {_lovelace_to_ada(received_lovelace)} ADA"

    _send_billing_user_event(
        user=user,
        template="billing_payment_failed",
        language=language,
        ctx={
            **_session_billing_context(session),
            "detail": detail,
        },
    )

def on_billing_balance_credited(
    *,
    user: Any,
    session: Any,
    balance: Any,
    language: str | None = None,
) -> None:
    _send_billing_user_event(
        user=user,
        template="billing_balance_credited",
        language=language,
        ctx={
            **_session_billing_context(session),
            **_balance_billing_context(balance),
        },
    )


def on_billing_premium_activated(
    *,
    user: Any,
    entitlement: Any,
    session: Any | None = None,
    balance: Any | None = None,
    language: str | None = None,
) -> None:
    _send_billing_user_event(
        user=user,
        template="billing_premium_activated",
        language=language,
        ctx={
            **_session_billing_context(session),
            **_entitlement_billing_context(entitlement),
            **_balance_billing_context(balance),
        },
    )


def on_billing_premium_extended(
    *,
    user: Any,
    entitlement: Any,
    session: Any | None = None,
    balance: Any | None = None,
    language: str | None = None,
) -> None:
    _send_billing_user_event(
        user=user,
        template="billing_premium_extended",
        language=language,
        ctx={
            **_session_billing_context(session),
            **_entitlement_billing_context(entitlement),
            **_balance_billing_context(balance),
        },
    )


def on_billing_auto_renew_enabled(
    *,
    user: Any,
    plan_code: str = "cap_premium_access",
    language: str | None = None,
) -> None:
    _send_billing_user_event(
        user=user,
        template="billing_auto_renew_enabled",
        language=language,
        ctx={"plan_code": plan_code},
    )


def on_billing_auto_renew_disabled(
    *,
    user: Any,
    plan_code: str = "cap_premium_access",
    language: str | None = None,
) -> None:
    _send_billing_user_event(
        user=user,
        template="billing_auto_renew_disabled",
        language=language,
        ctx={"plan_code": plan_code},
    )


def on_billing_auto_renew_succeeded(
    *,
    user: Any,
    entitlement: Any,
    balance: Any,
    amount_lovelace: int | None = None,
    plan_code: str = "cap_premium_access",
    language: str | None = None,
) -> None:
    _send_billing_user_event(
        user=user,
        template="billing_auto_renew_succeeded",
        language=language,
        ctx={
            "amount_ada": _lovelace_to_ada(amount_lovelace),
            "plan_code": plan_code,
            **_entitlement_billing_context(entitlement),
            **_balance_billing_context(balance),
        },
    )


def on_billing_auto_renew_failed(
    *,
    user: Any,
    balance_lovelace: int,
    required_lovelace: int,
    missing_lovelace: int,
    plan_code: str = "cap_premium_access",
    language: str | None = None,
) -> None:
    _send_billing_user_event(
        user=user,
        template="billing_auto_renew_failed",
        language=language,
        ctx={
            "amount_ada": _lovelace_to_ada(required_lovelace),
            "balance_ada": _lovelace_to_ada(balance_lovelace),
            "plan_code": plan_code,
            "detail": f"Missing {_lovelace_to_ada(missing_lovelace)} ADA.",
        },
    )


def on_admin_billing_premium_granted(
    *,
    user: Any,
    entitlement: Any,
    days: int,
    note: str | None = None,
    language: str | None = None,
) -> None:
    _send_billing_user_event(
        user=user,
        template="billing_admin_premium_granted",
        language=language,
        ctx={
            "duration_days": days,
            "detail": note or "",
            **_entitlement_billing_context(entitlement),
        },
    )


def on_admin_billing_premium_revoked(
    *,
    user: Any,
    note: str | None = None,
    language: str | None = None,
) -> None:
    _send_billing_user_event(
        user=user,
        template="billing_admin_premium_revoked",
        language=language,
        ctx={"detail": note or ""},
    )


def on_admin_billing_balance_adjusted(
    *,
    user: Any,
    amount_lovelace: int,
    balance: Any,
    reason: str,
    note: str | None = None,
    language: str | None = None,
) -> None:
    _send_billing_user_event(
        user=user,
        template="billing_admin_balance_adjusted",
        language=language,
        ctx={
            "amount_ada": _lovelace_to_ada(amount_lovelace),
            "plan_code": reason,
            "detail": note or "",
            **_balance_billing_context(balance),
        },
    )

def on_beta_program_invitation(
    *,
    to: Iterable[str] | str,
    language: str | None = "en",
    full_name: str | None = "",
    beta_url: str | None = None,
) -> None:
    """Invite a beta registrant to try the CAP closed beta."""
    _send(
        template="beta_program_invitation",
        to=to,
        language=language,
        ctx={
            "full_name": full_name or "",
            "beta_url": beta_url or f"{_app_url()}/login",
            "app_url": _app_url(),
        },
        template_type="beta",
    )


def on_admin_beta_query_created(
    *,
    to: Iterable[str] | str,
    language: str | None = "en",
    query_id: int | None = None,
    user_id: int | None = None,
    user_email: str | None = "",
    username: str | None = "",
    beta_registration_id: int | None = None,
    beta_status: str | None = "",
    nl_query: str | None = "",
    detected_language: str | None = "",
    succeeded: bool | None = None,
    total_latency_ms: int | None = None,
    complexity_score: int | None = None,
) -> None:
    """Notify admins about a new query from a registered beta-program user."""
    _send(
        template="admin_beta_query_created",
        to=to,
        language=language,
        ctx={
            "query_id": query_id,
            "user_id": user_id,
            "user_email": user_email or "",
            "username": username or "",
            "beta_registration_id": beta_registration_id,
            "beta_status": beta_status or "",
            "nl_query": nl_query or "",
            "detected_language": detected_language or "",
            "succeeded": succeeded,
            "total_latency_ms": total_latency_ms,
            "complexity_score": complexity_score,
            "app_url": _app_url(),
        },
        template_type="admin",
    )
