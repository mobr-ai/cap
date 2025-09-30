# cap/src/cap/mailing/event_triggers.py
"""
Mailing triggers for CAP.

Thin wrappers around send_async_email so API/business code stays tidy.
As CAP grows, more triggers should be added here (alerts, dashboards, shares, etc.).
"""

from __future__ import annotations

import os
from typing import Iterable, Mapping, Any

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
    return os.getenv("PUBLIC_BASE_URL", "https://cap.mobr.ai").rstrip("/")

def _app_url() -> str:
    """Canonical app entry point (used in templates)."""
    # Adjust route to dashboard/home in the future
    return f"{_public_base_url()}/signup"

def _send(template: str, to: Iterable[str] | str, language: str | None, ctx: Mapping[str, Any]) -> None:
    """Internal helper to reduce repetition."""
    send_async_email(
        to_email=to,
        language=_lang_or_default(language),
        template_name=template,
        context=dict(ctx),  # ensure a plain dict
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
      - app_url (str)  # used by translations to link back to CAP
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
    provider: str = "google",
) -> None:
    """
    Optional: OAuth login notification.

    Template: oauth_login
    Vars:
      - provider (str)
    """
    _send("oauth_login", to, language, ctx={"provider": provider})

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
    _send("wallet_login", to, language, ctx={"wallet_address": wallet_address})
