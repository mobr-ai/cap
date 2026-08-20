"""Beta program feature flags.

Runtime flags:
- CAP_BETA_PROGRAM_ENABLED=false disables public beta registration.
- CAP_BETA_ADMIN_ENABLED=false disables admin beta-management APIs.
"""
import os

_FALSE_VALUES = {"0", "false", "no", "off", "disabled"}


def _flag(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return bool(default)

    return str(raw).strip().lower() not in _FALSE_VALUES


def beta_program_enabled() -> bool:
    return _flag("CAP_BETA_PROGRAM_ENABLED", False)


def beta_admin_enabled() -> bool:
    return _flag("CAP_BETA_ADMIN_ENABLED", True)
