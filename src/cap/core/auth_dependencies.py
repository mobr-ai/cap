# cap/core/auth_dependencies.py
from typing import Optional, Callable, Dict, Any
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from sqlalchemy import select

from cap.database.session import get_db
from cap.database.model import User

try:
    from cap.core.security import decode_access_token as _decode_token
except Exception:
    _decode_token = None

# Reusable bearer scheme
bearer_scheme = HTTPBearer(auto_error=False)


def _extract_token(request: Request, creds: Optional[HTTPAuthorizationCredentials]) -> Optional[str]:
    """Get access token from Authorization header, cookie, or query string."""
    if creds and creds.scheme.lower() == "bearer" and creds.credentials:
        return creds.credentials

    # Cookie fallback (if you set it on login)
    cookie_token = request.cookies.get("access_token")
    if cookie_token:
        return cookie_token

    # Dev fallback: ?access_token=...
    q = request.query_params.get("access_token")
    if q:
        return q

    return None


def _decode(token: str) -> Dict[str, Any]:
    """Decode/verify a JWT using whatever helper your project exposes."""
    if not token:
        raise HTTPException(
            status_code=401,
            detail="notAuthenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if _decode_token:
        return _decode_token(token)

    # If neither helper exists, raise a helpful error
    raise HTTPException(
        status_code=500,
        detail="Server misconfiguration: no token decoder available",
    )


def _extract_user_id(payload: Dict[str, Any]) -> int:
    """
    Pull the user id from common JWT shapes:
    - {"sub": "123"}  (string)
    - {"uid": 123}    (int)
    """
    uid = payload.get("sub") or payload.get("uid") or payload.get("user_id")
    if uid is None:
        raise HTTPException(401, detail="invalidToken")

    try:
        return int(uid)
    except Exception:
        raise HTTPException(401, detail="invalidToken")


def _current_user_factory(require_confirmed: bool = True) -> Callable:
    """
    Returns a dependency that:
    - extracts & verifies the JWT
    - loads the User from DB
    - (optionally) enforces is_confirmed
    """
    def _dep(
        request: Request,
        db: Session = Depends(get_db),
        creds: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
    ) -> User:
        token = _extract_token(request, creds)
        if not token:
            raise HTTPException(
                status_code=401,
                detail="notAuthenticated",
                headers={"WWW-Authenticate": "Bearer"},
            )

        payload = _decode(token)
        user_id = _extract_user_id(payload)

        user = db.scalar(select(User).where(User.user_id == user_id))
        if not user:
            raise HTTPException(401, detail="userNotFound", headers={"WWW-Authenticate": "Bearer"})

        if require_confirmed and not bool(user.is_confirmed):
            # 403 is consistent with login flow when not confirmed
            raise HTTPException(403, detail="confirmationError")

        return user

    return _dep


# === Public dependencies ===

# Require confirmed e-mail (default for most protected routes)
get_current_user = _current_user_factory(require_confirmed=True)

# Allow unconfirmed users (useful for flows that must see their own data pre-confirmation)
get_current_user_unconfirmed = _current_user_factory(require_confirmed=False)

def get_current_admin_user(
    current_user: User = Depends(get_current_user),
) -> User:
    """
    Require a confirmed user who is also an admin.

    Uses the normal get_current_user dependency (which already validates
    JWT, loads the DB user, and checks confirmation), then enforces is_admin.
    """
    if not getattr(current_user, "is_admin", False):
        # frontend expects "adminOnly"
        raise HTTPException(status_code=403, detail="adminOnly")

    return current_user
