"""API key authentication for ICEA (required for POST /v1/report when ICEA_DEMO is not set)."""
import os
from fastapi import Header, HTTPException, status


def get_required_api_key() -> str | None:
    """Return configured API key if set (ICEA_API_KEY)."""
    return (os.environ.get("ICEA_API_KEY") or os.environ.get("ICEA_API_KEYS", "").strip()) or None


def _get_provided_key(x_api_key: str | None, authorization: str | None) -> str | None:
    if x_api_key:
        return x_api_key.strip()
    if authorization and authorization.startswith("Bearer "):
        return authorization[7:].strip()
    return None


def verify_api_key(
    x_api_key: str | None = Header(None, alias="X-API-Key"),
    authorization: str | None = Header(None),
) -> None:
    """
    Dependency: require a valid API key. Accepts X-API-Key or Authorization: Bearer <key>.
    Use require_report_auth for POST /v1/report (allows demo OR key).
    """
    required = get_required_api_key()
    if not required:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="API key is not configured. Set ICEA_API_KEY for API access.",
        )
    provided = _get_provided_key(x_api_key, authorization)
    if not provided or provided != required:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key. Use X-API-Key or Authorization: Bearer.",
        )


def _is_demo_enabled() -> bool:
    """True if ICEA_DEMO is set to a value that enables demo (1, true, yes). Never True when ICEA_ENV or NODE_ENV is production."""
    env = (os.environ.get("ICEA_ENV") or os.environ.get("NODE_ENV") or "").strip().lower()
    if env in ("production", "prod"):
        return False
    v = (os.environ.get("ICEA_DEMO") or "").strip().lower()
    return v in ("1", "true", "yes")


def require_report_auth(
    x_api_key: str | None = Header(None, alias="X-API-Key"),
    authorization: str | None = Header(None),
) -> None:
    """
    Dependency for POST /v1/report: allow if ICEA_DEMO=1 (or true/yes), else require valid API key.
    In production (ICEA_DEMO not set) this blocks unauthenticated report generation.
    """
    if _is_demo_enabled():
        return
    required = get_required_api_key()
    if not required:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Report requires authentication. Set ICEA_DEMO=1 for demo or ICEA_API_KEY for API access.",
        )
    provided = _get_provided_key(x_api_key, authorization)
    if not provided or provided != required:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key. Use X-API-Key or Authorization: Bearer.",
        )
