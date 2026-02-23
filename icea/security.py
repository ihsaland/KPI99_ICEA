"""Rate limiting and production guards."""
import logging
import os
import time
from collections import defaultdict

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)

# Per-IP sliding window: (count, window_start). Max 120 requests per 60 seconds per IP by default.
_RATE_LIMIT_REQUESTS = int(os.environ.get("ICEA_RATE_LIMIT_REQUESTS", "120"))
_RATE_LIMIT_WINDOW_SEC = int(os.environ.get("ICEA_RATE_LIMIT_WINDOW_SEC", "60"))
_store: dict[str, tuple[int, float]] = defaultdict(lambda: (0, 0.0))


def _client_ip(request: Request) -> str:
    return (request.scope.get("client") and request.scope["client"][0]) or request.headers.get("x-forwarded-for", "").split(",")[0].strip() or "unknown"


class RateLimitMiddleware(BaseHTTPMiddleware):
    """In-memory per-IP rate limit. For multi-instance use Redis or similar."""

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path
        if path == "/v1/health" or path == "/health":
            return await call_next(request)
        if path.startswith("/v1/") or path == "/":
            ip = _client_ip(request)
            now = time.time()
            count, start = _store[ip]
            if now - start >= _RATE_LIMIT_WINDOW_SEC:
                _store[ip] = (1, now)
            elif count >= _RATE_LIMIT_REQUESTS:
                from starlette.responses import JSONResponse
                return JSONResponse(
                    status_code=429,
                    content={"detail": "Too many requests. Try again later."},
                    headers={"Retry-After": str(_RATE_LIMIT_WINDOW_SEC)},
                )
            else:
                _store[ip] = (count + 1, start)
        return await call_next(request)


def check_demo_in_production() -> None:
    """Log critical and optionally fail if ICEA_DEMO is set in production."""
    from icea.auth import _is_demo_enabled
    if not _is_demo_enabled():
        return
    env = (os.environ.get("ICEA_ENV") or os.environ.get("NODE_ENV") or "").lower()
    if env == "production" or env == "prod":
        logger.critical(
            "ICEA_DEMO=1 is set in production. Disable demo (unset ICEA_DEMO) for production deployments."
        )
        if os.environ.get("ICEA_DEMO_FAIL_IN_PROD", "").strip() == "1":
            raise RuntimeError("ICEA_DEMO=1 must not be set in production. Unset ICEA_DEMO.")
