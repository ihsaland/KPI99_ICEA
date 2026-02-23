"""Structured logging and metrics for ICEA."""
import logging
import time
import uuid
from collections import defaultdict

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

_LOG = logging.getLogger(__name__)

# In-process counters for /v1/metrics (reset on restart). For multi-instance use Prometheus client or similar.
_request_total: dict[str, int] = defaultdict(int)
_request_duration_sec: list[float] = []  # keep last N for simple stats, or use a proper histogram
_start_time = time.time()
_MAX_DURATION_SAMPLES = 1000


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Add request_id and log structured request/response with duration."""

    async def dispatch(self, request: Request, call_next) -> Response:
        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        request.state.request_id = request_id
        path = request.url.path
        method = request.method
        start = time.time()
        response = await call_next(request)
        duration_ms = (time.time() - start) * 1000
        status = response.status_code
        _request_total[f"{method} {path}"] += 1
        _request_total["_total"] += 1
        _request_duration_sec.append(time.time() - start)
        if len(_request_duration_sec) > _MAX_DURATION_SAMPLES:
            _request_duration_sec[:] = _request_duration_sec[-_MAX_DURATION_SAMPLES:]
        _LOG.info(
            "request finished",
            extra={
                "request_id": request_id,
                "method": method,
                "path": path,
                "status": status,
                "duration_ms": round(duration_ms, 2),
            },
        )
        response.headers["X-Request-ID"] = request_id
        return response


def get_metrics_text() -> str:
    """Prometheus-style text for GET /v1/metrics."""
    uptime = time.time() - _start_time
    lines = [
        "# HELP icea_uptime_seconds Process uptime in seconds.",
        "# TYPE icea_uptime_seconds gauge",
        f"icea_uptime_seconds {uptime:.2f}",
        "# HELP icea_http_requests_total Total HTTP requests by method and path.",
        "# TYPE icea_http_requests_total counter",
    ]
    for key, count in sorted(_request_total.items()):
        if key == "_total":
            lines.append(f'icea_http_requests_total{{aggregate="all"}} {count}')
        else:
            parts = key.split(" ", 1)
            method, path = (parts[0], parts[1]) if len(parts) == 2 else (key, "")
            path = path.replace('"', r"\"")
            lines.append(f'icea_http_requests_total{{method="{method}",path="{path}"}} {count}')
    if _request_duration_sec:
        avg = sum(_request_duration_sec) / len(_request_duration_sec)
        lines.extend([
            "# HELP icea_http_request_duration_seconds Recent request duration (avg).",
            "# TYPE icea_http_request_duration_seconds gauge",
            f"icea_http_request_duration_seconds {avg:.4f}",
        ])
    return "\n".join(lines) + "\n"
