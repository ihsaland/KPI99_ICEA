"""Timeouts and optional caching for analyze/report."""
import hashlib
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="icea")

_ANALYZE_TIMEOUT_SEC = int(os.environ.get("ICEA_ANALYZE_TIMEOUT_SEC", "60"))
_REPORT_TIMEOUT_SEC = int(os.environ.get("ICEA_REPORT_TIMEOUT_SEC", "120"))
_CACHE_TTL_SEC = int(os.environ.get("ICEA_ANALYZE_CACHE_TTL_SEC", "300"))
_CACHE_MAX_SIZE = int(os.environ.get("ICEA_ANALYZE_CACHE_MAX", "500"))
_analyze_cache: dict[str, tuple[dict, float]] = {}
_cache_order: list[str] = []


def run_sync_with_timeout(seconds: int, func, *args, **kwargs):
    """Run sync function in a thread with timeout. Raises TimeoutError on timeout."""
    future = _executor.submit(func, *args, **kwargs)
    try:
        return future.result(timeout=seconds)
    except FuturesTimeoutError:
        future.cancel()
        raise TimeoutError(f"Operation timed out after {seconds}s")


def _analyze_cache_key(request_dict: dict) -> str:
    canonical = json.dumps(request_dict, sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


def get_cached_analyze(request_dict: dict) -> dict | None:
    """Return cached AnalyzeResponse dict if present and not expired."""
    if _CACHE_TTL_SEC <= 0:
        return None
    key = _analyze_cache_key(request_dict)
    if key not in _analyze_cache:
        return None
    data, created = _analyze_cache[key]
    if time.time() - created > _CACHE_TTL_SEC:
        _analyze_cache.pop(key, None)
        if key in _cache_order:
            _cache_order.remove(key)
        return None
    return data


def set_cached_analyze(request_dict: dict, response_dict: dict) -> None:
    """Store AnalyzeResponse in cache (with size/eviction)."""
    if _CACHE_TTL_SEC <= 0:
        return
    key = _analyze_cache_key(request_dict)
    while len(_analyze_cache) >= _CACHE_MAX_SIZE and _cache_order:
        old = _cache_order.pop(0)
        _analyze_cache.pop(old, None)
    _analyze_cache[key] = (response_dict, time.time())
    if key not in _cache_order:
        _cache_order.append(key)


def get_analyze_timeout_sec() -> int:
    return _ANALYZE_TIMEOUT_SEC


def get_report_timeout_sec() -> int:
    return _REPORT_TIMEOUT_SEC
