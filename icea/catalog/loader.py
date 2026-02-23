"""Load provider/region/instance catalog from JSON. Regional pricing supported."""
import json
import os
from pathlib import Path
from typing import Any

_CATALOG_DIR = Path(__file__).resolve().parent
_PROVIDERS_CACHE: dict[str, dict] = {}


def _load_provider(provider_id: str) -> dict | None:
    if provider_id in _PROVIDERS_CACHE:
        return _PROVIDERS_CACHE[provider_id]
    path = _CATALOG_DIR / f"{provider_id}.json"
    if not path.is_file():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        data["id"] = data.get("id", provider_id)
        _PROVIDERS_CACHE[provider_id] = data
        return data
    except (json.JSONDecodeError, OSError):
        return None


def _all_provider_ids() -> list[str]:
    ids = []
    for p in _CATALOG_DIR.iterdir():
        if p.suffix == ".json" and p.stem not in ("README", "schema"):
            ids.append(p.stem)
    return sorted(ids)


def get_providers() -> list[dict[str, Any]]:
    """Return list of { id, name } for all providers with catalog files."""
    result = []
    for pid in _all_provider_ids():
        p = _load_provider(pid)
        if p:
            result.append({"id": p["id"], "name": p.get("name", pid.upper())})
    return result


def get_regions(cloud: str) -> list[dict[str, Any]]:
    """Return list of { id, name } for the given cloud. Empty if no catalog."""
    p = _load_provider(cloud)
    if not p:
        return []
    regions = p.get("regions") or []
    return [{"id": r.get("id", ""), "name": r.get("name", r.get("id", ""))} for r in regions]


def _hourly_for_region(inst: dict, region_id: str | None) -> float:
    prices = inst.get("prices")
    if isinstance(prices, dict) and region_id and region_id in prices:
        return float(prices[region_id])
    if isinstance(inst.get("hourly_usd"), (int, float)):
        return float(inst["hourly_usd"])
    if isinstance(prices, dict) and prices:
        return float(next(iter(prices.values())))
    return 0.0


def get_instance_types(cloud: str, region: str | None = None) -> list[dict[str, Any]]:
    """
    Return instance types for cloud, with hourly_usd resolved for region.
    Each item: { id, name, cores, memory_gb, hourly_usd }.
    """
    p = _load_provider(cloud)
    if not p:
        return []
    instances = p.get("instance_types") or []
    out = []
    for i in instances:
        hourly = _hourly_for_region(i, region)
        out.append({
            "id": i.get("id", ""),
            "name": i.get("name", i.get("id", "")),
            "cores": int(i.get("cores", 0)),
            "memory_gb": float(i.get("memory_gb", 0)),
            "hourly_usd": round(hourly, 4),
        })
    return out


def get_instance_by_id(cloud: str, instance_id: str, region: str | None = None) -> dict | None:
    """Return a single instance type by id, or None."""
    for inst in get_instance_types(cloud, region):
        if inst["id"] == instance_id:
            return inst
    return None
