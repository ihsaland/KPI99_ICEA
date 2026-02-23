"""Cost calculations and projections (pure functions)."""
from icea.models import NodeConfig, WorkloadConfig, PackingResult, CostResult


def compute_cost(
    node: NodeConfig,
    workload: WorkloadConfig,
    packing: PackingResult,
    utilization_factor: float | None = None,
) -> CostResult:
    """
    cluster_cost_hr = P * N
    daily_cost = cluster_cost_hr * (R / 60) * J
    If utilization_factor < 1 (shared cluster), scale effective usage cost.
    waste_month = daily_cost * waste * 30
    """
    P = node.hourly_cost_usd
    N = node.count
    R = workload.avg_runtime_minutes
    J = workload.jobs_per_day
    waste = packing.waste
    u = utilization_factor if utilization_factor is not None and 0 < utilization_factor <= 1 else 1.0

    hourly_cluster_cost = P * N
    daily_cost_raw = hourly_cluster_cost * (R / 60.0) * J
    daily_cost = round(daily_cost_raw * u, 2)
    waste_cost_daily = round(daily_cost * waste, 2)
    waste_cost_monthly = round(waste_cost_daily * 30, 2)

    return CostResult(
        hourly_cluster_cost_usd=round(hourly_cluster_cost, 2),
        daily_cost_usd=daily_cost,
        waste_cost_daily_usd=waste_cost_daily,
        waste_cost_monthly_usd=waste_cost_monthly,
    )


def compute_forecast(
    cost_current_monthly: float,
    cost_recommended_monthly: float,
    months: int,
    growth_rate_pct: float | None = None,
) -> list[dict]:
    """
    Project monthly costs over N months. If growth_rate_pct, scale usage (and thus cost) each month.
    Returns list of {month, current_usd, recommended_usd, savings_usd}.
    """
    out = []
    g = (growth_rate_pct / 100.0) / 12.0 if growth_rate_pct is not None else 0.0  # monthly growth factor
    cur, rec = cost_current_monthly, cost_recommended_monthly
    for m in range(1, months + 1):
        if m > 1 and g != 0:
            cur = cur * (1 + g)
            rec = rec * (1 + g)
        out.append({
            "month": m,
            "current_usd": round(cur, 2),
            "recommended_usd": round(rec, 2),
            "savings_usd": round(cur - rec, 2),
        })
    return out
