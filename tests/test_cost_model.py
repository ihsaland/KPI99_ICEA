"""Tests for cost model."""
from icea.models import NodeConfig, WorkloadConfig, PackingResult
from icea.cost_model import compute_cost


def test_cost_basic():
    """Daily cost = hourly_cluster * (R/60) * J; waste_month = daily_cost * waste * 30."""
    node = NodeConfig(cores=16, memory_gb=64, hourly_cost_usd=1.0, count=10)
    workload = WorkloadConfig(avg_runtime_minutes=60, jobs_per_day=10)
    packing = PackingResult(
        executors_per_node=4,
        cpu_utilization=1.0,
        mem_utilization=1.0,
        cpu_waste=0,
        mem_waste=0,
        waste=0,
        efficiency_score=100,
    )
    cost = compute_cost(node, workload, packing)
    # hourly = 1 * 10 = 10, daily = 10 * (60/60) * 10 = 100
    assert cost.hourly_cluster_cost_usd == 10
    assert cost.daily_cost_usd == 100
    assert cost.waste_cost_daily_usd == 0
    assert cost.waste_cost_monthly_usd == 0


def test_cost_with_waste():
    """Waste cost is daily_cost * waste * 30."""
    node = NodeConfig(cores=16, memory_gb=64, hourly_cost_usd=2.0, count=5)
    workload = WorkloadConfig(avg_runtime_minutes=30, jobs_per_day=20)
    packing = PackingResult(
        executors_per_node=2,
        cpu_utilization=0.5,
        mem_utilization=0.5,
        cpu_waste=0.5,
        mem_waste=0.5,
        waste=0.5,
        efficiency_score=50,
    )
    cost = compute_cost(node, workload, packing)
    # hourly = 2 * 5 = 10, daily = 10 * (30/60) * 20 = 100
    assert cost.daily_cost_usd == 100
    assert cost.waste_cost_daily_usd == 50
    assert cost.waste_cost_monthly_usd == 50 * 30  # 1500
