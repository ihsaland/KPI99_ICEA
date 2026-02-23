"""Tests for executor packing."""
import pytest
from icea.models import NodeConfig, ExecutorConfig, Assumptions
from icea.packing import compute_packing


def test_packing_balanced():
    """Balanced CPU/memory yields full utilization."""
    node = NodeConfig(cores=16, memory_gb=64, hourly_cost_usd=1.0, count=10)
    executor = ExecutorConfig(cores=4, memory_gb=16)
    assumptions = Assumptions(reserve_cores=0, reserve_memory_gb=0)
    r = compute_packing(node, executor, assumptions)
    assert r.executors_per_node == 4  # min(16/4, 64/16)
    assert r.cpu_utilization == 1.0
    assert r.mem_utilization == 1.0
    assert r.waste == 0
    assert r.efficiency_score == 100


def test_packing_with_reserves():
    """Reserve cores and memory reduce effective capacity."""
    node = NodeConfig(cores=16, memory_gb=64, hourly_cost_usd=1.0, count=10)
    executor = ExecutorConfig(cores=4, memory_gb=16)
    assumptions = Assumptions(reserve_cores=1, reserve_memory_gb=4)
    r = compute_packing(node, executor, assumptions)
    # effective: 15 cores, 60 GB -> min(15/4, 60/16) = min(3, 3) = 3
    assert r.executors_per_node == 3
    assert r.cpu_utilization == (3 * 4) / 15
    assert r.mem_utilization == (3 * 16) / 60


def test_packing_cpu_bound():
    """CPU-bound: CPU limits executors; memory underutilized."""
    node = NodeConfig(cores=8, memory_gb=64, hourly_cost_usd=1.0, count=5)
    executor = ExecutorConfig(cores=4, memory_gb=8)
    assumptions = Assumptions(reserve_cores=0, reserve_memory_gb=0)
    r = compute_packing(node, executor, assumptions)
    assert r.executors_per_node == 2  # min(8/4, 64/8) = min(2, 8)
    assert r.cpu_utilization == 1.0
    assert r.mem_utilization < 1.0  # 2*8/64 = 0.25
    assert r.efficiency_score == 25  # waste = max(0, 0.75) = 0.75 -> score 25


def test_packing_zero_executors():
    """Executor larger than node yields 0 executors."""
    node = NodeConfig(cores=4, memory_gb=16, hourly_cost_usd=1.0, count=1)
    executor = ExecutorConfig(cores=8, memory_gb=32)
    assumptions = Assumptions(reserve_cores=0, reserve_memory_gb=0)
    r = compute_packing(node, executor, assumptions)
    assert r.executors_per_node == 0
    assert r.efficiency_score == 0
