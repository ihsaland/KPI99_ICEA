"""Recommendation engine: grid search with guardrails."""
import math
from icea.models import (
    NodeConfig,
    ExecutorConfig,
    WorkloadConfig,
    Assumptions,
    PackingResult,
    CostResult,
    RecommendedConfig,
)
from icea.packing import compute_packing
from icea.cost_model import compute_cost

# Candidate executor cores (filter by node cores)
CORE_CANDIDATES = [1, 2, 3, 4, 5, 6, 8]

# Guardrails (configurable)
MIN_EXECUTOR_MEMORY_GB = 6
MAX_EXECUTORS_PER_NODE = 12


def recommend(
    node: NodeConfig,
    current_executor: ExecutorConfig,
    workload: WorkloadConfig,
    assumptions: Assumptions,
    current_packing: PackingResult,
    current_cost: CostResult,
) -> RecommendedConfig | None:
    """
    Search candidate executor configs; pick one that minimizes waste
    while respecting min memory and max executors per node.
    """
    C = node.cores - assumptions.reserve_cores
    M = node.memory_gb - assumptions.reserve_memory_gb
    if C <= 0 or M <= 0:
        return None

    best: tuple[float, float, int, float, float, float] | None = None  # waste, waste_monthly, score, c, m, savings

    for c in CORE_CANDIDATES:
        if c > C:
            continue
        # Memory candidates: 1 GB steps from MIN_EXECUTOR_MEMORY_GB up to M
        mem_min = max(MIN_EXECUTOR_MEMORY_GB, 1)
        for m in range(int(mem_min), int(M) + 1, 1):
            if m > M:
                break
            exec_candidate = ExecutorConfig(cores=c, memory_gb=float(m))
            packing = compute_packing(node, exec_candidate, assumptions)
            if packing.executors_per_node == 0:
                continue
            if packing.executors_per_node > MAX_EXECUTORS_PER_NODE:
                continue
            cost = compute_cost(node, workload, packing)
            # Prefer lower waste; then higher efficiency score
            if best is None or packing.waste < best[0]:
                savings = current_cost.waste_cost_monthly_usd - cost.waste_cost_monthly_usd
                best = (
                    packing.waste,
                    cost.waste_cost_monthly_usd,
                    packing.efficiency_score,
                    float(c),
                    float(m),
                    savings,
                )

    if best is None:
        return None

    waste, waste_monthly, score, c, m, savings = best
    executors = min(
        math.floor(C / int(c)),
        math.floor(M / int(m)),
    )
    return RecommendedConfig(
        executor_cores=int(c),
        executor_memory_gb=m,
        executors_per_node=executors,
        efficiency_score=score,
        waste=round(waste, 4),
        waste_cost_monthly_usd=round(waste_monthly, 2),
        savings_vs_current_monthly_usd=round(savings, 2),
    )
