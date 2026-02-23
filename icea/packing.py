"""Executor packing and utilization (pure functions, no I/O)."""
import math
from icea.models import NodeConfig, ExecutorConfig, Assumptions, PackingResult


def compute_packing(
    node: NodeConfig,
    executor: ExecutorConfig,
    assumptions: Assumptions,
) -> PackingResult:
    """
    Compute executors per node and utilization from Appendix A.
    executors_per_node = min(floor((C - reserve_cores) / c), floor((M - reserve_mem) / m))
    """
    C = node.cores
    M = node.memory_gb
    c = executor.cores
    m = executor.memory_gb
    reserve_cores = assumptions.reserve_cores
    reserve_mem = assumptions.reserve_memory_gb

    effective_cores = C - reserve_cores
    effective_mem = M - reserve_mem
    if effective_cores <= 0 or effective_mem <= 0:
        return PackingResult(
            executors_per_node=0,
            cpu_utilization=0,
            mem_utilization=0,
            cpu_waste=1,
            mem_waste=1,
            waste=1,
            efficiency_score=0,
        )

    e_cpu = math.floor(effective_cores / c) if c > 0 else 0
    e_mem = math.floor(effective_mem / m) if m > 0 else 0
    executors_per_node = min(e_cpu, e_mem)

    cpu_util = (executors_per_node * c) / effective_cores if effective_cores else 0
    mem_util = (executors_per_node * m) / effective_mem if effective_mem else 0

    cpu_waste = 1 - cpu_util
    mem_waste = 1 - mem_util
    waste = max(cpu_waste, mem_waste)
    score = round(100 * (1 - waste))

    return PackingResult(
        executors_per_node=executors_per_node,
        cpu_utilization=round(cpu_util, 4),
        mem_utilization=round(mem_util, 4),
        cpu_waste=round(cpu_waste, 4),
        mem_waste=round(mem_waste, 4),
        waste=round(waste, 4),
        efficiency_score=min(100, max(0, score)),
    )
