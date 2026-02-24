from __future__ import annotations
"""Report section builders (text/structure for PDF)."""
from datetime import date
from icea.models import (
    AnalyzeRequest,
    PackingResult,
    CostResult,
    RecommendedConfig,
)


def executive_summary(
    packing: PackingResult,
    cost: CostResult,
    recommendation: RecommendedConfig | None,
) -> dict:
    """Build executive summary section data."""
    return {
        "efficiency_score": packing.efficiency_score,
        "waste_cost_monthly_usd": cost.waste_cost_monthly_usd,
        "has_recommendation": recommendation is not None,
        "savings_monthly": recommendation.savings_vs_current_monthly_usd if recommendation else 0,
        "recommended_cores": recommendation.executor_cores if recommendation else None,
        "recommended_memory_gb": recommendation.executor_memory_gb if recommendation else None,
    }


def executive_summary_narrative(
    req: AnalyzeRequest,
    packing: PackingResult,
    cost: CostResult,
    recommendation: RecommendedConfig | None,
) -> str:
    """Full executive summary paragraph for the report."""
    score = packing.efficiency_score
    waste_mo = cost.waste_cost_monthly_usd
    daily = cost.daily_cost_usd
    nodes = req.node.count
    exec_per_node = packing.executors_per_node
    total_executors = nodes * exec_per_node if exec_per_node else 0
    parts = [
        f"This report analyzes your Spark cluster configuration: {nodes} node(s), "
        f"{req.executor.cores} cores and {req.executor.memory_gb:.0f} GB memory per executor, "
        f"with an average of {req.workload.avg_runtime_minutes:.1f} minutes runtime and {req.workload.jobs_per_day:.0f} jobs per day. "
        f"The current configuration packs {exec_per_node} executor(s) per node (total {total_executors} executors). "
        f"The packing efficiency score is {score}/100: higher means less idle CPU and memory. "
        f"Estimated monthly cost from this workload is approximately ${daily * 30:,.2f}; "
        f"of that, about ${waste_mo:,.2f}/month is attributed to resource waste (unused capacity).",
    ]
    if recommendation and recommendation.savings_vs_current_monthly_usd > 0:
        parts.append(
            f" A recommended configuration ({recommendation.executor_cores} cores, "
            f"{recommendation.executor_memory_gb:.0f} GB per executor, {recommendation.executors_per_node} executors per node) "
            f"could reduce monthly waste by approximately ${recommendation.savings_vs_current_monthly_usd:,.2f}."
        )
    else:
        parts.append(
            " Consider adjusting executor size or node count to improve utilization and reduce waste."
        )
    return "".join(parts)


def explanations_section() -> list[dict[str, str]]:
    """Short explanations for key metrics (for 'Understanding this report')."""
    return [
        {
            "title": "Efficiency score",
            "body": "A 0–100 score showing how well CPU and memory are used on each node. "
            "It is based on the fraction of node capacity used by executors; the remainder counts as waste. "
            "Higher is better. Low scores usually mean executor size does not fit the node well.",
        },
        {
            "title": "Waste and waste cost",
            "body": "Waste is the share of node CPU and memory reserved for executors but not fully used by your workload, "
            "plus any capacity left over because executors do not pack evenly. "
            "Waste cost is the estimated dollar value of that unused capacity at your node price and usage.",
        },
        {
            "title": "Executors per node",
            "body": "The number of executors that fit on one worker node given your executor cores, memory, and the node’s capacity. "
            "More executors per node usually improve parallelism but can increase scheduling and GC overhead if the number is too high.",
        },
        {
            "title": "Resource utilization (CPU and memory)",
            "body": "The percentage of available node CPU and memory allocated to executors. "
            "The rest is reserved for the OS and system processes. Utilization is limited by the tighter of the two (CPU or memory).",
        },
        {
            "title": "Recommended configuration",
            "body": "An alternative executor size (cores and memory) that improves packing and reduces waste for the same node type. "
            "Savings are estimates; actual results depend on workload behavior and tuning.",
        },
    ]


def definitions_section() -> list[dict[str, str]]:
    """Glossary of terms for the report."""
    return [
        {
            "term": "Node",
            "definition": "A worker machine in the cluster (e.g. an EC2 or Dataproc instance). Each node has a fixed number of vCPUs and memory (GB) available for executors after reserves.",
        },
        {
            "term": "Executor",
            "definition": "A Spark executor process running on a node. Each executor is configured with a fixed number of cores and memory (GB). Multiple executors can run on a single node.",
        },
        {
            "term": "Packing",
            "definition": "The process of fitting executors onto nodes. Executors per node = min(floor(available_cores / executor_cores), floor(available_memory / executor_memory)), after reserving capacity for the OS. The limiting resource (CPU or memory) determines the result.",
        },
        {
            "term": "Waste",
            "definition": "Unused capacity: CPU or memory left over after packing, or capacity allocated to executors but not fully utilized by the workload. Reported as a fraction (0–1) and as an estimated monthly cost (waste cost).",
        },
        {
            "term": "Efficiency score",
            "definition": "A 0–100 score derived from the waste fraction: 100 × (1 − waste). Higher means better use of node capacity; low scores indicate poor fit between executor size and node.",
        },
        {
            "term": "Reserve (cores and memory)",
            "definition": "Capacity reserved on each node for the OS, daemons, and system processes. This capacity is not used for executors and is excluded from packing calculations.",
        },
        {
            "term": "Utilization factor",
            "definition": "An optional multiplier (0–1) for shared clusters, representing the fraction of time the cluster is used for this workload. Applied to scale daily cost when the cluster runs other jobs.",
        },
    ]


def current_vs_recommended(
    req: AnalyzeRequest,
    packing: PackingResult,
    recommendation: RecommendedConfig | None,
) -> dict:
    """Current vs recommended configuration table."""
    return {
        "current": {
            "executor_cores": req.executor.cores,
            "executor_memory_gb": req.executor.memory_gb,
            "executors_per_node": packing.executors_per_node,
            "efficiency_score": packing.efficiency_score,
            "cpu_util": packing.cpu_utilization,
            "mem_util": packing.mem_utilization,
        },
        "recommended": (
            {
                "executor_cores": recommendation.executor_cores,
                "executor_memory_gb": recommendation.executor_memory_gb,
                "executors_per_node": recommendation.executors_per_node,
                "efficiency_score": recommendation.efficiency_score,
            }
            if recommendation
            else None
        ),
    }


def cost_assumptions(req: AnalyzeRequest, cost: CostResult) -> dict:
    """Cost model assumptions and inputs."""
    out = {
        "cloud": req.cloud,
        "node_cores": req.node.cores,
        "node_memory_gb": req.node.memory_gb,
        "node_hourly_cost_usd": req.node.hourly_cost_usd,
        "node_count": req.node.count,
        "avg_runtime_minutes": req.workload.avg_runtime_minutes,
        "jobs_per_day": req.workload.jobs_per_day,
        "hourly_cluster_cost_usd": cost.hourly_cluster_cost_usd,
        "daily_cost_usd": cost.daily_cost_usd,
    }
    if getattr(req, "region", None):
        out["region"] = req.region
    if getattr(req, "instance_type", None):
        out["instance_type"] = req.instance_type
    if getattr(req, "utilization_factor", None) is not None:
        out["utilization_factor"] = req.utilization_factor
    if getattr(req.workload, "min_runtime_minutes", None) is not None:
        out["min_runtime_minutes"] = req.workload.min_runtime_minutes
    if getattr(req.workload, "max_runtime_minutes", None) is not None:
        out["max_runtime_minutes"] = req.workload.max_runtime_minutes
    if getattr(req.workload, "partition_count", None) is not None:
        out["partition_count"] = req.workload.partition_count
    if getattr(req.workload, "input_data_gb", None) is not None:
        out["input_data_gb"] = req.workload.input_data_gb
    if getattr(req.workload, "concurrent_jobs", None) is not None:
        out["concurrent_jobs"] = req.workload.concurrent_jobs
    if getattr(req.workload, "peak_executor_memory_gb", None) is not None:
        out["peak_executor_memory_gb"] = req.workload.peak_executor_memory_gb
    if getattr(req.workload, "shuffle_read_mb", None) is not None:
        out["shuffle_read_mb"] = req.workload.shuffle_read_mb
    if getattr(req.workload, "shuffle_write_mb", None) is not None:
        out["shuffle_write_mb"] = req.workload.shuffle_write_mb
    if getattr(req.workload, "data_skew", None) is not None:
        out["data_skew"] = req.workload.data_skew
    if getattr(req.workload, "spot_pct", None) is not None:
        out["spot_pct"] = req.workload.spot_pct
    if getattr(req.workload, "autoscale_min_nodes", None) is not None:
        out["autoscale_min_nodes"] = req.workload.autoscale_min_nodes
    if getattr(req.workload, "autoscale_max_nodes", None) is not None:
        out["autoscale_max_nodes"] = req.workload.autoscale_max_nodes
    return out


def savings_section(cost: CostResult, recommendation: RecommendedConfig | None) -> dict:
    """Savings projection and sensitivity."""
    return {
        "waste_cost_daily_usd": cost.waste_cost_daily_usd,
        "waste_cost_monthly_usd": cost.waste_cost_monthly_usd,
        "savings_monthly_usd": recommendation.savings_vs_current_monthly_usd if recommendation else 0,
        "has_recommendation": recommendation is not None,
    }


def engineering_notes(
    packing: PackingResult,
    recommendation: RecommendedConfig | None,
    risk_notes: list[str],
) -> list[str]:
    """2–5 bullet engineering notes."""
    notes = []
    if packing.cpu_waste > packing.mem_waste:
        notes.append("CPU is the dominant constraint; consider increasing executor cores or reducing cores per executor to pack more executors per node.")
    else:
        notes.append("Memory is the dominant constraint; consider increasing executor memory or reducing memory per executor to improve packing.")
    if packing.executors_per_node == 0:
        notes.append("Current configuration yields zero executors per node; executor size exceeds available node resources. Adjust executor cores/memory or node size.")
    if recommendation and recommendation.savings_vs_current_monthly_usd > 0:
        notes.append(f"Recommended configuration could reduce monthly waste cost by approximately ${recommendation.savings_vs_current_monthly_usd:,.2f}.")
    notes.append("Estimates are directional and depend on actual workload behavior, I/O, and shuffle patterns.")
    notes.extend(risk_notes[:3])
    return notes[:5]


REPORT_VERSION = "0.1.0"


def report_metadata() -> dict:
    """Doc name and date for footer."""
    return {
        "doc_name": "KPI99 ICEA — Infrastructure Cost & Efficiency Report",
        "doc_name_short": "KPI99 ICEA Report",
        "date": date.today().strftime("%B %d, %Y"),
        "brand": "KPI99",
        "tagline": "Performance. Scale. Reliability—Engineered.",
        "copyright": "© 2026 KPI99 LLC. All rights reserved.",
        "report_version": REPORT_VERSION,
    }


def next_steps_section(
    req: AnalyzeRequest,
    packing: PackingResult,
    recommendation: RecommendedConfig | None,
) -> list[str]:
    """3–5 concrete next steps / action items."""
    steps = [
        "Try the recommended executor configuration in a staging or dev environment before rolling out to production.",
        "Capture peak executor memory from Spark UI (Executor → Storage Memory / peak) and re-run the analysis for better OOM and sizing guidance.",
        "Re-run this analysis after significant workload changes (e.g. new jobs, more data, different runtime).",
    ]
    if recommendation and recommendation.savings_vs_current_monthly_usd > 0:
        steps.append(
            f"Compare job runtimes and stability with the recommended config ({recommendation.executor_cores} cores, "
            f"{recommendation.executor_memory_gb:.0f} GB per executor) to validate savings."
        )
    if packing.efficiency_score < 60:
        steps.append(
            "Review node type vs executor size: consider a different instance family or executor dimensions to improve packing."
        )
    return steps[:5]


def _mitigation_for_risk(note: str) -> str:
    """One-line mitigation for a risk note (keyword-based)."""
    note_lower = note.lower()
    if "oom" in note_lower or "memory" in note_lower and "executor" in note_lower:
        return "Increase executor memory or add peak executor memory from Spark UI for precise sizing."
    if "shuffle" in note_lower or "spill" in note_lower:
        return "Increase executor memory or reduce shuffle/data per task; consider partition count alignment."
    if "spot" in note_lower or "preemptible" in note_lower:
        return "Use spot for non-critical workloads; consider mixed OD/spot or capacity reservations for critical paths."
    if "skew" in note_lower:
        return "Repartition or salt keys to balance load; monitor tail tasks in Spark UI."
    if "partition" in note_lower and "cores" in note_lower:
        return "Align partition count with total executor cores (e.g. 1–4× cores) or scale cluster/partitions."
    if "concurrent" in note_lower or "utilization factor" in note_lower:
        return "Set utilization factor in the analysis to reflect shared cluster usage."
    if "executors per node" in note_lower or "scheduling" in note_lower or "gc" in note_lower:
        return "Reduce executors per node or executor size to lower scheduling/GC overhead."
    return "Review recommendation and workload tuning; consider Tier 2 Expert analysis for custom optimization."


def risks_mitigations_section(risk_notes: list[str]) -> list[dict[str, str]]:
    """Risks with one-line mitigations for report."""
    return [{"risk": r, "mitigation": _mitigation_for_risk(r)} for r in risk_notes]


def cost_breakdown_sentence(cost: CostResult) -> str:
    """One sentence: monthly cost from node×usage and estimated waste."""
    total_monthly = cost.daily_cost_usd * 30
    waste = cost.waste_cost_monthly_usd
    return (
        f"Monthly cost is approximately ${total_monthly:,.2f} from node price × usage; "
        f"${waste:,.2f} of that is estimated waste (unused allocated capacity)."
    )


def methodology_section() -> list[dict[str, str]]:
    """How this was calculated: packing, cost, waste (for Methodology section)."""
    return [
        {
            "title": "Packing (executors per node)",
            "body": "Executors per node = min(floor((node_cores − reserve_cores) / executor_cores), "
            "floor((node_memory_gb − reserve_memory) / executor_memory_gb)). "
            "Efficiency score = 100 × (1 − waste), where waste = max(CPU waste, memory waste) after packing. "
            "Implementation: icea/packing.py.",
        },
        {
            "title": "Cost (daily and monthly)",
            "body": "Hourly cluster cost = node_hourly_price × node_count. "
            "Daily cost = hourly_cluster_cost × (avg_runtime_minutes / 60) × jobs_per_day × utilization_factor. "
            "Monthly cost ≈ daily_cost × 30. Implementation: icea/cost_model.py.",
        },
        {
            "title": "Waste and waste cost",
            "body": "Waste is the unused fraction of allocated capacity (CPU or memory left over after packing, "
            "or capacity not fully utilized by the workload). Waste cost = daily_cost × waste × 30.",
        },
    ]


def sensitivity_section(
    req: AnalyzeRequest,
    cost: CostResult,
    recommendation: RecommendedConfig | None,
) -> dict:
    """What-if: node count ±1, runtime +20%; estimated monthly costs."""
    n = req.node.count
    daily = cost.daily_cost_usd
    monthly = daily * 30
    # Cost scales linearly with node count (same packing per node)
    monthly_plus_one = (monthly * (n + 1) / n) if n else monthly
    monthly_minus_one = (monthly * (n - 1) / n) if n > 1 else monthly
    # Cost scales linearly with runtime
    runtime_pct_20 = monthly * 1.2
    return {
        "current_monthly_usd": round(monthly, 2),
        "if_nodes_plus_one_monthly_usd": round(monthly_plus_one, 2),
        "if_nodes_minus_one_monthly_usd": round(monthly_minus_one, 2) if n > 1 else None,
        "if_runtime_plus_20_pct_monthly_usd": round(runtime_pct_20, 2),
        "node_count": n,
        "has_recommendation": recommendation is not None,
    }


def data_quality_note(req: AnalyzeRequest) -> dict:
    """What optional inputs were used; suggest adding peak executor memory."""
    used = []
    w = req.workload
    if getattr(w, "partition_count", None) is not None:
        used.append("partition count")
    if getattr(w, "input_data_gb", None) is not None:
        used.append("input data (GB)")
    if getattr(w, "concurrent_jobs", None) is not None:
        used.append("concurrent jobs")
    if getattr(w, "peak_executor_memory_gb", None) is not None:
        used.append("peak executor memory (GB)")
    if getattr(w, "shuffle_read_mb", None) is not None or getattr(w, "shuffle_write_mb", None) is not None:
        used.append("shuffle read/write")
    if getattr(w, "data_skew", None) is not None:
        used.append("data skew")
    if getattr(w, "spot_pct", None) is not None:
        used.append("spot %")
    if getattr(req, "utilization_factor", None) is not None:
        used.append("utilization factor")
    suggest_peak = getattr(w, "peak_executor_memory_gb", None) is None
    return {
        "optional_inputs_used": used,
        "suggest_peak_executor_memory": suggest_peak,
    }


def tier_cta_section() -> str:
    """One line CTA for Tier 2/3."""
    return (
        "For a detailed review and custom optimization plan, request Tier 2 Expert or Tier 3 Enterprise analysis "
        "(contact via your KPI99 account or the ICEA app)."
    )


def benchmark_context(efficiency_score: int) -> dict:
    """Efficiency score vs typical bands: below / in line / above."""
    if efficiency_score >= 80:
        band = "above"
        text = "above typical for similar clusters"
    elif efficiency_score >= 50:
        band = "typical"
        text = "in line with typical for similar clusters"
    else:
        band = "below"
        text = "below typical for similar clusters"
    return {"band": band, "text": text, "efficiency_score": efficiency_score}


def rerun_cta(app_url: str | None) -> str:
    """Re-run CTA with app URL if provided."""
    if app_url:
        return f"Update your configuration and re-run the analysis at {app_url.rstrip('/')}."
    return "Update your configuration and re-run the analysis in your ICEA dashboard."


def forecast_data(
    req: AnalyzeRequest,
    cost: CostResult,
    recommendation: "RecommendedConfig | None",
) -> list[dict] | None:
    """Build forecast table when forecast_months set. Uses growth_rate_pct if set."""
    months = getattr(req, "forecast_months", None) or 0
    if months < 1:
        return None
    current_monthly = cost.daily_cost_usd * 30
    if recommendation and recommendation.savings_vs_current_monthly_usd:
        recommended_monthly = current_monthly - recommendation.savings_vs_current_monthly_usd
    else:
        recommended_monthly = current_monthly
    from icea.cost_model import compute_forecast
    growth = getattr(req, "growth_rate_pct", None)
    return compute_forecast(current_monthly, recommended_monthly, months, growth)


def utilization_chart_data(packing: PackingResult) -> dict:
    """Data for utilization bar: CPU and memory utilized vs wasted."""
    return {
        "cpu_util_pct": round(packing.cpu_utilization * 100, 1),
        "mem_util_pct": round(packing.mem_utilization * 100, 1),
        "cpu_waste_pct": round(packing.cpu_waste * 100, 1),
        "mem_waste_pct": round(packing.mem_waste * 100, 1),
    }


def cost_breakdown_chart_data(
    cost: CostResult,
    recommendation: "RecommendedConfig | None",
) -> dict:
    """Data for cost breakdown: current monthly, waste, and potential savings."""
    total_monthly = cost.daily_cost_usd * 30
    waste = cost.waste_cost_monthly_usd
    utilized = total_monthly - waste
    savings = recommendation.savings_vs_current_monthly_usd if recommendation else 0
    return {
        "total_monthly_usd": round(total_monthly, 2),
        "waste_monthly_usd": round(waste, 2),
        "utilized_monthly_usd": round(utilized, 2),
        "savings_monthly_usd": round(savings, 2),
    }
