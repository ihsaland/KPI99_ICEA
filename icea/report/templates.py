from __future__ import annotations
"""Report section builders (text/structure for PDF). lang=en|es for English/Spanish."""
from datetime import date
from icea.models import (
    AnalyzeRequest,
    PackingResult,
    CostResult,
    RecommendedConfig,
)


def _t(lang: str, en: str, es: str) -> str:
    """Return Spanish or English string by lang."""
    return es if (lang or "en").strip().lower() == "es" else en


_ES_MONTHS = ("enero", "febrero", "marzo", "abril", "mayo", "junio",
              "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre")


def _format_date_es(d: date) -> str:
    """Format date for Spanish (e.g. 22 de febrero de 2026)."""
    return f"{d.day} de {_ES_MONTHS[d.month - 1]} de {d.year}"


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
    *,
    lang: str = "en",
) -> str:
    """Full executive summary paragraph for the report."""
    score = packing.efficiency_score
    waste_mo = cost.waste_cost_monthly_usd
    daily = cost.daily_cost_usd
    nodes = req.node.count
    exec_per_node = packing.executors_per_node
    total_executors = nodes * exec_per_node if exec_per_node else 0
    if _t(lang, "en", "es") == "es":
        parts = [
            f"Este informe analiza la configuración de su clúster Spark: {nodes} nodo(s), "
            f"{req.executor.cores} núcleos y {req.executor.memory_gb:.0f} GB de memoria por ejecutor, "
            f"con un promedio de {req.workload.avg_runtime_minutes:.1f} minutos de tiempo de ejecución y {req.workload.jobs_per_day:.0f} trabajos por día. "
            f"La configuración actual empaqueta {exec_per_node} ejecutor(es) por nodo (total {total_executors} ejecutores). "
            f"La puntuación de eficiencia de empaquetado es {score}/100: mayor significa menos CPU y memoria inactivas. "
            f"El costo mensual estimado de esta carga es aproximadamente ${daily * 30:,.2f}; "
            f"de ello, unos ${waste_mo:,.2f}/mes se atribuyen a desperdicio de recursos (capacidad no utilizada).",
        ]
        if recommendation and recommendation.savings_vs_current_monthly_usd > 0:
            parts.append(
                f" Una configuración recomendada ({recommendation.executor_cores} núcleos, "
                f"{recommendation.executor_memory_gb:.0f} GB por ejecutor, {recommendation.executors_per_node} ejecutores por nodo) "
                f"podría reducir el desperdicio mensual en aproximadamente ${recommendation.savings_vs_current_monthly_usd:,.2f}."
            )
        else:
            parts.append(
                " Considere ajustar el tamaño del ejecutor o el número de nodos para mejorar la utilización y reducir el desperdicio."
            )
        return "".join(parts)
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


def explanations_section(*, lang: str = "en") -> list[dict[str, str]]:
    """Short explanations for key metrics (for 'Understanding this report')."""
    if _t(lang, "en", "es") == "es":
        return [
            {"title": "Puntuación de eficiencia", "body": "Puntuación 0–100 que indica qué tan bien se usan CPU y memoria en cada nodo. Se basa en la fracción de capacidad del nodo usada por los ejecutores; el resto cuenta como desperdicio. Mayor es mejor. Puntuaciones bajas suelen indicar que el tamaño del ejecutor no se ajusta bien al nodo."},
            {"title": "Desperdicio y costo de desperdicio", "body": "El desperdicio es la parte de CPU y memoria del nodo reservada para ejecutores pero no usada por su carga, más la capacidad sobrante porque los ejecutores no se empaquetan de forma uniforme. El costo de desperdicio es el valor en dólares estimado de esa capacidad no utilizada según el precio del nodo y el uso."},
            {"title": "Ejecutores por nodo", "body": "Número de ejecutores que caben en un nodo según los núcleos y memoria del ejecutor y la capacidad del nodo. Más ejecutores por nodo suelen mejorar el paralelismo pero pueden aumentar la sobrecarga de planificación y GC si el número es demasiado alto."},
            {"title": "Utilización de recursos (CPU y memoria)", "body": "Porcentaje de CPU y memoria disponibles del nodo asignados a ejecutores. El resto está reservado para el SO y procesos del sistema. La utilización está limitada por el más restrictivo de los dos (CPU o memoria)."},
            {"title": "Configuración recomendada", "body": "Un tamaño de ejecutor alternativo (núcleos y memoria) que mejora el empaquetado y reduce el desperdicio para el mismo tipo de nodo. Los ahorros son estimaciones; los resultados reales dependen del comportamiento de la carga y del ajuste."},
        ]
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


def definitions_section(*, lang: str = "en") -> list[dict[str, str]]:
    """Glossary of terms for the report."""
    if _t(lang, "en", "es") == "es":
        return [
            {"term": "Nodo", "definition": "Una máquina de trabajo en el clúster (p. ej. una instancia EC2 o Dataproc). Cada nodo tiene un número fijo de vCPUs y memoria (GB) disponibles para ejecutores tras las reservas."},
            {"term": "Ejecutor", "definition": "Un proceso ejecutor de Spark en un nodo. Cada ejecutor se configura con un número fijo de núcleos y memoria (GB). Varios ejecutores pueden ejecutarse en un solo nodo."},
            {"term": "Empaquetado", "definition": "Proceso de colocar ejecutores en nodos. Ejecutores por nodo = mín(floor(núcleos_disponibles / núcleos_ejecutor), floor(memoria_disponible / memoria_ejecutor)), tras reservar capacidad para el SO. El recurso limitante (CPU o memoria) determina el resultado."},
            {"term": "Desperdicio", "definition": "Capacidad no utilizada: CPU o memoria sobrante tras el empaquetado, o capacidad asignada a ejecutores pero no utilizada por la carga. Se reporta como fracción (0–1) y como costo mensual estimado (costo de desperdicio)."},
            {"term": "Puntuación de eficiencia", "definition": "Puntuación 0–100 derivada de la fracción de desperdicio: 100 × (1 − desperdicio). Mayor significa mejor uso de la capacidad del nodo; puntuaciones bajas indican mal ajuste entre tamaño del ejecutor y nodo."},
            {"term": "Reserva (núcleos y memoria)", "definition": "Capacidad reservada en cada nodo para el SO, daemons y procesos del sistema. Esta capacidad no se usa para ejecutores y se excluye de los cálculos de empaquetado."},
            {"term": "Factor de utilización", "definition": "Multiplicador opcional (0–1) para clústeres compartidos que representa la fracción de tiempo que el clúster se usa para esta carga. Se aplica para escalar el costo diario cuando el clúster ejecuta otros trabajos."},
        ]
    return [
        {"term": "Node", "definition": "A worker machine in the cluster (e.g. an EC2 or Dataproc instance). Each node has a fixed number of vCPUs and memory (GB) available for executors after reserves."},
        {"term": "Executor", "definition": "A Spark executor process running on a node. Each executor is configured with a fixed number of cores and memory (GB). Multiple executors can run on a single node."},
        {"term": "Packing", "definition": "The process of fitting executors onto nodes. Executors per node = min(floor(available_cores / executor_cores), floor(available_memory / executor_memory)), after reserving capacity for the OS. The limiting resource (CPU or memory) determines the result."},
        {"term": "Waste", "definition": "Unused capacity: CPU or memory left over after packing, or capacity allocated to executors but not fully utilized by the workload. Reported as a fraction (0–1) and as an estimated monthly cost (waste cost)."},
        {"term": "Efficiency score", "definition": "A 0–100 score derived from the waste fraction: 100 × (1 − waste). Higher means better use of node capacity; low scores indicate poor fit between executor size and node."},
        {"term": "Reserve (cores and memory)", "definition": "Capacity reserved on each node for the OS, daemons, and system processes. This capacity is not used for executors and is excluded from packing calculations."},
        {"term": "Utilization factor", "definition": "An optional multiplier (0–1) for shared clusters, representing the fraction of time the cluster is used for this workload. Applied to scale daily cost when the cluster runs other jobs."},
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
    *,
    lang: str = "en",
) -> list[str]:
    """2–5 bullet engineering notes."""
    is_es = _t(lang, "en", "es") == "es"
    notes = []
    if packing.cpu_waste > packing.mem_waste:
        notes.append(_t(lang,
            "CPU is the dominant constraint; consider increasing executor cores or reducing cores per executor to pack more executors per node.",
            "CPU es la restricción dominante; considere aumentar los núcleos del ejecutor o reducir núcleos por ejecutor para empaquetar más ejecutores por nodo."))
    else:
        notes.append(_t(lang,
            "Memory is the dominant constraint; consider increasing executor memory or reducing memory per executor to improve packing.",
            "La memoria es la restricción dominante; considere aumentar la memoria del ejecutor o reducir memoria por ejecutor para mejorar el empaquetado."))
    if packing.executors_per_node == 0:
        notes.append(_t(lang,
            "Current configuration yields zero executors per node; executor size exceeds available node resources. Adjust executor cores/memory or node size.",
            "La configuración actual produce cero ejecutores por nodo; el tamaño del ejecutor excede los recursos del nodo. Ajuste núcleos/memoria del ejecutor o tamaño del nodo."))
    if recommendation and recommendation.savings_vs_current_monthly_usd > 0:
        if is_es:
            notes.append(f"La configuración recomendada podría reducir el costo de desperdicio mensual en aproximadamente ${recommendation.savings_vs_current_monthly_usd:,.2f}.")
        else:
            notes.append(f"Recommended configuration could reduce monthly waste cost by approximately ${recommendation.savings_vs_current_monthly_usd:,.2f}.")
    notes.append(_t(lang,
        "Estimates are directional and depend on actual workload behavior, I/O, and shuffle patterns.",
        "Las estimaciones son orientativas y dependen del comportamiento real de la carga, I/O y patrones de shuffle."))
    notes.extend(risk_notes[:3])
    return notes[:5]


REPORT_VERSION = "0.1.0"


def report_metadata(*, lang: str = "en") -> dict:
    """Doc name and date for footer."""
    if _t(lang, "en", "es") == "es":
        return {
            "doc_name": "KPI99 ICEA — Informe de Costos y Eficiencia de Infraestructura",
            "doc_name_short": "Informe KPI99 ICEA",
            "date": _format_date_es(date.today()),
            "brand": "KPI99",
            "tagline": "Rendimiento. Escala. Confiabilidad — Diseñado.",
            "copyright": "© 2026 KPI99 LLC. Todos los derechos reservados.",
            "report_version": REPORT_VERSION,
        }
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
    *,
    lang: str = "en",
) -> list[str]:
    """3–5 concrete next steps / action items."""
    is_es = _t(lang, "en", "es") == "es"
    steps = [
        _t(lang,
           "Try the recommended executor configuration in a staging or dev environment before rolling out to production.",
           "Pruebe la configuración de ejecutor recomendada en un entorno de staging o desarrollo antes de llevarla a producción."),
        _t(lang,
           "Capture peak executor memory from Spark UI (Executor → Storage Memory / peak) and re-run the analysis for better OOM and sizing guidance.",
           "Capture la memoria pico del ejecutor desde Spark UI (Executor → Storage Memory / peak) y vuelva a ejecutar el análisis para mejor guía de OOM y dimensionamiento."),
        _t(lang,
           "Re-run this analysis after significant workload changes (e.g. new jobs, more data, different runtime).",
           "Vuelva a ejecutar este análisis tras cambios significativos en la carga (p. ej. nuevos trabajos, más datos, distinto tiempo de ejecución)."),
    ]
    if recommendation and recommendation.savings_vs_current_monthly_usd > 0:
        if is_es:
            steps.append(f"Compare los tiempos de ejecución y la estabilidad con la config recomendada ({recommendation.executor_cores} núcleos, {recommendation.executor_memory_gb:.0f} GB por ejecutor) para validar ahorros.")
        else:
            steps.append(f"Compare job runtimes and stability with the recommended config ({recommendation.executor_cores} cores, {recommendation.executor_memory_gb:.0f} GB per executor) to validate savings.")
    if packing.efficiency_score < 60:
        steps.append(_t(lang,
            "Review node type vs executor size: consider a different instance family or executor dimensions to improve packing.",
            "Revise el tipo de nodo frente al tamaño del ejecutor: considere una familia de instancia distinta o dimensiones del ejecutor para mejorar el empaquetado."))
    return steps[:5]


def _mitigation_for_risk(note: str, *, lang: str = "en") -> str:
    """One-line mitigation for a risk note (keyword-based)."""
    is_es = _t(lang, "en", "es") == "es"
    note_lower = note.lower()
    if "oom" in note_lower or "memory" in note_lower and "executor" in note_lower:
        return _t(lang, "Increase executor memory or add peak executor memory from Spark UI for precise sizing.", "Aumente la memoria del ejecutor o agregue la memoria pico desde Spark UI para un dimensionamiento preciso.")
    if "shuffle" in note_lower or "spill" in note_lower:
        return _t(lang, "Increase executor memory or reduce shuffle/data per task; consider partition count alignment.", "Aumente la memoria del ejecutor o reduzca shuffle/datos por tarea; considere alinear el número de particiones.")
    if "spot" in note_lower or "preemptible" in note_lower:
        return _t(lang, "Use spot for non-critical workloads; consider mixed OD/spot or capacity reservations for critical paths.", "Use spot para cargas no críticas; considere OD/spot mixto o reservas de capacidad para rutas críticas.")
    if "skew" in note_lower:
        return _t(lang, "Repartition or salt keys to balance load; monitor tail tasks in Spark UI.", "Redistribuya o use salt en claves para equilibrar la carga; monitoree las tareas finales en Spark UI.")
    if "partition" in note_lower and "cores" in note_lower:
        return _t(lang, "Align partition count with total executor cores (e.g. 1–4× cores) or scale cluster/partitions.", "Alinee el número de particiones con el total de núcleos de ejecutores (p. ej. 1–4× núcleos) o escale clúster/particiones.")
    if "concurrent" in note_lower or "utilization factor" in note_lower:
        return _t(lang, "Set utilization factor in the analysis to reflect shared cluster usage.", "Configure el factor de utilización en el análisis para reflejar el uso del clúster compartido.")
    if "executors per node" in note_lower or "scheduling" in note_lower or "gc" in note_lower:
        return _t(lang, "Reduce executors per node or executor size to lower scheduling/GC overhead.", "Reduzca ejecutores por nodo o tamaño del ejecutor para reducir la sobrecarga de planificación/GC.")
    return _t(lang, "Review recommendation and workload tuning; consider Tier 2 Expert analysis for custom optimization.", "Revise la recomendación y el ajuste de la carga; considere el análisis Tier 2 Experto para optimización personalizada.")


def risks_mitigations_section(risk_notes: list[str], *, lang: str = "en") -> list[dict[str, str]]:
    """Risks with one-line mitigations for report."""
    return [{"risk": r, "mitigation": _mitigation_for_risk(r, lang=lang)} for r in risk_notes]


def cost_breakdown_sentence(cost: CostResult, *, lang: str = "en") -> str:
    """One sentence: monthly cost from node×usage and estimated waste."""
    total_monthly = cost.daily_cost_usd * 30
    waste = cost.waste_cost_monthly_usd
    if _t(lang, "en", "es") == "es":
        return f"El costo mensual es aproximadamente ${total_monthly:,.2f} por precio de nodo × uso; ${waste:,.2f} de ello es desperdicio estimado (capacidad asignada no utilizada)."
    return (
        f"Monthly cost is approximately ${total_monthly:,.2f} from node price × usage; "
        f"${waste:,.2f} of that is estimated waste (unused allocated capacity)."
    )


def methodology_section(*, lang: str = "en") -> list[dict[str, str]]:
    """How this was calculated: packing, cost, waste (for Methodology section)."""
    if _t(lang, "en", "es") == "es":
        return [
            {"title": "Empaquetado (ejecutores por nodo)", "body": "Ejecutores por nodo = mín(floor((node_cores − reserve_cores) / executor_cores), floor((node_memory_gb − reserve_memory) / executor_memory_gb)). Puntuación de eficiencia = 100 × (1 − desperdicio), donde desperdicio = máx(desperdicio CPU, desperdicio memoria) tras el empaquetado. Implementación: icea/packing.py."},
            {"title": "Costo (diario y mensual)", "body": "Costo horario del clúster = node_hourly_price × node_count. Costo diario = hourly_cluster_cost × (avg_runtime_minutes / 60) × jobs_per_day × utilization_factor. Costo mensual ≈ daily_cost × 30. Implementación: icea/cost_model.py."},
            {"title": "Desperdicio y costo de desperdicio", "body": "El desperdicio es la fracción no utilizada de la capacidad asignada (CPU o memoria sobrante tras el empaquetado, o capacidad no utilizada por la carga). Costo de desperdicio = daily_cost × waste × 30."},
        ]
    return [
        {"title": "Packing (executors per node)", "body": "Executors per node = min(floor((node_cores − reserve_cores) / executor_cores), floor((node_memory_gb − reserve_memory) / executor_memory_gb)). Efficiency score = 100 × (1 − waste), where waste = max(CPU waste, memory waste) after packing. Implementation: icea/packing.py."},
        {"title": "Cost (daily and monthly)", "body": "Hourly cluster cost = node_hourly_price × node_count. Daily cost = hourly_cluster_cost × (avg_runtime_minutes / 60) × jobs_per_day × utilization_factor. Monthly cost ≈ daily_cost × 30. Implementation: icea/cost_model.py."},
        {"title": "Waste and waste cost", "body": "Waste is the unused fraction of allocated capacity (CPU or memory left over after packing, or capacity not fully utilized by the workload). Waste cost = daily_cost × waste × 30."},
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


def data_quality_note(req: AnalyzeRequest, *, lang: str = "en") -> dict:
    """What optional inputs were used; suggest adding peak executor memory."""
    is_es = _t(lang, "en", "es") == "es"
    used = []
    w = req.workload
    labels = [
        ("partition count", "cantidad de particiones"),
        ("input data (GB)", "datos de entrada (GB)"),
        ("concurrent jobs", "trabajos concurrentes"),
        ("peak executor memory (GB)", "memoria pico del ejecutor (GB)"),
        ("shuffle read/write", "lectura/escritura shuffle"),
        ("data skew", "sesgo de datos"),
        ("spot %", "spot %"),
        ("utilization factor", "factor de utilización"),
    ]
    if getattr(w, "partition_count", None) is not None:
        used.append(labels[0][1] if is_es else labels[0][0])
    if getattr(w, "input_data_gb", None) is not None:
        used.append(labels[1][1] if is_es else labels[1][0])
    if getattr(w, "concurrent_jobs", None) is not None:
        used.append(labels[2][1] if is_es else labels[2][0])
    if getattr(w, "peak_executor_memory_gb", None) is not None:
        used.append(labels[3][1] if is_es else labels[3][0])
    if getattr(w, "shuffle_read_mb", None) is not None or getattr(w, "shuffle_write_mb", None) is not None:
        used.append(labels[4][1] if is_es else labels[4][0])
    if getattr(w, "data_skew", None) is not None:
        used.append(labels[5][1] if is_es else labels[5][0])
    if getattr(w, "spot_pct", None) is not None:
        used.append(labels[6][1] if is_es else labels[6][0])
    if getattr(req, "utilization_factor", None) is not None:
        used.append(labels[7][1] if is_es else labels[7][0])
    suggest_peak = getattr(w, "peak_executor_memory_gb", None) is None
    return {
        "optional_inputs_used": used,
        "suggest_peak_executor_memory": suggest_peak,
    }


def tier_cta_section(*, lang: str = "en") -> str:
    """One line CTA for Tier 2/3."""
    return _t(lang,
        "For a detailed review and custom optimization plan, request Tier 2 Expert or Tier 3 Enterprise analysis (contact via your KPI99 account or the ICEA app).",
        "Para una revisión detallada y un plan de optimización personalizado, solicite el análisis Tier 2 Experto o Tier 3 Empresa (contacto vía su cuenta KPI99 o la app ICEA).")


def benchmark_context(efficiency_score: int, *, lang: str = "en") -> dict:
    """Efficiency score vs typical bands: below / in line / above."""
    is_es = _t(lang, "en", "es") == "es"
    if efficiency_score >= 80:
        band = "above"
        text = _t(lang, "above typical for similar clusters", "por encima de lo típico para clústeres similares")
    elif efficiency_score >= 50:
        band = "typical"
        text = _t(lang, "in line with typical for similar clusters", "acorde con lo típico para clústeres similares")
    else:
        band = "below"
        text = _t(lang, "below typical for similar clusters", "por debajo de lo típico para clústeres similares")
    return {"band": band, "text": text, "efficiency_score": efficiency_score}


def rerun_cta(app_url: str | None, *, lang: str = "en") -> str:
    """Re-run CTA with app URL if provided."""
    if app_url:
        return _t(lang, f"Update your configuration and re-run the analysis at {app_url.rstrip('/')}.", f"Actualice su configuración y vuelva a ejecutar el análisis en {app_url.rstrip('/')}.")
    return _t(lang, "Update your configuration and re-run the analysis in your ICEA dashboard.", "Actualice su configuración y vuelva a ejecutar el análisis en su panel ICEA.")


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
