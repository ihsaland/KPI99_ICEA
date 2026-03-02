"""KPI99-branded HTML report (same content as PDF, with optional PDF download link)."""
from pathlib import Path

from icea.models import AnalyzeRequest, PackingResult, CostResult, RecommendedConfig
from icea.report.templates import (
    executive_summary,
    executive_summary_narrative,
    explanations_section,
    definitions_section,
    current_vs_recommended,
    cost_assumptions,
    savings_section,
    engineering_notes,
    report_metadata,
    forecast_data,
    utilization_chart_data,
    cost_breakdown_chart_data,
    next_steps_section,
    risks_mitigations_section,
    cost_breakdown_sentence,
    methodology_section,
    sensitivity_section,
    data_quality_note,
    tier_cta_section,
    benchmark_context,
    rerun_cta,
)
from icea.report.constants import (
    KPI99_DARK,
    KPI99_MUTED,
    KPI99_ACCENT,
    KPI99_ACCENT_DARK,
    KPI99_GRID,
    KPI99_SUCCESS,
    KPI99_WARN,
    KPI99_WHITE,
)


# Section titles and labels by language (en / es)
_SECTIONS = {
    "en": {
        "report_title": "Infrastructure Cost & Efficiency Report",
        "exec_summary": "Executive Summary",
        "summary_score": "Efficiency score",
        "summary_waste": "Estimated monthly waste",
        "summary_recommended": "Recommended configuration (executor: {} cores, {} GB) could save approximately",
        "your_score": "Your efficiency score ({}/100) is {}.",
        "current_vs_rec": "Current vs Recommended Configuration",
        "current": "Current",
        "recommended": "Recommended",
        "executor_cores": "Executor cores",
        "executor_memory_gb": "Executor memory (GB)",
        "executors_per_node": "Executors per node",
        "efficiency_score": "Efficiency score",
        "resource_util": "Resource Utilization",
        "cpu": "CPU",
        "memory": "Memory",
        "cost_breakdown": "Cost Breakdown (Monthly)",
        "total_mo": "Total ${}/mo (utilized vs waste)",
        "potential_savings": "Potential savings ${}/mo",
        "cost_assumptions": "Cost Model Assumptions",
        "input": "Input",
        "value": "Value",
        "savings_proj": "Savings Projection",
        "waste_daily": "Waste cost (daily)",
        "waste_monthly": "Waste cost (monthly)",
        "proj_savings_monthly": "Projected savings (monthly)",
        "estimates_note": "Estimates are directional and depend on workload behavior.",
        "forecast_title": "Cost & Savings Forecast",
        "forecast_subtitle": "Projected monthly costs over {} months{}.",
        "month": "Month",
        "current_usd": "Current (USD)",
        "recommended_usd": "Recommended (USD)",
        "savings_usd": "Savings (USD)",
        "engineering_notes": "Engineering Notes",
        "next_steps": "Next Steps",
        "risks_mitigations": "Risks & Mitigations",
        "risk": "Risk",
        "mitigation": "Mitigation",
        "methodology": "Methodology",
        "how_calculated": "How this was calculated.",
        "sensitivity": "Sensitivity (What-If)",
        "sensitivity_intro": "If node count or runtime changes, estimated monthly cost scales roughly as follows:",
        "current_label": "Current",
        "if_nodes_plus": "If node count → {}",
        "if_nodes_minus": "If node count → {}",
        "if_runtime_20": "If avg runtime +20%",
        "data_quality": "Data Quality & Inputs",
        "optional_used": "Optional inputs used in this run: {}.",
        "no_optional": "No optional workload inputs were provided.",
        "suggest_peak": " Adding peak executor memory from Spark UI will improve OOM and sizing guidance.",
        "further_analysis": "Further Analysis",
        "understanding": "Understanding This Report",
        "understanding_intro": "Brief explanations of the metrics and terms used in this report.",
        "definitions": "Definitions",
        "definitions_intro": "Key terms used in this report.",
        "term": "Term",
        "definition": "Definition",
        "download_pdf": "Download PDF",
        "report_version_footer": "ICEA report v{}. {}",
    },
    "es": {
        "report_title": "Informe de Costos y Eficiencia de Infraestructura",
        "exec_summary": "Resumen ejecutivo",
        "summary_score": "Puntuación de eficiencia",
        "summary_waste": "Desperdicio mensual estimado",
        "summary_recommended": "La configuración recomendada (ejecutor: {} núcleos, {} GB) podría ahorrar aproximadamente",
        "your_score": "Su puntuación de eficiencia ({}/100) está {}.",
        "current_vs_rec": "Configuración actual vs recomendada",
        "current": "Actual",
        "recommended": "Recomendada",
        "executor_cores": "Núcleos por ejecutor",
        "executor_memory_gb": "Memoria del ejecutor (GB)",
        "executors_per_node": "Ejecutores por nodo",
        "efficiency_score": "Puntuación de eficiencia",
        "resource_util": "Utilización de recursos",
        "cpu": "CPU",
        "memory": "Memoria",
        "cost_breakdown": "Desglose de costos (mensual)",
        "total_mo": "Total ${}/mes (utilizado vs desperdicio)",
        "potential_savings": "Ahorro potencial ${}/mes",
        "cost_assumptions": "Supuestos del modelo de costos",
        "input": "Entrada",
        "value": "Valor",
        "savings_proj": "Proyección de ahorros",
        "waste_daily": "Costo de desperdicio (diario)",
        "waste_monthly": "Costo de desperdicio (mensual)",
        "proj_savings_monthly": "Ahorro proyectado (mensual)",
        "estimates_note": "Las estimaciones son orientativas y dependen del comportamiento de la carga.",
        "forecast_title": "Pronóstico de costos y ahorros",
        "forecast_subtitle": "Costos mensuales proyectados a {} meses{}.",
        "month": "Mes",
        "current_usd": "Actual (USD)",
        "recommended_usd": "Recomendada (USD)",
        "savings_usd": "Ahorro (USD)",
        "engineering_notes": "Notas de ingeniería",
        "next_steps": "Próximos pasos",
        "risks_mitigations": "Riesgos y mitigaciones",
        "risk": "Riesgo",
        "mitigation": "Mitigación",
        "methodology": "Metodología",
        "how_calculated": "Cómo se calculó.",
        "sensitivity": "Sensibilidad (qué pasaría si)",
        "sensitivity_intro": "Si cambia el número de nodos o el tiempo de ejecución, el costo mensual estimado escala aproximadamente así:",
        "current_label": "Actual",
        "if_nodes_plus": "Si número de nodos → {}",
        "if_nodes_minus": "Si número de nodos → {}",
        "if_runtime_20": "Si tiempo promedio +20%",
        "data_quality": "Calidad de datos e entradas",
        "optional_used": "Entradas opcionales usadas en esta ejecución: {}.",
        "no_optional": "No se proporcionaron entradas opcionales de carga.",
        "suggest_peak": " Agregar la memoria pico del ejecutor desde Spark UI mejorará la guía de OOM y dimensionamiento.",
        "further_analysis": "Análisis adicional",
        "understanding": "Comprender este informe",
        "understanding_intro": "Breves explicaciones de las métricas y términos usados en este informe.",
        "definitions": "Definiciones",
        "definitions_intro": "Términos clave usados en este informe.",
        "term": "Término",
        "definition": "Definición",
        "download_pdf": "Descargar PDF",
        "report_version_footer": "Informe ICEA v{}. {}",
    },
}


def _bar_css(util_pct: float, waste_pct: float, bar_w: str = "200px") -> str:
    return (
        f"width:{bar_w};height:20px;background:linear-gradient(to right, {KPI99_SUCCESS} 0%, {KPI99_SUCCESS} {util_pct}%, "
        f"{KPI99_WARN} {util_pct}%, {KPI99_WARN} 100%);border-radius:4px;"
    )


def generate_report_html(
    req: AnalyzeRequest,
    packing: PackingResult,
    cost: CostResult,
    recommendation: RecommendedConfig | None,
    risk_notes: list[str] | None = None,
    *,
    pdf_download_url: str | None = None,
    pdf_download_via_post: bool = False,
    request_payload_json: str | None = None,
    app_url: str | None = None,
    lang: str = "en",
) -> str:
    """
    Generate KPI99-branded HTML report. Same sections as PDF.
    pdf_download_url: direct link for PDF (e.g. /v1/report-paid?token=xxx&format=pdf).
    If pdf_download_via_post and request_payload_json are set, the page will have a button
    that POSTs the payload to /v1/report to get the PDF. lang=en|es for report language.
    """
    risk_notes = risk_notes or []
    lang = (lang or "en").strip().lower()
    if lang not in ("en", "es"):
        lang = "en"
    meta = report_metadata(lang=lang)
    ex = executive_summary(packing, cost, recommendation)
    exec_narrative = executive_summary_narrative(req, packing, cost, recommendation, lang=lang)
    explanations = explanations_section(lang=lang)
    definitions = definitions_section(lang=lang)
    cvr = current_vs_recommended(req, packing, recommendation)
    ca = cost_assumptions(req, cost)
    sav = savings_section(cost, recommendation)
    ucd = utilization_chart_data(packing)
    cbd = cost_breakdown_chart_data(cost, recommendation)
    notes = engineering_notes(packing, recommendation, risk_notes, lang=lang)
    forecast = forecast_data(req, cost, recommendation)
    next_steps = next_steps_section(req, packing, recommendation, lang=lang)
    risks_mitigations = risks_mitigations_section(risk_notes, lang=lang)
    cost_sentence = cost_breakdown_sentence(cost, lang=lang)
    methodology = methodology_section(lang=lang)
    sensitivity = sensitivity_section(req, cost, recommendation)
    data_quality = data_quality_note(req, lang=lang)
    tier_cta = tier_cta_section(lang=lang)
    benchmark = benchmark_context(packing.efficiency_score, lang=lang)
    rerun = rerun_cta(app_url, lang=lang)

    css = f"""
    :root {{ --kpi-dark: {KPI99_DARK}; --kpi-muted: {KPI99_MUTED}; --kpi-accent: {KPI99_ACCENT}; --kpi-accent-dark: {KPI99_ACCENT_DARK}; --kpi-grid: {KPI99_GRID}; --kpi-success: {KPI99_SUCCESS}; --kpi-warn: {KPI99_WARN}; }}
    * {{ box-sizing: border-box; }}
    body {{ font-family: 'Manrope', -apple-system, BlinkMacSystemFont, sans-serif; color: {KPI99_DARK}; background: #f8fafc; margin: 0; padding: 24px; line-height: 1.5; }}
    .report {{ max-width: 800px; margin: 0 auto; background: {KPI99_WHITE}; padding: 40px; border-radius: 12px; box-shadow: 0 1px 3px rgba(0,0,0,.08); }}
    .header {{ display: flex; align-items: center; gap: 12px; margin-bottom: 24px; padding-bottom: 16px; border-bottom: 1px solid {KPI99_GRID}; }}
    .header img.favicon {{ width: 32px; height: 32px; }}
    .header .brand {{ font-weight: 700; color: {KPI99_DARK}; }}
    .header .tagline {{ font-size: 0.85rem; color: {KPI99_MUTED}; }}
    h1 {{ font-size: 1.75rem; color: {KPI99_DARK}; margin: 0 0 4px 0; }}
    .subtitle {{ font-size: 0.9rem; color: {KPI99_MUTED}; margin-bottom: 16px; line-height: 1.5; }}
    .section-intro {{ font-size: 0.9rem; color: {KPI99_MUTED}; margin: -4px 0 16px 0; line-height: 1.5; }}
    h2 {{ font-size: 1.25rem; font-weight: 700; color: {KPI99_ACCENT_DARK}; margin: 32px 0 0 0; padding-bottom: 8px; border-bottom: 2px solid {KPI99_ACCENT}; letter-spacing: 0.02em; }}
    h2:first-of-type {{ margin-top: 0; }}
    h3 {{ font-size: 1rem; font-weight: 600; color: {KPI99_DARK}; margin: 18px 0 6px 0; }}
    p {{ margin: 0 0 8px 0; color: {KPI99_DARK}; }}
    table {{ width: 100%; border-collapse: collapse; margin: 12px 0; font-size: 0.9rem; }}
    th, td {{ border: 1px solid {KPI99_GRID}; padding: 10px 12px; text-align: left; vertical-align: top; }}
    th {{ background: {KPI99_ACCENT_DARK}; color: white; font-weight: 600; }}
    tr:nth-child(even) {{ background: #f8fafc; }}
    table.definitions-table {{ margin-top: 8px; }}
    table.definitions-table td:first-child {{ font-weight: 600; color: {KPI99_ACCENT_DARK}; width: 28%; min-width: 140px; }}
    table.definitions-table td:last-child {{ line-height: 1.5; }}
    .bar-wrap {{ margin: 8px 0; }}
    .bar {{ height: 20px; border-radius: 4px; margin: 4px 0; }}
    .bar-label {{ font-size: 0.85rem; color: {KPI99_MUTED}; margin-bottom: 2px; }}
    .footer {{ margin-top: 32px; padding-top: 16px; border-top: 1px solid {KPI99_GRID}; font-size: 0.8rem; color: {KPI99_MUTED}; }}
    .btn-pdf {{ display: inline-block; background: {KPI99_ACCENT}; color: white; padding: 10px 20px; border-radius: 8px; text-decoration: none; font-weight: 600; margin-top: 16px; border: none; cursor: pointer; font-size: 1rem; }}
    .btn-pdf:hover {{ background: {KPI99_ACCENT_DARK}; }}
    ul {{ margin: 8px 0; padding-left: 20px; }}
    li {{ margin: 4px 0; color: {KPI99_DARK}; }}
    """

    L = _SECTIONS[lang]
    summary_text = (
        f"{L['summary_score']}: <strong>{ex['efficiency_score']}/100</strong>. "
        f"{L['summary_waste']}: <strong>${ex['waste_cost_monthly_usd']:,.2f}</strong>."
    )
    if ex["has_recommendation"] and ex["savings_monthly"] > 0:
        rec_msg = L["summary_recommended"].format(ex["recommended_cores"], ex["recommended_memory_gb"])
        summary_text += f" {rec_msg} <strong>${ex['savings_monthly']:,.2f}/month</strong>."
    explanations_html = "".join(
        f"<h3>{e['title']}</h3><p>{e['body']}</p>" for e in explanations
    )
    definitions_html = "".join(
        f"<tr><td><strong>{d['term']}</strong></td><td>{d['definition']}</td></tr>" for d in definitions
    )

    # Current vs recommended table
    cvr_rows = [
        f"<tr><th></th><th>{L['current']}</th><th>{L['recommended']}</th></tr>" if cvr["recommended"] else f"<tr><th></th><th>{L['current']}</th><th>—</th></tr>",
        f"<tr><td>{L['executor_cores']}</td><td>{cvr['current']['executor_cores']}</td><td>{cvr['recommended']['executor_cores'] if cvr['recommended'] else '—'}</td></tr>",
        f"<tr><td>{L['executor_memory_gb']}</td><td>{cvr['current']['executor_memory_gb']}</td><td>{cvr['recommended']['executor_memory_gb'] if cvr['recommended'] else '—'}</td></tr>",
        f"<tr><td>{L['executors_per_node']}</td><td>{cvr['current']['executors_per_node']}</td><td>{cvr['recommended']['executors_per_node'] if cvr['recommended'] else '—'}</td></tr>",
        f"<tr><td>{L['efficiency_score']}</td><td>{cvr['current']['efficiency_score']}</td><td>{cvr['recommended']['efficiency_score'] if cvr['recommended'] else '—'}</td></tr>",
    ]
    cvr_table = "<table><tbody>" + "\n".join(cvr_rows) + "</tbody></table>"

    # Cost assumptions table
    assump_rows = []
    for k in ["cloud", "region", "instance_type", "node_cores", "node_memory_gb", "node_hourly_cost_usd", "node_count",
              "avg_runtime_minutes", "min_runtime_minutes", "max_runtime_minutes", "jobs_per_day", "partition_count",
              "input_data_gb", "concurrent_jobs", "peak_executor_memory_gb",
              "shuffle_read_mb", "shuffle_write_mb", "data_skew", "spot_pct", "autoscale_min_nodes", "autoscale_max_nodes",
              "utilization_factor", "hourly_cluster_cost_usd", "daily_cost_usd"]:
        if k not in ca or ca[k] is None:
            continue
        v = ca[k]
        if isinstance(v, float) and k in ("node_hourly_cost_usd", "hourly_cluster_cost_usd", "daily_cost_usd", "utilization_factor", "spot_pct"):
            disp = f"{v:.0%}" if k == "utilization_factor" else (f"{v:.0f}%" if k == "spot_pct" else f"{v:.2f}")
        else:
            disp = str(v)
        assump_rows.append(f"<tr><td>{k.replace('_', ' ').title()}</td><td>{disp}</td></tr>")
    assump_table = f"<table><tbody><tr><th>{L['input']}</th><th>{L['value']}</th></tr>" + "\n".join(assump_rows) + "</tbody></table>"

    # Savings table
    sav_rows = [
        f"<tr><td>{L['waste_daily']}</td><td>{sav['waste_cost_daily_usd']:,.2f}</td></tr>",
        f"<tr><td>{L['waste_monthly']}</td><td>{sav['waste_cost_monthly_usd']:,.2f}</td></tr>",
    ]
    if sav["has_recommendation"]:
        sav_rows.append(f"<tr><td>{L['proj_savings_monthly']}</td><td>{sav['savings_monthly_usd']:,.2f}</td></tr>")
    sav_table = f"<table><tbody><tr><th>{L['input']}</th><th>USD</th></tr>" + "\n".join(sav_rows) + "</tbody></table>"

    # Forecast table
    forecast_html = ""
    if forecast:
        gr = getattr(req, "growth_rate_pct", None)
        growth_note = f" (growth {gr:.1f}%/year)" if gr is not None and gr != 0 else ""
        if lang == "es":
            growth_note = f" (crecimiento {gr:.1f}%/año)" if gr is not None and gr != 0 else ""
        forecast_html = f"<h3>{L['forecast_title']}</h3><p class='subtitle'>{L['forecast_subtitle'].format(len(forecast), growth_note)}</p><table><tbody><tr><th>{L['month']}</th><th>{L['current_usd']}</th><th>{L['recommended_usd']}</th><th>{L['savings_usd']}</th></tr>"
        for row in forecast[:24]:
            forecast_html += f"<tr><td>{row['month']}</td><td>{row['current_usd']:,.2f}</td><td>{row['recommended_usd']:,.2f}</td><td>{row['savings_usd']:,.2f}</td></tr>"
        forecast_html += "</tbody></table>"

    # Download PDF block
    pdf_block = ""
    if pdf_download_url:
        pdf_block = f'<a href="{pdf_download_url}" class="btn-pdf" download="icea-report.pdf">{L["download_pdf"]}</a>'
    elif pdf_download_via_post and request_payload_json:
        esc = request_payload_json.replace("\\", "\\\\").replace("</script>", "<\\/script>").replace("'", "\\'")
        pdf_block = f'''<button type="button" class="btn-pdf" id="icea-download-pdf">{L["download_pdf"]}</button>
<script>
(function() {{
  var payload = '{esc}';
  document.getElementById('icea-download-pdf').onclick = function() {{
    fetch('/v1/report', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: payload
    }}).then(function(r) {{
      if (!r.ok) throw new Error('Failed to generate PDF');
      return r.blob();
    }}).then(function(blob) {{
      var a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = 'icea-report.pdf';
      a.click();
      URL.revokeObjectURL(a.href);
    }}).catch(function(e) {{ alert(e.message || 'Download failed'); }});
  }};
}})();
</script>'''

    cpu_util = ucd["cpu_util_pct"]
    mem_util = ucd["mem_util_pct"]
    total_mo = cbd["total_monthly_usd"]
    waste_mo = cbd["waste_monthly_usd"]
    util_mo = cbd["utilized_monthly_usd"]
    save_mo = cbd["savings_monthly_usd"]
    waste_pct = (waste_mo / total_mo * 100) if total_mo else 0
    util_pct = 100 - waste_pct

    # Data quality text
    if data_quality["optional_inputs_used"]:
        dq_text = L["optional_used"].format(", ".join(data_quality["optional_inputs_used"]))
    else:
        dq_text = L["no_optional"]
    if data_quality["suggest_peak_executor_memory"]:
        dq_text += L["suggest_peak"]

    html = f"""<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{meta["doc_name_short"]} | {meta["brand"]}</title>
<link rel="icon" type="image/png" href="/favicon.png">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>{css}</style>
</head>
<body>
<div class="report">
<div class="header">
<img src="/favicon.png" alt="" class="favicon" width="32" height="32">
<div>
<div class="brand">{meta["brand"]}</div>
<div class="tagline">{meta["tagline"]}</div>
</div>
</div>
<h1>{L["report_title"]}</h1>
<p class="subtitle">{meta["date"]}</p>
{pdf_block}
<h2>{L["exec_summary"]}</h2>
<p>{exec_narrative}</p>
<p style="margin-top: 12px;">{summary_text}</p>
<p class="subtitle">{L['your_score'].format(benchmark['efficiency_score'], benchmark['text'])}</p>
<h2>{L["current_vs_rec"]}</h2>
{cvr_table}
<h2>{L["resource_util"]}</h2>
<div class="bar-wrap"><div class="bar-label">{L["cpu"]}</div><div class="bar" style="width: 100%; max-width: 300px; background: linear-gradient(to right, {KPI99_ACCENT} 0%, {KPI99_ACCENT} {cpu_util}%, {KPI99_GRID} {cpu_util}%, {KPI99_GRID} 100%);"></div><span>{cpu_util}%</span></div>
<div class="bar-wrap"><div class="bar-label">{L["memory"]}</div><div class="bar" style="width: 100%; max-width: 300px; background: linear-gradient(to right, {KPI99_SUCCESS} 0%, {KPI99_SUCCESS} {mem_util}%, {KPI99_GRID} {mem_util}%, {KPI99_GRID} 100%);"></div><span>{mem_util}%</span></div>
<h2>{L["cost_breakdown"]}</h2>
<p class="subtitle">{cost_sentence}</p>
<div class="bar-wrap"><div class="bar-label">{L['total_mo'].format(f'{total_mo:,.0f}')}</div><div class="bar" style="width: 100%; max-width: 300px; background: linear-gradient(to right, {KPI99_SUCCESS} 0%, {KPI99_SUCCESS} {util_pct}%, {KPI99_WARN} {util_pct}%, {KPI99_WARN} 100%);"></div></div>
{save_mo and f'<div class="bar-wrap"><div class="bar-label">{L["potential_savings"].format(f"{save_mo:,.0f}")}</div></div>' or ''}
<h2>{L["cost_assumptions"]}</h2>
{assump_table}
<h2>{L["savings_proj"]}</h2>
{sav_table}
<p class="subtitle">{L["estimates_note"]}</p>
{forecast_html}
<h2>{L["engineering_notes"]}</h2>
<ul>
{"".join(f"<li>{n}</li>" for n in notes)}
</ul>
<h2>{L["next_steps"]}</h2>
<ul>
{"".join(f"<li>{s}</li>" for s in next_steps)}
</ul>
{"<h2>" + L["risks_mitigations"] + "</h2><table><tbody><tr><th>" + L["risk"] + "</th><th>" + L["mitigation"] + "</th></tr>" + "".join(f"<tr><td>{rm['risk']}</td><td>{rm['mitigation']}</td></tr>" for rm in risks_mitigations) + "</tbody></table>" if risks_mitigations else ""}
<h2>{L["methodology"]}</h2>
<p class="section-intro">{L["how_calculated"]}</p>
{"".join(f"<h3>{m['title']}</h3><p>{m['body']}</p>" for m in methodology)}
<h2>{L["sensitivity"]}</h2>
<p class="subtitle">{L["sensitivity_intro"]}</p>
<ul>
<li>{L["current_label"]}: ${sensitivity["current_monthly_usd"]:,.2f}/month</li>
<li>{L["if_nodes_plus"].format(sensitivity["node_count"] + 1)}: ≈ ${sensitivity["if_nodes_plus_one_monthly_usd"]:,.2f}/month</li>
{f'<li>{L["if_nodes_minus"].format(sensitivity["node_count"] - 1)}: ≈ ${sensitivity["if_nodes_minus_one_monthly_usd"]:,.2f}/month</li>' if sensitivity.get("if_nodes_minus_one_monthly_usd") is not None else ""}
<li>{L["if_runtime_20"]}: ≈ ${sensitivity["if_runtime_plus_20_pct_monthly_usd"]:,.2f}/month</li>
</ul>
<h2>{L["data_quality"]}</h2>
<p class="subtitle">{dq_text}</p>
<h2>{L["further_analysis"]}</h2>
<p>{tier_cta}</p>
<p>{rerun}</p>
<h2>{L["understanding"]}</h2>
<p class="section-intro">{L["understanding_intro"]}</p>
{explanations_html}
<h2>{L["definitions"]}</h2>
<p class="section-intro">{L["definitions_intro"]}</p>
<table class="definitions-table"><tbody><tr><th>{L["term"]}</th><th>{L["definition"]}</th></tr>{definitions_html}</tbody></table>
<div class="footer">{L["report_version_footer"].format(meta["report_version"], meta["copyright"])}</div>
</div>
</body>
</html>"""
    return html
