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
) -> str:
    """
    Generate KPI99-branded HTML report. Same sections as PDF.
    pdf_download_url: direct link for PDF (e.g. /v1/report-paid?token=xxx&format=pdf).
    If pdf_download_via_post and request_payload_json are set, the page will have a button
    that POSTs the payload to /v1/report to get the PDF.
    """
    risk_notes = risk_notes or []
    meta = report_metadata()
    ex = executive_summary(packing, cost, recommendation)
    exec_narrative = executive_summary_narrative(req, packing, cost, recommendation)
    explanations = explanations_section()
    definitions = definitions_section()
    cvr = current_vs_recommended(req, packing, recommendation)
    ca = cost_assumptions(req, cost)
    sav = savings_section(cost, recommendation)
    ucd = utilization_chart_data(packing)
    cbd = cost_breakdown_chart_data(cost, recommendation)
    notes = engineering_notes(packing, recommendation, risk_notes)
    forecast = forecast_data(req, cost, recommendation)
    next_steps = next_steps_section(req, packing, recommendation)
    risks_mitigations = risks_mitigations_section(risk_notes)
    cost_sentence = cost_breakdown_sentence(cost)
    methodology = methodology_section()
    sensitivity = sensitivity_section(req, cost, recommendation)
    data_quality = data_quality_note(req)
    tier_cta = tier_cta_section()
    benchmark = benchmark_context(packing.efficiency_score)
    rerun = rerun_cta(app_url)

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
    .subtitle {{ font-size: 0.9rem; color: {KPI99_MUTED}; margin-bottom: 24px; }}
    h2 {{ font-size: 1.15rem; color: {KPI99_ACCENT_DARK}; margin: 24px 0 12px 0; }}
    h3 {{ font-size: 1rem; color: {KPI99_DARK}; margin: 16px 0 8px 0; }}
    p {{ margin: 0 0 8px 0; color: {KPI99_DARK}; }}
    table {{ width: 100%; border-collapse: collapse; margin: 12px 0; font-size: 0.9rem; }}
    th, td {{ border: 1px solid {KPI99_GRID}; padding: 10px 12px; text-align: left; }}
    th {{ background: {KPI99_ACCENT_DARK}; color: white; font-weight: 600; }}
    tr:nth-child(even) {{ background: #f8fafc; }}
    .bar-wrap {{ margin: 8px 0; }}
    .bar {{ height: 20px; border-radius: 4px; margin: 4px 0; }}
    .bar-label {{ font-size: 0.85rem; color: {KPI99_MUTED}; margin-bottom: 2px; }}
    .footer {{ margin-top: 32px; padding-top: 16px; border-top: 1px solid {KPI99_GRID}; font-size: 0.8rem; color: {KPI99_MUTED}; }}
    .btn-pdf {{ display: inline-block; background: {KPI99_ACCENT}; color: white; padding: 10px 20px; border-radius: 8px; text-decoration: none; font-weight: 600; margin-top: 16px; border: none; cursor: pointer; font-size: 1rem; }}
    .btn-pdf:hover {{ background: {KPI99_ACCENT_DARK}; }}
    ul {{ margin: 8px 0; padding-left: 20px; }}
    li {{ margin: 4px 0; color: {KPI99_DARK}; }}
    """

    summary_text = (
        f"Efficiency score: <strong>{ex['efficiency_score']}/100</strong>. "
        f"Estimated monthly waste: <strong>${ex['waste_cost_monthly_usd']:,.2f}</strong>."
    )
    if ex["has_recommendation"] and ex["savings_monthly"] > 0:
        summary_text += (
            f" Recommended configuration (executor: {ex['recommended_cores']} cores, "
            f"{ex['recommended_memory_gb']:.0f} GB) could save approximately "
            f"<strong>${ex['savings_monthly']:,.2f}/month</strong>."
        )
    explanations_html = "".join(
        f"<h3>{e['title']}</h3><p>{e['body']}</p>" for e in explanations
    )
    definitions_html = "".join(
        f"<tr><td><strong>{d['term']}</strong></td><td>{d['definition']}</td></tr>" for d in definitions
    )

    # Current vs recommended table
    cvr_rows = [
        "<tr><th></th><th>Current</th><th>Recommended</th></tr>" if cvr["recommended"] else "<tr><th></th><th>Current</th><th>—</th></tr>",
        f"<tr><td>Executor cores</td><td>{cvr['current']['executor_cores']}</td><td>{cvr['recommended']['executor_cores'] if cvr['recommended'] else '—'}</td></tr>",
        f"<tr><td>Executor memory (GB)</td><td>{cvr['current']['executor_memory_gb']}</td><td>{cvr['recommended']['executor_memory_gb'] if cvr['recommended'] else '—'}</td></tr>",
        f"<tr><td>Executors per node</td><td>{cvr['current']['executors_per_node']}</td><td>{cvr['recommended']['executors_per_node'] if cvr['recommended'] else '—'}</td></tr>",
        f"<tr><td>Efficiency score</td><td>{cvr['current']['efficiency_score']}</td><td>{cvr['recommended']['efficiency_score'] if cvr['recommended'] else '—'}</td></tr>",
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
    assump_table = "<table><tbody><tr><th>Input</th><th>Value</th></tr>" + "\n".join(assump_rows) + "</tbody></table>"

    # Savings table
    sav_rows = [
        "<tr><td>Waste cost (daily)</td><td>" + f"{sav['waste_cost_daily_usd']:,.2f}" + "</td></tr>",
        "<tr><td>Waste cost (monthly)</td><td>" + f"{sav['waste_cost_monthly_usd']:,.2f}" + "</td></tr>",
    ]
    if sav["has_recommendation"]:
        sav_rows.append(f"<tr><td>Projected savings (monthly)</td><td>{sav['savings_monthly_usd']:,.2f}</td></tr>")
    sav_table = "<table><tbody><tr><th>Metric</th><th>USD</th></tr>" + "\n".join(sav_rows) + "</tbody></table>"

    # Forecast table
    forecast_html = ""
    if forecast:
        gr = getattr(req, "growth_rate_pct", None)
        growth_note = f" (growth {gr:.1f}%/year)" if gr is not None and gr != 0 else ""
        forecast_html = f"<h3>Cost & Savings Forecast</h3><p class='subtitle'>Projected monthly costs over {len(forecast)} months{growth_note}.</p><table><tbody><tr><th>Month</th><th>Current (USD)</th><th>Recommended (USD)</th><th>Savings (USD)</th></tr>"
        for row in forecast[:24]:
            forecast_html += f"<tr><td>{row['month']}</td><td>{row['current_usd']:,.2f}</td><td>{row['recommended_usd']:,.2f}</td><td>{row['savings_usd']:,.2f}</td></tr>"
        forecast_html += "</tbody></table>"

    # Download PDF block
    pdf_block = ""
    if pdf_download_url:
        pdf_block = f'<a href="{pdf_download_url}" class="btn-pdf" download="icea-report.pdf">Download PDF</a>'
    elif pdf_download_via_post and request_payload_json:
        esc = request_payload_json.replace("\\", "\\\\").replace("</script>", "<\\/script>").replace("'", "\\'")
        pdf_block = f'''<button type="button" class="btn-pdf" id="icea-download-pdf">Download PDF</button>
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

    html = f"""<!DOCTYPE html>
<html lang="en">
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
<h1>Infrastructure Cost & Efficiency Report</h1>
<p class="subtitle">{meta["date"]}</p>
{pdf_block}
<h2>Executive Summary</h2>
<p>{exec_narrative}</p>
<p style="margin-top: 12px;">{summary_text}</p>
<p class="subtitle">Your efficiency score ({benchmark["efficiency_score"]}/100) is {benchmark["text"]}.</p>
<h2>Current vs Recommended Configuration</h2>
{cvr_table}
<h2>Resource Utilization</h2>
<div class="bar-wrap"><div class="bar-label">CPU</div><div class="bar" style="width: 100%; max-width: 300px; background: linear-gradient(to right, {KPI99_ACCENT} 0%, {KPI99_ACCENT} {cpu_util}%, {KPI99_GRID} {cpu_util}%, {KPI99_GRID} 100%);"></div><span>{cpu_util}%</span></div>
<div class="bar-wrap"><div class="bar-label">Memory</div><div class="bar" style="width: 100%; max-width: 300px; background: linear-gradient(to right, {KPI99_SUCCESS} 0%, {KPI99_SUCCESS} {mem_util}%, {KPI99_GRID} {mem_util}%, {KPI99_GRID} 100%);"></div><span>{mem_util}%</span></div>
<h2>Cost Breakdown (Monthly)</h2>
<p class="subtitle">{cost_sentence}</p>
<div class="bar-wrap"><div class="bar-label">Total ${total_mo:,.0f}/mo (utilized vs waste)</div><div class="bar" style="width: 100%; max-width: 300px; background: linear-gradient(to right, {KPI99_SUCCESS} 0%, {KPI99_SUCCESS} {util_pct}%, {KPI99_WARN} {util_pct}%, {KPI99_WARN} 100%);"></div></div>
{save_mo and f'<div class="bar-wrap"><div class="bar-label">Potential savings ${save_mo:,.0f}/mo</div></div>' or ''}
<h2>Cost Model Assumptions</h2>
{assump_table}
<h2>Savings Projection</h2>
{sav_table}
<p class="subtitle">Estimates are directional and depend on workload behavior.</p>
{forecast_html}
<h2>Engineering Notes</h2>
<ul>
{"".join(f"<li>{n}</li>" for n in notes)}
</ul>
<h2>Next Steps</h2>
<ul>
{"".join(f"<li>{s}</li>" for s in next_steps)}
</ul>
{"<h2>Risks & Mitigations</h2><table><tbody><tr><th>Risk</th><th>Mitigation</th></tr>" + "".join(f"<tr><td>{rm['risk']}</td><td>{rm['mitigation']}</td></tr>" for rm in risks_mitigations) + "</tbody></table>" if risks_mitigations else ""}
<h2>Methodology</h2>
<p class="subtitle">How this was calculated.</p>
{"".join(f"<h3>{m['title']}</h3><p>{m['body']}</p>" for m in methodology)}
<h2>Sensitivity (What-If)</h2>
<p class="subtitle">If node count or runtime changes, estimated monthly cost scales roughly as follows:</p>
<ul>
<li>Current: ${sensitivity["current_monthly_usd"]:,.2f}/month</li>
<li>If node count → {sensitivity["node_count"] + 1}: ≈ ${sensitivity["if_nodes_plus_one_monthly_usd"]:,.2f}/month</li>
{f'<li>If node count → {sensitivity["node_count"] - 1}: ≈ ${sensitivity["if_nodes_minus_one_monthly_usd"]:,.2f}/month</li>' if sensitivity.get("if_nodes_minus_one_monthly_usd") is not None else ""}
<li>If avg runtime +20%: ≈ ${sensitivity["if_runtime_plus_20_pct_monthly_usd"]:,.2f}/month</li>
</ul>
<h2>Data Quality & Inputs</h2>
<p class="subtitle">{"Optional inputs used in this run: " + ", ".join(data_quality["optional_inputs_used"]) + "." if data_quality["optional_inputs_used"] else "No optional workload inputs were provided."}{" Adding peak executor memory from Spark UI will improve OOM and sizing guidance." if data_quality["suggest_peak_executor_memory"] else ""}</p>
<h2>Further Analysis</h2>
<p>{tier_cta}</p>
<p>{rerun}</p>
<h2>Understanding This Report</h2>
<p class="subtitle">Brief explanations of the metrics and terms used above.</p>
{explanations_html}
<h2>Definitions</h2>
<table><tbody><tr><th>Term</th><th>Definition</th></tr>{definitions_html}</tbody></table>
<div class="footer">ICEA report v{meta["report_version"]}. {meta["copyright"]}</div>
</div>
</body>
</html>"""
    return html
