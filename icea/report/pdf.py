"""PDF report rendering with ReportLab (spec: US Letter, margins, typography, KPI99 colors)."""
import io
from pathlib import Path
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Image,
)
from reportlab.graphics.shapes import Drawing, Rect, String, Group
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

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
    get_logo_path,
    get_favicon_path,
    KPI99_DARK,
    KPI99_MUTED,
    KPI99_ACCENT,
    KPI99_ACCENT_DARK,
    KPI99_GRID,
    KPI99_SUCCESS,
    KPI99_WARN,
)

# KPI99 color scheme
COLOR_DARK = KPI99_DARK
COLOR_MUTED = KPI99_MUTED
COLOR_ACCENT = KPI99_ACCENT
COLOR_TABLE_HEADER = KPI99_ACCENT_DARK
COLOR_GRID = KPI99_GRID

MARGIN_LR = 0.75 * inch
MARGIN_TOP = 0.85 * inch
MARGIN_BOTTOM = 0.8 * inch
SECTION_SPACER = 0.25 * inch


def _styles():
    """Paragraph styles per spec (Helvetica/Courier). Plain dict to avoid ReportLab stylesheet name clashes."""
    styles = {}
    styles["ICEA_Title"] = ParagraphStyle(
        name="ICEA_Title",
        fontName="Helvetica-Bold",
        fontSize=28,
        textColor=COLOR_DARK,
        spaceAfter=12,
    )
    styles["ICEA_H1"] = ParagraphStyle(
        name="ICEA_H1",
        fontName="Helvetica-Bold",
        fontSize=16,
        textColor=COLOR_DARK,
        spaceBefore=18,
        spaceAfter=8,
    )
    styles["ICEA_H2"] = ParagraphStyle(
        name="ICEA_H2",
        fontName="Helvetica-Bold",
        fontSize=13,
        textColor=colors.HexColor(KPI99_ACCENT_DARK),
        spaceBefore=20,
        spaceAfter=6,
    )
    styles["ICEA_Body"] = ParagraphStyle(
        name="ICEA_Body",
        fontName="Helvetica",
        fontSize=10.5,
        textColor=COLOR_DARK,
        spaceAfter=6,
    )
    styles["ICEA_Small"] = ParagraphStyle(
        name="ICEA_Small",
        fontName="Helvetica",
        fontSize=9,
        textColor=COLOR_MUTED,
        spaceAfter=4,
    )
    styles["ICEA_Code"] = ParagraphStyle(
        name="ICEA_Code",
        fontName="Courier",
        fontSize=10,
        textColor=COLOR_DARK,
    )
    return styles


def _header_flowables(styles, meta, favicon_path=None, logo_path=None, static_dir=None):
    """Build report header: favicon (required when available), optional logo, brand, tagline, title."""
    from reportlab.platypus import Table
    flowables = []
    fav_path = favicon_path or get_favicon_path(static_dir)
    logo_path = logo_path or get_logo_path(static_dir)
    # Row 1: favicon (always when found) + brand/tagline in a small table
    if fav_path:
        try:
            fav_img = Image(str(fav_path), width=0.4 * inch, height=0.4 * inch)
            brand_para = Paragraph(
                f"<b>{meta['brand']}</b><br/><font size='8' color='#64748b'>{meta['tagline']}</font>",
                styles["ICEA_Small"],
            )
            header_tbl = Table([[fav_img, brand_para]], colWidths=[0.5 * inch, 4.5 * inch])
            header_tbl.setStyle(TableStyle([
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (0, 0), 0),
                ("RIGHTPADDING", (1, 0), (1, 0), 8),
            ]))
            flowables.append(header_tbl)
            flowables.append(Spacer(1, 0.12 * inch))
        except Exception:
            pass
    if not fav_path:
        flowables.append(Paragraph(meta["brand"], styles["ICEA_Small"]))
        flowables.append(Paragraph(meta["tagline"], styles["ICEA_Small"]))
        flowables.append(Spacer(1, 0.08 * inch))
    if logo_path:
        try:
            flowables.append(Image(str(logo_path), width=1.4 * inch, height=0.45 * inch))
            flowables.append(Spacer(1, 0.12 * inch))
        except Exception:
            pass
    return flowables


def _pdf_labels(lang: str) -> dict:
    """Section titles and labels for PDF by language."""
    if (lang or "en").strip().lower() == "es":
        return {
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
            "cost_breakdown": "Desglose de costos (mensual)",
            "cost_assumptions": "Supuestos del modelo de costos",
            "input": "Entrada",
            "value": "Valor",
            "savings_proj": "Proyección de ahorros",
            "metric": "Métrica",
            "waste_daily": "Costo de desperdicio (diario)",
            "waste_monthly": "Costo de desperdicio (mensual)",
            "proj_savings": "Ahorro proyectado (mensual)",
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
        }
    return {
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
        "cost_breakdown": "Cost Breakdown (Monthly)",
        "cost_assumptions": "Cost Model Assumptions",
        "input": "Input",
        "value": "Value",
        "savings_proj": "Savings Projection",
        "metric": "Metric",
        "waste_daily": "Waste cost (daily)",
        "waste_monthly": "Waste cost (monthly)",
        "proj_savings": "Projected savings (monthly)",
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
    }


def _make_footer(static_dir=None, lang: str = "en"):
    """Return a footer callback that can use static_dir for favicon."""
    def _add_footer(canvas, doc):
        meta = report_metadata(lang=lang)
        canvas.saveState()
        fav = get_favicon_path(static_dir)
        y_footer = MARGIN_BOTTOM - 0.25 * inch
        if fav:
            try:
                canvas.drawImage(str(fav), MARGIN_LR, y_footer - 0.2 * inch, width=0.22 * inch, height=0.22 * inch)
            except Exception:
                pass
        x_text = MARGIN_LR + (0.28 * inch if fav else 0)
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.HexColor(COLOR_MUTED))
        canvas.drawString(x_text, y_footer, f"ICEA report v{meta['report_version']} — {meta['doc_name_short']} — {meta['date']}")
        canvas.drawString(x_text, y_footer - 0.14 * inch, meta["copyright"])
        canvas.drawRightString(letter[0] - MARGIN_LR, y_footer, f"Page {doc.page}")
        canvas.restoreState()
    return _add_footer


def _draw_utilization_bars(cpu_util_pct: float, mem_util_pct: float) -> Drawing:
    """Simple horizontal bar chart: CPU and Memory utilization %."""
    w, h = 400, 95
    d = Drawing(w, h)
    bar_h, gap = 28, 12
    bar_w = 280
    x0, y0 = 100, h - 25
    # CPU bar (KPI99 blue)
    d.add(Rect(x0, y0, bar_w * (cpu_util_pct / 100), bar_h, fillColor=colors.HexColor(KPI99_ACCENT), strokeColor=None))
    d.add(Rect(x0 + bar_w * (cpu_util_pct / 100), y0, bar_w * (1 - cpu_util_pct / 100), bar_h, fillColor=colors.HexColor(COLOR_GRID), strokeColor=None))
    d.add(String(x0 - 5, y0 + bar_h / 2 - 4, "CPU", fontName="Helvetica", fontSize=9, textAnchor="end", fillColor=colors.HexColor(COLOR_DARK)))
    d.add(String(x0 + bar_w + 8, y0 + bar_h / 2 - 4, f"{cpu_util_pct:.0f}%", fontName="Helvetica", fontSize=9, fillColor=colors.HexColor(COLOR_DARK)))
    # Memory bar (KPI99 green)
    y1 = y0 - bar_h - gap
    d.add(Rect(x0, y1, bar_w * (mem_util_pct / 100), bar_h, fillColor=colors.HexColor(KPI99_SUCCESS), strokeColor=None))
    d.add(Rect(x0 + bar_w * (mem_util_pct / 100), y1, bar_w * (1 - mem_util_pct / 100), bar_h, fillColor=colors.HexColor(COLOR_GRID), strokeColor=None))
    d.add(String(x0 - 5, y1 + bar_h / 2 - 4, "Mem", fontName="Helvetica", fontSize=9, textAnchor="end", fillColor=colors.HexColor(COLOR_DARK)))
    d.add(String(x0 + bar_w + 8, y1 + bar_h / 2 - 4, f"{mem_util_pct:.0f}%", fontName="Helvetica", fontSize=9, fillColor=colors.HexColor(COLOR_DARK)))
    return d


def _draw_cost_breakdown(total: float, waste: float, savings: float) -> Drawing:
    """Simple cost breakdown bar: utilized vs waste, and savings if any."""
    w, h = 400, 70
    d = Drawing(w, h)
    bar_w = 280
    x0, y0 = 100, h - 45
    bar_h = 22
    if total <= 0:
        total = 1
    util_pct = ((total - waste) / total) * 100
    waste_pct = (waste / total) * 100
    d.add(Rect(x0, y0, bar_w * (util_pct / 100), bar_h, fillColor=colors.HexColor(KPI99_SUCCESS), strokeColor=None))
    d.add(Rect(x0 + bar_w * (util_pct / 100), y0, bar_w * (waste_pct / 100), bar_h, fillColor=colors.HexColor(KPI99_WARN), strokeColor=None))
    d.add(String(x0 - 5, y0 + bar_h / 2 - 4, "Cost", fontName="Helvetica", fontSize=9, textAnchor="end", fillColor=colors.HexColor(COLOR_DARK)))
    d.add(String(x0 + bar_w + 8, y0 + bar_h / 2 - 4, f"${total:,.0f}/mo", fontName="Helvetica", fontSize=9, fillColor=colors.HexColor(COLOR_DARK)))
    if savings > 0:
        y1 = y0 - bar_h - 8
        d.add(String(x0 - 5, y1 + bar_h / 2 - 4, "Save", fontName="Helvetica", fontSize=9, textAnchor="end", fillColor=colors.HexColor(COLOR_DARK)))
        d.add(String(x0 + bar_w + 8, y1 + bar_h / 2 - 4, f"${savings:,.0f}/mo", fontName="Helvetica", fontSize=9, fillColor=colors.HexColor(KPI99_ACCENT)))
    return d


def _table_with_header(data, col_widths=None, definitions_style=False):
    """Table with header row (KPI99 dark blue), grid, padding. definitions_style: bold term column, light bg."""
    t = Table(data, colWidths=col_widths)
    style = [
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(COLOR_TABLE_HEADER)),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 10),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
        ("TOPPADDING", (0, 0), (-1, 0), 8),
        ("BACKGROUND", (0, 1), (-1, -1), colors.white),
        ("TEXTCOLOR", (0, 1), (-1, -1), colors.HexColor(COLOR_DARK)),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 10),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor(COLOR_GRID)),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]
    if definitions_style and len(data) > 1:
        style.append(("FONTNAME", (0, 1), (0, -1), "Helvetica-Bold"))
        style.append(("TEXTCOLOR", (0, 1), (0, -1), colors.HexColor(KPI99_ACCENT_DARK)))
        style.append(("BACKGROUND", (0, 1), (0, -1), colors.HexColor("#f0f9ff")))
    t.setStyle(TableStyle(style))
    return t


def generate_report_pdf(
    req: AnalyzeRequest,
    packing: PackingResult,
    cost: CostResult,
    recommendation: RecommendedConfig | None,
    risk_notes: list[str] | None = None,
    static_dir: Path | None = None,
    app_url: str | None = None,
    lang: str = "en",
) -> bytes:
    """Generate branded PDF report; returns PDF bytes. lang=en|es (Spanish content when lang=es)."""
    risk_notes = risk_notes or []
    lang = (lang or "en").strip().lower()
    if lang not in ("en", "es"):
        lang = "en"
    L = _pdf_labels(lang)
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        leftMargin=MARGIN_LR,
        rightMargin=MARGIN_LR,
        topMargin=MARGIN_TOP,
        bottomMargin=MARGIN_BOTTOM,
    )
    styles = _styles()
    story = []
    meta = report_metadata(lang=lang)

    # Header: favicon (required) + optional logo + brand/tagline
    story.extend(_header_flowables(styles, meta, static_dir=static_dir))
    story.append(Paragraph(L["report_title"], styles["ICEA_Title"]))
    story.append(Spacer(1, SECTION_SPACER))

    # Executive summary: full narrative + callout
    ex = executive_summary(packing, cost, recommendation)
    exec_narrative = executive_summary_narrative(req, packing, cost, recommendation, lang=lang)
    story.append(Paragraph(L["exec_summary"], styles["ICEA_H2"]))
    story.append(Paragraph(exec_narrative, styles["ICEA_Body"]))
    score = ex["efficiency_score"]
    waste_usd = ex["waste_cost_monthly_usd"]
    summary_text = (
        f"{L['summary_score']}: <b>{score}/100</b>. "
        f"{L['summary_waste']}: <b>${waste_usd:,.2f}</b>."
    )
    if ex["has_recommendation"] and ex["savings_monthly"] > 0:
        rec_msg = L["summary_recommended"].format(ex["recommended_cores"], ex["recommended_memory_gb"])
        summary_text += f" {rec_msg} <b>${ex['savings_monthly']:,.2f}/month</b>."
    story.append(Paragraph(summary_text, styles["ICEA_Body"]))
    benchmark = benchmark_context(packing.efficiency_score, lang=lang)
    story.append(Paragraph(
        L["your_score"].format(benchmark["efficiency_score"], benchmark["text"]),
        styles["ICEA_Small"],
    ))
    story.append(Spacer(1, SECTION_SPACER))

    # Current vs recommended
    story.append(Paragraph(L["current_vs_rec"], styles["ICEA_H1"]))
    cvr = current_vs_recommended(req, packing, recommendation)
    table_data = [
        ["", L["current"], L["recommended"] if cvr["recommended"] else "—"],
        [L["executor_cores"], str(cvr["current"]["executor_cores"]),
         str(cvr["recommended"]["executor_cores"]) if cvr["recommended"] else "—"],
        [L["executor_memory_gb"], str(cvr["current"]["executor_memory_gb"]),
         str(cvr["recommended"]["executor_memory_gb"]) if cvr["recommended"] else "—"],
        [L["executors_per_node"], str(cvr["current"]["executors_per_node"]),
         str(cvr["recommended"]["executors_per_node"]) if cvr["recommended"] else "—"],
        [L["efficiency_score"], str(cvr["current"]["efficiency_score"]),
         str(cvr["recommended"]["efficiency_score"]) if cvr["recommended"] else "—"],
    ]
    story.append(_table_with_header(table_data, [2 * inch, 2 * inch, 2 * inch]))
    story.append(Spacer(1, SECTION_SPACER))

    # Utilization visualization
    story.append(Paragraph(L["resource_util"], styles["ICEA_H2"]))
    ucd = utilization_chart_data(packing)
    story.append(_draw_utilization_bars(ucd["cpu_util_pct"], ucd["mem_util_pct"]))
    story.append(Spacer(1, SECTION_SPACER))

    # Cost breakdown visualization
    story.append(Paragraph(L["cost_breakdown"], styles["ICEA_H2"]))
    story.append(Paragraph(cost_breakdown_sentence(cost, lang=lang), styles["ICEA_Small"]))
    cbd = cost_breakdown_chart_data(cost, recommendation)
    story.append(_draw_cost_breakdown(cbd["total_monthly_usd"], cbd["waste_monthly_usd"], cbd["savings_monthly_usd"]))
    story.append(Spacer(1, SECTION_SPACER))

    # Cost assumptions (dynamic rows from ca)
    story.append(Paragraph(L["cost_assumptions"], styles["ICEA_H2"]))
    ca = cost_assumptions(req, cost)
    assump_data = [[L["input"], L["value"]]]
    for k in ["cloud", "region", "instance_type", "node_cores", "node_memory_gb", "node_hourly_cost_usd", "node_count",
              "avg_runtime_minutes", "min_runtime_minutes", "max_runtime_minutes", "jobs_per_day", "partition_count",
              "input_data_gb", "concurrent_jobs", "peak_executor_memory_gb",
              "shuffle_read_mb", "shuffle_write_mb", "data_skew", "spot_pct", "autoscale_min_nodes", "autoscale_max_nodes",
              "utilization_factor", "hourly_cluster_cost_usd", "daily_cost_usd"]:
        if k not in ca:
            continue
        v = ca[k]
        if v is None:
            continue
        if isinstance(v, float) and k in ("node_hourly_cost_usd", "hourly_cluster_cost_usd", "daily_cost_usd", "utilization_factor", "spot_pct"):
            disp = f"{v:.0%}" if k == "utilization_factor" else (f"{v:.0f}%" if k == "spot_pct" else f"{v:.2f}")
            assump_data.append([k.replace("_", " ").title(), disp])
        else:
            assump_data.append([k.replace("_", " ").title(), str(v)])
    story.append(_table_with_header(assump_data, [2.5 * inch, 2.5 * inch]))
    story.append(Spacer(1, SECTION_SPACER))

    # Savings
    story.append(Paragraph(L["savings_proj"], styles["ICEA_H2"]))
    sav = savings_section(cost, recommendation)
    sav_data = [
        [L["metric"], "USD"],
        [L["waste_daily"], f"{sav['waste_cost_daily_usd']:,.2f}"],
        [L["waste_monthly"], f"{sav['waste_cost_monthly_usd']:,.2f}"],
    ]
    if sav["has_recommendation"]:
        sav_data.append([L["proj_savings"], f"{sav['savings_monthly_usd']:,.2f}"])
    story.append(_table_with_header(sav_data, [2.5 * inch, 2 * inch]))
    story.append(Paragraph(L["estimates_note"], styles["ICEA_Small"]))
    story.append(Spacer(1, SECTION_SPACER))

    # Forecast (when forecast_months set)
    forecast = forecast_data(req, cost, recommendation)
    if forecast:
        story.append(Paragraph(L["forecast_title"], styles["ICEA_H2"]))
        gr = getattr(req, "growth_rate_pct", None)
        growth_note = f" (growth {gr:.1f}%/year)" if gr is not None and gr != 0 else ""
        if lang == "es":
            growth_note = f" (crecimiento {gr:.1f}%/año)" if gr is not None and gr != 0 else ""
        story.append(Paragraph(L["forecast_subtitle"].format(len(forecast), growth_note), styles["ICEA_Small"]))
        fc_data = [[L["month"], L["current_usd"], L["recommended_usd"], L["savings_usd"]]]
        for row in forecast[:24]:  # cap at 24 rows
            fc_data.append([str(row["month"]), f"{row['current_usd']:,.2f}", f"{row['recommended_usd']:,.2f}", f"{row['savings_usd']:,.2f}"])
        story.append(_table_with_header(fc_data, [1 * inch, 1.5 * inch, 1.5 * inch, 1.2 * inch]))
        story.append(Spacer(1, SECTION_SPACER))

    # Engineering notes
    story.append(Paragraph(L["engineering_notes"], styles["ICEA_H2"]))
    notes = engineering_notes(packing, recommendation, risk_notes, lang=lang)
    for n in notes:
        story.append(Paragraph(f"• {n}", styles["ICEA_Body"]))
    story.append(Spacer(1, SECTION_SPACER))

    # Next steps
    story.append(Paragraph(L["next_steps"], styles["ICEA_H2"]))
    for s in next_steps_section(req, packing, recommendation, lang=lang):
        story.append(Paragraph(f"• {s}", styles["ICEA_Body"]))
    story.append(Spacer(1, SECTION_SPACER))

    # Risks & mitigations
    risks_mitigations = risks_mitigations_section(risk_notes, lang=lang)
    if risks_mitigations:
        story.append(Paragraph(L["risks_mitigations"], styles["ICEA_H2"]))
        rm_data = [[L["risk"], L["mitigation"]]]
        for rm in risks_mitigations:
            rm_data.append([rm["risk"], rm["mitigation"]])
        story.append(_table_with_header(rm_data, [2.5 * inch, 3 * inch]))
        story.append(Spacer(1, SECTION_SPACER))

    # Methodology
    story.append(Paragraph(L["methodology"], styles["ICEA_H2"]))
    story.append(Paragraph(L["how_calculated"], styles["ICEA_Small"]))
    for m in methodology_section(lang=lang):
        story.append(Paragraph(f"<b>{m['title']}</b>", styles["ICEA_Body"]))
        story.append(Paragraph(m["body"], styles["ICEA_Body"]))
        story.append(Spacer(1, 0.08 * inch))
    story.append(Spacer(1, SECTION_SPACER))

    # Sensitivity
    sens = sensitivity_section(req, cost, recommendation)
    story.append(Paragraph(L["sensitivity"], styles["ICEA_H2"]))
    story.append(Paragraph(L["sensitivity_intro"], styles["ICEA_Small"]))
    sens_items = [
        f"{L['current_label']}: ${sens['current_monthly_usd']:,.2f}/month",
        f"{L['if_nodes_plus'].format(sens['node_count'] + 1)}: ≈ ${sens['if_nodes_plus_one_monthly_usd']:,.2f}/month",
    ]
    if sens.get("if_nodes_minus_one_monthly_usd") is not None:
        sens_items.append(f"{L['if_nodes_minus'].format(sens['node_count'] - 1)}: ≈ ${sens['if_nodes_minus_one_monthly_usd']:,.2f}/month")
    sens_items.append(f"{L['if_runtime_20']}: ≈ ${sens['if_runtime_plus_20_pct_monthly_usd']:,.2f}/month")
    for item in sens_items:
        story.append(Paragraph(f"• {item}", styles["ICEA_Body"]))
    story.append(Spacer(1, SECTION_SPACER))

    # Data quality
    dq = data_quality_note(req, lang=lang)
    story.append(Paragraph(L["data_quality"], styles["ICEA_H2"]))
    if dq["optional_inputs_used"]:
        dq_text = L["optional_used"].format(", ".join(dq["optional_inputs_used"]))
    else:
        dq_text = L["no_optional"]
    if dq["suggest_peak_executor_memory"]:
        dq_text += L["suggest_peak"]
    story.append(Paragraph(dq_text, styles["ICEA_Small"]))
    story.append(Spacer(1, SECTION_SPACER))

    # Tier CTA and Re-run CTA
    story.append(Paragraph(L["further_analysis"], styles["ICEA_H2"]))
    story.append(Paragraph(tier_cta_section(lang=lang), styles["ICEA_Body"]))
    story.append(Paragraph(rerun_cta(app_url, lang=lang), styles["ICEA_Body"]))
    story.append(Spacer(1, SECTION_SPACER))

    # Understanding this report (explanations)
    story.append(Paragraph(L["understanding"], styles["ICEA_H2"]))
    story.append(Paragraph(L["understanding_intro"], styles["ICEA_Small"]))
    story.append(Spacer(1, 0.15 * inch))
    for e in explanations_section(lang=lang):
        story.append(Paragraph(f"<b>{e['title']}</b>", styles["ICEA_Body"]))
        story.append(Paragraph(e["body"], styles["ICEA_Body"]))
        story.append(Spacer(1, 0.1 * inch))
    story.append(Spacer(1, SECTION_SPACER))

    # Definitions (glossary)
    story.append(Paragraph(L["definitions"], styles["ICEA_H2"]))
    story.append(Paragraph(L["definitions_intro"], styles["ICEA_Small"]))
    story.append(Spacer(1, 0.12 * inch))
    defs = definitions_section(lang=lang)
    def_data = [[L["term"], L["definition"]]]
    for d in defs:
        def_data.append([d["term"], d["definition"]])
    story.append(_table_with_header(def_data, [1.6 * inch, 4.2 * inch], definitions_style=True))
    story.append(Spacer(1, SECTION_SPACER))

    footer_cb = _make_footer(static_dir, lang=lang)
    doc.build(story, onFirstPage=footer_cb, onLaterPages=footer_cb)
    return buffer.getvalue()
