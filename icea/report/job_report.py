"""Job-level report PDF from event log ingestion (matches Tier 1 report formatting)."""
import io
import xml.sax.saxutils
from pathlib import Path
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image

from icea.models import JobLevelSummary
from icea.report.templates import report_metadata
from icea.report.constants import (
    get_logo_path,
    get_favicon_path,
    KPI99_DARK,
    KPI99_MUTED,
    KPI99_ACCENT_DARK,
    KPI99_GRID,
)
from icea.report.pdf import (
    _header_flowables,
    _make_footer,
    _table_with_header,
    MARGIN_LR,
    MARGIN_TOP,
    MARGIN_BOTTOM,
    SECTION_SPACER,
)

COLOR_DARK = KPI99_DARK
COLOR_MUTED = KPI99_MUTED
COLOR_GRID = KPI99_GRID


def _escape_para(text: str) -> str:
    """Escape &, <, > for ReportLab Paragraph."""
    if not text:
        return ""
    return xml.sax.saxutils.escape(text, {"&": "&amp;", "<": "&lt;", ">": "&gt;"})


def _styles():
    """Match Tier 1 report paragraph styles."""
    styles = {}
    styles["ICEA_Title"] = ParagraphStyle(
        name="J_ICEA_Title",
        fontName="Helvetica-Bold",
        fontSize=28,
        textColor=colors.HexColor(COLOR_DARK),
        spaceAfter=12,
    )
    styles["ICEA_H1"] = ParagraphStyle(
        name="J_ICEA_H1",
        fontName="Helvetica-Bold",
        fontSize=16,
        textColor=colors.HexColor(COLOR_DARK),
        spaceBefore=18,
        spaceAfter=8,
    )
    styles["ICEA_H2"] = ParagraphStyle(
        name="J_ICEA_H2",
        fontName="Helvetica-Bold",
        fontSize=13,
        textColor=colors.HexColor(KPI99_ACCENT_DARK),
        spaceBefore=20,
        spaceAfter=6,
    )
    styles["ICEA_Body"] = ParagraphStyle(
        name="J_ICEA_Body",
        fontName="Helvetica",
        fontSize=10.5,
        textColor=colors.HexColor(COLOR_DARK),
        spaceAfter=6,
    )
    styles["ICEA_Small"] = ParagraphStyle(
        name="J_ICEA_Small",
        fontName="Helvetica",
        fontSize=9,
        textColor=colors.HexColor(COLOR_MUTED),
        spaceAfter=4,
    )
    return styles


def generate_job_report_pdf(
    jobs: list[JobLevelSummary | dict],
    executor_hourly_cost_usd: float | None = None,
    source_filename: str = "",
    static_dir: Path | None = None,
    lang: str = "en",
) -> bytes:
    """Generate a job-level analysis PDF from ingested event log data. Formatting matches Tier 1 report."""
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

    # Same header as Tier 1
    story.extend(_header_flowables(styles, meta, static_dir=static_dir))

    # Title: match Tier 1 main title style (28pt, dark)
    story.append(Paragraph("ICEA — Job-Level Analysis Report", styles["ICEA_Title"]))
    story.append(Paragraph("Generated from Spark event log ingestion.", styles["ICEA_Small"]))
    if source_filename:
        story.append(Paragraph(_escape_para(f"Source: {source_filename}"), styles["ICEA_Small"]))
    story.append(Spacer(1, SECTION_SPACER))

    footer_cb = _make_footer(static_dir, lang=lang, doc_name_short_override="ICEA Job Report")

    if not jobs:
        story.append(Paragraph("No job data found in the event log.", styles["ICEA_Body"]))
        doc.build(story, onFirstPage=footer_cb, onLaterPages=footer_cb)
        return buffer.getvalue()

    # Normalize to dicts
    rows = []
    for j in jobs:
        if isinstance(j, JobLevelSummary):
            j = j.model_dump()
        rows.append(j)

    total_executor_hours = sum(r.get("executor_hours") or 0 for r in rows)
    total_cost = sum(r.get("estimated_cost_usd") or 0 for r in rows)

    # Summary section (ICEA_H1 like Tier 1 section headings)
    story.append(Paragraph("Summary", styles["ICEA_H1"]))
    story.append(Paragraph(
        f"Total jobs: {len(rows)}. Total executor-hours: {total_executor_hours:.4f}. "
        + (f"Total estimated cost: ${total_cost:,.2f}." if total_cost else "No cost rate provided."),
        styles["ICEA_Body"],
    ))
    story.append(Spacer(1, SECTION_SPACER))

    # Job-level metrics table (ICEA_H2)
    story.append(Paragraph("Job-level metrics", styles["ICEA_H2"]))
    table_data = [
        ["Job ID", "Duration (s)", "Executor hrs", "Bytes read", "Bytes written", "Est. cost (USD)"]
    ]
    for r in rows:
        cost_str = f"{r['estimated_cost_usd']:,.4f}" if r.get("estimated_cost_usd") is not None else "—"
        table_data.append([
            str(r.get("job_id", "")),
            str(r.get("duration_sec", "")),
            f"{r.get('executor_hours', 0):.4f}",
            str(r.get("bytes_read", 0)),
            str(r.get("bytes_written", 0)),
            cost_str,
        ])

    col_widths = [0.7 * inch, 1 * inch, 1.1 * inch, 1.1 * inch, 1.2 * inch, 1.2 * inch]
    story.append(_table_with_header(table_data, col_widths))
    story.append(Spacer(1, 0.12 * inch))
    story.append(Paragraph(
        "Estimates are based on executor run time from the event log. Cost requires executor hourly rate.",
        styles["ICEA_Small"],
    ))

    doc.build(story, onFirstPage=footer_cb, onLaterPages=footer_cb)
    return buffer.getvalue()
