"""Job-level report PDF from event log ingestion (KPI99 branding)."""
import io
from pathlib import Path
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image

from icea.models import JobLevelSummary
from icea.report.templates import report_metadata
from icea.report.constants import (
    get_logo_path,
    get_favicon_path,
    KPI99_DARK,
    KPI99_MUTED,
    KPI99_ACCENT,
    KPI99_ACCENT_DARK,
    KPI99_GRID,
)

COLOR_DARK = KPI99_DARK
COLOR_MUTED = KPI99_MUTED
COLOR_ACCENT = KPI99_ACCENT
COLOR_HEADER = KPI99_ACCENT_DARK
COLOR_GRID = KPI99_GRID
MARGIN = 0.75 * inch
SECTION_SPACER = 0.2 * inch


def _job_header_flowables(styles, meta, static_dir=None):
    """Header with favicon (required) + optional logo + brand/tagline."""
    flowables = []
    fav_path = get_favicon_path(static_dir)
    logo_path = get_logo_path(static_dir)
    if fav_path:
        try:
            fav_img = Image(str(fav_path), width=0.4 * inch, height=0.4 * inch)
            brand_para = Paragraph(
                f"<b>{meta['brand']}</b><br/><font size='8' color='#64748b'>{meta['tagline']}</font>",
                styles["Small"],
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
        flowables.append(Paragraph(meta["brand"], styles["Brand"]))
        flowables.append(Paragraph(meta["tagline"], styles["Small"]))
        flowables.append(Spacer(1, 0.08 * inch))
    if logo_path:
        try:
            flowables.append(Image(str(logo_path), width=1.4 * inch, height=0.45 * inch))
            flowables.append(Spacer(1, 0.12 * inch))
        except Exception:
            pass
    return flowables


def _styles():
    styles = {}
    styles["Title"] = ParagraphStyle(
        name="JTitle",
        fontName="Helvetica-Bold",
        fontSize=18,
        textColor=colors.HexColor(COLOR_ACCENT),
        spaceAfter=6,
    )
    styles["Body"] = ParagraphStyle(
        name="JBody",
        fontName="Helvetica",
        fontSize=10,
        textColor=colors.HexColor(COLOR_DARK),
        spaceAfter=4,
    )
    styles["Small"] = ParagraphStyle(
        name="JSmall",
        fontName="Helvetica",
        fontSize=9,
        textColor=colors.HexColor(COLOR_MUTED),
        spaceAfter=4,
    )
    styles["Brand"] = ParagraphStyle(
        name="JBrand",
        fontName="Helvetica-Bold",
        fontSize=10,
        textColor=colors.HexColor(COLOR_ACCENT),
        spaceAfter=2,
    )
    return styles


def _table_with_header(data, col_widths=None):
    t = Table(data, colWidths=col_widths)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(COLOR_HEADER)),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 10),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
        ("TOPPADDING", (0, 0), (-1, 0), 8),
        ("BACKGROUND", (0, 1), (-1, -1), colors.white),
        ("TEXTCOLOR", (0, 1), (-1, -1), colors.HexColor(COLOR_DARK)),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 9),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor(COLOR_GRID)),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    return t


def _job_report_footer(canvas, doc, static_dir=None):
    """Footer: favicon, KPI99 branding and page number."""
    meta = report_metadata()
    canvas.saveState()
    fav = get_favicon_path(static_dir)
    y_footer = MARGIN - 0.25 * inch
    if fav:
        try:
            canvas.drawImage(str(fav), MARGIN, y_footer - 0.2 * inch, width=0.22 * inch, height=0.22 * inch)
        except Exception:
            pass
    x_text = MARGIN + (0.28 * inch if fav else 0)
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor(COLOR_MUTED))
    canvas.drawString(x_text, y_footer, f"KPI99 ICEA Job Report — {meta['date']}")
    canvas.drawString(x_text, y_footer - 0.14 * inch, meta["copyright"])
    canvas.drawRightString(letter[0] - MARGIN, y_footer, f"Page {doc.page}")
    canvas.restoreState()


def generate_job_report_pdf(
    jobs: list[JobLevelSummary | dict],
    executor_hourly_cost_usd: float | None = None,
    source_filename: str = "",
    static_dir: Path | None = None,
) -> bytes:
    """Generate a job-level analysis PDF from ingested event log data."""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        leftMargin=MARGIN,
        rightMargin=MARGIN,
        topMargin=0.8 * inch,
        bottomMargin=MARGIN,
    )
    styles = _styles()
    story = []
    meta = report_metadata()

    story.extend(_job_header_flowables(styles, meta, static_dir))
    story.append(Paragraph("ICEA — Job-Level Analysis Report", styles["Title"]))
    story.append(Paragraph("Generated from Spark event log ingestion.", styles["Small"]))
    if source_filename:
        story.append(Paragraph(f"Source: {source_filename}", styles["Small"]))
    story.append(Spacer(1, SECTION_SPACER))

    def footer(canvas, doc):
        _job_report_footer(canvas, doc, static_dir)

    if not jobs:
        story.append(Paragraph("No job data found in the event log.", styles["Body"]))
        doc.build(story, onFirstPage=footer, onLaterPages=footer)
        return buffer.getvalue()

    # Normalize to dicts
    rows = []
    for j in jobs:
        if isinstance(j, JobLevelSummary):
            j = j.model_dump()
        rows.append(j)

    total_executor_hours = sum(r.get("executor_hours") or 0 for r in rows)
    total_cost = sum(r.get("estimated_cost_usd") or 0 for r in rows)

    story.append(Paragraph("Summary", styles["Body"]))
    story.append(Paragraph(
        f"Total jobs: {len(rows)}. Total executor-hours: {total_executor_hours:.4f}. "
        + (f"Total estimated cost: ${total_cost:,.2f}." if total_cost else "No cost rate provided."),
        styles["Small"],
    ))
    story.append(Spacer(1, SECTION_SPACER))

    # Table: Job ID, Duration (sec), Executor hours, Bytes read, Bytes written, Est. cost (USD)
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
    story.append(Spacer(1, SECTION_SPACER))
    story.append(Paragraph(
        "Estimates are based on executor run time from the event log. Cost requires executor hourly rate.",
        styles["Small"],
    ))

    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    return buffer.getvalue()
