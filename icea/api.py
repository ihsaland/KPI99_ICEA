"""FastAPI routes for ICEA MVP."""
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, HTTPException, File, UploadFile, Form, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles

from icea.models import (
    AnalyzeRequest,
    AnalyzeResponse,
    Assumptions,
    CheckoutTier1Request,
    ExpertRequest,
    JobLevelSummary,
    JobReportRequest,
    PackingResult,
    CostResult,
)
from icea.packing import compute_packing
from icea.cost_model import compute_cost
from icea.recommend import recommend
from icea.report.pdf import generate_report_pdf
from icea.report.html_report import generate_report_html
from icea.report.job_report import generate_job_report_pdf
from icea.eventlog import parse_event_log, aggregate_job_level
from icea.payments import (
    create_pending_report,
    consume_pending_report,
    create_checkout_session,
    get_stripe_secret_key,
)
from icea.auth import require_report_auth
from icea.store import add_expert_request as store_expert_request, audit_report_delivered, prune_retention, get_pending_report
from icea.notify import notify_expert_request
from icea.security import RateLimitMiddleware, check_demo_in_production
from icea.observability import RequestLoggingMiddleware, get_metrics_text
from icea.resilience import (
    run_sync_with_timeout,
    get_cached_analyze,
    set_cached_analyze,
    get_analyze_timeout_sec,
    get_report_timeout_sec,
)
from icea.catalog import get_providers, get_regions, get_instance_types

_LOG = logging.getLogger(__name__)


def _cors_origins() -> list[str]:
    raw = (os.environ.get("ICEA_CORS_ORIGINS") or "").strip()
    if not raw:
        return []
    if raw == "*":
        return ["*"]
    return [o.strip() for o in raw.split(",") if o.strip()]


@asynccontextmanager
async def lifespan(_app: FastAPI):
    check_demo_in_production()
    try:
        prune_retention()
    except Exception as e:
        _LOG.warning("Retention prune on startup failed: %s", e)
    yield


app = FastAPI(
    title="ICEA",
    description="Infrastructure Cost & Efficiency Analyzer — MVP",
    version="0.1.0",
    lifespan=lifespan,
)
origins = _cors_origins()
if origins:
    app.add_middleware(CORSMiddleware, allow_origins=origins, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
app.add_middleware(RateLimitMiddleware)
app.add_middleware(RequestLoggingMiddleware)


def _risk_notes(req: AnalyzeRequest, packing: PackingResult) -> list[str]:
    """Optional risk flags (advisory)."""
    notes = []
    if req.executor.memory_gb < 6:
        notes.append("Executor memory below 6 GB may increase OOM likelihood for many workloads.")
    if packing.executors_per_node > 10:
        notes.append("High executors per node may increase scheduling/GC overhead.")
    # Partition count vs parallelism (total executor cores)
    pc = getattr(req.workload, "partition_count", None)
    if pc is not None and packing.executors_per_node > 0:
        total_cores = req.node.count * packing.executors_per_node * req.executor.cores
        if total_cores > 0:
            if pc > 4 * total_cores:
                notes.append(
                    f"Partition count ({pc:,}) is much higher than total executor cores ({total_cores:,}). "
                    "Consider increasing cluster size or reducing partitions to limit task scheduling overhead."
                )
            elif total_cores > 4 * pc:
                notes.append(
                    f"Total executor cores ({total_cores:,}) is much higher than partition count ({pc:,}). "
                    "Some cores may be underutilized; consider aligning partitions with parallelism."
                )
    # Input data size vs executor memory (spill / shuffle risk)
    data_gb = getattr(req.workload, "input_data_gb", None)
    if data_gb is not None and data_gb > 0 and req.executor.memory_gb > 0:
        # Rough heuristic: data per executor (if evenly split) vs executor memory
        total_executors = req.node.count * packing.executors_per_node
        if total_executors > 0:
            data_per_executor_gb = data_gb / total_executors
            if data_per_executor_gb > 0.5 * req.executor.memory_gb:
                notes.append(
                    f"Input data per job ({data_gb:.0f} GB) is large relative to executor memory ({req.executor.memory_gb:.0f} GB). "
                    "Consider increasing executor memory or partition count to reduce spill and shuffle pressure."
                )
    # Concurrent jobs: if cluster is shared, utilization_factor may understate cost
    cj = getattr(req.workload, "concurrent_jobs", None)
    if cj is not None and cj > 1:
        uf = getattr(req, "utilization_factor", None)
        if uf is None or uf >= 1.0:
            notes.append(
                f"Cluster runs ~{cj:.0f} concurrent jobs. Set utilization factor if cost should reflect shared usage."
            )
    # Peak observed memory vs configured executor memory
    peak_mem = getattr(req.workload, "peak_executor_memory_gb", None)
    if peak_mem is not None and peak_mem > 0 and req.executor.memory_gb > 0:
        if peak_mem >= 0.9 * req.executor.memory_gb:
            notes.append(
                f"Observed peak executor memory ({peak_mem:.1f} GB) is close to or above configured ({req.executor.memory_gb:.0f} GB). "
                "Consider increasing executor memory to reduce OOM and spill risk."
            )
    # Shuffle volume vs executor memory
    shuffle_r = getattr(req.workload, "shuffle_read_mb", None) or 0
    shuffle_w = getattr(req.workload, "shuffle_write_mb", None) or 0
    shuffle_gb = (shuffle_r + shuffle_w) / 1024.0
    if shuffle_gb > 0 and req.executor.memory_gb > 0:
        total_executors = req.node.count * packing.executors_per_node
        if total_executors > 0 and (shuffle_gb / total_executors) > 0.25 * req.executor.memory_gb:
            notes.append(
                "High shuffle volume relative to executor memory may increase spill and I/O. "
                "Consider increasing executor memory or reducing shuffle."
            )
    # Data skew
    skew = getattr(req.workload, "data_skew", None)
    if skew == "high":
        notes.append(
            "High data skew can cause tail latency and underutilized executors. "
            "Consider repartitioning or salting keys to balance load."
        )
    # Spot / preemptible
    spot = getattr(req.workload, "spot_pct", None)
    if spot is not None and spot > 50:
        notes.append(
            f"Cluster uses {spot:.0f}% spot/preemptible capacity. "
            "Expect higher interrupt risk and possible cost variance."
        )
    return notes


def _demo_available() -> bool:
    """True when ICEA_DEMO is 1, true, or yes (development only; do not set in production)."""
    from icea.auth import _is_demo_enabled
    return _is_demo_enabled()


def _do_analyze(request: AnalyzeRequest) -> AnalyzeResponse:
    """Compute packing, cost, recommendation; return AnalyzeResponse (for timeout/cache)."""
    assumptions = request.assumptions or Assumptions()
    packing = compute_packing(request.node, request.executor, assumptions)
    cost = compute_cost(
        request.node,
        request.workload,
        packing,
        utilization_factor=getattr(request, "utilization_factor", None),
    )
    rec = recommend(
        request.node,
        request.executor,
        request.workload,
        assumptions,
        packing,
        cost,
    )
    risk = _risk_notes(request, packing)
    return AnalyzeResponse(packing=packing, cost=cost, recommendation=rec, risk_notes=risk)


@app.get("/v1/health")
def health():
    """Health check. Includes demo_available when ICEA_DEMO=1 (dev only)."""
    out = {"status": "ok", "service": "icea"}
    if _demo_available():
        out["demo_available"] = True
    return out


@app.get("/v1/metrics")
def metrics():
    """Prometheus-style metrics (request counts, uptime, duration)."""
    from fastapi.responses import PlainTextResponse
    return PlainTextResponse(get_metrics_text(), media_type="text/plain; charset=utf-8")


@app.get("/v1/catalog/providers")
def catalog_providers():
    """List all cloud and managed-Spark providers with instance catalogs."""
    return get_providers()


@app.get("/v1/catalog/regions")
def catalog_regions(cloud: str):
    """List regions for a provider (regional pricing)."""
    return get_regions(cloud)


@app.get("/v1/catalog/instances")
def catalog_instances(cloud: str, region: str | None = None):
    """List instance types for a provider; optional region for regional pricing."""
    return get_instance_types(cloud, region)


@app.post("/v1/analyze", response_model=AnalyzeResponse)
def analyze(request: AnalyzeRequest):
    """
    Compute packing, cost, and recommendation; return JSON. Uses optional cache and timeout.
    """
    request_dict = request.model_dump()
    cached = get_cached_analyze(request_dict)
    if cached is not None:
        return AnalyzeResponse.model_validate(cached)
    try:
        result = run_sync_with_timeout(get_analyze_timeout_sec(), _do_analyze, request)
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Analysis timed out. Try again or reduce input size.")
    set_cached_analyze(request_dict, result.model_dump())
    return result


def _report_pdf(
    request: AnalyzeRequest,
    static_dir: Path | None,
    app_url: str | None = None,
) -> bytes:
    """Generate report PDF (sync, for timeout wrapper)."""
    assumptions = request.assumptions or Assumptions()
    packing = compute_packing(request.node, request.executor, assumptions)
    cost = compute_cost(
        request.node,
        request.workload,
        packing,
        utilization_factor=getattr(request, "utilization_factor", None),
    )
    rec = recommend(
        request.node,
        request.executor,
        request.workload,
        assumptions,
        packing,
        cost,
    )
    risk = _risk_notes(request, packing)
    return generate_report_pdf(
        request, packing, cost, rec, risk,
        static_dir=static_dir,
        app_url=app_url,
    )


@app.post("/v1/report")
def report(
    request: AnalyzeRequest,
    req: Request,
    _: None = Depends(require_report_auth),
):
    """
    Generate and return PDF report. In production requires API key (or use Tier 1 payment flow via /v1/report-paid).
    When ICEA_DEMO=1, no auth is required for demo use.
    """
    static_dir = getattr(req.app.state, "static_dir", None)
    base = str(req.base_url).rstrip("/")
    try:
        pdf_bytes = run_sync_with_timeout(
            get_report_timeout_sec(),
            _report_pdf,
            request,
            static_dir,
            base,
        )
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Report generation timed out.")
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=icea-report.pdf"},
    )


@app.post("/v1/checkout/tier1")
def checkout_tier1(body: CheckoutTier1Request, req: Request):
    """
    Create Stripe Checkout for Tier 1 ($299). Returns checkout_url and token for success redirect.
    """
    if not get_stripe_secret_key():
        raise HTTPException(
            status_code=503,
            detail="Payment is not configured. Set STRIPE_SECRET_KEY to enable Tier 1 checkout.",
        )
    request_dict = body.request.model_dump()
    token = create_pending_report(request_dict)
    base = str(req.base_url).rstrip("/")
    success_url = (body.success_url_base or f"{base}/report-success.html") + f"?token={token}"
    cancel_url = body.cancel_url or base + "/"
    amount = body.amount_cents if body.amount_cents is not None else 29900
    checkout_url = create_checkout_session(
        token=token,
        amount_cents=amount,
        success_url=success_url,
        cancel_url=cancel_url,
    )
    if not checkout_url:
        raise HTTPException(status_code=503, detail="Could not create checkout session.")
    return {"checkout_url": checkout_url, "token": token}


@app.get("/v1/report-paid")
def report_paid(token: str, req: Request, format: str | None = None):
    """
    After Tier 1 payment success: view KPI99 report in HTML (default) or download PDF (?format=pdf).
    Token is consumed only when format=pdf.
    """
    if (format or "").lower() == "pdf":
        request_dict = consume_pending_report(token)
        if not request_dict:
            raise HTTPException(status_code=404, detail="Invalid or expired token. Please run the analysis again.")
        audit_report_delivered(token, getattr(req.state, "request_id", None))
        request_obj = AnalyzeRequest.model_validate(request_dict)
        static_dir = getattr(req.app.state, "static_dir", None)
        base = str(req.base_url).rstrip("/")
        try:
            pdf_bytes = run_sync_with_timeout(
                get_report_timeout_sec(),
                _report_pdf,
                request_obj,
                static_dir,
                base,
            )
        except TimeoutError:
            raise HTTPException(status_code=504, detail="Report generation timed out.")
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": "attachment; filename=icea-report.pdf"},
        )
    request_dict = get_pending_report(token)
    if not request_dict:
        raise HTTPException(status_code=404, detail="Invalid or expired token. Please run the analysis again.")
    request_obj = AnalyzeRequest.model_validate(request_dict)
    assumptions = request_obj.assumptions or Assumptions()
    packing = compute_packing(request_obj.node, request_obj.executor, assumptions)
    cost = compute_cost(
        request_obj.node,
        request_obj.workload,
        packing,
        utilization_factor=getattr(request_obj, "utilization_factor", None),
    )
    rec = recommend(
        request_obj.node,
        request_obj.executor,
        request_obj.workload,
        assumptions,
        packing,
        cost,
    )
    risk = _risk_notes(request_obj, packing)
    base = str(req.base_url).rstrip("/")
    pdf_url = f"{base}/v1/report-paid?token={token}&format=pdf"
    html = generate_report_html(
        request_obj, packing, cost, rec, risk,
        pdf_download_url=pdf_url,
        app_url=base,
    )
    return HTMLResponse(html)


@app.api_route("/v1/report/html", methods=["GET", "POST"], response_class=HTMLResponse)
@app.api_route("/v1/report/html/", methods=["GET", "POST"], response_class=HTMLResponse)
async def report_html_route(req: Request, token: str | None = None):
    """
    GET: with ?token=xxx return report; without token return help page.
    POST: body = AnalyzeRequest → generate HTML report (auth required when not demo).
    Single route avoids 405 Method Not Allowed when both methods are used for the same path.
    """
    if req.method == "GET":
        if token:
            request_dict = get_pending_report(token)
            if not request_dict:
                raise HTTPException(status_code=404, detail="Invalid or expired token. Please run the analysis again.")
            request_obj = AnalyzeRequest.model_validate(request_dict)
            assumptions = request_obj.assumptions or Assumptions()
            packing = compute_packing(request_obj.node, request_obj.executor, assumptions)
            cost = compute_cost(
                request_obj.node,
                request_obj.workload,
                packing,
                utilization_factor=getattr(request_obj, "utilization_factor", None),
            )
            rec = recommend(
                request_obj.node,
                request_obj.executor,
                request_obj.workload,
                assumptions,
                packing,
                cost,
            )
            risk = _risk_notes(request_obj, packing)
            base = str(req.base_url).rstrip("/")
            pdf_url = f"{base}/v1/report-paid?token={token}&format=pdf"
            html = generate_report_html(
                request_obj, packing, cost, rec, risk,
                pdf_download_url=pdf_url,
                app_url=base,
            )
            return HTMLResponse(html)
        return HTMLResponse(
            """<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>KPI99 Report</title></head>
<body style="font-family: system-ui, sans-serif; max-width: 36em; margin: 2em auto; padding: 1em;">
  <h1>View report</h1>
  <p>To view your HTML report, use the <strong>View report (HTML)</strong> button on the analysis page.</p>
  <p><a href="/">Back to KPI99 ICEA</a></p>
</body>
</html>"""
        )
    # POST
    from icea.auth import require_report_auth
    require_report_auth(
        x_api_key=req.headers.get("X-API-Key"),
        authorization=req.headers.get("Authorization"),
    )
    body = await req.json()
    request = AnalyzeRequest.model_validate(body)
    assumptions = request.assumptions or Assumptions()
    packing = compute_packing(request.node, request.executor, assumptions)
    cost = compute_cost(
        request.node,
        request.workload,
        packing,
        utilization_factor=getattr(request, "utilization_factor", None),
    )
    rec = recommend(
        request.node,
        request.executor,
        request.workload,
        assumptions,
        packing,
        cost,
    )
    risk = _risk_notes(request, packing)
    request_dict = request.model_dump()
    pdf_token = create_pending_report(request_dict)
    base = str(req.base_url).rstrip("/")
    pdf_url = f"{base}/v1/report-paid?token={pdf_token}&format=pdf"
    html = generate_report_html(
        request, packing, cost, rec, risk,
        pdf_download_url=pdf_url,
        app_url=base,
    )
    return HTMLResponse(html)


@app.post("/v1/request-expert")
def request_expert(body: ExpertRequest):
    """
    Tier 2 or Tier 3: submit contact + optional config. Persisted to DB; optional webhook/email notification.
    """
    config_dict = body.config.model_dump() if body.config else None
    request_id = store_expert_request(
        tier=body.tier,
        name=body.name,
        email=body.email,
        company=body.company,
        message=body.message,
        config=config_dict,
    )
    notify_expert_request(
        request_id=request_id,
        tier=body.tier,
        name=body.name,
        email=body.email,
        company=body.company,
        message=body.message,
        config=config_dict,
    )
    tier_label = "Expert Analysis ($1,500–$5,000)" if body.tier == "2" else "Enterprise Analysis ($5,000+)"
    return {
        "ok": True,
        "message": "Request received. We will contact you to complete payment and deliver your analysis.",
        "tier": body.tier,
        "tier_label": tier_label,
        "request_id": request_id,
    }


@app.post("/v1/ingest/eventlog")
async def ingest_eventlog(
    file: UploadFile = File(..., description="Spark event log (.json or .json.gz)"),
    executor_hourly_cost_usd: float | None = Form(None, description="Optional: hourly cost per executor for cost attribution"),
):
    """
    Ingest a Spark event log and return per-job metrics. Use the result with POST /v1/report/jobs to get a PDF.
    """
    if not file.filename or not file.filename.lower().endswith((".json", ".gz")):
        raise HTTPException(status_code=400, detail="File must be a .json or .json.gz Spark event log.")
    content = await file.read()
    if len(content) > 50 * 1024 * 1024:  # 50 MB limit
        raise HTTPException(status_code=400, detail="Event log too large (max 50 MB).")
    try:
        job_stages, job_times, stage_metrics = parse_event_log(content, file.filename or "")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse event log: {e!s}")
    jobs = aggregate_job_level(job_stages, job_times, stage_metrics, executor_hourly_cost_usd)
    total_executor_hours = sum(j["executor_hours"] for j in jobs)
    total_estimated_cost = sum(j["estimated_cost_usd"] or 0 for j in jobs)
    return {
        "jobs": [JobLevelSummary(**j) for j in jobs],
        "total_jobs": len(jobs),
        "total_executor_hours": round(total_executor_hours, 4),
        "total_estimated_cost_usd": round(total_estimated_cost, 2) if executor_hourly_cost_usd else None,
        "source_filename": file.filename,
    }


@app.post("/v1/report/jobs")
def report_jobs(body: JobReportRequest, request: Request):
    """
    Generate a job-level analysis PDF from ingested event log data (e.g. from POST /v1/ingest/eventlog).
    """
    static_dir = getattr(request.app.state, "static_dir", None)
    pdf_bytes = generate_job_report_pdf(
        [j.model_dump() for j in body.jobs],
        executor_hourly_cost_usd=body.executor_hourly_cost_usd,
        source_filename=body.source_filename or "",
        static_dir=static_dir,
    )
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=icea-job-report.pdf"},
    )


# Minimal sample event log (2 jobs); served by route registered in main.py so it wins over static mount
SAMPLE_EVENTLOG_BYTES = b"""{"Event":"SparkListenerJobStart","Job ID":0,"Submission Time":1000000,"Stage Infos":[{"Stage ID":0,"Stage Attempt ID":0,"Number of Tasks":2}]}
{"Event":"SparkListenerTaskEnd","Stage ID":0,"Task Info":{},"Task Metrics":{"Executor Run Time":120000,"Input Metrics":{"Bytes Read":1048576},"Output Metrics":{"Bytes Written":524288}}}
{"Event":"SparkListenerTaskEnd","Stage ID":0,"Task Info":{},"Task Metrics":{"Executor Run Time":95000,"Input Metrics":{"Bytes Read":524288}}}
{"Event":"SparkListenerJobEnd","Job ID":0,"Completion Time":1025000,"Job Result":{"Result":"JobSucceeded"}}
{"Event":"SparkListenerJobStart","Job ID":1,"Submission Time":2000000,"Stage Infos":[{"Stage ID":1,"Stage Attempt ID":0,"Number of Tasks":1}]}
{"Event":"SparkListenerTaskEnd","Stage ID":1,"Task Info":{},"Task Metrics":{"Executor Run Time":45000}}
{"Event":"SparkListenerJobEnd","Job ID":1,"Completion Time":2012000,"Job Result":{"Result":"JobSucceeded"}}
"""


def sample_eventlog_response():
    """Return the sample event log (used by main.py so route is registered before static mount)."""
    return Response(content=SAMPLE_EVENTLOG_BYTES, media_type="application/json")


def mount_static(app: FastAPI, static_dir: Path):
    """Mount static frontend if directory exists. Store for report logo path."""
    resolved = Path(static_dir).resolve()
    app.state.static_dir = resolved
    if resolved.is_dir():
        app.mount("/", StaticFiles(directory=str(resolved), html=True), name="static")
