#!/usr/bin/env python3
"""Generate static/sample-report.html from the real report generator (sample inputs).
Run from repo root: python scripts/generate_sample_report.py
"""
import sys
from pathlib import Path

# Allow running without PYTHONPATH
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))


from icea.models import (
    AnalyzeRequest,
    Assumptions,
    NodeConfig,
    ExecutorConfig,
    WorkloadConfig,
)
from icea.packing import compute_packing
from icea.cost_model import compute_cost
from icea.recommend import recommend
from icea.report.html_report import generate_report_html
from icea.api import _risk_notes

def main():
    root = Path(__file__).resolve().parent.parent
    static_dir = root / "static"
    out_path = static_dir / "sample-report.html"

    req = AnalyzeRequest(
        cloud="aws",
        node=NodeConfig(cores=16, memory_gb=64, hourly_cost_usd=0.85, count=10),
        executor=ExecutorConfig(cores=4, memory_gb=16),
        workload=WorkloadConfig(avg_runtime_minutes=30, jobs_per_day=100),
        assumptions=Assumptions(),
        region="us-east-1",
        instance_type="r6g.4xlarge",
    )
    assumptions = req.assumptions or Assumptions()
    packing = compute_packing(req.node, req.executor, assumptions)
    cost = compute_cost(req.node, req.workload, packing, utilization_factor=None)
    rec = recommend(req.node, req.executor, req.workload, assumptions, packing, cost)
    risk = _risk_notes(req, packing)

    html = generate_report_html(
        req, packing, cost, rec,
        risk_notes=risk,
        pdf_download_url=None,
        app_url=None,
    )
    static_dir.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    print(f"Wrote {out_path}")

if __name__ == "__main__":
    main()
