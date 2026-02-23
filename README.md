# ICEA — Infrastructure Cost & Efficiency Analyzer (MVP)

Lightweight diagnostic tool that converts Spark cluster and executor configuration into efficiency scores, waste estimates, and recommended configurations. **Pay-per-use monetization** with three tiers.

## Pricing (pay per analysis)

| Tier | Price | What you get |
|------|--------|----------------|
| **Tier 1 — Automated Report** | $299 | Upload config → receive PDF report (Stripe checkout). |
| **Tier 2 — Expert Analysis** | $1,500–$5,000 | Tool output + manual review, optimization recommendations, configuration guidance. |
| **Tier 3 — Enterprise** | $5,000+ | Custom scope, dedicated review, ongoing support. |

Tier 1 requires **Stripe** (set `STRIPE_SECRET_KEY`). Tiers 2 and 3 submit a request form; you follow up manually for payment and delivery.

## Quick start

```bash
# Install dependencies
python3 -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements.txt

# Run locally
PYTHONPATH=. python main.py
```

Open **http://127.0.0.1:8000** in your browser.

**Enable Tier 1 payments:** copy `.env.example` to `.env` and set `STRIPE_SECRET_KEY` (use `sk_test_...` for test mode). Restart the app.

**Development only — demo mode:** set `ICEA_DEMO=1` to show a "Demo — get report" option that returns the PDF without payment. Do not set in production.

## Features

- **Web form**: Environment (cloud, node cores/memory/cost/count), executors (cores, memory, reserves), workload (avg runtime, jobs/day).
- **Live preview**: Efficiency score (0–100), estimated monthly waste, executors per node, projected savings.
- **PDF report**: Generate a full branded report (executive summary, current vs recommended config, cost assumptions, savings, engineering notes).
- **Monetization**: Tier 1 → `POST /v1/checkout/tier1` (Stripe Checkout), then `GET /v1/report-paid?token=...` after payment. Tiers 2/3 → `POST /v1/request-expert` (contact form).
- **API**: `POST /v1/analyze`, `POST /v1/report` (PDF; requires API key or demo), `GET /v1/health`, `GET /v1/metrics`.

## Project layout

- `icea/` — Core engine and API  
  - `models.py` — Input/output types  
  - `packing.py` — Executor packing and utilization  
  - `cost_model.py` — Cost calculations  
  - `recommend.py` — Recommendation grid search  
  - `report/` — PDF generation (ReportLab)  
  - `api.py` — FastAPI routes  
- `static/` — Frontend (single-page form)  
- `tests/` — Unit tests for packing and cost model  

## Run tests

```bash
pip install pytest httpx
PYTHONPATH=. pytest tests/ -v
```

Estimates are directional and depend on workload behavior.

---

## Security

- **Secrets**: Do not commit `.env`. Use env vars or a secrets manager (e.g. Vault) for `STRIPE_SECRET_KEY`, `ICEA_API_KEY`, `SMTP_*`, and webhook URLs. See `.env.example` for all options.
- **Production**: Set `ICEA_ENV=production` (or `NODE_ENV=production`). Do **not** set `ICEA_DEMO=1` in production; the app logs a critical warning and can fail startup if `ICEA_DEMO_FAIL_IN_PROD=1` is set.
- **API access**: When not in demo mode, `POST /v1/report` requires an API key via `X-API-Key` or `Authorization: Bearer <key>`. Set `ICEA_API_KEY` and send it with programmatic requests.
- **Rate limiting**: Per-IP limits apply (default 120 requests per 60s). Configure with `ICEA_RATE_LIMIT_REQUESTS` and `ICEA_RATE_LIMIT_WINDOW_SEC`.
- **CORS**: Set `ICEA_CORS_ORIGINS` to a comma-separated list of allowed origins (or `*` for all). Empty = same-origin only.

## Integration

- **API key**: Set `ICEA_API_KEY`; use header `X-API-Key: <key>` or `Authorization: Bearer <key>` for `POST /v1/report` when `ICEA_DEMO` is not set.
- **Expert requests (Tier 2/3)**: Stored in the same SQLite DB. Optional notifications:
  - **Webhook**: Set `ICEA_EXPERT_REQUEST_WEBHOOK` to a URL; the app POSTs a JSON body on each new request.
  - **Email**: Set `ICEA_EXPERT_REQUEST_EMAIL` and SMTP env vars (`SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, `SMTP_FROM`).
- **State**: Payment tokens and expert requests use SQLite at `ICEA_DB_PATH` (default `data/icea.db`). For multiple instances, use a shared path or replace with a shared DB/Redis.

## Deployment

- **Docker**: Build and run with a volume for data:
  ```bash
  docker build -t icea:latest .
  docker run -p 8000:8000 -v icea-data:/data -e ICEA_ENV=production icea:latest
  ```
- **Kubernetes**: Example manifests in `deploy/k8s-deployment.yaml` (Deployment, Service, PVC). Probes use `GET /v1/health` for liveness and readiness.
- **Health**: `GET /v1/health` returns `{"status":"ok","service":"icea"}`. Use for load balancers and orchestrator health checks.
- **Metrics**: `GET /v1/metrics` returns Prometheus-style metrics (request counts, uptime, duration).

## SLA and limits

- **Timeouts**: Analyze and report generation have configurable timeouts (`ICEA_ANALYZE_TIMEOUT_SEC`, `ICEA_REPORT_TIMEOUT_SEC`). Defaults 60s and 120s; 504 on timeout.
- **Cache**: Optional in-memory cache for `POST /v1/analyze` (`ICEA_ANALYZE_CACHE_TTL_SEC`, `ICEA_ANALYZE_CACHE_MAX`). Set TTL to 0 to disable.
- **Retention**: On startup, the app prunes expert requests and report-audit rows older than `ICEA_RETENTION_DAYS_EXPERT` (default 90) and `ICEA_RETENTION_DAYS_AUDIT` (default 365). Payment tokens expire after 1 hour.
- **Audit**: Report delivery (Tier 1 post-payment) is logged to `report_audit` (token preview, timestamp, request ID) for compliance.
