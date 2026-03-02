"""ICEA MVP — run locally with: python main.py or uvicorn main:app --reload."""
from pathlib import Path

# Load .env so ICEA_DEMO=1 and other vars are set when running python main.py
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

from fastapi.responses import FileResponse, RedirectResponse
from icea.api import app, mount_static, sample_eventlog_response

# Register sample route before static mount so GET /v1/sample-eventlog is never caught by static
app.add_api_route("/v1/sample-eventlog", sample_eventlog_response, methods=["GET"])

# Serve frontend from project_root/static
STATIC_DIR = Path(__file__).resolve().parent / "static"
INDEX_HTML = STATIC_DIR / "index.html"

# Redirect full sample report to preview so production always shows the preview
@app.get("/sample-report.html")
def _redirect_sample_report():
    return RedirectResponse(url="/sample-report-preview.html", status_code=302)


# So that icea.kpi99.co/en/ and icea.kpi99.co/es/ are accessible (same single-page app as /)
@app.get("/en", response_class=FileResponse)
@app.get("/en/", response_class=FileResponse)
@app.get("/es", response_class=FileResponse)
@app.get("/es/", response_class=FileResponse)
def _serve_app_lang_path():
    if INDEX_HTML.is_file():
        return FileResponse(INDEX_HTML)
    from fastapi import HTTPException
    raise HTTPException(status_code=404, detail="Not found")

mount_static(app, STATIC_DIR)

if __name__ == "__main__":
    import os
    import uvicorn
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
