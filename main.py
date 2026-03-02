"""ICEA MVP — run locally with: python main.py or uvicorn main:app --reload."""
from pathlib import Path

# Load .env so ICEA_DEMO=1 and other vars are set when running python main.py
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

from fastapi import HTTPException
from fastapi.responses import FileResponse, RedirectResponse
from icea.api import app, mount_static, sample_eventlog_response

# Register sample route before static mount so GET /v1/sample-eventlog is never caught by static
app.add_api_route("/v1/sample-eventlog", sample_eventlog_response, methods=["GET"])

# Serve frontend from project_root/static
STATIC_DIR = Path(__file__).resolve().parent / "static"
STATIC_DIR = STATIC_DIR.resolve()
INDEX_HTML = STATIC_DIR / "index.html"


def _static_file_safe(relative: str) -> Path | None:
    """Resolve path under STATIC_DIR; return None if outside or missing."""
    if not relative or ".." in relative or relative.startswith("/"):
        return None
    path = (STATIC_DIR / relative).resolve()
    if not path.is_relative_to(STATIC_DIR):
        return None
    return path if path.is_file() else (path / "index.html" if (path / "index.html").is_file() else None)


# Redirect full sample report to preview so production always shows the preview
@app.get("/sample-report.html")
def _redirect_sample_report():
    return RedirectResponse(url="/sample-report-preview.html", status_code=302)


# Root: support ?lang=es / ?lang=en so language is consistent when path is rewritten (e.g. by proxy)
@app.get("/")
def _serve_root(lang: str | None = None):
    if lang == "es":
        path = STATIC_DIR / "es" / "index.html"
        if path.is_file():
            return FileResponse(path)
    if lang == "en":
        path = STATIC_DIR / "en" / "index.html"
        if path.is_file():
            return FileResponse(path)
    if INDEX_HTML.is_file():
        return FileResponse(INDEX_HTML)
    raise HTTPException(status_code=404, detail="Not found")


# Language paths: serve en/ and es/ from static/en/ and static/es/ (must be before static catch-all)
@app.get("/en")
def _redirect_en():
    return RedirectResponse(url="/en/", status_code=302)


@app.get("/en/")
def _serve_en_index():
    path = STATIC_DIR / "en" / "index.html"
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(path)


@app.get("/en/{path:path}")
def _serve_en_path(path: str):
    resolved = _static_file_safe("en/" + path)
    if resolved is None:
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(resolved)


@app.get("/es")
def _redirect_es():
    return RedirectResponse(url="/es/", status_code=302)


@app.get("/es/")
def _serve_es_index():
    path = STATIC_DIR / "es" / "index.html"
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(path)


@app.get("/es/{path:path}")
def _serve_es_path(path: str):
    resolved = _static_file_safe("es/" + path)
    if resolved is None:
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(resolved)


# Static files: catch-all that does NOT handle /es, /en, or /v1 (so language and API routes always win)
@app.get("/{full_path:path}")
def _serve_static(full_path: str):
    if full_path.startswith("v1/") or full_path == "v1":
        raise HTTPException(status_code=404, detail="Not found")
    if full_path.startswith("es/") or full_path.startswith("es") or full_path.startswith("en/") or full_path.startswith("en"):
        raise HTTPException(status_code=404, detail="Not found")
    resolved = _static_file_safe(full_path)
    if resolved is None:
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(resolved)


mount_static(app, STATIC_DIR)

if __name__ == "__main__":
    import os
    import uvicorn
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
