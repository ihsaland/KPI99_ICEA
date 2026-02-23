"""ICEA MVP — run locally with: python main.py or uvicorn main:app --reload."""
from pathlib import Path

# Load .env so ICEA_DEMO=1 and other vars are set when running python main.py
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

from icea.api import app, mount_static, sample_eventlog_response

# Register sample route before static mount so GET /v1/sample-eventlog is never caught by static
app.add_api_route("/v1/sample-eventlog", sample_eventlog_response, methods=["GET"])

# Serve frontend from project_root/static
STATIC_DIR = Path(__file__).resolve().parent / "static"
mount_static(app, STATIC_DIR)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
