# ICEA MVP — production image
FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application
COPY main.py .
COPY icea/ icea/
COPY static/ static/

# Persistent data (mount a volume at /data in production)
ENV ICEA_DB_PATH=/data/icea.db
RUN mkdir -p /data

EXPOSE 8000

# Railway and similar platforms set PORT at runtime; default 8000 for local Docker
ENV PORT=8000
# Health check: GET /v1/health (use PORT so it matches the app in cloud environments)
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD python -c "import os, urllib.request; port = os.environ.get('PORT', '8000'); urllib.request.urlopen(f'http://127.0.0.1:{port}/v1/health', timeout=5)" || exit 1

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT}"]
