# ICEA at icea.kpi99.co

This document describes how to run and deploy [ICEA](https://github.com/ihsaland/KPI99_ICEA) as the subdomain **icea.kpi99.co**.

## Local development (use icea.kpi99.co on your machine)

1. **Run ICEA** from the project root (e.g. `/Users/iansalandy/KPI99_ICEA`):
   ```bash
   cd /Users/iansalandy/KPI99_ICEA
   python3 -m venv .venv
   source .venv/bin/activate   # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   PYTHONPATH=. python main.py
   ```
   The app listens on **http://0.0.0.0:8000** (http://localhost:8000).

2. **Resolve icea.kpi99.co to your machine** so the browser uses the subdomain:
   - Edit hosts (requires admin):
     - **macOS/Linux:** `sudo nano /etc/hosts`
     - **Windows:** `C:\Windows\System32\drivers\etc\hosts`
   - Add a line:
     ```
     127.0.0.1   icea.kpi99.co
     ```
   Save and exit.

3. **Allow CORS for the subdomain** so the app accepts requests when opened as http://icea.kpi99.co:8000:
   - Copy `.env.example` to `.env` if you don’t have one.
   - Set (for local use):
     ```
     ICEA_CORS_ORIGINS=http://icea.kpi99.co:8000,http://localhost:8000,http://127.0.0.1:8000
     ```
   - Restart the app.

4. **Open in browser:**  
   **http://icea.kpi99.co:8000**  
   (Port 8000 is required because there is no local reverse proxy.)

For local dev without the subdomain, use **http://localhost:8000** and omit the hosts change; CORS can be `http://localhost:8000` or `*` for same-machine testing.

## Production (icea.kpi99.co on the internet)

1. **Deploy ICEA** to a host (e.g. Railway, Render, or a VPS):
   - Use the repo: https://github.com/ihsaland/KPI99_ICEA
   - Run with Docker (`docker build -t icea . && docker run -p 8000:8000 ...`) or with `python main.py` behind a process manager (e.g. gunicorn/uvicorn).
   - Set env: `ICEA_ENV=production`, and do **not** set `ICEA_DEMO=1`.
   - Set CORS for the public subdomain:
     ```
     ICEA_CORS_ORIGINS=https://icea.kpi99.co,https://kpi99.co
     ```

2. **DNS for icea.kpi99.co** (at your domain registrar or DNS provider for kpi99.co):
   - Add a **CNAME** record: `icea` → your deployment hostname (e.g. `your-app.railway.app`),  
     **or**
   - Add an **A** record: `icea` → IP of the server running ICEA.

3. **HTTPS:** Use a reverse proxy (e.g. Nginx, Caddy, or the platform’s built-in SSL) in front of the app so icea.kpi99.co is served over HTTPS (port 443) and proxies to the ICEA process (e.g. port 8000).

4. **Optional:** Restrict access (e.g. password or VPN) the same way as for diagnostic.kpi99.co if you want the subdomain to be non-public.

## Summary

| Context   | URL                      | Notes                                      |
|----------|--------------------------|--------------------------------------------|
| Local    | http://icea.kpi99.co:8000 | After hosts + CORS; app in KPI99_ICEA.     |
| Local    | http://localhost:8000    | No hosts change; CORS localhost or `*`.    |
| Production | https://icea.kpi99.co   | DNS + deploy + HTTPS proxy; CORS set above.|
