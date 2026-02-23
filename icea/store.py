"""Persistent store for payment tokens, expert requests, and audit (SQLite). Multi-instance: use shared DB path or Redis for scale-out."""
import json
import logging
import os
import secrets
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

_LOG = logging.getLogger(__name__)
_EXPIRY_SECONDS = 3600
_FALLBACK_DB = str(Path(__file__).resolve().parent.parent / "data" / "icea.db")


def _get_db_path() -> str:
    return (os.environ.get("ICEA_DB_PATH") or "").strip() or _FALLBACK_DB
_RETENTION_DAYS_EXPERT = int(os.environ.get("ICEA_RETENTION_DAYS_EXPERT", "90"))
_RETENTION_DAYS_AUDIT = int(os.environ.get("ICEA_RETENTION_DAYS_AUDIT", "365"))


def _conn(db_path: str | None = None) -> sqlite3.Connection:
    path = (db_path or _get_db_path()).strip() or _get_db_path()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=15.0)
    conn.row_factory = sqlite3.Row
    return conn


_lock = threading.Lock()


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS pending_reports (
            token TEXT PRIMARY KEY,
            request_json TEXT NOT NULL,
            created_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_pending_reports_created ON pending_reports(created_at);
        CREATE TABLE IF NOT EXISTS expert_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tier TEXT NOT NULL,
            name TEXT,
            email TEXT,
            company TEXT,
            message TEXT,
            config_json TEXT,
            created_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_expert_requests_created ON expert_requests(created_at);
        CREATE TABLE IF NOT EXISTS report_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_preview TEXT NOT NULL,
            delivered_at REAL NOT NULL,
            request_id TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_report_audit_delivered ON report_audit(delivered_at);
    """)
    conn.commit()


def audit_report_delivered(token: str, request_id: str | None, db_path: str | None = None) -> None:
    """Record that a report was delivered (for compliance/audit). Token stored as last 4 chars only."""
    preview = token[-4:] if len(token) >= 4 else "****"
    now = time.time()
    with _lock:
        conn = _conn(db_path)
        try:
            _init_schema(conn)
            conn.execute(
                "INSERT INTO report_audit (token_preview, delivered_at, request_id) VALUES (?, ?, ?)",
                (preview, now, request_id or ""),
            )
            conn.commit()
        finally:
            conn.close()


def prune_retention(db_path: str | None = None) -> tuple[int, int]:
    """Delete expert_requests and report_audit older than configured retention days. Returns (expert_deleted, audit_deleted)."""
    now = time.time()
    expert_cut = now - _RETENTION_DAYS_EXPERT * 86400
    audit_cut = now - _RETENTION_DAYS_AUDIT * 86400
    with _lock:
        conn = _conn(db_path)
        try:
            _init_schema(conn)
            cur = conn.execute("DELETE FROM expert_requests WHERE created_at < ?", (expert_cut,))
            expert_deleted = cur.rowcount
            cur = conn.execute("DELETE FROM report_audit WHERE delivered_at < ?", (audit_cut,))
            audit_deleted = cur.rowcount
            conn.commit()
            if expert_deleted or audit_deleted:
                _LOG.info("Retention prune: expert_requests=%s, report_audit=%s", expert_deleted, audit_deleted)
            return (expert_deleted, audit_deleted)
        finally:
            conn.close()


def create_pending_report(request_dict: dict[str, Any], db_path: str | None = None) -> str:
    """Store request for Tier 1; return one-time token. Prune expired rows."""
    token = secrets.token_urlsafe(32)
    now = time.time()
    with _lock:
        conn = _conn(db_path)
        try:
            _init_schema(conn)
            conn.execute(
                "INSERT INTO pending_reports (token, request_json, created_at) VALUES (?, ?, ?)",
                (token, json.dumps(request_dict), now),
            )
            conn.execute("DELETE FROM pending_reports WHERE created_at < ?", (now - _EXPIRY_SECONDS,))
            conn.commit()
        finally:
            conn.close()
    return token


def get_pending_report(token: str, db_path: str | None = None) -> dict[str, Any] | None:
    """Return request for token if valid and not expired (does not consume token)."""
    with _lock:
        conn = _conn(db_path)
        try:
            _init_schema(conn)
            row = conn.execute(
                "SELECT request_json, created_at FROM pending_reports WHERE token = ?",
                (token,),
            ).fetchone()
            if not row:
                return None
            created = row["created_at"]
            if time.time() - created > _EXPIRY_SECONDS:
                conn.execute("DELETE FROM pending_reports WHERE token = ?", (token,))
                conn.commit()
                return None
            return json.loads(row["request_json"])
        finally:
            conn.close()


def consume_pending_report(token: str, db_path: str | None = None) -> dict[str, Any] | None:
    """Return and remove request for token if valid and not expired."""
    with _lock:
        conn = _conn(db_path)
        try:
            _init_schema(conn)
            row = conn.execute(
                "SELECT request_json, created_at FROM pending_reports WHERE token = ?",
                (token,),
            ).fetchone()
            if not row:
                return None
            created = row["created_at"]
            if time.time() - created > _EXPIRY_SECONDS:
                conn.execute("DELETE FROM pending_reports WHERE token = ?", (token,))
                conn.commit()
                return None
            conn.execute("DELETE FROM pending_reports WHERE token = ?", (token,))
            conn.commit()
            return json.loads(row["request_json"])
        finally:
            conn.close()


def add_expert_request(
    tier: str,
    name: str | None,
    email: str | None,
    company: str | None,
    message: str | None,
    config: dict[str, Any] | None,
    db_path: str | None = None,
) -> int:
    """Persist expert request; return row id."""
    now = time.time()
    config_json = json.dumps(config) if config else None
    with _lock:
        conn = _conn(db_path)
        try:
            _init_schema(conn)
            cur = conn.execute(
                """INSERT INTO expert_requests (tier, name, email, company, message, config_json, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (tier, name or "", email or "", company or "", message or "", config_json, now),
            )
            conn.commit()
            return cur.lastrowid or 0
        finally:
            conn.close()


def get_store_db_path() -> str:
    """Return the DB path in use (for logging/debug)."""
    return _get_db_path()
