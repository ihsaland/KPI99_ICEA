"""Optional notifications for expert requests (webhook or email)."""
import json
import os
import smtplib
import urllib.request
from email.mime.text import MIMEText
from email.utils import formatdate
from typing import Any


def notify_expert_request(
    request_id: int,
    tier: str,
    name: str,
    email: str,
    company: str | None,
    message: str | None,
    config: dict[str, Any] | None,
) -> None:
    """
    If ICEA_EXPERT_REQUEST_WEBHOOK or ICEA_EXPERT_REQUEST_EMAIL is set, send a notification.
    Does not raise; log and ignore errors.
    """
    webhook = (os.environ.get("ICEA_EXPERT_REQUEST_WEBHOOK") or "").strip()
    if webhook:
        _send_webhook(webhook, request_id, tier, name, email, company, message, config)
    notify_email = (os.environ.get("ICEA_EXPERT_REQUEST_EMAIL") or "").strip()
    if notify_email:
        _send_email(notify_email, request_id, tier, name, email, company, message, config)


def _send_webhook(
    url: str,
    request_id: int,
    tier: str,
    name: str,
    email: str,
    company: str | None,
    message: str | None,
    config: dict[str, Any] | None,
) -> None:
    try:
        body = json.dumps({
            "event": "expert_request",
            "request_id": request_id,
            "tier": tier,
            "name": name,
            "email": email,
            "company": company,
            "message": message,
            "config": config,
        }).encode("utf-8")
        req = urllib.request.Request(url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning("Expert request webhook failed: %s", e)


def _send_email(
    to_addr: str,
    request_id: int,
    tier: str,
    name: str,
    email: str,
    company: str | None,
    message: str | None,
    config: dict[str, Any] | None,
) -> None:
    try:
        host = (os.environ.get("SMTP_HOST") or "localhost").strip()
        port = int(os.environ.get("SMTP_PORT") or "25")
        user = os.environ.get("SMTP_USER", "").strip() or None
        password = os.environ.get("SMTP_PASSWORD", "").strip() or None
        from_addr = (os.environ.get("SMTP_FROM") or user or "icea@localhost").strip()
        subject = f"ICEA Expert Request #{request_id} — Tier {tier} — {name}"
        body = f"""New expert request:

Request ID: {request_id}
Tier: {tier}
Name: {name}
Email: {email}
Company: {company or '-'}
Message: {message or '-'}
Config: {json.dumps(config) if config else '-'}
"""
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = to_addr
        msg["Date"] = formatdate(localtime=True)
        with smtplib.SMTP(host, port, timeout=10) as smtp:
            if user and password:
                smtp.starttls()
                smtp.login(user, password)
            smtp.sendmail(from_addr, [to_addr], msg.as_string())
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning("Expert request email failed: %s", e)
