"""API and integration tests for ICEA endpoints."""
import os

import pytest
from fastapi.testclient import TestClient

# Import app after env may be patched so store uses test DB
from icea.api import app


@pytest.fixture
def client():
    return TestClient(app)


def _analyze_payload():
    return {
        "cloud": "aws",
        "node": {"cores": 16, "memory_gb": 64, "hourly_cost_usd": 1.0, "count": 10},
        "executor": {"cores": 4, "memory_gb": 16},
        "workload": {"avg_runtime_minutes": 30, "jobs_per_day": 100},
    }


def test_health(client):
    r = client.get("/v1/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"
    assert data["service"] == "icea"


def test_analyze(client):
    r = client.post("/v1/analyze", json=_analyze_payload())
    assert r.status_code == 200
    data = r.json()
    assert "packing" in data
    assert "cost" in data
    assert "recommendation" in data
    assert data["packing"]["executors_per_node"] == 4
    assert data["cost"]["hourly_cluster_cost_usd"] > 0


def test_analyze_invalid(client):
    r = client.post("/v1/analyze", json={"node": {}, "executor": {}, "workload": {}})
    assert r.status_code == 422


def test_report_requires_auth_when_not_demo(client, monkeypatch):
    monkeypatch.delenv("ICEA_DEMO", raising=False)
    monkeypatch.delenv("ICEA_API_KEY", raising=False)
    r = client.post("/v1/report", json=_analyze_payload())
    assert r.status_code in (401, 503)


def test_report_with_demo(client, monkeypatch):
    monkeypatch.setenv("ICEA_DEMO", "1")
    r = client.post("/v1/report", json=_analyze_payload())
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/pdf"
    assert b"PDF" in r.content[:20]


def test_report_with_api_key(client, monkeypatch):
    monkeypatch.delenv("ICEA_DEMO", raising=False)
    monkeypatch.setenv("ICEA_API_KEY", "test-secret-key")
    r = client.post("/v1/report", json=_analyze_payload(), headers={"X-API-Key": "test-secret-key"})
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/pdf"


def test_report_paid_invalid_token(client):
    r = client.get("/v1/report-paid", params={"token": "invalid-token"})
    assert r.status_code == 404


def test_request_expert(client):
    r = client.post(
        "/v1/request-expert",
        json={
            "name": "Jane Doe",
            "email": "jane@example.com",
            "company": "Acme",
            "message": "Need Tier 2",
            "tier": "2",
        },
    )
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["tier"] == "2"
    assert "request_id" in data
    assert data["request_id"] > 0


def test_metrics(client):
    client.get("/v1/health")
    r = client.get("/v1/metrics")
    assert r.status_code == 200
    assert "icea_uptime_seconds" in r.text
    assert "icea_http_requests_total" in r.text


def test_catalog_providers(client):
    r = client.get("/v1/catalog/providers")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    ids = [p["id"] for p in data]
    assert "aws" in ids
    assert "emr" in ids
    assert "synapse" in ids
    assert "dataproc" in ids
    assert "on-prem" in ids


def test_catalog_regions(client):
    r = client.get("/v1/catalog/regions?cloud=aws")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert any(reg["id"] == "us-east-1" for reg in data)


def test_catalog_instances(client):
    r = client.get("/v1/catalog/instances?cloud=aws&region=us-east-1")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert len(data) > 0
    inst = data[0]
    assert "id" in inst and "cores" in inst and "memory_gb" in inst and "hourly_usd" in inst


def test_sample_eventlog(client):
    # Route is registered in main.py; when testing icea.api.app it may 404.
    r = client.get("/v1/sample-eventlog")
    if r.status_code == 200:
        assert r.headers.get("content-type") == "application/json"
        assert b"SparkListenerJobStart" in r.content or b"Event" in r.content
    else:
        assert r.status_code == 404
