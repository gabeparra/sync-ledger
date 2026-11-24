"""Integration tests for /health, /health/ready, and Prometheus /metrics endpoints."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from sync_ledger.worker.runner import OutboxWorker
from tests.conftest import signed_webhook_headers

pytestmark = pytest.mark.integration


def test_health_and_readiness_endpoints(client: TestClient) -> None:
    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    assert health.json()["service"] == "sync-ledger"

    ready = client.get("/health/ready")
    assert ready.status_code == 200
    body = ready.json()
    assert body["status"] == "ready"
    assert body["database"] == "connected"
    assert body["outbox_pending"] == 0
    assert body["dlq_open"] == 0


def test_prometheus_metrics_expose_ingestion_and_queue_series(
    client: TestClient,
    worker: OutboxWorker,
) -> None:
    payload = {
        "event_type": "customer.upserted",
        "entity_type": "customer",
        "external_id": "sf_obs_1",
        "data": {"name": "Metrics Co", "email": "metrics@example.com"},
    }
    raw, headers = signed_webhook_headers(payload, secret="crm-secret", idempotency_key="obs-1")
    assert client.post("/v1/webhooks/crm", content=raw, headers=headers).status_code == 202

    metrics_before = client.get("/metrics")
    assert metrics_before.status_code == 200
    assert "text/plain" in metrics_before.headers["content-type"]
    text_before = metrics_before.text
    assert "sync_ledger_webhooks_received_total" in text_before
    assert "sync_ledger_outbox_pending_messages 1.0" in text_before

    worker.run_once()

    metrics_after = client.get("/metrics")
    assert metrics_after.status_code == 200
    text_after = metrics_after.text
    assert "sync_ledger_outbox_processed_total" in text_after
    assert "sync_ledger_reconciliation_matches_total" in text_after
    assert "sync_ledger_reconciliation_duration_seconds_count" in text_after
    assert "sync_ledger_outbox_pending_messages 0.0" in text_after
