"""Integration tests for HMAC webhook ingestion, idempotency deduplication, and outbox writes."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from sync_ledger.db.models import IdempotencyKey, InboundEvent, OutboxMessage
from tests.conftest import signed_webhook_headers

pytestmark = pytest.mark.integration


def test_webhook_writes_transactional_outbox_and_deduplicates_retries(
    client: TestClient,
    db_session: Session,
) -> None:
    payload = {
        "event_type": "customer.upserted",
        "entity_type": "customer",
        "external_id": "sf_001",
        "data": {"name": "Acme Corp.", "email": "ops@acme.io"},
    }
    raw, headers = signed_webhook_headers(
        payload,
        secret="crm-secret",
        idempotency_key="crm-evt-1001",
    )

    first = client.post("/v1/webhooks/crm", content=raw, headers=headers)
    assert first.status_code == 202
    first_json = first.json()
    assert first_json["status"] == "accepted"
    assert first_json["replayed"] is False
    assert "X-Idempotency-Replayed" not in first.headers

    # Duplicate delivery with identical Idempotency-Key & payload returns cached result
    duplicate = client.post("/v1/webhooks/crm", content=raw, headers=headers)
    assert duplicate.status_code == 202
    assert duplicate.headers["X-Idempotency-Replayed"] == "true"
    dup_json = duplicate.json()
    assert dup_json["replayed"] is True
    assert dup_json["event_id"] == first_json["event_id"]
    assert dup_json["outbox_id"] == first_json["outbox_id"]

    # Exactly ONE inbound_event, ONE outbox_message, and ONE idempotency_key persisted
    assert db_session.scalar(select(func.count()).select_from(InboundEvent)) == 1
    assert db_session.scalar(select(func.count()).select_from(OutboxMessage)) == 1
    assert db_session.scalar(select(func.count()).select_from(IdempotencyKey)) == 1


def test_idempotency_key_conflict_with_different_payload_returns_409(
    client: TestClient,
) -> None:
    payload_a = {
        "event_type": "customer.upserted",
        "entity_type": "customer",
        "external_id": "sf_002",
        "data": {"name": "Beta LLC", "email": "a@beta.io"},
    }
    raw_a, headers_a = signed_webhook_headers(
        payload_a,
        secret="crm-secret",
        idempotency_key="shared-key-conflict",
    )
    resp_a = client.post("/v1/webhooks/crm", content=raw_a, headers=headers_a)
    assert resp_a.status_code == 202

    payload_b = {
        "event_type": "customer.upserted",
        "entity_type": "customer",
        "external_id": "sf_002",
        "data": {"name": "Beta LLC", "email": "changed@beta.io"},
    }
    raw_b, headers_b = signed_webhook_headers(
        payload_b,
        secret="crm-secret",
        idempotency_key="shared-key-conflict",
    )
    resp_b = client.post("/v1/webhooks/crm", content=raw_b, headers=headers_b)
    assert resp_b.status_code == 409
    assert "different request payload" in resp_b.json()["detail"]


def test_webhook_rejects_invalid_signature_and_missing_idempotency_key(
    client: TestClient,
) -> None:
    payload = {
        "event_type": "customer.upserted",
        "entity_type": "customer",
        "external_id": "sf_unauth",
        "data": {"name": "Unauthorized Inc", "email": "u@example.com"},
    }
    raw, bad_headers = signed_webhook_headers(
        payload,
        secret="wrong-secret",
        idempotency_key="unauth-1",
    )
    resp_unauth = client.post("/v1/webhooks/crm", content=raw, headers=bad_headers)
    assert resp_unauth.status_code == 401

    raw_ok, ok_headers = signed_webhook_headers(
        payload,
        secret="crm-secret",
        idempotency_key="temp",
    )
    ok_headers.pop("Idempotency-Key")
    resp_missing_key = client.post("/v1/webhooks/crm", content=raw_ok, headers=ok_headers)
    assert resp_missing_key.status_code == 400
