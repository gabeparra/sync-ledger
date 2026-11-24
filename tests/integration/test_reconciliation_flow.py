"""Integration tests for cross-system reconciliation without a shared primary key."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from sync_ledger.worker.runner import OutboxWorker
from tests.conftest import signed_webhook_headers

pytestmark = pytest.mark.integration


def _post_event(
    client: TestClient,
    *,
    source: str,
    secret: str,
    idempotency_key: str,
    body: dict[str, object],
) -> None:
    raw, headers = signed_webhook_headers(body, secret=secret, idempotency_key=idempotency_key)
    resp = client.post(f"/v1/webhooks/{source}", content=raw, headers=headers)
    assert resp.status_code == 202, resp.text


def test_three_systems_reconcile_without_shared_id_and_emit_drift_diff_snapshot(
    client: TestClient,
    worker: OutboxWorker,
) -> None:
    # 1. CRM sends native ID sf_001A (name + email + phone + postal)
    _post_event(
        client,
        source="crm",
        secret="crm-secret",
        idempotency_key="k-crm-1",
        body={
            "event_type": "customer.upserted",
            "entity_type": "customer",
            "external_id": "sf_001A",
            "data": {
                "name": "Acme Corp.",
                "email": "ops@acme.io",
                "phone": "(415) 555-0199",
                "postal_code": "94107",
            },
        },
    )

    # 2. Billing sends Stripe ID cus_stripe_99 (no sf_001A! Matches via email/phone/name)
    _post_event(
        client,
        source="billing",
        secret="billing-secret",
        idempotency_key="k-billing-1",
        body={
            "event_type": "customer.updated",
            "entity_type": "customer",
            "external_id": "cus_stripe_99",
            "data": {
                "name": "ACME Inc",
                "email": "OPS@acme.io",
                "phone": "+1-415-555-0199",
                "tax_id": "12-3456789",
                "balance_cents": 245000,
                "currency": "USD",
            },
        },
    )

    # 3. ERP sends NetSuite ID NS-4401 (no sf_001A or cus_stripe_99! Matches via tax_id,
    #    introduces genuine drift in both balance_cents and email)
    _post_event(
        client,
        source="erp",
        secret="erp-secret",
        idempotency_key="k-erp-1",
        body={
            "event_type": "account.synced",
            "entity_type": "customer",
            "external_id": "NS-4401",
            "data": {
                "name": "Acme Corporation",
                "tax_id": "123456789",
                "email": "ap@acme.io",
                "balance_cents": 240000,
                "currency": "USD",
                "postal_code": "94107-1234",
            },
        },
    )

    stats = worker.run_once()
    assert stats.claimed == 3
    assert stats.processed == 3
    assert stats.dead_lettered == 0

    entities = client.get("/v1/entities?entity_type=customer").json()
    assert len(entities) == 1
    entity = entities[0]
    assert entity["has_active_drift"] is True
    assert entity["version"] == 3

    linked_pairs = {(lnk["source_system"], lnk["external_id"]) for lnk in entity["links"]}
    assert linked_pairs == {
        ("crm", "sf_001A"),
        ("billing", "cus_stripe_99"),
        ("erp", "NS-4401"),
    }

    # Authoritative values: billing owns balance_cents, crm owns email, erp owns tax_id
    assert entity["canonical_data"]["balance_cents"] == 245000
    assert entity["canonical_data"]["email"] == "ops@acme.io"
    assert entity["canonical_data"]["tax_id"] == "123456789"

    # Verify drift audit snapshot captured the exact field diffs
    drift_snapshots = client.get("/v1/reconciliations/snapshots?drift_only=true").json()
    assert len(drift_snapshots) == 1
    snap = drift_snapshots[0]
    assert snap["status"] == "drifted"
    assert snap["match_strategy"] == "natural_key"
    assert snap["drift_count"] == 2
    diff_fields = {d["field"]: d for d in snap["field_diffs"]}
    assert diff_fields["balance_cents"]["severity"] == "high"
    assert diff_fields["balance_cents"]["source_values"] == {"billing": 245000, "erp": 240000}
    assert diff_fields["email"]["source_values"]["erp"] == "ap@acme.io"

    # Verify anomaly table recorded cross_system_drift
    anomalies = client.get("/v1/reconciliations/anomalies?anomaly_type=cross_system_drift").json()
    assert len(anomalies) == 1
    assert anomalies[0]["severity"] == "high"


def test_intra_source_duplicate_record_is_flagged_as_anomaly(
    client: TestClient,
    worker: OutboxWorker,
) -> None:
    _post_event(
        client,
        source="crm",
        secret="crm-secret",
        idempotency_key="dup-1",
        body={
            "event_type": "customer.upserted",
            "entity_type": "customer",
            "external_id": "sf_orig_100",
            "data": {"name": "Globex Corp", "email": "ceo@globex.io", "tax_id": "98-7654321"},
        },
    )
    # Same CRM system accidentally creates a duplicate contact ID for the exact same company!
    _post_event(
        client,
        source="crm",
        secret="crm-secret",
        idempotency_key="dup-2",
        body={
            "event_type": "customer.upserted",
            "entity_type": "customer",
            "external_id": "sf_dup_999",
            "data": {"name": "Globex Inc.", "email": "ceo@globex.io", "tax_id": "987654321"},
        },
    )

    stats = worker.run_once()
    assert stats.processed == 2

    anomalies = client.get("/v1/reconciliations/anomalies?anomaly_type=duplicate_record").json()
    assert len(anomalies) == 1
    assert anomalies[0]["external_id"] == "sf_dup_999"
    assert anomalies[0]["details"]["existing_external_ids"] == ["sf_orig_100"]


def test_invoice_reconciliation_across_billing_and_erp_detects_amount_drift(
    client: TestClient,
    worker: OutboxWorker,
) -> None:
    _post_event(
        client,
        source="billing",
        secret="billing-secret",
        idempotency_key="inv-b-1",
        body={
            "event_type": "invoice.finalized",
            "entity_type": "invoice",
            "external_id": "in_stripe_42",
            "data": {
                "invoice_number": "INV-2026-0042",
                "customer_email": "ap@initech.io",
                "amount_cents": 150000,
                "currency": "USD",
                "payment_status": "paid",
            },
        },
    )
    _post_event(
        client,
        source="erp",
        secret="erp-secret",
        idempotency_key="inv-e-1",
        body={
            "event_type": "invoice.posted",
            "entity_type": "invoice",
            "external_id": "NS-INV-882",
            "data": {
                "invoice_number": "inv20260042",
                "customer_email": "ap@initech.io",
                "amount_cents": 145000,
                "currency": "usd",
                "payment_status": "open",
            },
        },
    )

    stats = worker.run_once()
    assert stats.processed == 2

    entities = client.get("/v1/entities?entity_type=invoice&drift_only=true").json()
    assert len(entities) == 1
    assert len(entities[0]["links"]) == 2
    assert entities[0]["canonical_data"]["amount_cents"] == 150000
    assert entities[0]["canonical_data"]["payment_status"] == "paid"


def test_ambiguous_multi_candidate_match_is_flagged_instead_of_corrupting_entities(
    client: TestClient,
    worker: OutboxWorker,
) -> None:
    # Two distinct legal entities (different tax_ids) sharing name & postal code
    _post_event(
        client,
        source="erp",
        secret="erp-secret",
        idempotency_key="amb-1",
        body={
            "event_type": "account.synced",
            "entity_type": "customer",
            "external_id": "NS-100",
            "data": {"name": "Northstar Corp", "postal_code": "10001", "tax_id": "11-1111111"},
        },
    )
    _post_event(
        client,
        source="erp",
        secret="erp-secret",
        idempotency_key="amb-2",
        body={
            "event_type": "account.synced",
            "entity_type": "customer",
            "external_id": "NS-200",
            "data": {"name": "Northstar Inc.", "postal_code": "10001", "tax_id": "22-2222222"},
        },
    )
    assert worker.run_once().processed == 2

    # CRM sends a record with NO tax_id that ties identically with BOTH entities (score=0.70 each)
    _post_event(
        client,
        source="crm",
        secret="crm-secret",
        idempotency_key="amb-3",
        body={
            "event_type": "customer.upserted",
            "entity_type": "customer",
            "external_id": "sf_amb_3",
            "data": {"name": "Northstar LLC", "postal_code": "10001"},
        },
    )
    assert worker.run_once().processed == 1

    anomalies = client.get("/v1/reconciliations/anomalies?anomaly_type=ambiguous_match").json()
    assert len(anomalies) == 1
    assert anomalies[0]["severity"] == "high"
    assert len(anomalies[0]["details"]["candidate_entity_ids"]) == 2
