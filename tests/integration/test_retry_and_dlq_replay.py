"""Integration tests for exponential retry backoff, poison-pill DLQ routing, and replay."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session
from typer.testing import CliRunner

from sync_ledger.cli import app as cli_app
from sync_ledger.config import Settings
from sync_ledger.db.models import CanonicalEntity, DeadLetterMessage, OutboxMessage
from sync_ledger.db.session import DatabaseManager, init_db
from sync_ledger.reconciliation.engine import ReconciliationEngine, ReconciliationOutcome
from sync_ledger.reconciliation.validator import TransientReconciliationError
from sync_ledger.worker.runner import OutboxWorker
from tests.conftest import signed_webhook_headers

pytestmark = pytest.mark.integration


def test_transient_failures_use_exponential_backoff_and_succeed_before_dlq(
    client: TestClient,
    settings: Settings,
    db_manager: DatabaseManager,
    db_session: Session,
) -> None:
    body = {
        "event_type": "customer.upserted",
        "entity_type": "customer",
        "external_id": "sf_flaky_1",
        "data": {"name": "Flaky Co", "email": "ops@flaky.io"},
    }
    raw, headers = signed_webhook_headers(body, secret="crm-secret", idempotency_key="flaky-key-1")
    assert client.post("/v1/webhooks/crm", content=raw, headers=headers).status_code == 202

    real_engine = ReconciliationEngine(settings)
    calls = {"count": 0}

    def flaky_handler(session: Session, msg: OutboxMessage) -> ReconciliationOutcome:
        calls["count"] += 1
        if calls["count"] <= 2:
            raise TransientReconciliationError(f"Simulated timeout #{calls['count']}")
        return real_engine.process_event(session, event_id=msg.event_id, payload=dict(msg.payload))

    flaky_worker = OutboxWorker(db_manager.session_factory, settings, handler=flaky_handler)

    t0 = datetime.now(UTC)
    # Attempt 1 fails -> backoff = 2s
    stats1 = flaky_worker.run_once(now=t0)
    assert stats1.retried == 1
    msg = db_session.scalar(select(OutboxMessage))
    assert msg is not None
    assert msg.status == "retry_scheduled"
    assert msg.attempts == 1

    # Running before available_at claims 0 messages
    assert flaky_worker.run_once(now=t0 + timedelta(seconds=1)).claimed == 0

    # Attempt 2 fails -> backoff = 4s
    t1 = t0 + timedelta(seconds=3)
    stats2 = flaky_worker.run_once(now=t1)
    assert stats2.retried == 1
    db_session.expire_all()
    msg = db_session.scalar(select(OutboxMessage))
    assert msg is not None
    assert msg.attempts == 2
    assert msg.status == "retry_scheduled"

    # Attempt 3 succeeds after 4s backoff
    t2 = t1 + timedelta(seconds=5)
    stats3 = flaky_worker.run_once(now=t2)
    assert stats3.processed == 1
    assert stats3.dead_lettered == 0
    db_session.expire_all()
    msg = db_session.scalar(select(OutboxMessage))
    assert msg is not None
    assert msg.status == "processed"
    assert msg.attempts == 3
    assert db_session.scalar(select(DeadLetterMessage)) is None


def test_exhausted_transient_retries_route_to_dlq(
    client: TestClient,
    settings: Settings,
    db_manager: DatabaseManager,
) -> None:
    body = {
        "event_type": "customer.upserted",
        "entity_type": "customer",
        "external_id": "sf_always_down",
        "data": {"name": "Downstream Outage LLC", "email": "down@example.com"},
    }
    raw, headers = signed_webhook_headers(body, secret="crm-secret", idempotency_key="outage-1")
    assert client.post("/v1/webhooks/crm", content=raw, headers=headers).status_code == 202

    def always_failing_handler(_session: Session, _msg: OutboxMessage) -> ReconciliationOutcome:
        raise TransientReconciliationError("Downstream ERP 503 Service Unavailable")

    worker = OutboxWorker(db_manager.session_factory, settings, handler=always_failing_handler)
    t = datetime.now(UTC)

    # settings.retry_max_attempts == 3
    assert worker.run_once(now=t).retried == 1
    assert worker.run_once(now=t + timedelta(seconds=5)).retried == 1
    assert worker.run_once(now=t + timedelta(seconds=15)).dead_lettered == 1

    dlq_entries = client.get("/v1/dlq?status=open").json()
    assert len(dlq_entries) == 1
    assert dlq_entries[0]["error_type"] == "retry_exhausted"
    assert dlq_entries[0]["attempts"] == 3


def test_poison_pill_routed_to_dlq_then_patched_and_replayed_via_api(
    client: TestClient,
    worker: OutboxWorker,
) -> None:
    poison_body = {
        "event_type": "customer.upserted",
        "entity_type": "customer",
        "external_id": "sf_poison_77",
        "data": {"name": "Broken Payload Co", "email": "invalid-email-no-at-sign"},
    }
    raw, headers = signed_webhook_headers(
        poison_body,
        secret="crm-secret",
        idempotency_key="poison-key-77",
    )
    assert client.post("/v1/webhooks/crm", content=raw, headers=headers).status_code == 202

    # Permanent validation error bypasses retries and goes straight to DLQ
    stats = worker.run_once()
    assert stats.dead_lettered == 1
    assert stats.retried == 0

    dlq_entries = client.get("/v1/dlq?status=open").json()
    assert len(dlq_entries) == 1
    dlq_id = dlq_entries[0]["id"]
    assert dlq_entries[0]["error_type"] == "malformed_payload"

    # Malformed anomaly & snapshot were recorded
    anomalies = client.get("/v1/reconciliations/anomalies?anomaly_type=malformed_record").json()
    assert len(anomalies) == 1

    # Replay with patched email via API
    replay_resp = client.post(
        f"/v1/dlq/{dlq_id}/replay",
        json={
            "patch_data": {"email": "fixed@brokenpayload.io"},
            "notes": "Corrected malformed CRM email address",
        },
    )
    assert replay_resp.status_code == 200
    replay_json = replay_resp.json()
    assert replay_json["status"] == "replayed"
    assert replay_json["replay_count"] == 1

    # Process the replayed outbox message -> succeeds cleanly!
    stats_after = worker.run_once()
    assert stats_after.processed == 1
    assert stats_after.dead_lettered == 0

    entities = client.get("/v1/entities?entity_type=customer").json()
    assert len(entities) == 1
    assert entities[0]["canonical_data"]["email"] == "fixed@brokenpayload.io"


def test_dlq_replay_cli_workflow(tmp_path: Path) -> None:
    db_file = tmp_path / "cli_test.db"
    db_url = f"sqlite+pysqlite:///{db_file}"
    cli_settings = Settings(database_url=db_url, webhook_secret="cli-secret", log_level="WARNING")
    manager = DatabaseManager(cli_settings)
    init_db(manager.engine)

    # Ingest a poison-pill invoice with negative amount via TestClient
    from sync_ledger.main import create_app

    app = create_app(settings=cli_settings, db_manager=manager)
    with TestClient(app) as client:
        poison = {
            "event_type": "invoice.created",
            "entity_type": "invoice",
            "external_id": "inv_neg_1",
            "data": {"invoice_number": "INV-900", "amount_cents": -500, "currency": "USD"},
        }
        raw, headers = signed_webhook_headers(
            poison, secret="cli-secret", idempotency_key="cli-idem-1"
        )
        assert client.post("/v1/webhooks/billing", content=raw, headers=headers).status_code == 202

    runner = CliRunner()

    # 1. Run worker --once via CLI -> routes to DLQ
    run_res = runner.invoke(cli_app, ["worker", "run", "--once", "--database-url", db_url])
    assert run_res.exit_code == 0, run_res.output
    assert "dead_lettered=1" in run_res.output

    # 2. List open DLQ entries via CLI
    list_res = runner.invoke(cli_app, ["dlq", "list", "--database-url", db_url])
    assert list_res.exit_code == 0, list_res.output
    assert "malformed_payload" in list_res.output
    dlq_id = list_res.output.strip().split()[0]

    # 3. Inspect DLQ item via CLI
    inspect_res = runner.invoke(cli_app, ["dlq", "inspect", dlq_id, "--database-url", db_url])
    assert inspect_res.exit_code == 0, inspect_res.output
    assert "inv_neg_1" in inspect_res.output

    # 4. Replay with --patch-json and --process-now via CLI
    replay_res = runner.invoke(
        cli_app,
        [
            "dlq",
            "replay",
            dlq_id,
            "--patch-json",
            '{"amount_cents": 5000}',
            "--notes",
            "Fixed negative amount",
            "--process-now",
            "--database-url",
            db_url,
        ],
    )
    assert replay_res.exit_code == 0, replay_res.output
    assert "processed=1" in replay_res.output

    # 5. Reconcile report confirms 1 canonical entity and 0 open DLQ
    report_res = runner.invoke(cli_app, ["reconcile", "report", "--database-url", db_url])
    assert report_res.exit_code == 0, report_res.output
    assert '"canonical_entities": 1' in report_res.output
    assert '"dlq_open": 0' in report_res.output

    with manager.session() as session:
        ent = session.scalar(select(CanonicalEntity))
        assert ent is not None
        assert ent.canonical_data["amount_cents"] == 5000

    manager.dispose()
