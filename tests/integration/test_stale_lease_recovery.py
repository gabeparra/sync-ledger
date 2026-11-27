"""Integration tests for reclaiming outbox messages left behind by a dead worker."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from sync_ledger.config import Settings
from sync_ledger.db.models import DeadLetterMessage, OutboxMessage
from sync_ledger.db.session import DatabaseManager
from sync_ledger.worker.runner import OutboxWorker
from tests.conftest import signed_webhook_headers

pytestmark = pytest.mark.integration


def _enqueue_one(client: TestClient, key: str) -> None:
    body = {
        "event_type": "customer.upserted",
        "entity_type": "customer",
        "external_id": f"sf_{key}",
        "data": {"name": "Lease Co", "email": f"{key}@lease.io"},
    }
    raw, headers = signed_webhook_headers(body, secret="crm-secret", idempotency_key=key)
    assert client.post("/v1/webhooks/crm", content=raw, headers=headers).status_code == 202


def _worker(db_manager: DatabaseManager, settings: Settings, worker_id: str) -> OutboxWorker:
    return OutboxWorker(
        db_manager.session_factory,
        settings.model_copy(update={"worker_id": worker_id, "outbox_lease_timeout_seconds": 60}),
    )


def test_claim_abandoned_by_dead_worker_is_reclaimed_after_lease_timeout(
    client: TestClient,
    settings: Settings,
    db_manager: DatabaseManager,
    db_session: Session,
) -> None:
    _enqueue_one(client, "lease-1")
    t0 = datetime.now(UTC)

    # Worker A claims the message and then dies before processing it.
    dead = _worker(db_manager, settings, "worker-dead")
    with db_manager.session_factory() as session:
        assert len(dead.claim_batch(session, now=t0)) == 1

    survivor = _worker(db_manager, settings, "worker-survivor")

    # Inside the lease window the claim still belongs to worker A.
    assert survivor.run_once(now=t0 + timedelta(seconds=30)).claimed == 0

    # Past the lease the survivor picks it up and finishes the job.
    stats = survivor.run_once(now=t0 + timedelta(seconds=61))
    assert stats.claimed == 1
    assert stats.processed == 1

    db_session.expire_all()
    msg = db_session.scalar(select(OutboxMessage))
    assert msg is not None
    assert msg.status == "processed"
    # The lost run plus the successful one.
    assert msg.attempts == 2
    assert msg.locked_by is None


def test_message_that_keeps_killing_workers_is_dead_lettered(
    client: TestClient,
    settings: Settings,
    db_manager: DatabaseManager,
    db_session: Session,
) -> None:
    _enqueue_one(client, "lease-poison")
    msg = db_session.scalar(select(OutboxMessage))
    assert msg is not None
    max_attempts = msg.max_attempts

    # Every worker that claims the message dies before finishing it. The first claim is
    # fresh; each later one reclaims an expired lease and charges the lost run as an attempt.
    now = datetime.now(UTC)
    for claim in range(max_attempts + 1):
        worker = _worker(db_manager, settings, f"worker-{claim}")
        with db_manager.session_factory() as session:
            claimed = worker.claim_batch(session, now=now)
        if claim < max_attempts:
            assert len(claimed) == 1
        else:
            # Reclaiming after the last allowed run spends it, so nothing is handed out.
            assert claimed == []
        now += timedelta(seconds=61)

    db_session.expire_all()
    msg = db_session.scalar(select(OutboxMessage))
    assert msg is not None
    assert msg.status == "dead_lettered"
    assert msg.attempts == max_attempts
    assert msg.locked_by is None

    dlq = db_session.scalars(select(DeadLetterMessage)).all()
    assert len(dlq) == 1
    assert dlq[0].error_type == "lease_expired"
    assert "worker-" in dlq[0].error_message
