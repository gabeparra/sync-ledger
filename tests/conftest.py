"""Shared pytest fixtures supporting both fast SQLite and PostgreSQL integration runs."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Generator
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.orm import Session

from sync_ledger.config import Settings
from sync_ledger.db.models import Base
from sync_ledger.db.session import DatabaseManager, init_db
from sync_ledger.main import create_app
from sync_ledger.security import compute_webhook_signature
from sync_ledger.worker.runner import OutboxWorker


@pytest.fixture()
def settings() -> Settings:
    """Return isolated test settings (uses TEST_DATABASE_URL if set, else in-memory SQLite)."""
    db_url = os.environ.get("TEST_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    return Settings(
        database_url=db_url,
        webhook_secret="test-default-secret",
        source_secrets={"crm": "crm-secret", "billing": "billing-secret", "erp": "erp-secret"},
        signature_tolerance_seconds=300,
        retry_max_attempts=3,
        retry_base_delay_seconds=2.0,
        retry_max_delay_seconds=30.0,
        retry_jitter_ratio=0.0,
        log_level="WARNING",
        worker_id="worker-test-1",
    )


@pytest.fixture()
def db_manager(settings: Settings) -> Generator[DatabaseManager, None, None]:
    """Create a clean schema per test and dispose cleanly afterward."""
    manager = DatabaseManager(settings)
    if settings.database_url.startswith("postgresql"):
        # Ensure clean slate on shared CI Postgres instance while preserving Alembic tables
        Base.metadata.create_all(bind=manager.engine)
        with manager.engine.begin() as conn:
            table_names = ", ".join(t.name for t in reversed(Base.metadata.sorted_tables))
            conn.execute(text(f"TRUNCATE TABLE {table_names} CASCADE"))
    else:
        init_db(manager.engine)

    try:
        yield manager
    finally:
        manager.dispose()


@pytest.fixture()
def db_session(db_manager: DatabaseManager) -> Generator[Session, None, None]:
    with db_manager.session() as session:
        yield session


@pytest.fixture()
def client(settings: Settings, db_manager: DatabaseManager) -> Generator[TestClient, None, None]:
    app = create_app(settings=settings, db_manager=db_manager)
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture()
def worker(settings: Settings, db_manager: DatabaseManager) -> OutboxWorker:
    return OutboxWorker(db_manager.session_factory, settings)


def signed_webhook_headers(
    body: dict[str, Any],
    *,
    secret: str,
    idempotency_key: str,
    timestamp: int | None = None,
) -> tuple[bytes, dict[str, str]]:
    """Serialize body deterministically and attach valid HMAC + Idempotency headers."""
    raw = json.dumps(body, separators=(",", ":")).encode("utf-8")
    ts = int(time.time()) if timestamp is None else timestamp
    sig = compute_webhook_signature(secret, ts, raw)
    headers = {
        "Content-Type": "application/json",
        "Idempotency-Key": idempotency_key,
        "X-SyncLedger-Timestamp": str(ts),
        "X-SyncLedger-Signature": sig,
    }
    return raw, headers
