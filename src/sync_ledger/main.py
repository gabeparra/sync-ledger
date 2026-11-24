"""FastAPI application entrypoint and factory."""

from __future__ import annotations

from collections.abc import AsyncIterator, Generator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy.orm import Session

from sync_ledger import __version__
from sync_ledger.api.routes import build_router, get_db
from sync_ledger.config import Settings, get_settings
from sync_ledger.db.session import DatabaseManager
from sync_ledger.logging import configure_logging


def create_app(
    *,
    settings: Settings | None = None,
    db_manager: DatabaseManager | None = None,
) -> FastAPI:
    """Build and configure the sync-ledger FastAPI application."""
    resolved_settings = settings or get_settings()
    configure_logging(resolved_settings.log_level)

    owns_db = db_manager is None
    manager = db_manager or DatabaseManager(resolved_settings)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            if owns_db:
                manager.dispose()

    application = FastAPI(
        title="Sync-Ledger Integration & Reconciliation Engine",
        description=(
            "Event-driven integration and cross-system reconciliation engine featuring "
            "HMAC webhook verification, idempotency deduplication, transactional outbox, "
            "no-shared-ID entity resolution, drift audit snapshots, and DLQ replay."
        ),
        version=__version__,
        lifespan=lifespan,
    )
    application.state.settings = resolved_settings
    application.state.db_manager = manager

    def _get_session_override() -> Generator[Session, None, None]:
        yield from manager.get_session()

    application.dependency_overrides[get_db] = _get_session_override
    application.include_router(build_router())
    return application


app = create_app()
