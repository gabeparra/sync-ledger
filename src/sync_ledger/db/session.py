"""SQLAlchemy engine and session lifecycle management."""

from __future__ import annotations

from collections.abc import Generator
from typing import Any

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from sync_ledger.config import Settings, get_settings
from sync_ledger.db.models import Base


def create_db_engine(database_url: str, *, echo: bool = False) -> Engine:
    """Create a configured SQLAlchemy engine supporting PostgreSQL and SQLite."""
    connect_args: dict[str, Any] = {}
    kwargs: dict[str, Any] = {"echo": echo, "future": True, "pool_pre_ping": True}

    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
        if ":memory:" in database_url:
            kwargs["poolclass"] = StaticPool
            kwargs.pop("pool_pre_ping", None)

    engine = create_engine(database_url, connect_args=connect_args, **kwargs)

    if database_url.startswith("sqlite"):

        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(dbapi_connection: Any, _connection_record: Any) -> None:
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    return engine


def create_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Create a sessionmaker bound to the provided engine."""
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def init_db(engine: Engine) -> None:
    """Create all tables directly (primarily for tests or local SQLite development)."""
    Base.metadata.create_all(bind=engine)


class DatabaseManager:
    """Holds engine and session factory for dependency injection."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.engine = create_db_engine(
            self.settings.database_url,
            echo=self.settings.database_echo,
        )
        self.session_factory = create_session_factory(self.engine)

    def session(self) -> Session:
        """Open a new SQLAlchemy session."""
        return self.session_factory()

    def get_session(self) -> Generator[Session, None, None]:
        """FastAPI dependency yielding a scoped DB session."""
        db = self.session_factory()
        try:
            yield db
        finally:
            db.close()

    def dispose(self) -> None:
        """Dispose underlying connection pool."""
        self.engine.dispose()
