"""Application configuration loaded from environment variables."""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings for the sync-ledger API and reconciliation worker."""

    model_config = SettingsConfigDict(
        env_prefix="SYNC_LEDGER_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "sync-ledger"
    environment: str = "development"
    log_level: str = "INFO"

    database_url: str = Field(
        default="postgresql+psycopg://sync_ledger:sync_ledger@localhost:5432/sync_ledger",
        description="SQLAlchemy 2.0 database URL.",
    )
    database_echo: bool = False

    webhook_secret: str = Field(
        default="dev-shared-webhook-secret-change-me",
        description="Default HMAC-SHA256 signing secret for webhook verification.",
    )
    source_secrets: dict[str, str] = Field(
        default_factory=dict,
        description="Optional per-source-system HMAC-SHA256 signing secrets.",
    )
    signature_tolerance_seconds: int = Field(
        default=300,
        ge=0,
        description="Allowed clock skew / replay window in seconds for webhook timestamps.",
    )

    outbox_batch_size: int = Field(default=25, ge=1, le=500)
    outbox_poll_interval_seconds: float = Field(default=1.0, gt=0.0)
    worker_id: str = Field(default="worker-local-1")
    outbox_lease_timeout_seconds: float = Field(
        default=300.0,
        gt=0.0,
        description="How long a 'processing' claim may sit before another worker reclaims it.",
    )

    retry_max_attempts: int = Field(default=4, ge=1, le=20)
    retry_base_delay_seconds: float = Field(default=2.0, ge=0.0)
    retry_max_delay_seconds: float = Field(default=60.0, ge=0.0)
    retry_jitter_ratio: float = Field(default=0.15, ge=0.0, le=1.0)

    match_auto_link_threshold: float = Field(default=0.70, ge=0.0, le=1.0)
    match_ambiguous_margin: float = Field(default=0.10, ge=0.0, le=1.0)

    @field_validator("source_secrets", mode="before")
    @classmethod
    def _coerce_source_secrets(cls, value: Any) -> dict[str, str]:
        if value is None or value == "":
            return {}
        if isinstance(value, dict):
            return {str(k).lower(): str(v) for k, v in value.items()}
        raise ValueError("source_secrets must be a JSON object mapping source -> secret")

    def secret_for_source(self, source_system: str) -> str:
        """Return source-specific HMAC secret if configured, else default secret."""
        return self.source_secrets.get(source_system.lower(), self.webhook_secret)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached settings singleton."""
    return Settings()
