"""Pydantic v2 request and response schemas for sync-ledger APIs."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class WebhookEventPayload(BaseModel):
    """Inbound webhook payload delivered by an external source system."""

    model_config = ConfigDict(extra="allow")

    event_type: str = Field(..., min_length=1, max_length=128, examples=["customer.upserted"])
    entity_type: str = Field(..., min_length=1, max_length=64, examples=["customer", "invoice"])
    external_id: str = Field(..., min_length=1, max_length=255, examples=["cus_9f8a7b"])
    occurred_at: datetime | None = Field(
        default=None, description="Event timestamp in the source system (ISO-8601 UTC)."
    )
    schema_version: str = Field(default="1.0", max_length=32)
    data: dict[str, Any] = Field(default_factory=dict)


class WebhookAcceptedResponse(BaseModel):
    """Response returned after transactional outbox ingestion."""

    status: str = "accepted"
    event_id: str
    outbox_id: str
    source_system: str
    idempotency_key: str
    replayed: bool = False


class EntityLinkResponse(BaseModel):
    """Cross-system external ID mapping linked to a canonical entity."""

    model_config = ConfigDict(from_attributes=True)

    id: str
    source_system: str
    entity_type: str
    external_id: str
    match_strategy: str
    match_confidence: float
    matched_signals: list[str]
    created_at: datetime


class SourceRecordResponse(BaseModel):
    """Latest normalized state reported by an individual source system."""

    model_config = ConfigDict(from_attributes=True)

    id: str
    source_system: str
    entity_type: str
    external_id: str
    attributes: dict[str, Any]
    source_updated_at: datetime
    updated_at: datetime


class CanonicalEntityResponse(BaseModel):
    """Golden canonical record with cross-system links and source snapshots."""

    model_config = ConfigDict(from_attributes=True)

    id: str
    entity_type: str
    canonical_data: dict[str, Any]
    has_active_drift: bool
    version: int
    created_at: datetime
    updated_at: datetime
    links: list[EntityLinkResponse] = Field(default_factory=list)
    source_records: list[SourceRecordResponse] = Field(default_factory=list)


class ReconciliationSnapshotResponse(BaseModel):
    """Audit and field-level diff snapshot emitted on reconciliation."""

    model_config = ConfigDict(from_attributes=True)

    id: str
    canonical_entity_id: str | None
    trigger_event_id: str
    source_system: str
    entity_type: str
    external_id: str
    status: str
    match_strategy: str
    match_confidence: float
    matched_signals: list[str]
    before_state: dict[str, Any]
    after_state: dict[str, Any]
    field_diffs: list[dict[str, Any]]
    drift_detected: bool
    drift_count: int
    created_at: datetime


class ReconciliationAnomalyResponse(BaseModel):
    """Flagged data quality, duplicate, ambiguous-match, or drift anomaly."""

    model_config = ConfigDict(from_attributes=True)

    id: str
    canonical_entity_id: str | None
    event_id: str
    source_system: str
    entity_type: str
    external_id: str
    anomaly_type: str
    severity: str
    status: str
    details: dict[str, Any]
    created_at: datetime
    resolved_at: datetime | None


class DeadLetterResponse(BaseModel):
    """Dead-lettered outbox message available for inspection and replay."""

    model_config = ConfigDict(from_attributes=True)

    id: str
    outbox_message_id: str
    event_id: str
    source_system: str
    topic: str
    error_type: str
    error_message: str
    payload: dict[str, Any]
    attempts: int
    status: str
    replay_count: int
    replayed_outbox_id: str | None
    replay_notes: str | None
    created_at: datetime
    replayed_at: datetime | None


class DeadLetterReplayRequest(BaseModel):
    """Optional payload patch and operator note when replaying a DLQ message."""

    patch_data: dict[str, Any] | None = Field(
        default=None,
        description="Optional partial fields merged into payload['data'] before replay.",
    )
    notes: str | None = Field(default=None, max_length=500)


class DeadLetterReplayResponse(BaseModel):
    """Result of re-enqueuing a dead-letter message back onto the transactional outbox."""

    dlq_id: str
    new_outbox_id: str
    status: str
    replay_count: int


class HealthResponse(BaseModel):
    """Liveness probe response."""

    status: str = "ok"
    service: str = "sync-ledger"
    version: str


class ReadinessResponse(BaseModel):
    """Readiness probe response including DB and queue telemetry."""

    status: str
    database: str
    outbox_pending: int
    dlq_open: int
