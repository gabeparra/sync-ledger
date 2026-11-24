"""SQLAlchemy 2.0 relational schema for sync-ledger."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

JSONType = JSON().with_variant(JSONB(), "postgresql")


def utc_now() -> datetime:
    """Return timezone-aware UTC timestamp."""
    return datetime.now(UTC)


def new_uuid() -> str:
    """Generate a string UUIDv4 identifier."""
    return str(uuid.uuid4())


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


class IdempotencyKey(Base):
    """Tracks processed idempotency keys and cached HTTP responses per source system."""

    __tablename__ = "idempotency_keys"
    __table_args__ = (
        UniqueConstraint("source_system", "idempotency_key", name="uq_idempotency_source_key"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    source_system: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="completed")
    response_status: Mapped[int] = mapped_column(Integer, nullable=False, default=202)
    response_body: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False, default=dict)
    event_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("inbound_events.id", ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )


class InboundEvent(Base):
    """Immutable log of verified inbound integration webhooks/events."""

    __tablename__ = "inbound_events"
    __table_args__ = (Index("ix_inbound_events_source_received", "source_system", "received_at"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    source_system: Mapped[str] = mapped_column(String(64), nullable=False)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    external_id: Mapped[str] = mapped_column(String(255), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False)
    signature_digest: Mapped[str] = mapped_column(String(128), nullable=False)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )


class OutboxMessage(Base):
    """Transactional outbox message processed asynchronously by reconciliation workers."""

    __tablename__ = "outbox_messages"
    __table_args__ = (
        Index("ix_outbox_poll", "status", "available_at"),
        Index("ix_outbox_event_id", "event_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    event_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("inbound_events.id", ondelete="CASCADE"), nullable=False
    )
    topic: Mapped[str] = mapped_column(String(128), nullable=False)
    aggregate_type: Mapped[str] = mapped_column(String(64), nullable=False)
    aggregate_id: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="pending",
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=4)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    locked_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )


class CanonicalEntity(Base):
    """Golden canonical record unifying disparate systems without a shared primary key."""

    __tablename__ = "canonical_entities"
    __table_args__ = (
        Index("ix_canonical_entity_type_tax_id", "entity_type", "normalized_tax_id"),
        Index("ix_canonical_entity_type_email", "entity_type", "normalized_email"),
        Index("ix_canonical_entity_type_phone", "entity_type", "normalized_phone"),
        Index("ix_canonical_entity_type_invoice", "entity_type", "normalized_invoice_number"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    canonical_data: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False, default=dict)
    normalized_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    normalized_phone: Mapped[str | None] = mapped_column(String(64), nullable=True)
    normalized_tax_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    normalized_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    normalized_postal_code: Mapped[str | None] = mapped_column(String(32), nullable=True)
    normalized_invoice_number: Mapped[str | None] = mapped_column(String(128), nullable=True)
    has_active_drift: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )

    links: Mapped[list[EntityLink]] = relationship(
        back_populates="canonical_entity", cascade="all, delete-orphan"
    )
    source_records: Mapped[list[SourceRecord]] = relationship(
        back_populates="canonical_entity", cascade="all, delete-orphan"
    )


class EntityLink(Base):
    """Cross-reference table mapping a source system's native external_id to a CanonicalEntity."""

    __tablename__ = "entity_links"
    __table_args__ = (
        UniqueConstraint(
            "source_system", "entity_type", "external_id", name="uq_entity_link_source_external"
        ),
        Index("ix_entity_link_canonical", "canonical_entity_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    canonical_entity_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("canonical_entities.id", ondelete="CASCADE"), nullable=False
    )
    source_system: Mapped[str] = mapped_column(String(64), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    external_id: Mapped[str] = mapped_column(String(255), nullable=False)
    match_strategy: Mapped[str] = mapped_column(String(64), nullable=False)
    match_confidence: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    matched_signals: Mapped[list[str]] = mapped_column(JSONType, nullable=False, default=list)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )

    canonical_entity: Mapped[CanonicalEntity] = relationship(back_populates="links")


class SourceRecord(Base):
    """Latest normalized snapshot reported by each source system for a given external_id."""

    __tablename__ = "source_records"
    __table_args__ = (
        UniqueConstraint(
            "source_system", "entity_type", "external_id", name="uq_source_record_external"
        ),
        Index("ix_source_record_canonical", "canonical_entity_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    canonical_entity_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("canonical_entities.id", ondelete="CASCADE"), nullable=False
    )
    source_system: Mapped[str] = mapped_column(String(64), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    external_id: Mapped[str] = mapped_column(String(255), nullable=False)
    attributes: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False, default=dict)
    last_event_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("inbound_events.id", ondelete="SET NULL"), nullable=True
    )
    source_updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )

    canonical_entity: Mapped[CanonicalEntity] = relationship(back_populates="source_records")


class ReconciliationSnapshot(Base):
    """Immutable audit & diff trail produced on every reconciliation execution."""

    __tablename__ = "reconciliation_snapshots"
    __table_args__ = (
        Index("ix_snapshot_entity_created", "canonical_entity_id", "created_at"),
        Index("ix_snapshot_drift", "drift_detected", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    canonical_entity_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("canonical_entities.id", ondelete="SET NULL"), nullable=True
    )
    trigger_event_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("inbound_events.id", ondelete="CASCADE"), nullable=False
    )
    source_system: Mapped[str] = mapped_column(String(64), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    external_id: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    match_strategy: Mapped[str] = mapped_column(String(64), nullable=False)
    match_confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    matched_signals: Mapped[list[str]] = mapped_column(JSONType, nullable=False, default=list)
    before_state: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False, default=dict)
    after_state: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False, default=dict)
    field_diffs: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONType, nullable=False, default=list
    )
    drift_detected: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    drift_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )


class ReconciliationAnomaly(Base):
    """Flagged data quality, duplicate, ambiguous match, or cross-system drift anomalies."""

    __tablename__ = "reconciliation_anomalies"
    __table_args__ = (Index("ix_anomaly_status_type", "status", "anomaly_type"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    canonical_entity_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("canonical_entities.id", ondelete="SET NULL"), nullable=True
    )
    event_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("inbound_events.id", ondelete="CASCADE"), nullable=False
    )
    source_system: Mapped[str] = mapped_column(String(64), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    external_id: Mapped[str] = mapped_column(String(255), nullable=False)
    anomaly_type: Mapped[str] = mapped_column(String(64), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False, default="warning")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="open")
    details: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class DeadLetterMessage(Base):
    """Dead Letter Queue (DLQ) storing poisoned or retry-exhausted outbox messages for replay."""

    __tablename__ = "dead_letter_messages"
    __table_args__ = (
        Index("ix_dlq_status_created", "status", "created_at"),
        Index("ix_dlq_source", "source_system", "status"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    outbox_message_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("outbox_messages.id", ondelete="CASCADE"), nullable=False
    )
    event_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("inbound_events.id", ondelete="CASCADE"), nullable=False
    )
    source_system: Mapped[str] = mapped_column(String(64), nullable=False)
    topic: Mapped[str] = mapped_column(String(128), nullable=False)
    error_type: Mapped[str] = mapped_column(String(64), nullable=False)
    error_message: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="open")
    replay_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    replayed_outbox_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    replay_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    replayed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
