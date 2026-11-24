"""Initial sync-ledger schema.

Revision ID: 001_initial_schema
Revises:
Create Date: 2026-11-24 02:35:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "001_initial_schema"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

JSONType = sa.JSON().with_variant(JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "inbound_events",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("source_system", sa.String(length=64), nullable=False),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("entity_type", sa.String(length=64), nullable=False),
        sa.Column("external_id", sa.String(length=255), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("payload", JSONType, nullable=False),
        sa.Column("signature_digest", sa.String(length=128), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_inbound_events_source_received",
        "inbound_events",
        ["source_system", "received_at"],
    )

    op.create_table(
        "idempotency_keys",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("source_system", sa.String(length=64), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("response_status", sa.Integer(), nullable=False),
        sa.Column("response_body", JSONType, nullable=False),
        sa.Column(
            "event_id",
            sa.String(length=36),
            sa.ForeignKey("inbound_events.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "source_system", "idempotency_key", name="uq_idempotency_source_key"
        ),
    )
    op.create_index("ix_idempotency_keys_source_system", "idempotency_keys", ["source_system"])

    op.create_table(
        "outbox_messages",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "event_id",
            sa.String(length=36),
            sa.ForeignKey("inbound_events.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("topic", sa.String(length=128), nullable=False),
        sa.Column("aggregate_type", sa.String(length=64), nullable=False),
        sa.Column("aggregate_id", sa.String(length=255), nullable=False),
        sa.Column("payload", JSONType, nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("locked_by", sa.String(length=128), nullable=True),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_outbox_poll", "outbox_messages", ["status", "available_at"])
    op.create_index("ix_outbox_event_id", "outbox_messages", ["event_id"])

    op.create_table(
        "canonical_entities",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("entity_type", sa.String(length=64), nullable=False),
        sa.Column("canonical_data", JSONType, nullable=False),
        sa.Column("normalized_email", sa.String(length=255), nullable=True),
        sa.Column("normalized_phone", sa.String(length=64), nullable=True),
        sa.Column("normalized_tax_id", sa.String(length=64), nullable=True),
        sa.Column("normalized_name", sa.String(length=255), nullable=True),
        sa.Column("normalized_postal_code", sa.String(length=32), nullable=True),
        sa.Column("normalized_invoice_number", sa.String(length=128), nullable=True),
        sa.Column("has_active_drift", sa.Boolean(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_canonical_entities_entity_type", "canonical_entities", ["entity_type"])
    op.create_index(
        "ix_canonical_entity_type_tax_id",
        "canonical_entities",
        ["entity_type", "normalized_tax_id"],
    )
    op.create_index(
        "ix_canonical_entity_type_email",
        "canonical_entities",
        ["entity_type", "normalized_email"],
    )
    op.create_index(
        "ix_canonical_entity_type_phone",
        "canonical_entities",
        ["entity_type", "normalized_phone"],
    )
    op.create_index(
        "ix_canonical_entity_type_invoice",
        "canonical_entities",
        ["entity_type", "normalized_invoice_number"],
    )

    op.create_table(
        "entity_links",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "canonical_entity_id",
            sa.String(length=36),
            sa.ForeignKey("canonical_entities.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source_system", sa.String(length=64), nullable=False),
        sa.Column("entity_type", sa.String(length=64), nullable=False),
        sa.Column("external_id", sa.String(length=255), nullable=False),
        sa.Column("match_strategy", sa.String(length=64), nullable=False),
        sa.Column("match_confidence", sa.Float(), nullable=False),
        sa.Column("matched_signals", JSONType, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "source_system", "entity_type", "external_id", name="uq_entity_link_source_external"
        ),
    )
    op.create_index("ix_entity_link_canonical", "entity_links", ["canonical_entity_id"])

    op.create_table(
        "source_records",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "canonical_entity_id",
            sa.String(length=36),
            sa.ForeignKey("canonical_entities.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source_system", sa.String(length=64), nullable=False),
        sa.Column("entity_type", sa.String(length=64), nullable=False),
        sa.Column("external_id", sa.String(length=255), nullable=False),
        sa.Column("attributes", JSONType, nullable=False),
        sa.Column(
            "last_event_id",
            sa.String(length=36),
            sa.ForeignKey("inbound_events.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("source_updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "source_system", "entity_type", "external_id", name="uq_source_record_external"
        ),
    )
    op.create_index("ix_source_record_canonical", "source_records", ["canonical_entity_id"])

    op.create_table(
        "reconciliation_snapshots",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "canonical_entity_id",
            sa.String(length=36),
            sa.ForeignKey("canonical_entities.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "trigger_event_id",
            sa.String(length=36),
            sa.ForeignKey("inbound_events.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source_system", sa.String(length=64), nullable=False),
        sa.Column("entity_type", sa.String(length=64), nullable=False),
        sa.Column("external_id", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("match_strategy", sa.String(length=64), nullable=False),
        sa.Column("match_confidence", sa.Float(), nullable=False),
        sa.Column("matched_signals", JSONType, nullable=False),
        sa.Column("before_state", JSONType, nullable=False),
        sa.Column("after_state", JSONType, nullable=False),
        sa.Column("field_diffs", JSONType, nullable=False),
        sa.Column("drift_detected", sa.Boolean(), nullable=False),
        sa.Column("drift_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_snapshot_entity_created",
        "reconciliation_snapshots",
        ["canonical_entity_id", "created_at"],
    )
    op.create_index(
        "ix_snapshot_drift",
        "reconciliation_snapshots",
        ["drift_detected", "created_at"],
    )

    op.create_table(
        "reconciliation_anomalies",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "canonical_entity_id",
            sa.String(length=36),
            sa.ForeignKey("canonical_entities.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "event_id",
            sa.String(length=36),
            sa.ForeignKey("inbound_events.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source_system", sa.String(length=64), nullable=False),
        sa.Column("entity_type", sa.String(length=64), nullable=False),
        sa.Column("external_id", sa.String(length=255), nullable=False),
        sa.Column("anomaly_type", sa.String(length=64), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("details", JSONType, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_anomaly_status_type", "reconciliation_anomalies", ["status", "anomaly_type"]
    )

    op.create_table(
        "dead_letter_messages",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "outbox_message_id",
            sa.String(length=36),
            sa.ForeignKey("outbox_messages.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "event_id",
            sa.String(length=36),
            sa.ForeignKey("inbound_events.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source_system", sa.String(length=64), nullable=False),
        sa.Column("topic", sa.String(length=128), nullable=False),
        sa.Column("error_type", sa.String(length=64), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=False),
        sa.Column("payload", JSONType, nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("replay_count", sa.Integer(), nullable=False),
        sa.Column("replayed_outbox_id", sa.String(length=36), nullable=True),
        sa.Column("replay_notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("replayed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_dlq_status_created", "dead_letter_messages", ["status", "created_at"])
    op.create_index("ix_dlq_source", "dead_letter_messages", ["source_system", "status"])


def downgrade() -> None:
    op.drop_table("dead_letter_messages")
    op.drop_table("reconciliation_anomalies")
    op.drop_table("reconciliation_snapshots")
    op.drop_table("source_records")
    op.drop_table("entity_links")
    op.drop_table("canonical_entities")
    op.drop_table("outbox_messages")
    op.drop_table("idempotency_keys")
    op.drop_table("inbound_events")
