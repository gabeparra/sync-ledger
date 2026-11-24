"""Prometheus metrics for sync-ledger ingestion, reconciliation, and DLQ operations."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from sync_ledger.db.models import DeadLetterMessage, OutboxMessage

METRICS_REGISTRY = CollectorRegistry(auto_describe=True)

WEBHOOKS_RECEIVED_TOTAL = Counter(
    "sync_ledger_webhooks_received_total",
    "Total webhook requests received by source system and outcome.",
    labelnames=("source_system", "outcome"),
    registry=METRICS_REGISTRY,
)

OUTBOX_PROCESSED_TOTAL = Counter(
    "sync_ledger_outbox_processed_total",
    "Total outbox messages processed by topic and outcome.",
    labelnames=("topic", "outcome"),
    registry=METRICS_REGISTRY,
)

RECONCILIATION_MATCHES_TOTAL = Counter(
    "sync_ledger_reconciliation_matches_total",
    "Total cross-system reconciliation resolutions by match strategy and entity type.",
    labelnames=("strategy", "entity_type"),
    registry=METRICS_REGISTRY,
)

RECONCILIATION_DRIFTS_TOTAL = Counter(
    "sync_ledger_reconciliation_drifts_total",
    "Total field-level cross-system drifts detected.",
    labelnames=("entity_type", "field"),
    registry=METRICS_REGISTRY,
)

ANOMALIES_TOTAL = Counter(
    "sync_ledger_anomalies_total",
    "Total reconciliation anomalies flagged (malformed, duplicate, ambiguous, drift).",
    labelnames=("anomaly_type", "source_system"),
    registry=METRICS_REGISTRY,
)

DLQ_REPLAYS_TOTAL = Counter(
    "sync_ledger_dlq_replays_total",
    "Total dead-letter queue replay operations by outcome.",
    labelnames=("outcome",),
    registry=METRICS_REGISTRY,
)

RECONCILIATION_DURATION_SECONDS = Histogram(
    "sync_ledger_reconciliation_duration_seconds",
    "Latency of single outbox message reconciliation execution.",
    labelnames=("entity_type",),
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
    registry=METRICS_REGISTRY,
)

OUTBOX_PENDING_GAUGE = Gauge(
    "sync_ledger_outbox_pending_messages",
    "Current number of pending or retry-scheduled outbox messages.",
    registry=METRICS_REGISTRY,
)

DLQ_OPEN_GAUGE = Gauge(
    "sync_ledger_dlq_open_messages",
    "Current number of open messages in the dead-letter queue.",
    registry=METRICS_REGISTRY,
)


def refresh_queue_gauges(session: Session) -> tuple[int, int]:
    """Query live outbox backlog and open DLQ counts and update Prometheus gauges."""
    pending_outbox = (
        session.scalar(
            select(func.count())
            .select_from(OutboxMessage)
            .where(OutboxMessage.status.in_(("pending", "retry_scheduled")))
        )
        or 0
    )
    open_dlq = (
        session.scalar(
            select(func.count())
            .select_from(DeadLetterMessage)
            .where(DeadLetterMessage.status == "open")
        )
        or 0
    )
    OUTBOX_PENDING_GAUGE.set(pending_outbox)
    DLQ_OPEN_GAUGE.set(open_dlq)
    return int(pending_outbox), int(open_dlq)


def render_prometheus_metrics() -> bytes:
    """Serialize the sync-ledger Prometheus registry into exposition text format."""
    return generate_latest(METRICS_REGISTRY)
