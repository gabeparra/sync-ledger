"""Core reconciliation engine orchestrating validation, matching, diffing, and auditing."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from sync_ledger.config import Settings
from sync_ledger.db.models import (
    CanonicalEntity,
    EntityLink,
    ReconciliationAnomaly,
    ReconciliationSnapshot,
    SourceRecord,
    utc_now,
)
from sync_ledger.metrics import (
    ANOMALIES_TOTAL,
    RECONCILIATION_DRIFTS_TOTAL,
    RECONCILIATION_DURATION_SECONDS,
    RECONCILIATION_MATCHES_TOTAL,
)
from sync_ledger.reconciliation.differ import compute_canonical_and_drift
from sync_ledger.reconciliation.matcher import CrossSystemMatcher
from sync_ledger.reconciliation.normalizers import (
    NormalizedSignals,
    extract_normalized_signals,
)
from sync_ledger.reconciliation.validator import validate_reconciliation_payload

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReconciliationOutcome:
    """Summary returned by the reconciliation engine after processing one event."""

    snapshot_id: str
    canonical_entity_id: str | None
    status: str
    match_strategy: str
    match_confidence: float
    drift_detected: bool
    drift_count: int
    anomaly_ids: list[str]


def _parse_occurred_at(raw_value: Any) -> datetime:
    if isinstance(raw_value, datetime):
        return raw_value
    if isinstance(raw_value, str) and raw_value.strip():
        try:
            return datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        except ValueError:
            pass
    return utc_now()


class ReconciliationEngine:
    """Reconciles inbound source events into canonical entities with full audit trails."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.matcher = CrossSystemMatcher(
            auto_link_threshold=settings.match_auto_link_threshold,
            ambiguous_margin=settings.match_ambiguous_margin,
        )

    def process_event(
        self,
        session: Session,
        *,
        event_id: str,
        payload: dict[str, Any],
    ) -> ReconciliationOutcome:
        """Validate, resolve, merge, diff, and audit a single inbound integration event."""
        start = time.perf_counter()
        entity_type = str(payload.get("entity_type", "unknown")).strip().lower()
        source_system = str(payload.get("source_system", "unknown")).strip().lower()
        external_id = str(payload.get("external_id", "")).strip()

        try:
            signals = validate_reconciliation_payload(payload)
            data: dict[str, Any] = dict(payload["data"])
            occurred_at = _parse_occurred_at(payload.get("occurred_at"))

            decision = self.matcher.resolve(
                session,
                source_system=source_system,
                entity_type=entity_type,
                external_id=external_id,
                signals=signals,
            )
            RECONCILIATION_MATCHES_TOTAL.labels(
                strategy=decision.strategy,
                entity_type=entity_type,
            ).inc()

            anomaly_ids: list[str] = []

            # Handle ambiguous multi-candidate matches without corrupting canonical state
            if decision.strategy == "ambiguous_match":
                anomaly = ReconciliationAnomaly(
                    canonical_entity_id=None,
                    event_id=event_id,
                    source_system=source_system,
                    entity_type=entity_type,
                    external_id=external_id,
                    anomaly_type="ambiguous_match",
                    severity="high",
                    status="open",
                    details={
                        "candidate_entity_ids": decision.ambiguous_candidate_ids,
                        "matched_signals": decision.matched_signals,
                        "confidence": decision.confidence,
                    },
                )
                session.add(anomaly)
                session.flush()
                anomaly_ids.append(anomaly.id)
                ANOMALIES_TOTAL.labels(
                    anomaly_type="ambiguous_match",
                    source_system=source_system,
                ).inc()

                snapshot = ReconciliationSnapshot(
                    canonical_entity_id=None,
                    trigger_event_id=event_id,
                    source_system=source_system,
                    entity_type=entity_type,
                    external_id=external_id,
                    status="ambiguous_flagged",
                    match_strategy=decision.strategy,
                    match_confidence=decision.confidence,
                    matched_signals=decision.matched_signals,
                    before_state={},
                    after_state={},
                    field_diffs=[],
                    drift_detected=False,
                    drift_count=0,
                )
                session.add(snapshot)
                session.flush()
                return ReconciliationOutcome(
                    snapshot_id=snapshot.id,
                    canonical_entity_id=None,
                    status=snapshot.status,
                    match_strategy=decision.strategy,
                    match_confidence=decision.confidence,
                    drift_detected=False,
                    drift_count=0,
                    anomaly_ids=anomaly_ids,
                )

            # Create or load canonical entity
            if decision.canonical_entity is None:
                canonical = CanonicalEntity(
                    entity_type=entity_type,
                    canonical_data={},
                    has_active_drift=False,
                    version=1,
                )
                session.add(canonical)
                session.flush()
                before_state: dict[str, Any] = {}
                is_new_entity = True
            else:
                canonical = decision.canonical_entity
                before_state = dict(canonical.canonical_data)
                is_new_entity = False

            # Ensure EntityLink exists for (source_system, entity_type, external_id)
            if decision.strategy != "exact_link":
                link = EntityLink(
                    canonical_entity_id=canonical.id,
                    source_system=source_system,
                    entity_type=entity_type,
                    external_id=external_id,
                    match_strategy=decision.strategy,
                    match_confidence=decision.confidence,
                    matched_signals=decision.matched_signals,
                )
                session.add(link)

            # Flag intra-source duplicates (same source reporting a second external_id)
            if decision.duplicate_external_ids:
                dup_anomaly = ReconciliationAnomaly(
                    canonical_entity_id=canonical.id,
                    event_id=event_id,
                    source_system=source_system,
                    entity_type=entity_type,
                    external_id=external_id,
                    anomaly_type="duplicate_record",
                    severity="warning",
                    status="open",
                    details={
                        "existing_external_ids": decision.duplicate_external_ids,
                        "incoming_external_id": external_id,
                        "matched_signals": decision.matched_signals,
                    },
                )
                session.add(dup_anomaly)
                session.flush()
                anomaly_ids.append(dup_anomaly.id)
                ANOMALIES_TOTAL.labels(
                    anomaly_type="duplicate_record",
                    source_system=source_system,
                ).inc()

            # Upsert SourceRecord for this (source_system, entity_type, external_id)
            source_record = session.scalar(
                select(SourceRecord).where(
                    SourceRecord.source_system == source_system,
                    SourceRecord.entity_type == entity_type,
                    SourceRecord.external_id == external_id,
                )
            )
            if source_record is None:
                source_record = SourceRecord(
                    canonical_entity_id=canonical.id,
                    source_system=source_system,
                    entity_type=entity_type,
                    external_id=external_id,
                    attributes=data,
                    last_event_id=event_id,
                    source_updated_at=occurred_at,
                )
                session.add(source_record)
            else:
                source_record.canonical_entity_id = canonical.id
                source_record.attributes = data
                source_record.last_event_id = event_id
                source_record.source_updated_at = occurred_at
            session.flush()

            # Build per-source attribute view across all linked source records
            all_source_records = list(
                session.scalars(
                    select(SourceRecord)
                    .where(SourceRecord.canonical_entity_id == canonical.id)
                    .order_by(SourceRecord.source_updated_at.asc())
                )
            )
            source_attrs_by_system: dict[str, dict[str, Any]] = {}
            for rec in all_source_records:
                source_attrs_by_system[rec.source_system] = dict(rec.attributes)

            diff_result = compute_canonical_and_drift(
                previous_canonical=before_state,
                source_attributes_by_system=source_attrs_by_system,
                incoming_source=source_system,
                incoming_attributes=data,
            )

            # Update CanonicalEntity & indexed normalized lookup keys
            canonical.canonical_data = diff_result.merged_canonical
            canonical.has_active_drift = diff_result.drift_detected
            if not is_new_entity:
                canonical.version += 1
            self._apply_indexed_signals(canonical, diff_result.merged_canonical, signals)

            # Record drift anomalies & Prometheus metrics
            if diff_result.drift_detected:
                for field_diff in diff_result.field_diffs:
                    RECONCILIATION_DRIFTS_TOTAL.labels(
                        entity_type=entity_type,
                        field=field_diff.field,
                    ).inc()

                drift_anomaly = ReconciliationAnomaly(
                    canonical_entity_id=canonical.id,
                    event_id=event_id,
                    source_system=source_system,
                    entity_type=entity_type,
                    external_id=external_id,
                    anomaly_type="cross_system_drift",
                    severity=(
                        "high"
                        if any(d.severity == "high" for d in diff_result.field_diffs)
                        else "medium"
                    ),
                    status="open",
                    details={
                        "drifted_fields": [d.to_dict() for d in diff_result.field_diffs],
                    },
                )
                session.add(drift_anomaly)
                session.flush()
                anomaly_ids.append(drift_anomaly.id)
                ANOMALIES_TOTAL.labels(
                    anomaly_type="cross_system_drift",
                    source_system=source_system,
                ).inc()

            # Determine snapshot status
            if decision.duplicate_external_ids:
                snapshot_status = "duplicate_flagged"
            elif diff_result.drift_detected:
                snapshot_status = "drifted"
            elif is_new_entity:
                snapshot_status = "created"
            else:
                snapshot_status = "matched"

            snapshot = ReconciliationSnapshot(
                canonical_entity_id=canonical.id,
                trigger_event_id=event_id,
                source_system=source_system,
                entity_type=entity_type,
                external_id=external_id,
                status=snapshot_status,
                match_strategy=decision.strategy,
                match_confidence=decision.confidence,
                matched_signals=decision.matched_signals,
                before_state=before_state,
                after_state=diff_result.merged_canonical,
                field_diffs=[d.to_dict() for d in diff_result.field_diffs],
                drift_detected=diff_result.drift_detected,
                drift_count=diff_result.drift_count,
            )
            session.add(snapshot)
            session.flush()

            logger.info(
                "reconciliation_completed",
                extra={
                    "event_id": event_id,
                    "canonical_entity_id": canonical.id,
                    "source_system": source_system,
                    "entity_type": entity_type,
                    "external_id": external_id,
                    "strategy": decision.strategy,
                    "confidence": decision.confidence,
                    "status": snapshot_status,
                    "drift_count": diff_result.drift_count,
                },
            )
            return ReconciliationOutcome(
                snapshot_id=snapshot.id,
                canonical_entity_id=canonical.id,
                status=snapshot_status,
                match_strategy=decision.strategy,
                match_confidence=decision.confidence,
                drift_detected=diff_result.drift_detected,
                drift_count=diff_result.drift_count,
                anomaly_ids=anomaly_ids,
            )
        finally:
            elapsed = time.perf_counter() - start
            RECONCILIATION_DURATION_SECONDS.labels(entity_type=entity_type).observe(elapsed)

    @staticmethod
    def _apply_indexed_signals(
        canonical: CanonicalEntity,
        merged_data: dict[str, Any],
        incoming_signals: NormalizedSignals,
    ) -> None:
        merged_signals = extract_normalized_signals(canonical.entity_type, merged_data)
        canonical.normalized_email = (
            merged_signals.email or canonical.normalized_email or incoming_signals.email
        )
        canonical.normalized_phone = (
            merged_signals.phone or canonical.normalized_phone or incoming_signals.phone
        )
        canonical.normalized_tax_id = (
            merged_signals.tax_id or canonical.normalized_tax_id or incoming_signals.tax_id
        )
        canonical.normalized_name = (
            merged_signals.name or canonical.normalized_name or incoming_signals.name
        )
        canonical.normalized_postal_code = (
            merged_signals.postal_code
            or canonical.normalized_postal_code
            or incoming_signals.postal_code
        )
        canonical.normalized_invoice_number = (
            merged_signals.invoice_number
            or canonical.normalized_invoice_number
            or incoming_signals.invoice_number
        )
