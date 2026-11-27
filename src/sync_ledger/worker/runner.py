"""Transactional outbox polling worker with exponential retries and DLQ routing."""

from __future__ import annotations

import logging
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session, sessionmaker

from sync_ledger.config import Settings
from sync_ledger.db.models import (
    DeadLetterMessage,
    OutboxMessage,
    ReconciliationAnomaly,
    ReconciliationSnapshot,
    utc_now,
)
from sync_ledger.metrics import ANOMALIES_TOTAL, OUTBOX_PROCESSED_TOTAL, refresh_queue_gauges
from sync_ledger.reconciliation.engine import ReconciliationEngine, ReconciliationOutcome
from sync_ledger.reconciliation.validator import (
    PermanentReconciliationError,
    TransientReconciliationError,
)
from sync_ledger.worker.backoff import next_retry_at

logger = logging.getLogger(__name__)

MessageHandler = Callable[[Session, OutboxMessage], ReconciliationOutcome]


@dataclass(frozen=True)
class WorkerBatchStats:
    """Summary of one worker poll-and-process cycle."""

    claimed: int = 0
    processed: int = 0
    retried: int = 0
    dead_lettered: int = 0


class OutboxWorker:
    """Leases outbox messages safely and dispatches them through the ReconciliationEngine."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        settings: Settings,
        *,
        engine: ReconciliationEngine | None = None,
        handler: MessageHandler | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.settings = settings
        self.engine = engine or ReconciliationEngine(settings)
        self._handler = handler
        self._stop_requested = False

    def stop(self) -> None:
        """Signal the continuous polling loop to exit cleanly after the current batch."""
        self._stop_requested = True

    def claim_batch(self, session: Session, *, now: datetime | None = None) -> list[str]:
        """Claim up to `outbox_batch_size` due messages using FOR UPDATE SKIP LOCKED on Postgres.

        A message left in 'processing' longer than `outbox_lease_timeout_seconds` belongs to a
        worker that died mid-batch, so it is reclaimed. The lost run counts as an attempt, which
        keeps a message that crashes every worker from cycling forever: once its attempts are
        spent it goes to the DLQ as `lease_expired` instead of being retried again.
        """
        current_time = now or utc_now()
        lease_cutoff = current_time - timedelta(seconds=self.settings.outbox_lease_timeout_seconds)
        stmt = (
            select(OutboxMessage)
            .where(
                or_(
                    and_(
                        OutboxMessage.status.in_(("pending", "retry_scheduled")),
                        OutboxMessage.available_at <= current_time,
                    ),
                    and_(
                        OutboxMessage.status == "processing",
                        OutboxMessage.locked_at.is_not(None),
                        OutboxMessage.locked_at <= lease_cutoff,
                    ),
                )
            )
            .order_by(OutboxMessage.available_at.asc(), OutboxMessage.created_at.asc())
            .limit(self.settings.outbox_batch_size)
        )

        bind = session.get_bind()
        if bind is not None and bind.dialect.name == "postgresql":
            stmt = stmt.with_for_update(skip_locked=True)

        messages = list(session.scalars(stmt))
        claimed_ids: list[str] = []
        for msg in messages:
            if msg.status == "processing":
                previous_owner = msg.locked_by
                msg.attempts += 1
                msg.last_error = f"lease expired while held by {previous_owner or 'unknown worker'}"
                logger.warning(
                    "outbox_stale_lease_reclaimed",
                    extra={
                        "outbox_id": msg.id,
                        "event_id": msg.event_id,
                        "previous_owner": previous_owner,
                        "attempts": msg.attempts,
                    },
                )
                if msg.attempts >= msg.max_attempts:
                    self._dead_letter_expired_lease(session, msg, current_time)
                    continue

            msg.status = "processing"
            msg.locked_by = self.settings.worker_id
            msg.locked_at = current_time
            claimed_ids.append(msg.id)

        session.commit()
        return claimed_ids

    def _dead_letter_expired_lease(
        self, session: Session, msg: OutboxMessage, current_time: datetime
    ) -> None:
        """Park a message whose lease expired after its last allowed attempt."""
        payload: dict[str, Any] = dict(msg.payload)
        msg.status = "dead_lettered"
        msg.locked_by = None
        msg.locked_at = None
        session.add(
            DeadLetterMessage(
                outbox_message_id=msg.id,
                event_id=msg.event_id,
                source_system=str(payload.get("source_system", "unknown")).lower(),
                topic=msg.topic,
                error_type="lease_expired",
                error_message=msg.last_error or "lease expired",
                payload=payload,
                attempts=msg.attempts,
                status="open",
                created_at=current_time,
            )
        )
        OUTBOX_PROCESSED_TOTAL.labels(topic=msg.topic, outcome="dead_lettered").inc()

    def process_message(
        self,
        session: Session,
        outbox_id: str,
        *,
        now: datetime | None = None,
    ) -> str:
        """Process one claimed outbox message ('processed', 'retried', or 'dead_lettered')."""
        current_time = now or utc_now()
        msg = session.get(OutboxMessage, outbox_id)
        if msg is None:
            return "missing"

        topic = msg.topic
        payload: dict[str, Any] = dict(msg.payload)
        source_system = str(payload.get("source_system", "unknown")).lower()
        entity_type = str(payload.get("entity_type", msg.aggregate_type)).lower()
        external_id = str(payload.get("external_id", "")).strip()

        try:
            with session.begin_nested():
                if self._handler is not None:
                    self._handler(session, msg)
                else:
                    self.engine.process_event(
                        session,
                        event_id=msg.event_id,
                        payload=payload,
                    )

            msg.status = "processed"
            msg.attempts += 1
            msg.processed_at = current_time
            msg.locked_by = None
            msg.locked_at = None
            msg.last_error = None
            session.commit()

            OUTBOX_PROCESSED_TOTAL.labels(topic=topic, outcome="succeeded").inc()
            return "processed"

        except PermanentReconciliationError as exc:
            msg.attempts += 1
            msg.status = "dead_lettered"
            msg.locked_by = None
            msg.locked_at = None
            msg.last_error = f"{exc.error_type}: {exc}"

            dlq_entry = DeadLetterMessage(
                outbox_message_id=msg.id,
                event_id=msg.event_id,
                source_system=source_system,
                topic=topic,
                error_type=exc.error_type,
                error_message=str(exc),
                payload=payload,
                attempts=msg.attempts,
                status="open",
                created_at=current_time,
            )
            anomaly = ReconciliationAnomaly(
                canonical_entity_id=None,
                event_id=msg.event_id,
                source_system=source_system,
                entity_type=entity_type,
                external_id=external_id or "unknown",
                anomaly_type="malformed_record",
                severity="high",
                status="open",
                details={
                    "error_type": exc.error_type,
                    "error_message": str(exc),
                    **exc.details,
                },
                created_at=current_time,
            )
            snapshot = ReconciliationSnapshot(
                canonical_entity_id=None,
                trigger_event_id=msg.event_id,
                source_system=source_system,
                entity_type=entity_type,
                external_id=external_id or "unknown",
                status="malformed",
                match_strategy="validation_failed",
                match_confidence=0.0,
                matched_signals=[],
                before_state={},
                after_state={},
                field_diffs=[],
                drift_detected=False,
                drift_count=0,
                created_at=current_time,
            )
            session.add_all([dlq_entry, anomaly, snapshot])
            session.commit()

            OUTBOX_PROCESSED_TOTAL.labels(topic=topic, outcome="dead_lettered").inc()
            ANOMALIES_TOTAL.labels(
                anomaly_type="malformed_record",
                source_system=source_system,
            ).inc()
            logger.warning(
                "outbox_message_dead_lettered",
                extra={
                    "outbox_id": msg.id,
                    "event_id": msg.event_id,
                    "error_type": exc.error_type,
                    "error_message": str(exc),
                },
            )
            return "dead_lettered"

        except (TransientReconciliationError, Exception) as exc:
            msg.attempts += 1
            msg.locked_by = None
            msg.locked_at = None
            msg.last_error = str(exc)

            if msg.attempts >= msg.max_attempts:
                msg.status = "dead_lettered"
                dlq_entry = DeadLetterMessage(
                    outbox_message_id=msg.id,
                    event_id=msg.event_id,
                    source_system=source_system,
                    topic=topic,
                    error_type="retry_exhausted",
                    error_message=str(exc),
                    payload=payload,
                    attempts=msg.attempts,
                    status="open",
                    created_at=current_time,
                )
                session.add(dlq_entry)
                session.commit()

                OUTBOX_PROCESSED_TOTAL.labels(topic=topic, outcome="dead_lettered").inc()
                logger.error(
                    "outbox_retries_exhausted_dead_lettered",
                    extra={
                        "outbox_id": msg.id,
                        "event_id": msg.event_id,
                        "attempts": msg.attempts,
                        "error_message": str(exc),
                    },
                )
                return "dead_lettered"

            msg.status = "retry_scheduled"
            msg.available_at = next_retry_at(
                current_time,
                msg.attempts,
                base_delay_seconds=self.settings.retry_base_delay_seconds,
                max_delay_seconds=self.settings.retry_max_delay_seconds,
                jitter_ratio=self.settings.retry_jitter_ratio,
            )
            session.commit()

            OUTBOX_PROCESSED_TOTAL.labels(topic=topic, outcome="retried").inc()
            logger.warning(
                "outbox_retry_scheduled",
                extra={
                    "outbox_id": msg.id,
                    "event_id": msg.event_id,
                    "attempts": msg.attempts,
                    "next_available_at": msg.available_at.isoformat(),
                    "error_message": str(exc),
                },
            )
            return "retried"

    def run_once(self, *, now: datetime | None = None) -> WorkerBatchStats:
        """Claim and process a single batch of due outbox messages."""
        with self.session_factory() as session:
            claimed_ids = self.claim_batch(session, now=now)

        processed = 0
        retried = 0
        dead_lettered = 0

        for outbox_id in claimed_ids:
            with self.session_factory() as session:
                outcome = self.process_message(session, outbox_id, now=now)
                if outcome == "processed":
                    processed += 1
                elif outcome == "retried":
                    retried += 1
                elif outcome == "dead_lettered":
                    dead_lettered += 1

        with self.session_factory() as session:
            refresh_queue_gauges(session)

        return WorkerBatchStats(
            claimed=len(claimed_ids),
            processed=processed,
            retried=retried,
            dead_lettered=dead_lettered,
        )

    def run_forever(self) -> None:
        """Continuously poll the outbox until interrupted by SIGINT/SIGTERM."""
        self._stop_requested = False

        def _handle_signal(_signum: int, _frame: Any) -> None:
            logger.info(
                "worker_shutdown_signal_received", extra={"worker_id": self.settings.worker_id}
            )
            self.stop()

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

        logger.info("worker_started", extra={"worker_id": self.settings.worker_id})
        while not self._stop_requested:
            stats = self.run_once()
            if stats.claimed == 0:
                time.sleep(self.settings.outbox_poll_interval_seconds)
        logger.info("worker_stopped", extra={"worker_id": self.settings.worker_id})
