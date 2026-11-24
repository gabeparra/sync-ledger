"""Dead Letter Queue (DLQ) inspection and transactional replay service."""

from __future__ import annotations

import copy
import logging
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from sync_ledger.config import Settings
from sync_ledger.db.models import DeadLetterMessage, OutboxMessage, new_uuid, utc_now
from sync_ledger.metrics import DLQ_REPLAYS_TOTAL, refresh_queue_gauges

logger = logging.getLogger(__name__)


class DeadLetterNotFoundError(Exception):
    """Raised when a requested dead-letter message does not exist."""


@dataclass(frozen=True)
class ReplayOutcome:
    """Result of replaying a single dead-letter message."""

    dlq_id: str
    new_outbox_id: str | None
    status: str
    replay_count: int
    dry_run: bool = False


class DeadLetterService:
    """Manages DLQ queries and safe transactional re-enqueueing back into the outbox."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    @staticmethod
    def list_messages(
        session: Session,
        *,
        status: str | None = None,
        source_system: str | None = None,
        error_type: str | None = None,
        limit: int = 50,
    ) -> list[DeadLetterMessage]:
        stmt = select(DeadLetterMessage).order_by(DeadLetterMessage.created_at.desc()).limit(limit)
        if status:
            stmt = stmt.where(DeadLetterMessage.status == status)
        if source_system:
            stmt = stmt.where(DeadLetterMessage.source_system == source_system.lower())
        if error_type:
            stmt = stmt.where(DeadLetterMessage.error_type == error_type)
        return list(session.scalars(stmt))

    @staticmethod
    def get_message(session: Session, dlq_id: str) -> DeadLetterMessage:
        entry = session.get(DeadLetterMessage, dlq_id)
        if entry is None:
            raise DeadLetterNotFoundError(f"Dead-letter message '{dlq_id}' not found")
        return entry

    def replay_message(
        self,
        session: Session,
        dlq_id: str,
        *,
        patch_data: dict[str, Any] | None = None,
        notes: str | None = None,
        dry_run: bool = False,
    ) -> ReplayOutcome:
        """Re-enqueue a dead-lettered message onto outbox_messages with optional payload patch."""
        entry = self.get_message(session, dlq_id)

        payload = copy.deepcopy(entry.payload)
        if patch_data:
            existing_data = payload.get("data")
            merged_data = dict(existing_data) if isinstance(existing_data, dict) else {}
            merged_data.update(patch_data)
            payload["data"] = merged_data

        if dry_run:
            return ReplayOutcome(
                dlq_id=entry.id,
                new_outbox_id=None,
                status=entry.status,
                replay_count=entry.replay_count,
                dry_run=True,
            )

        now = utc_now()
        new_outbox_id = new_uuid()
        entity_type = str(payload.get("entity_type", "unknown")).lower()
        external_id = str(payload.get("external_id", "unknown"))

        replayed_outbox = OutboxMessage(
            id=new_outbox_id,
            event_id=entry.event_id,
            topic=entry.topic,
            aggregate_type=entity_type,
            aggregate_id=f"{entry.source_system}:{external_id}",
            payload=payload,
            status="pending",
            attempts=0,
            max_attempts=self.settings.retry_max_attempts,
            available_at=now,
            created_at=now,
            updated_at=now,
        )

        entry.payload = payload
        entry.status = "replayed"
        entry.replay_count += 1
        entry.replayed_outbox_id = new_outbox_id
        entry.replayed_at = now
        if notes:
            entry.replay_notes = notes

        session.add(replayed_outbox)
        session.commit()

        DLQ_REPLAYS_TOTAL.labels(outcome="requeued").inc()
        refresh_queue_gauges(session)
        logger.info(
            "dlq_message_replayed",
            extra={
                "dlq_id": entry.id,
                "new_outbox_id": new_outbox_id,
                "replay_count": entry.replay_count,
                "patched": bool(patch_data),
            },
        )
        return ReplayOutcome(
            dlq_id=entry.id,
            new_outbox_id=new_outbox_id,
            status=entry.status,
            replay_count=entry.replay_count,
            dry_run=False,
        )

    def replay_batch(
        self,
        session: Session,
        *,
        source_system: str | None = None,
        error_type: str | None = None,
        limit: int = 50,
        notes: str | None = None,
        dry_run: bool = False,
    ) -> list[ReplayOutcome]:
        """Replay all open DLQ entries matching the provided filters."""
        open_entries = self.list_messages(
            session,
            status="open",
            source_system=source_system,
            error_type=error_type,
            limit=limit,
        )
        outcomes: list[ReplayOutcome] = []
        for entry in open_entries:
            outcomes.append(self.replay_message(session, entry.id, notes=notes, dry_run=dry_run))
        return outcomes
