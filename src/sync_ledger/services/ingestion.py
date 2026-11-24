"""Transactional webhook ingestion service with idempotency deduplication and outbox writes."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from sync_ledger.config import Settings
from sync_ledger.db.models import IdempotencyKey, InboundEvent, OutboxMessage, new_uuid, utc_now
from sync_ledger.metrics import WEBHOOKS_RECEIVED_TOTAL
from sync_ledger.schemas import WebhookEventPayload
from sync_ledger.security import compute_request_hash

logger = logging.getLogger(__name__)


class IdempotencyConflictError(Exception):
    """Raised when an idempotency key is reused with a different request payload."""


@dataclass(frozen=True)
class IngestionResult:
    """Outcome of processing a webhook ingestion request."""

    event_id: str
    outbox_id: str
    source_system: str
    idempotency_key: str
    replayed: bool
    response_status: int
    response_body: dict[str, Any]


class WebhookIngestionService:
    """Atomically deduplicates webhooks and writes inbound_events + outbox_messages."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def ingest(
        self,
        session: Session,
        *,
        source_system: str,
        idempotency_key: str,
        raw_body: bytes,
        payload: WebhookEventPayload,
        signature_digest: str,
    ) -> IngestionResult:
        """Persist a verified webhook atomically with its outbox job and idempotency record."""
        normalized_source = source_system.strip().lower()
        normalized_entity_type = payload.entity_type.strip().lower()
        request_hash = compute_request_hash(raw_body)

        existing = self._lookup_idempotency_key(session, normalized_source, idempotency_key)
        if existing is not None:
            return self._handle_existing_key(existing, request_hash, normalized_source)

        event_id = new_uuid()
        outbox_id = new_uuid()
        now = utc_now()

        serialized_payload = payload.model_dump(mode="json")
        serialized_payload["source_system"] = normalized_source
        serialized_payload["entity_type"] = normalized_entity_type
        serialized_payload["event_id"] = event_id

        event_row = InboundEvent(
            id=event_id,
            source_system=normalized_source,
            event_type=payload.event_type,
            entity_type=normalized_entity_type,
            external_id=payload.external_id,
            idempotency_key=idempotency_key,
            payload=serialized_payload,
            signature_digest=signature_digest,
            received_at=now,
        )

        outbox_row = OutboxMessage(
            id=outbox_id,
            event_id=event_id,
            topic=f"reconcile.{normalized_entity_type}",
            aggregate_type=normalized_entity_type,
            aggregate_id=f"{normalized_source}:{payload.external_id}",
            payload=serialized_payload,
            status="pending",
            attempts=0,
            max_attempts=self.settings.retry_max_attempts,
            available_at=now,
        )

        response_body = {
            "status": "accepted",
            "event_id": event_id,
            "outbox_id": outbox_id,
            "source_system": normalized_source,
            "idempotency_key": idempotency_key,
            "replayed": False,
        }

        idempotency_row = IdempotencyKey(
            source_system=normalized_source,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            status="completed",
            response_status=202,
            response_body=response_body,
            event_id=event_id,
            created_at=now,
        )

        try:
            session.add(event_row)
            session.flush()
            session.add(outbox_row)
            session.add(idempotency_row)
            session.commit()
        except IntegrityError:
            session.rollback()
            concurrent_existing = self._lookup_idempotency_key(
                session, normalized_source, idempotency_key
            )
            if concurrent_existing is None:
                raise
            return self._handle_existing_key(concurrent_existing, request_hash, normalized_source)

        WEBHOOKS_RECEIVED_TOTAL.labels(source_system=normalized_source, outcome="accepted").inc()
        logger.info(
            "webhook_ingested",
            extra={
                "source_system": normalized_source,
                "entity_type": normalized_entity_type,
                "external_id": payload.external_id,
                "event_id": event_id,
                "outbox_id": outbox_id,
            },
        )
        return IngestionResult(
            event_id=event_id,
            outbox_id=outbox_id,
            source_system=normalized_source,
            idempotency_key=idempotency_key,
            replayed=False,
            response_status=202,
            response_body=response_body,
        )

    @staticmethod
    def _lookup_idempotency_key(
        session: Session, source_system: str, idempotency_key: str
    ) -> IdempotencyKey | None:
        return session.scalar(
            select(IdempotencyKey).where(
                IdempotencyKey.source_system == source_system,
                IdempotencyKey.idempotency_key == idempotency_key,
            )
        )

    @staticmethod
    def _handle_existing_key(
        existing: IdempotencyKey, request_hash: str, source_system: str
    ) -> IngestionResult:
        if existing.request_hash != request_hash:
            WEBHOOKS_RECEIVED_TOTAL.labels(source_system=source_system, outcome="conflict").inc()
            raise IdempotencyConflictError(
                f"Idempotency-Key '{existing.idempotency_key}' was already used with a "
                "different request payload."
            )

        WEBHOOKS_RECEIVED_TOTAL.labels(source_system=source_system, outcome="replayed").inc()
        replayed_body = dict(existing.response_body)
        replayed_body["replayed"] = True
        return IngestionResult(
            event_id=str(replayed_body.get("event_id", existing.event_id or "")),
            outbox_id=str(replayed_body.get("outbox_id", "")),
            source_system=source_system,
            idempotency_key=existing.idempotency_key,
            replayed=True,
            response_status=existing.response_status,
            response_body=replayed_body,
        )
