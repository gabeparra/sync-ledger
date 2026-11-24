"""FastAPI route definitions for webhooks, canonical entities, audit diffs, DLQ, and ops."""

from __future__ import annotations

import json
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response, status
from pydantic import ValidationError
from sqlalchemy import select, text
from sqlalchemy.orm import Session, selectinload

from sync_ledger import __version__
from sync_ledger.config import Settings
from sync_ledger.db.models import (
    CanonicalEntity,
    ReconciliationAnomaly,
    ReconciliationSnapshot,
)
from sync_ledger.metrics import (
    WEBHOOKS_RECEIVED_TOTAL,
    refresh_queue_gauges,
    render_prometheus_metrics,
)
from sync_ledger.schemas import (
    CanonicalEntityResponse,
    DeadLetterReplayRequest,
    DeadLetterReplayResponse,
    DeadLetterResponse,
    HealthResponse,
    ReadinessResponse,
    ReconciliationAnomalyResponse,
    ReconciliationSnapshotResponse,
    WebhookAcceptedResponse,
    WebhookEventPayload,
)
from sync_ledger.security import WebhookAuthError, verify_webhook_signature
from sync_ledger.services.dlq import DeadLetterNotFoundError, DeadLetterService
from sync_ledger.services.ingestion import IdempotencyConflictError, WebhookIngestionService

logger = logging.getLogger(__name__)


def get_db(request: Request) -> Session:  # pragma: no cover - overridden via dependency generator
    raise RuntimeError("Database dependency not configured")


def get_app_settings(request: Request) -> Settings:
    settings: Settings = request.app.state.settings
    return settings


def build_router() -> APIRouter:
    """Create the primary sync-ledger API router."""
    router = APIRouter()

    # ------------------------------------------------------------------
    # Observability & Health
    # ------------------------------------------------------------------
    @router.get("/health", response_model=HealthResponse, tags=["ops"])
    def health() -> HealthResponse:
        return HealthResponse(status="ok", service="sync-ledger", version=__version__)

    @router.get("/health/ready", response_model=ReadinessResponse, tags=["ops"])
    def health_ready(db: Annotated[Session, Depends(get_db)]) -> ReadinessResponse:
        try:
            db.execute(text("SELECT 1"))
            outbox_pending, dlq_open = refresh_queue_gauges(db)
        except Exception as exc:
            logger.exception("readiness_check_failed")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Database unavailable: {exc}",
            ) from exc

        return ReadinessResponse(
            status="ready",
            database="connected",
            outbox_pending=outbox_pending,
            dlq_open=dlq_open,
        )

    @router.get("/metrics", tags=["ops"])
    def metrics(db: Annotated[Session, Depends(get_db)]) -> Response:
        refresh_queue_gauges(db)
        return Response(
            content=render_prometheus_metrics(),
            media_type="text/plain; version=0.0.4; charset=utf-8",
        )

    # ------------------------------------------------------------------
    # Webhook Ingestion (HMAC + Idempotency + Transactional Outbox)
    # ------------------------------------------------------------------
    @router.post(
        "/v1/webhooks/{source_system}",
        response_model=WebhookAcceptedResponse,
        status_code=status.HTTP_202_ACCEPTED,
        tags=["webhooks"],
    )
    async def ingest_webhook(
        source_system: str,
        request: Request,
        response: Response,
        db: Annotated[Session, Depends(get_db)],
        settings: Annotated[Settings, Depends(get_app_settings)],
        idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
        signature: Annotated[str | None, Header(alias="X-SyncLedger-Signature")] = None,
        timestamp: Annotated[str | None, Header(alias="X-SyncLedger-Timestamp")] = None,
    ) -> WebhookAcceptedResponse:
        normalized_source = source_system.strip().lower()
        raw_body = await request.body()

        secret = settings.secret_for_source(normalized_source)
        try:
            verified_sig = verify_webhook_signature(
                secret=secret,
                raw_body=raw_body,
                signature_header=signature,
                timestamp_header=timestamp,
                tolerance_seconds=settings.signature_tolerance_seconds,
            )
        except WebhookAuthError as exc:
            WEBHOOKS_RECEIVED_TOTAL.labels(
                source_system=normalized_source,
                outcome="unauthorized",
            ).inc()
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=str(exc),
            ) from exc

        if not idempotency_key or not idempotency_key.strip():
            WEBHOOKS_RECEIVED_TOTAL.labels(
                source_system=normalized_source,
                outcome="invalid",
            ).inc()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required Idempotency-Key header",
            )

        try:
            parsed_json = json.loads(raw_body.decode("utf-8"))
            payload = WebhookEventPayload.model_validate(parsed_json)
        except (UnicodeDecodeError, json.JSONDecodeError, ValidationError) as exc:
            WEBHOOKS_RECEIVED_TOTAL.labels(
                source_system=normalized_source,
                outcome="invalid",
            ).inc()
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Invalid webhook JSON payload: {exc}",
            ) from exc

        service = WebhookIngestionService(settings)
        try:
            result = service.ingest(
                db,
                source_system=normalized_source,
                idempotency_key=idempotency_key.strip(),
                raw_body=raw_body,
                payload=payload,
                signature_digest=verified_sig,
            )
        except IdempotencyConflictError as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=str(exc),
            ) from exc

        response.status_code = result.response_status
        if result.replayed:
            response.headers["X-Idempotency-Replayed"] = "true"
        return WebhookAcceptedResponse.model_validate(result.response_body)

    # ------------------------------------------------------------------
    # Canonical Entities & Cross-System Links
    # ------------------------------------------------------------------
    @router.get("/v1/entities", response_model=list[CanonicalEntityResponse], tags=["entities"])
    def list_entities(
        db: Annotated[Session, Depends(get_db)],
        entity_type: str | None = Query(default=None),
        drift_only: bool = Query(default=False),
        limit: int = Query(default=50, ge=1, le=200),
    ) -> list[CanonicalEntity]:
        stmt = (
            select(CanonicalEntity)
            .options(
                selectinload(CanonicalEntity.links),
                selectinload(CanonicalEntity.source_records),
            )
            .order_by(CanonicalEntity.updated_at.desc())
            .limit(limit)
        )
        if entity_type:
            stmt = stmt.where(CanonicalEntity.entity_type == entity_type.lower())
        if drift_only:
            stmt = stmt.where(CanonicalEntity.has_active_drift.is_(True))
        return list(db.scalars(stmt))

    @router.get(
        "/v1/entities/{entity_id}",
        response_model=CanonicalEntityResponse,
        tags=["entities"],
    )
    def get_entity(
        entity_id: str,
        db: Annotated[Session, Depends(get_db)],
    ) -> CanonicalEntity:
        stmt = (
            select(CanonicalEntity)
            .where(CanonicalEntity.id == entity_id)
            .options(
                selectinload(CanonicalEntity.links),
                selectinload(CanonicalEntity.source_records),
            )
        )
        entity = db.scalar(stmt)
        if entity is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Canonical entity '{entity_id}' not found",
            )
        return entity

    # ------------------------------------------------------------------
    # Reconciliation Audit Snapshots & Anomalies
    # ------------------------------------------------------------------
    @router.get(
        "/v1/reconciliations/snapshots",
        response_model=list[ReconciliationSnapshotResponse],
        tags=["reconciliation"],
    )
    def list_snapshots(
        db: Annotated[Session, Depends(get_db)],
        canonical_entity_id: str | None = Query(default=None),
        source_system: str | None = Query(default=None),
        drift_only: bool = Query(default=False),
        limit: int = Query(default=50, ge=1, le=200),
    ) -> list[ReconciliationSnapshot]:
        stmt = (
            select(ReconciliationSnapshot)
            .order_by(ReconciliationSnapshot.created_at.desc())
            .limit(limit)
        )
        if canonical_entity_id:
            stmt = stmt.where(ReconciliationSnapshot.canonical_entity_id == canonical_entity_id)
        if source_system:
            stmt = stmt.where(ReconciliationSnapshot.source_system == source_system.lower())
        if drift_only:
            stmt = stmt.where(ReconciliationSnapshot.drift_detected.is_(True))
        return list(db.scalars(stmt))

    @router.get(
        "/v1/reconciliations/anomalies",
        response_model=list[ReconciliationAnomalyResponse],
        tags=["reconciliation"],
    )
    def list_anomalies(
        db: Annotated[Session, Depends(get_db)],
        anomaly_type: str | None = Query(default=None),
        status_filter: str | None = Query(default=None, alias="status"),
        limit: int = Query(default=50, ge=1, le=200),
    ) -> list[ReconciliationAnomaly]:
        stmt = (
            select(ReconciliationAnomaly)
            .order_by(ReconciliationAnomaly.created_at.desc())
            .limit(limit)
        )
        if anomaly_type:
            stmt = stmt.where(ReconciliationAnomaly.anomaly_type == anomaly_type)
        if status_filter:
            stmt = stmt.where(ReconciliationAnomaly.status == status_filter)
        return list(db.scalars(stmt))

    # ------------------------------------------------------------------
    # Dead Letter Queue (DLQ) & Replay
    # ------------------------------------------------------------------
    @router.get("/v1/dlq", response_model=list[DeadLetterResponse], tags=["dlq"])
    def list_dlq(
        db: Annotated[Session, Depends(get_db)],
        settings: Annotated[Settings, Depends(get_app_settings)],
        status_filter: str | None = Query(default=None, alias="status"),
        source_system: str | None = Query(default=None),
        error_type: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=200),
    ) -> list[DeadLetterResponse]:
        service = DeadLetterService(settings)
        entries = service.list_messages(
            db,
            status=status_filter,
            source_system=source_system,
            error_type=error_type,
            limit=limit,
        )
        return [DeadLetterResponse.model_validate(item) for item in entries]

    @router.post(
        "/v1/dlq/{dlq_id}/replay",
        response_model=DeadLetterReplayResponse,
        tags=["dlq"],
    )
    def replay_dlq_item(
        dlq_id: str,
        body: DeadLetterReplayRequest,
        db: Annotated[Session, Depends(get_db)],
        settings: Annotated[Settings, Depends(get_app_settings)],
    ) -> DeadLetterReplayResponse:
        service = DeadLetterService(settings)
        try:
            outcome = service.replay_message(
                db,
                dlq_id,
                patch_data=body.patch_data,
                notes=body.notes,
            )
        except DeadLetterNotFoundError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=str(exc),
            ) from exc

        return DeadLetterReplayResponse(
            dlq_id=outcome.dlq_id,
            new_outbox_id=outcome.new_outbox_id or "",
            status=outcome.status,
            replay_count=outcome.replay_count,
        )

    return router
