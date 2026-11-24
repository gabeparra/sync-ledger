"""Operational CLI for sync-ledger worker execution, DLQ replay, reporting, and demo."""

from __future__ import annotations

import json
import time
from typing import Annotated, Any

import typer
from sqlalchemy import func, select

from sync_ledger.config import Settings, get_settings
from sync_ledger.db.models import (
    CanonicalEntity,
    DeadLetterMessage,
    EntityLink,
    OutboxMessage,
    ReconciliationAnomaly,
    ReconciliationSnapshot,
)
from sync_ledger.db.session import DatabaseManager, init_db
from sync_ledger.logging import configure_logging
from sync_ledger.schemas import WebhookEventPayload
from sync_ledger.security import compute_webhook_signature
from sync_ledger.services.dlq import DeadLetterNotFoundError, DeadLetterService
from sync_ledger.services.ingestion import WebhookIngestionService
from sync_ledger.worker.runner import OutboxWorker

app = typer.Typer(
    name="sync-ledger",
    help="Sync-Ledger Event-Driven Integration & Cross-System Reconciliation CLI.",
    no_args_is_help=True,
)
dlq_app = typer.Typer(help="Inspect and replay Dead Letter Queue (DLQ) messages.")
worker_app = typer.Typer(help="Run the transactional outbox reconciliation worker.")
reconcile_app = typer.Typer(help="Inspect cross-system reconciliation health and audit summaries.")
demo_app = typer.Typer(help="Run an end-to-end local multi-system reconciliation demo.")

app.add_typer(dlq_app, name="dlq")
app.add_typer(worker_app, name="worker")
app.add_typer(reconcile_app, name="reconcile")
app.add_typer(demo_app, name="demo")


def _load_runtime(database_url: str | None = None) -> tuple[Settings, DatabaseManager]:
    settings = Settings(database_url=database_url) if database_url else get_settings()
    configure_logging(settings.log_level)
    manager = DatabaseManager(settings)
    return settings, manager


@dlq_app.command("list")
def dlq_list(
    status: Annotated[
        str | None, typer.Option(help="Filter by DLQ status (open|replayed).")
    ] = "open",
    source: Annotated[str | None, typer.Option(help="Filter by source system.")] = None,
    error_type: Annotated[str | None, typer.Option(help="Filter by error type.")] = None,
    limit: Annotated[int, typer.Option(help="Maximum messages to display.")] = 50,
    database_url: Annotated[str | None, typer.Option(help="Override database URL.")] = None,
) -> None:
    """List Dead Letter Queue entries."""
    settings, manager = _load_runtime(database_url)
    try:
        service = DeadLetterService(settings)
        with manager.session() as session:
            entries = service.list_messages(
                session,
                status=status if status != "all" else None,
                source_system=source,
                error_type=error_type,
                limit=limit,
            )
            if not entries:
                typer.echo("No dead-letter messages matched the given filters.")
                return
            for item in entries:
                typer.echo(
                    f"{item.id}  status={item.status:<8}  source={item.source_system:<8}  "
                    f"error={item.error_type:<20}  attempts={item.attempts}  "
                    f"replays={item.replay_count}  msg={item.error_message}"
                )
    finally:
        manager.dispose()


@dlq_app.command("inspect")
def dlq_inspect(
    dlq_id: Annotated[str, typer.Argument(help="Dead-letter message ID to inspect.")],
    database_url: Annotated[str | None, typer.Option(help="Override database URL.")] = None,
) -> None:
    """Print full JSON details for a single DLQ entry."""
    settings, manager = _load_runtime(database_url)
    try:
        service = DeadLetterService(settings)
        with manager.session() as session:
            try:
                entry = service.get_message(session, dlq_id)
            except DeadLetterNotFoundError as exc:
                typer.echo(str(exc), err=True)
                raise typer.Exit(code=1) from exc

            details = {
                "id": entry.id,
                "outbox_message_id": entry.outbox_message_id,
                "event_id": entry.event_id,
                "source_system": entry.source_system,
                "topic": entry.topic,
                "error_type": entry.error_type,
                "error_message": entry.error_message,
                "status": entry.status,
                "attempts": entry.attempts,
                "replay_count": entry.replay_count,
                "replayed_outbox_id": entry.replayed_outbox_id,
                "replay_notes": entry.replay_notes,
                "payload": entry.payload,
            }
            typer.echo(json.dumps(details, indent=2, default=str))
    finally:
        manager.dispose()


@dlq_app.command("replay")
def dlq_replay(
    dlq_id: Annotated[str, typer.Argument(help="Dead-letter message ID to replay.")],
    patch_json: Annotated[
        str | None,
        typer.Option(help="Optional JSON object merged into payload['data'] before replay."),
    ] = None,
    notes: Annotated[str | None, typer.Option(help="Audit note for this replay.")] = None,
    dry_run: Annotated[bool, typer.Option(help="Preview without mutating the database.")] = False,
    process_now: Annotated[
        bool, typer.Option(help="Immediately run a worker cycle after re-enqueueing.")
    ] = False,
    database_url: Annotated[str | None, typer.Option(help="Override database URL.")] = None,
) -> None:
    """Re-enqueue a dead-letter message back onto the transactional outbox."""
    patch_data: dict[str, Any] | None = None
    if patch_json:
        try:
            parsed = json.loads(patch_json)
            if not isinstance(parsed, dict):
                raise ValueError("--patch-json must decode to a JSON object")
            patch_data = parsed
        except (json.JSONDecodeError, ValueError) as exc:
            typer.echo(f"Invalid --patch-json: {exc}", err=True)
            raise typer.Exit(code=2) from exc

    settings, manager = _load_runtime(database_url)
    try:
        service = DeadLetterService(settings)
        with manager.session() as session:
            try:
                outcome = service.replay_message(
                    session,
                    dlq_id,
                    patch_data=patch_data,
                    notes=notes,
                    dry_run=dry_run,
                )
            except DeadLetterNotFoundError as exc:
                typer.echo(str(exc), err=True)
                raise typer.Exit(code=1) from exc

        if outcome.dry_run:
            typer.echo(
                f"[DRY RUN] Would replay DLQ {outcome.dlq_id} (current status={outcome.status})"
            )
            return

        typer.echo(
            f"Replayed DLQ {outcome.dlq_id} -> outbox {outcome.new_outbox_id} "
            f"(replay_count={outcome.replay_count})"
        )

        if process_now:
            worker = OutboxWorker(manager.session_factory, settings)
            stats = worker.run_once()
            typer.echo(
                f"Worker cycle complete: claimed={stats.claimed} processed={stats.processed} "
                f"retried={stats.retried} dead_lettered={stats.dead_lettered}"
            )
    finally:
        manager.dispose()


@dlq_app.command("replay-all")
def dlq_replay_all(
    source: Annotated[str | None, typer.Option(help="Filter by source system.")] = None,
    error_type: Annotated[str | None, typer.Option(help="Filter by error type.")] = None,
    limit: Annotated[int, typer.Option(help="Maximum open DLQ entries to replay.")] = 50,
    notes: Annotated[str | None, typer.Option(help="Audit note for batch replay.")] = None,
    dry_run: Annotated[bool, typer.Option(help="Preview count without mutating.")] = False,
    process_now: Annotated[bool, typer.Option(help="Run one worker batch immediately.")] = False,
    database_url: Annotated[str | None, typer.Option(help="Override database URL.")] = None,
) -> None:
    """Replay all open DLQ entries matching the specified filters."""
    settings, manager = _load_runtime(database_url)
    try:
        service = DeadLetterService(settings)
        with manager.session() as session:
            outcomes = service.replay_batch(
                session,
                source_system=source,
                error_type=error_type,
                limit=limit,
                notes=notes,
                dry_run=dry_run,
            )

        prefix = "[DRY RUN] " if dry_run else ""
        typer.echo(f"{prefix}Matched {len(outcomes)} open DLQ message(s).")
        for outcome in outcomes:
            typer.echo(f"  dlq={outcome.dlq_id} -> outbox={outcome.new_outbox_id}")

        if process_now and not dry_run and outcomes:
            worker = OutboxWorker(manager.session_factory, settings)
            stats = worker.run_once()
            typer.echo(
                f"Worker cycle complete: claimed={stats.claimed} processed={stats.processed} "
                f"retried={stats.retried} dead_lettered={stats.dead_lettered}"
            )
    finally:
        manager.dispose()


@worker_app.command("run")
def worker_run(
    once: Annotated[bool, typer.Option(help="Process a single due batch and exit.")] = False,
    batch_size: Annotated[int | None, typer.Option(help="Override outbox batch size.")] = None,
    database_url: Annotated[str | None, typer.Option(help="Override database URL.")] = None,
) -> None:
    """Start the outbox reconciliation worker."""
    settings, manager = _load_runtime(database_url)
    if batch_size is not None:
        settings.outbox_batch_size = batch_size
    try:
        worker = OutboxWorker(manager.session_factory, settings)
        if once:
            stats = worker.run_once()
            typer.echo(
                f"Batch complete: claimed={stats.claimed} processed={stats.processed} "
                f"retried={stats.retried} dead_lettered={stats.dead_lettered}"
            )
        else:
            worker.run_forever()
    finally:
        manager.dispose()


@reconcile_app.command("report")
def reconcile_report(
    database_url: Annotated[str | None, typer.Option(help="Override database URL.")] = None,
) -> None:
    """Print an operational summary of entities, links, drifts, anomalies, and queues."""
    _settings, manager = _load_runtime(database_url)
    try:
        with manager.session() as session:
            summary = {
                "canonical_entities": session.scalar(
                    select(func.count()).select_from(CanonicalEntity)
                )
                or 0,
                "entity_links": session.scalar(select(func.count()).select_from(EntityLink)) or 0,
                "entities_with_active_drift": session.scalar(
                    select(func.count())
                    .select_from(CanonicalEntity)
                    .where(CanonicalEntity.has_active_drift.is_(True))
                )
                or 0,
                "reconciliation_snapshots": session.scalar(
                    select(func.count()).select_from(ReconciliationSnapshot)
                )
                or 0,
                "open_anomalies": session.scalar(
                    select(func.count())
                    .select_from(ReconciliationAnomaly)
                    .where(ReconciliationAnomaly.status == "open")
                )
                or 0,
                "outbox_pending": session.scalar(
                    select(func.count())
                    .select_from(OutboxMessage)
                    .where(OutboxMessage.status.in_(("pending", "retry_scheduled")))
                )
                or 0,
                "dlq_open": session.scalar(
                    select(func.count())
                    .select_from(DeadLetterMessage)
                    .where(DeadLetterMessage.status == "open")
                )
                or 0,
            }
            typer.echo(json.dumps(summary, indent=2))
    finally:
        manager.dispose()


@demo_app.command("run")
def demo_run() -> None:
    """Run a self-contained in-memory demonstration of no-shared-ID reconciliation & DLQ replay."""
    settings = Settings(database_url="sqlite+pysqlite:///:memory:", log_level="WARNING")
    manager = DatabaseManager(settings)
    init_db(manager.engine)
    ingestion = WebhookIngestionService(settings)
    worker = OutboxWorker(manager.session_factory, settings)
    dlq_service = DeadLetterService(settings)

    demo_events = [
        (
            "crm",
            "idem-crm-1",
            {
                "event_type": "customer.upserted",
                "entity_type": "customer",
                "external_id": "sf_001A",
                "data": {
                    "name": "Acme Corp.",
                    "email": "ops@acme.io",
                    "phone": "(415) 555-0199",
                    "postal_code": "94107",
                },
            },
        ),
        (
            "billing",
            "idem-billing-1",
            {
                "event_type": "customer.updated",
                "entity_type": "customer",
                "external_id": "cus_stripe_99",
                "data": {
                    "name": "ACME Inc",
                    "email": "ops@acme.io",
                    "phone": "+14155550199",
                    "tax_id": "12-3456789",
                    "balance_cents": 245000,
                    "currency": "USD",
                },
            },
        ),
        (
            "erp",
            "idem-erp-1",
            {
                "event_type": "account.synced",
                "entity_type": "customer",
                "external_id": "NS-4401",
                "data": {
                    "name": "Acme Corporation",
                    "tax_id": "123456789",
                    "email": "ap@acme.io",
                    "balance_cents": 240000,
                    "currency": "USD",
                    "postal_code": "94107-1234",
                },
            },
        ),
        (
            "crm",
            "idem-crm-poison",
            {
                "event_type": "customer.upserted",
                "entity_type": "customer",
                "external_id": "sf_bad_99",
                "data": {"email": "not-a-valid-email"},
            },
        ),
    ]

    with manager.session() as session:
        for source, key, body in demo_events:
            raw = json.dumps(body).encode("utf-8")
            sig = compute_webhook_signature(settings.webhook_secret, int(time.time()), raw)
            ingestion.ingest(
                session,
                source_system=source,
                idempotency_key=key,
                raw_body=raw,
                payload=WebhookEventPayload.model_validate(body),
                signature_digest=sig,
            )

    stats = worker.run_once()
    typer.echo(f"1. Processed initial batch across CRM, Billing, ERP: {stats}")

    with manager.session() as session:
        entities = list(session.scalars(select(CanonicalEntity)))
        dlq_items = dlq_service.list_messages(session, status="open")
        typer.echo(
            f"2. Unified {len(entities)} canonical entity across 3 systems "
            "with NO shared primary key."
        )
        for ent in entities:
            links = list(
                session.scalars(select(EntityLink).where(EntityLink.canonical_entity_id == ent.id))
            )
            joined_links = ", ".join(
                f"{lnk.source_system}:{lnk.external_id} ({lnk.match_strategy})" for lnk in links
            )
            typer.echo(f"   Linked IDs: {joined_links}")
            typer.echo(f"   Active drift detected: {ent.has_active_drift}")

        if dlq_items:
            poison = dlq_items[0]
            typer.echo(f"3. Poison-pill routed to DLQ: {poison.id} ({poison.error_message})")
            replay = dlq_service.replay_message(
                session,
                poison.id,
                patch_data={"email": "fixed@globex.io", "name": "Globex Corp"},
                notes="Patched invalid email in demo",
            )
            typer.echo(f"4. Patched & replayed DLQ -> new outbox {replay.new_outbox_id}")

    stats2 = worker.run_once()
    typer.echo(f"5. Post-replay worker cycle: {stats2}")
    manager.dispose()
