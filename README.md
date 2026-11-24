# sync-ledger

**Event-Driven Integration & Cross-System Reconciliation Engine** built in Python (FastAPI, Pydantic v2, SQLAlchemy 2.0, PostgreSQL, Prometheus) to connect heterogeneous systems that share **no common primary key**: with cryptographic webhook verification, strict idempotency, transactional outbox delivery, field-level drift auditing, exponential retry backoff, and a Dead Letter Queue (DLQ) replay CLI.

---

## Why This Exists

Real-world enterprise integrations rarely share a clean global UUID:

- **CRM** identifies a customer as `sf_001A` (`Acme Corp.`, `(415) 555-0199`, `ops@acme.io`)
- **Billing** identifies the same customer as `cus_stripe_99` (`ACME Inc`, `+1-415-555-0199`, `12-3456789`, `$2,450.00`)
- **ERP** identifies the same customer as `NS-4401` (`Acme Corporation`, `123456789`, `ap@acme.io`, `$2,400.00`)

`sync-ledger` solves the hard operational problems that arise at this boundary:
1. **Untrusted / duplicated webhook delivery**: solved with constant-time HMAC-SHA256 verification, timestamp replay-window protection, and SHA-256 request-hash idempotency deduplication (`202` cached replay vs `409 Conflict` on payload mutation).
2. **Dual-write consistency**: solved with the **Transactional Outbox Pattern**: `inbound_events`, `idempotency_keys`, and `outbox_messages` commit atomically inside a single PostgreSQL transaction.
3. **Cross-system entity resolution without a shared primary key**: solved with indexed candidate blocking, canonical field normalization (E.164 phones, legal-suffix folding, tax ID / invoice reference stripping), weighted multi-signal confidence scoring, and explicit disqualifiers (conflicting legal `tax_id`s never auto-merge).
4. **Silent data drift & broken payloads**: solved with source-of-truth field authority policies, formatting-noise suppression (so `(415) 555-0199` vs `+14155550199` never pages on-call), immutable **Reconciliation Diff Snapshots**, anomaly flagging (`cross_system_drift`, `duplicate_record`, `ambiguous_match`, `malformed_record`), exponential retry backoff with jitter, and **DLQ replay with optional JSON payload patching**.

---

## Architecture

```mermaid
flowchart LR
    CRM["CRM Webhook\n(sf_001A)"] --> GW
    BILL["Billing Webhook\n(cus_stripe_99)"] --> GW
    ERP["ERP Webhook\n(NS-4401)"] --> GW

    subgraph API["FastAPI Ingestion Gateway"]
        GW["POST /v1/webhooks/{source}\nHMAC-SHA256 + Replay Window\nIdempotency-Key + Body Hash"]
    end

    GW -->|"Single ACID Tx"| PG_IN[("PostgreSQL\ninbound_events\nidempotency_keys\noutbox_messages")]

    subgraph WORKER["Outbox Reconciliation Worker"]
        POLL["Outbox Poller\nFOR UPDATE SKIP LOCKED"]
        VAL["Payload Validator\n(Permanent vs Transient)"]
        MATCH["No-Shared-ID Matcher\nNormalizers + Weighted Signals"]
        DIFF["Authority Merge &\nSemantic Drift Differ"]
        POLL --> VAL --> MATCH --> DIFF
    end

    PG_IN --> POLL
    DIFF --> PG_OUT[("PostgreSQL\ncanonical_entities\nentity_links + source_records\nreconciliation_snapshots\nreconciliation_anomalies")]

    VAL -->|"Transient Error\n(attempts < max)"| RETRY["Exponential Backoff\n+ Bounded Jitter\n(retry_scheduled)"]
    RETRY --> PG_IN

    VAL -->|"Poison Pill or\nRetries Exhausted"| DLQ[("dead_letter_messages\n(DLQ)")]
    DLQ -->|"sync-ledger dlq replay\n[--patch-json]"| PG_IN

    API --> OBS["Prometheus /metrics\n/health & /health/ready"]
    WORKER --> OBS
```

---

## Key Engineering Decisions

| Concern | Implementation | Why |
| :--- | :--- | :--- |
| **Webhook Security** | `HMAC-SHA256` over `<timestamp>.<raw_body>` with `hmac.compare_digest` + ±300s timestamp window + per-source secrets | Prevents tampering, timing side-channels, and stale replay attacks |
| **Idempotency** | `UNIQUE(source_system, idempotency_key)` + SHA-256 `request_hash` + `IntegrityError` concurrency fallback | Duplicate retries return cached `202` (`X-Idempotency-Replayed: true`); reusing a key with mutated bytes returns `409 Conflict` |
| **Transactional Outbox** | Single ACID transaction writes `inbound_events` + `idempotency_keys` + `outbox_messages` | Eliminates dual-write failure modes between DB commits and async processing |
| **Concurrent Workers** | `SELECT ... FOR UPDATE SKIP LOCKED` on PostgreSQL (`available_at <= now()`) | Allows horizontal worker scaling without double-processing leased messages |
| **Entity Resolution (No Shared ID)** | Indexed blocking (`tax_id`, `email`, `phone`, `name`, `invoice_number`) + weighted multi-signal scoring + `tax_id` conflict disqualifier | Connects CRM/Billing/ERP deterministically without O(N) full scans or false merges |
| **Drift Diffs & Audit Trail** | Field-level source-of-truth authority (`erp` for `tax_id`, `billing` for `balance_cents`, `crm` for `email`) + normalized comparison + immutable `reconciliation_snapshots` | Surfaces real business drift (`$2,450` vs `$2,400`) while ignoring cosmetic formatting noise |
| **Resilience & Recovery** | Capped exponential backoff + jitter for transient failures; immediate DLQ + anomaly on poison pills; transactional CLI/API replay with `--patch-json` | Operators can inspect, patch malformed fields, and replay safely with full audit counts |

---

## Quickstart

### 1. Local Setup (`uv` + `Makefile`)

```bash
make setup        # creates .venv on Python 3.12 and installs all deps
make check        # runs ruff lint, mypy --strict, and pytest
make demo         # runs an end-to-end 3-system reconciliation + poison-pill DLQ replay demo
```

### 2. Full Stack with Docker Compose (PostgreSQL 16 + Migrations + API + Worker)

```bash
cp .env.example .env
make docker-up
curl http://localhost:8000/health/ready
curl http://localhost:8000/metrics
```

### 3. Send a Signed Webhook

```bash
python3 - <<'EOF'
import hashlib, hmac, json, time, urllib.request

secret = "dev-shared-webhook-secret-change-me"
payload = {
    "event_type": "customer.upserted",
    "entity_type": "customer",
    "external_id": "sf_001A",
    "data": {
        "name": "Acme Corp.",
        "email": "ops@acme.io",
        "phone": "(415) 555-0199",
        "postal_code": "94107",
    },
}
raw = json.dumps(payload, separators=(",", ":")).encode()
ts = str(int(time.time()))
sig = "sha256=" + hmac.new(secret.encode(), f"{ts}.".encode() + raw, hashlib.sha256).hexdigest()

req = urllib.request.Request(
    "http://localhost:8000/v1/webhooks/crm",
    data=raw,
    headers={
        "Content-Type": "application/json",
        "Idempotency-Key": "crm-acme-001",
        "X-SyncLedger-Timestamp": ts,
        "X-SyncLedger-Signature": sig,
    },
)
print(urllib.request.urlopen(req).read().decode())
EOF
```

---

## Sample Reconciliation Drift Snapshot

When CRM (`sf_001A`), Billing (`cus_stripe_99`), and ERP (`NS-4401`) sync the same customer with conflicting balance and email values, `GET /v1/reconciliations/snapshots?drift_only=true` returns:

```json
{
  "status": "drifted",
  "source_system": "erp",
  "entity_type": "customer",
  "external_id": "NS-4401",
  "match_strategy": "natural_key",
  "match_confidence": 1.0,
  "matched_signals": ["tax_id", "name", "postal_code"],
  "drift_detected": true,
  "drift_count": 2,
  "field_diffs": [
    {
      "field": "balance_cents",
      "canonical_value": 245000,
      "previous_canonical_value": 245000,
      "incoming_value": 240000,
      "source_values": { "billing": 245000, "erp": 240000 },
      "severity": "high",
      "reason": "cross_system_value_mismatch"
    },
    {
      "field": "email",
      "canonical_value": "ops@acme.io",
      "previous_canonical_value": "ops@acme.io",
      "incoming_value": "ap@acme.io",
      "source_values": {
        "billing": "ops@acme.io",
        "crm": "ops@acme.io",
        "erp": "ap@acme.io"
      },
      "severity": "medium",
      "reason": "cross_system_value_mismatch"
    }
  ]
}
```

---

## REST API & CLI Reference

### HTTP Endpoints

- `POST /v1/webhooks/{source_system}`: Verified, idempotent webhook ingestion writing transactional outbox
- `GET /v1/entities` & `GET /v1/entities/{entity_id}`: Unified canonical records, cross-system `entity_links`, and `source_records`
- `GET /v1/reconciliations/snapshots`: Immutable audit & diff trail (`?drift_only=true`, `?canonical_entity_id=...`)
- `GET /v1/reconciliations/anomalies`: Open/historical anomalies (`malformed_record`, `duplicate_record`, `ambiguous_match`, `cross_system_drift`)
- `GET /v1/dlq` & `POST /v1/dlq/{dlq_id}/replay`: Inspect and replay dead-lettered messages
- `GET /health` & `GET /health/ready`: Liveness & readiness probes (DB connectivity + queue depth)
- `GET /metrics`: Prometheus exposition metrics

### Operations & DLQ Replay CLI (`sync-ledger`)

```bash
# Run the reconciliation worker (continuous or single-batch)
uv run sync-ledger worker run
uv run sync-ledger worker run --once

# Inspect and replay Dead Letter Queue entries
uv run sync-ledger dlq list --status open
uv run sync-ledger dlq inspect <dlq_id>
uv run sync-ledger dlq replay <dlq_id> \
  --patch-json '{"email": "fixed@acme.io"}' \
  --notes "Patched invalid CRM email" \
  --process-now
uv run sync-ledger dlq replay-all --source erp --dry-run

# Print live reconciliation health summary
uv run sync-ledger reconcile report

# Run the self-contained local demo
uv run sync-ledger demo run
```

---

## Project Layout

```text
sync-ledger/
├── .github/workflows/ci.yml      # Ruff, strict mypy, SQLite + Postgres 16 + Alembic CI, Docker build
├── alembic/                      # Database migrations (001_initial_schema.py)
├── docker-compose.yml            # PostgreSQL 16 + Alembic migrate + FastAPI API + Worker
├── Dockerfile                    # Multi-stage Python 3.12 + uv runtime container
├── Makefile                      # Developer & operations shortcuts
├── pyproject.toml                # Hatchling build, uv dependencies, ruff & strict mypy config
├── src/sync_ledger/
│   ├── api/routes.py             # Webhooks, entities, audit snapshots, anomalies, DLQ, health, metrics
│   ├── db/models.py              # 9 normalized SQLAlchemy 2.0 tables (JSONB on Postgres)
│   ├── reconciliation/
│   │   ├── normalizers.py        # Email, E.164 phone, tax ID, legal-suffix name, invoice normalizers
│   │   ├── validator.py          # Permanent (poison-pill) vs transient error classification
│   │   ├── matcher.py            # No-shared-ID candidate blocking, weighted scoring & duplicate detection
│   │   ├── differ.py             # Source-of-truth merge policy & semantic cross-system drift diffs
│   │   └── engine.py             # End-to-end reconciliation orchestration & audit snapshot writer
│   ├── services/
│   │   ├── ingestion.py          # Atomic idempotency + inbound_events + outbox_messages service
│   │   └── dlq.py                # DLQ inspection & transactional replay with payload patching
│   ├── worker/
│   │   ├── backoff.py            # Capped exponential retry backoff with bounded jitter
│   │   └── runner.py             # FOR UPDATE SKIP LOCKED outbox poller & retry/DLQ router
│   ├── cli.py                    # Typer operations & DLQ replay CLI
│   ├── metrics.py                # Prometheus counters, histograms, and live backlog gauges
│   └── security.py               # Constant-time HMAC-SHA256 verification & replay protection
└── tests/
    ├── unit/                     # HMAC security, normalizers, scoring, backoff, authority differ
    └── integration/              # Idempotency, 3-system reconciliation, retries/DLQ/CLI, metrics
```

---

## Running Tests

```bash
# Fast local suite (SQLite in-memory)
uv run pytest -v

# Full suite with coverage
uv run pytest --cov=sync_ledger --cov-report=term-missing

# Against a live PostgreSQL instance (after alembic upgrade head)
TEST_DATABASE_URL=postgresql+psycopg://sync_ledger:sync_ledger@localhost:5432/sync_ledger \
  uv run pytest -v
```
