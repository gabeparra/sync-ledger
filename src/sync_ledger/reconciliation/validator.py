"""Domain validation and typed error classification (permanent poison-pill vs transient)."""

from __future__ import annotations

import re
from typing import Any

from sync_ledger.reconciliation.normalizers import NormalizedSignals, extract_normalized_signals

SUPPORTED_ENTITY_TYPES = frozenset({"customer", "invoice", "order"})
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")


class ReconciliationError(Exception):
    """Base error raised during reconciliation processing."""


class TransientReconciliationError(ReconciliationError):
    """Retryable error (network timeout, lock contention, temporary downstream failure)."""


class PermanentReconciliationError(ReconciliationError):
    """Unrecoverable poison-pill or malformed payload error routed directly to DLQ."""

    def __init__(
        self,
        error_type: str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.details = details or {}


def validate_reconciliation_payload(payload: dict[str, Any]) -> NormalizedSignals:
    """Validate inbound event payload and return extracted normalized matching signals.

    Raises PermanentReconciliationError for malformed or unrecoverable records.
    """
    entity_type = str(payload.get("entity_type", "")).strip().lower()
    if entity_type not in SUPPORTED_ENTITY_TYPES:
        raise PermanentReconciliationError(
            "unsupported_entity_type",
            f"Unsupported entity_type '{entity_type}'. Supported: {sorted(SUPPORTED_ENTITY_TYPES)}",
            details={"entity_type": entity_type},
        )

    external_id = payload.get("external_id")
    if not isinstance(external_id, str) or not external_id.strip():
        raise PermanentReconciliationError(
            "malformed_payload",
            "Missing or empty external_id",
            details={"field": "external_id"},
        )

    data = payload.get("data")
    if not isinstance(data, dict):
        raise PermanentReconciliationError(
            "malformed_payload",
            "Field 'data' must be a JSON object",
            details={"field": "data"},
        )

    errors: list[str] = []
    signals = extract_normalized_signals(entity_type, data)

    # Validate email syntax whenever supplied
    raw_email = data.get("email") or data.get("customer_email")
    if raw_email is not None and (
        not isinstance(raw_email, str) or not _EMAIL_RE.match(raw_email.strip())
    ):
        errors.append(f"Invalid email format: {raw_email!r}")

    # Validate tax_id minimum length if supplied
    raw_tax_id = data.get("tax_id") or data.get("customer_tax_id") or data.get("ein")
    if raw_tax_id is not None and (signals.tax_id is None or len(signals.tax_id) < 5):
        errors.append(f"Invalid tax_id format: {raw_tax_id!r}")

    # Validate currency if supplied
    if (
        "currency" in data
        and data["currency"] is not None
        and (signals.currency is None or not _CURRENCY_RE.match(signals.currency))
    ):
        errors.append(f"Invalid ISO-4217 currency code: {data['currency']!r}")

    if entity_type == "customer":
        if not any((signals.email, signals.tax_id, signals.phone, signals.name)):
            errors.append(
                "Customer record must include at least one identifying field "
                "(email, tax_id, phone, or name)"
            )
        if "balance_cents" in data and data["balance_cents"] is not None:
            bal = data["balance_cents"]
            if not isinstance(bal, int) or isinstance(bal, bool):
                errors.append(f"balance_cents must be an integer, got {type(bal).__name__}")

    elif entity_type in {"invoice", "order"}:
        if not signals.invoice_number:
            errors.append(f"{entity_type} record requires invoice_number or order_number")
        amount = data.get("amount_cents")
        if amount is None or not isinstance(amount, int) or isinstance(amount, bool) or amount < 0:
            errors.append(f"amount_cents must be a non-negative integer, got {amount!r}")
        if not signals.currency or not _CURRENCY_RE.match(signals.currency):
            errors.append(f"{entity_type} requires a valid 3-letter ISO currency code")

    if errors:
        raise PermanentReconciliationError(
            "malformed_payload",
            "; ".join(errors),
            details={"validation_errors": errors, "entity_type": entity_type},
        )

    return signals
