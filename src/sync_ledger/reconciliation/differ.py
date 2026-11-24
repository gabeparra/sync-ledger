"""Cross-system field merge policy and structured drift diff computation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sync_ledger.reconciliation.normalizers import normalize_field_for_comparison

# Source-of-truth authority order per field (first present source wins for canonical value)
FIELD_AUTHORITY: dict[str, tuple[str, ...]] = {
    "tax_id": ("erp", "billing", "crm"),
    "customer_tax_id": ("erp", "billing", "crm"),
    "amount_cents": ("billing", "erp", "crm"),
    "balance_cents": ("billing", "erp", "crm"),
    "currency": ("billing", "erp", "crm"),
    "payment_status": ("billing", "erp", "crm"),
    "invoice_number": ("erp", "billing", "crm"),
    "email": ("crm", "billing", "erp"),
    "customer_email": ("crm", "billing", "erp"),
    "phone": ("crm", "billing", "erp"),
    "name": ("crm", "erp", "billing"),
    "postal_code": ("erp", "crm", "billing"),
    "country": ("erp", "crm", "billing"),
    "status": ("billing", "crm", "erp"),
}

HIGH_SEVERITY_FIELDS = frozenset(
    {
        "amount_cents",
        "balance_cents",
        "currency",
        "tax_id",
        "customer_tax_id",
        "payment_status",
        "invoice_number",
    }
)

IGNORED_METADATA_FIELDS = frozenset({"updated_at", "created_at", "sync_metadata"})


@dataclass(frozen=True)
class FieldDiff:
    """Represents a single cross-system field divergence or canonical state change."""

    field: str
    canonical_value: Any
    previous_canonical_value: Any
    incoming_value: Any
    source_values: dict[str, Any]
    severity: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "field": self.field,
            "canonical_value": self.canonical_value,
            "previous_canonical_value": self.previous_canonical_value,
            "incoming_value": self.incoming_value,
            "source_values": self.source_values,
            "severity": self.severity,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ReconciliationDiffResult:
    """Merged canonical state and detected cross-system field drifts."""

    merged_canonical: dict[str, Any]
    field_diffs: list[FieldDiff] = field(default_factory=list)

    @property
    def drift_detected(self) -> bool:
        return len(self.field_diffs) > 0

    @property
    def drift_count(self) -> int:
        return len(self.field_diffs)


def _pick_authoritative_value(
    field_name: str,
    values_by_source: dict[str, Any],
    incoming_source: str,
) -> Any:
    authority = FIELD_AUTHORITY.get(field_name, ())
    for authoritative_source in authority:
        if (
            authoritative_source in values_by_source
            and values_by_source[authoritative_source] is not None
        ):
            return values_by_source[authoritative_source]
    if incoming_source in values_by_source and values_by_source[incoming_source] is not None:
        return values_by_source[incoming_source]
    for source in sorted(values_by_source):
        if values_by_source[source] is not None:
            return values_by_source[source]
    return None


def compute_canonical_and_drift(
    *,
    previous_canonical: dict[str, Any],
    source_attributes_by_system: dict[str, dict[str, Any]],
    incoming_source: str,
    incoming_attributes: dict[str, Any],
) -> ReconciliationDiffResult:
    """Merge multi-system attributes by field authority and detect semantic drift.

    Formatting-only differences (e.g. `(415) 555-0100` vs `+14155550100`, or
    `Acme Corp.` vs `ACME Inc`) are normalized before comparison so they do not
    produce false-positive drift alerts.
    """
    all_fields: set[str] = set(previous_canonical.keys())
    for attrs in source_attributes_by_system.values():
        all_fields.update(attrs.keys())

    merged: dict[str, Any] = {}
    diffs: list[FieldDiff] = []

    for field_name in sorted(all_fields):
        if field_name.startswith("_") or field_name in IGNORED_METADATA_FIELDS:
            continue

        values_by_source: dict[str, Any] = {}
        for source, attrs in source_attributes_by_system.items():
            if field_name in attrs and attrs[field_name] is not None:
                values_by_source[source] = attrs[field_name]

        if not values_by_source:
            if field_name in previous_canonical:
                merged[field_name] = previous_canonical[field_name]
            continue

        canonical_value = _pick_authoritative_value(field_name, values_by_source, incoming_source)
        merged[field_name] = canonical_value

        # Detect semantic cross-system disagreement across 2+ reporting systems
        if len(values_by_source) >= 2:
            normalized_distinct = {
                normalize_field_for_comparison(field_name, val) for val in values_by_source.values()
            }
            if len(normalized_distinct) > 1:
                severity = "high" if field_name in HIGH_SEVERITY_FIELDS else "medium"
                diffs.append(
                    FieldDiff(
                        field=field_name,
                        canonical_value=canonical_value,
                        previous_canonical_value=previous_canonical.get(field_name),
                        incoming_value=incoming_attributes.get(field_name),
                        source_values=dict(sorted(values_by_source.items())),
                        severity=severity,
                        reason="cross_system_value_mismatch",
                    )
                )

    return ReconciliationDiffResult(merged_canonical=merged, field_diffs=diffs)
