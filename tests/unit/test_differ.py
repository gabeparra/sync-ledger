"""Unit tests for canonical field authority merging and semantic drift detection."""

from __future__ import annotations

import pytest

from sync_ledger.reconciliation.differ import compute_canonical_and_drift

pytestmark = pytest.mark.unit


def test_formatting_only_differences_do_not_trigger_false_positive_drift() -> None:
    result = compute_canonical_and_drift(
        previous_canonical={
            "name": "Acme Corp.",
            "phone": "(415) 555-0199",
            "tax_id": "12-3456789",
        },
        source_attributes_by_system={
            "crm": {"name": "Acme Corp.", "phone": "(415) 555-0199"},
            "billing": {"name": "ACME Inc", "phone": "+14155550199", "tax_id": "12-3456789"},
            "erp": {"name": "Acme Corporation", "tax_id": "123456789"},
        },
        incoming_source="erp",
        incoming_attributes={"name": "Acme Corporation", "tax_id": "123456789"},
    )

    assert result.drift_detected is False
    assert result.drift_count == 0
    # ERP is authoritative for tax_id; CRM is authoritative for name
    assert result.merged_canonical["tax_id"] == "123456789"
    assert result.merged_canonical["name"] == "Acme Corp."


def test_semantic_disagreement_produces_structured_diff_with_severity() -> None:
    result = compute_canonical_and_drift(
        previous_canonical={"email": "ops@acme.io", "balance_cents": 245000},
        source_attributes_by_system={
            "crm": {"email": "ops@acme.io"},
            "billing": {"email": "ops@acme.io", "balance_cents": 245000},
            "erp": {"email": "ap@acme.io", "balance_cents": 240000},
        },
        incoming_source="erp",
        incoming_attributes={"email": "ap@acme.io", "balance_cents": 240000},
    )

    assert result.drift_detected is True
    assert result.drift_count == 2

    diffs_by_field = {d.field: d for d in result.field_diffs}
    assert diffs_by_field["balance_cents"].severity == "high"
    assert diffs_by_field["balance_cents"].canonical_value == 245000  # billing wins
    assert diffs_by_field["balance_cents"].source_values == {"billing": 245000, "erp": 240000}

    assert diffs_by_field["email"].severity == "medium"
    assert diffs_by_field["email"].canonical_value == "ops@acme.io"  # crm wins
    assert diffs_by_field["email"].source_values == {
        "billing": "ops@acme.io",
        "crm": "ops@acme.io",
        "erp": "ap@acme.io",
    }
