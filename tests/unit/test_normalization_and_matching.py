"""Unit tests for canonical normalization, multi-signal scoring, and exponential backoff."""

from __future__ import annotations

import random

import pytest

from sync_ledger.db.models import CanonicalEntity
from sync_ledger.reconciliation.matcher import score_candidate
from sync_ledger.reconciliation.normalizers import (
    NormalizedSignals,
    normalize_email,
    normalize_invoice_number,
    normalize_name,
    normalize_phone,
    normalize_postal_code,
    normalize_tax_id,
)
from sync_ledger.worker.backoff import compute_backoff_seconds

pytestmark = pytest.mark.unit


def test_field_normalizers_handle_cross_system_formatting_noise() -> None:
    assert normalize_email("  Alice.Ops@ACME.io ") == "alice.ops@acme.io"
    assert normalize_phone("(415) 555-0199") == "+14155550199"
    assert normalize_phone("+1-415-555-0199") == "+14155550199"
    assert normalize_tax_id("12-3456789") == "123456789"
    assert normalize_name("Acme Corp.") == "acme"
    assert normalize_name("ACME, Incorporated") == "acme"
    assert normalize_name("Acme & Sons LLC") == "acme and sons"
    assert normalize_postal_code("94107-1234") == "94107"
    assert normalize_invoice_number("INV-2026-0042") == "INV20260042"


def test_score_candidate_composite_name_and_postal_links_without_email_or_tax_id() -> None:
    candidate = CanonicalEntity(
        id="ent-1",
        entity_type="customer",
        canonical_data={"name": "Acme Corp.", "postal_code": "94107"},
        normalized_name="acme",
        normalized_postal_code="94107",
    )
    incoming = NormalizedSignals(name="acme", postal_code="94107")

    result = score_candidate("customer", incoming, candidate)
    assert result.score == pytest.approx(0.70)
    assert result.signals == ["name", "postal_code"]


def test_score_candidate_disqualifies_conflicting_tax_ids() -> None:
    candidate = CanonicalEntity(
        id="ent-2",
        entity_type="customer",
        canonical_data={"email": "billing@holding.com", "tax_id": "11-1111111"},
        normalized_email="billing@holding.com",
        normalized_tax_id="111111111",
    )
    incoming = NormalizedSignals(email="billing@holding.com", tax_id="999999999")

    result = score_candidate("customer", incoming, candidate)
    assert result.score == 0.0
    assert result.signals == ["tax_id_conflict"]


def test_exponential_backoff_caps_and_applies_bounded_jitter() -> None:
    assert (
        compute_backoff_seconds(1, base_delay_seconds=2.0, max_delay_seconds=30.0, jitter_ratio=0.0)
        == 2.0
    )
    assert (
        compute_backoff_seconds(2, base_delay_seconds=2.0, max_delay_seconds=30.0, jitter_ratio=0.0)
        == 4.0
    )
    assert (
        compute_backoff_seconds(3, base_delay_seconds=2.0, max_delay_seconds=30.0, jitter_ratio=0.0)
        == 8.0
    )
    assert (
        compute_backoff_seconds(6, base_delay_seconds=2.0, max_delay_seconds=30.0, jitter_ratio=0.0)
        == 30.0
    )

    rng = random.Random(42)
    jittered = compute_backoff_seconds(
        3, base_delay_seconds=2.0, max_delay_seconds=30.0, jitter_ratio=0.20, rng=rng
    )
    assert 6.4 <= jittered <= 9.6
