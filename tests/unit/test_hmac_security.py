"""Unit tests for HMAC-SHA256 webhook signature verification and replay protection."""

from __future__ import annotations

import pytest

from sync_ledger.security import (
    WebhookAuthError,
    compute_request_hash,
    compute_webhook_signature,
    verify_webhook_signature,
)

pytestmark = pytest.mark.unit


def test_valid_hmac_signature_passes() -> None:
    body = b'{"event_type":"customer.upserted","external_id":"c1"}'
    ts = 1_700_000_000
    sig = compute_webhook_signature("super-secret", ts, body)

    verified = verify_webhook_signature(
        secret="super-secret",
        raw_body=body,
        signature_header=sig,
        timestamp_header=str(ts),
        tolerance_seconds=300,
        now_epoch=ts + 15,
    )
    assert verified == sig


def test_tampered_body_rejected() -> None:
    ts = 1_700_000_000
    sig = compute_webhook_signature("super-secret", ts, b'{"amount_cents":100}')

    with pytest.raises(WebhookAuthError, match="Invalid HMAC"):
        verify_webhook_signature(
            secret="super-secret",
            raw_body=b'{"amount_cents":999999}',
            signature_header=sig,
            timestamp_header=str(ts),
            now_epoch=ts,
        )


def test_wrong_secret_rejected() -> None:
    body = b'{"external_id":"x"}'
    ts = 1_700_000_000
    sig = compute_webhook_signature("wrong-secret", ts, body)

    with pytest.raises(WebhookAuthError, match="Invalid HMAC"):
        verify_webhook_signature(
            secret="expected-secret",
            raw_body=body,
            signature_header=sig,
            timestamp_header=str(ts),
            now_epoch=ts,
        )


def test_stale_timestamp_rejected_outside_replay_window() -> None:
    body = b'{"external_id":"x"}'
    ts = 1_700_000_000
    sig = compute_webhook_signature("secret", ts, body)

    with pytest.raises(WebhookAuthError, match="replay window"):
        verify_webhook_signature(
            secret="secret",
            raw_body=body,
            signature_header=sig,
            timestamp_header=str(ts),
            tolerance_seconds=300,
            now_epoch=ts + 601,
        )


def test_missing_or_malformed_headers_rejected() -> None:
    with pytest.raises(WebhookAuthError, match="Missing required"):
        verify_webhook_signature(
            secret="secret",
            raw_body=b"{}",
            signature_header=None,
            timestamp_header="1700000000",
        )

    with pytest.raises(WebhookAuthError, match="Malformed signature"):
        verify_webhook_signature(
            secret="secret",
            raw_body=b"{}",
            signature_header="md5=abc",
            timestamp_header="1700000000",
        )

    with pytest.raises(WebhookAuthError, match="Malformed X-SyncLedger-Timestamp"):
        verify_webhook_signature(
            secret="secret",
            raw_body=b"{}",
            signature_header="sha256=abc",
            timestamp_header="not-a-timestamp",
        )


def test_request_hash_is_deterministic_sha256() -> None:
    h1 = compute_request_hash(b'{"a":1}')
    h2 = compute_request_hash(b'{"a":1}')
    h3 = compute_request_hash(b'{"a":2}')
    assert h1 == h2
    assert h1 != h3
    assert len(h1) == 64
