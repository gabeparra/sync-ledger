"""HMAC-SHA256 webhook signature computation and constant-time verification."""

from __future__ import annotations

import hashlib
import hmac
import time

SIGNATURE_PREFIX = "sha256="


class WebhookAuthError(Exception):
    """Raised when a webhook fails HMAC signature or replay-window validation."""


def compute_webhook_signature(secret: str, timestamp: int | str, raw_body: bytes) -> str:
    """Compute canonical `sha256=<hex>` signature over `<timestamp>.<raw_body>`."""
    signed_payload = f"{timestamp}.".encode() + raw_body
    digest = hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
    return f"{SIGNATURE_PREFIX}{digest}"


def verify_webhook_signature(
    *,
    secret: str,
    raw_body: bytes,
    signature_header: str | None,
    timestamp_header: str | None,
    tolerance_seconds: int = 300,
    now_epoch: int | None = None,
) -> str:
    """Verify HMAC signature and replay timestamp window in constant time.

    Returns the verified signature string on success; raises WebhookAuthError otherwise.
    """
    if not signature_header or not timestamp_header:
        raise WebhookAuthError(
            "Missing required X-SyncLedger-Signature or X-SyncLedger-Timestamp header"
        )

    if not signature_header.startswith(SIGNATURE_PREFIX):
        raise WebhookAuthError("Malformed signature header; expected 'sha256=<hex>'")

    try:
        timestamp_int = int(timestamp_header.strip())
    except ValueError as exc:
        raise WebhookAuthError("Malformed X-SyncLedger-Timestamp header; expected integer") from exc

    current_time = int(time.time()) if now_epoch is None else now_epoch
    if tolerance_seconds > 0 and abs(current_time - timestamp_int) > tolerance_seconds:
        raise WebhookAuthError(
            f"Webhook timestamp outside allowed {tolerance_seconds}s replay window"
        )

    expected = compute_webhook_signature(secret, timestamp_int, raw_body)
    if not hmac.compare_digest(expected, signature_header.strip()):
        raise WebhookAuthError("Invalid HMAC webhook signature")

    return expected


def compute_request_hash(raw_body: bytes) -> str:
    """Compute SHA-256 digest of raw request bytes for idempotency conflict detection."""
    return hashlib.sha256(raw_body).hexdigest()
