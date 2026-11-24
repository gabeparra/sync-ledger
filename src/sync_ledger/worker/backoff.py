"""Exponential retry backoff with bounded jitter."""

from __future__ import annotations

import random
from datetime import datetime, timedelta


def compute_backoff_seconds(
    attempt: int,
    *,
    base_delay_seconds: float = 2.0,
    max_delay_seconds: float = 60.0,
    jitter_ratio: float = 0.15,
    rng: random.Random | None = None,
) -> float:
    """Calculate capped exponential backoff delay with symmetric bounded jitter.

    Attempt 1 -> base_delay, attempt 2 -> 2 * base_delay, attempt 3 -> 4 * base_delay, etc.
    """
    if attempt < 1:
        attempt = 1
    exponent: int = 1 << (attempt - 1)
    raw_delay = min(max_delay_seconds, base_delay_seconds * exponent)
    if jitter_ratio <= 0.0 or raw_delay <= 0.0:
        return float(raw_delay)
    generator = rng or random
    jitter_factor = 1.0 + generator.uniform(-jitter_ratio, jitter_ratio)
    return float(max(0.0, round(raw_delay * jitter_factor, 4)))


def next_retry_at(
    now: datetime,
    attempt: int,
    *,
    base_delay_seconds: float = 2.0,
    max_delay_seconds: float = 60.0,
    jitter_ratio: float = 0.15,
    rng: random.Random | None = None,
) -> datetime:
    """Compute the UTC timestamp when a retry-scheduled outbox message becomes available."""
    delay = compute_backoff_seconds(
        attempt,
        base_delay_seconds=base_delay_seconds,
        max_delay_seconds=max_delay_seconds,
        jitter_ratio=jitter_ratio,
        rng=rng,
    )
    return now + timedelta(seconds=delay)
