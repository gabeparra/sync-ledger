"""Canonical field normalization utilities for no-shared-ID entity resolution."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any

_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_NON_DIGIT_RE = re.compile(r"\D+")
_LEGAL_SUFFIXES = frozenset(
    {
        "inc",
        "incorporated",
        "llc",
        "ltd",
        "limited",
        "corp",
        "corporation",
        "co",
        "company",
        "plc",
        "gmbh",
        "sa",
        "srl",
        "lp",
        "llp",
    }
)


def normalize_email(value: Any) -> str | None:
    """Normalize an email address to lowercase trimmed canonical form."""
    if not isinstance(value, str):
        return None
    cleaned = value.strip().lower()
    return cleaned or None


def normalize_phone(value: Any) -> str | None:
    """Normalize phone numbers to E.164-like canonical digits (US 10-digit -> +1)."""
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    digits = _NON_DIGIT_RE.sub("", raw)
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    if len(digits) >= 7:
        return f"+{digits}"
    return None


def normalize_tax_id(value: Any) -> str | None:
    """Strip punctuation and uppercase tax IDs / EINs / VAT numbers."""
    if not isinstance(value, str):
        return None
    cleaned = re.sub(r"[^A-Za-z0-9]", "", value).upper()
    return cleaned or None


def normalize_name(value: Any) -> str | None:
    """Normalize company or person names by folding unicode, punctuation, and legal suffixes."""
    if not isinstance(value, str):
        return None
    folded = unicodedata.normalize("NFKC", value).casefold()
    folded = folded.replace("&", " and ")
    tokens = [tok for tok in _NON_ALNUM_RE.split(folded) if tok]
    while len(tokens) > 1 and tokens[-1] in _LEGAL_SUFFIXES:
        tokens.pop()
    normalized = " ".join(tokens).strip()
    return normalized or None


def normalize_postal_code(value: Any) -> str | None:
    """Normalize postal/ZIP codes (truncating US ZIP+4 extension for matching)."""
    if not isinstance(value, str):
        return None
    cleaned = re.sub(r"[^A-Za-z0-9]", "", value).upper()
    if not cleaned:
        return None
    if len(cleaned) == 9 and cleaned.isdigit():
        return cleaned[:5]
    return cleaned


def normalize_invoice_number(value: Any) -> str | None:
    """Normalize invoice/order reference numbers (`INV-0042` -> `INV0042`)."""
    if not isinstance(value, str):
        return None
    cleaned = re.sub(r"[^A-Za-z0-9]", "", value).upper()
    return cleaned or None


def normalize_currency(value: Any) -> str | None:
    """Normalize ISO-4217 currency code to 3-letter uppercase."""
    if not isinstance(value, str):
        return None
    cleaned = value.strip().upper()
    return cleaned or None


@dataclass(frozen=True)
class NormalizedSignals:
    """Extracted canonical matching signals from an inbound entity payload."""

    email: str | None = None
    phone: str | None = None
    tax_id: str | None = None
    name: str | None = None
    postal_code: str | None = None
    invoice_number: str | None = None
    currency: str | None = None
    amount_cents: int | None = None


def extract_normalized_signals(entity_type: str, data: dict[str, Any]) -> NormalizedSignals:
    """Extract normalized matching keys from arbitrary source system attributes."""
    email_raw = data.get("email") or data.get("customer_email")
    tax_raw = data.get("tax_id") or data.get("customer_tax_id") or data.get("ein")
    name_raw = data.get("name") or data.get("company_name") or data.get("legal_name")
    phone_raw = data.get("phone") or data.get("phone_number")
    postal_raw = data.get("postal_code") or data.get("zip") or data.get("zip_code")
    invoice_raw = data.get("invoice_number") or data.get("order_number")
    currency_raw = data.get("currency")
    amount_raw = data.get("amount_cents")

    amount_cents: int | None = None
    if isinstance(amount_raw, int) and not isinstance(amount_raw, bool):
        amount_cents = amount_raw

    return NormalizedSignals(
        email=normalize_email(email_raw),
        phone=normalize_phone(phone_raw),
        tax_id=normalize_tax_id(tax_raw),
        name=normalize_name(name_raw),
        postal_code=normalize_postal_code(postal_raw),
        invoice_number=normalize_invoice_number(invoice_raw),
        currency=normalize_currency(currency_raw),
        amount_cents=amount_cents,
    )


def normalize_field_for_comparison(field_name: str, value: Any) -> Any:
    """Normalize a field value for semantic cross-system drift comparison."""
    if value is None:
        return None
    if field_name in {"email", "customer_email"}:
        return normalize_email(value)
    if field_name in {"phone", "phone_number"}:
        return normalize_phone(value)
    if field_name in {"tax_id", "customer_tax_id", "ein"}:
        return normalize_tax_id(value)
    if field_name in {"name", "company_name", "legal_name"}:
        return normalize_name(value)
    if field_name in {"postal_code", "zip", "zip_code"}:
        return normalize_postal_code(value)
    if field_name in {"invoice_number", "order_number"}:
        return normalize_invoice_number(value)
    if field_name == "currency":
        return normalize_currency(value)
    if isinstance(value, str):
        return value.strip()
    return value
