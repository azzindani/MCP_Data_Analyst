"""What a text column holds, checked value by value: email, phone, IBAN, postal code, card number.

auto_detect_schema could say a column was text, a date or a number, and nothing
about what the text was; validate_dataset flagged nulls, zeros and duplicates,
and never a malformed email or an IBAN with a wrong check digit. This names five
kinds and checks every value by that kind's own rule -- the checksum where one
exists (IBAN mod 97, card Luhn), the shape where it does not. Standard library
only: each rule fits in a line, too little to justify a phone or ID library.

A kind is inferred when at least 90% of a column's non-empty values pass -- the
bar every other type guess in this repo uses -- and the values that then fail are
the column's invalid values, which validate_dataset reports by row.

Shapes are deliberately narrow where a loose one would claim ordinary columns: a
phone needs a leading + or a separator, so a column of bare 8-digit order numbers
is not a column of phones, and a date (2019-10-16) or a decimal is not one either; a postal code is a US ZIP, a UK or a Canadian postcode,
not any short number.

These checks are reported, and they do not feed the shared quality score
(shared/quality.py), which scores the same file the same way in
MCP_Machine_Learning.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

KINDS: tuple[str, ...] = ("email", "iban", "card", "phone", "postal")
MATCH_THRESHOLD = 0.9

_EMAIL = re.compile(r"[A-Za-z0-9._%+'-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}")
_IBAN = re.compile(r"[A-Z]{2}[0-9]{2}[A-Z0-9]{11,30}")
_PHONE = re.compile(r"\+?[0-9()\-. ]+")
# Digits and separators that are a date or a decimal, not a phone: 2019-10-16
# has eight digits and a separator, and was read as one in a real Date column.
_NOT_PHONE = re.compile(r"[0-9]{4}([-/.])[0-9]{1,2}\1[0-9]{1,2}|[0-9]{1,2}([-/.])[0-9]{1,2}\2[0-9]{2,4}|[0-9]+\.[0-9]+")
_POSTAL = (
    re.compile(r"[0-9]{5}(?:-[0-9]{4})?"),  # US ZIP, ZIP+4
    re.compile(r"(?:GIR 0AA|[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2})"),  # UK
    re.compile(r"[A-Z][0-9][A-Z] [0-9][A-Z][0-9]"),  # Canada
)


def _compact(value: str) -> str:
    return re.sub(r"[\s-]", "", value)


def _luhn(digits: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(digits)):
        d = int(ch)
        if i % 2:
            d = d * 2 - 9 if d > 4 else d * 2
        total += d
    return total % 10 == 0


def _iban_ok(value: str) -> bool:
    iban = _compact(value).upper()
    if not _IBAN.fullmatch(iban):
        return False
    moved = iban[4:] + iban[:4]
    return int("".join(str(int(ch, 36)) for ch in moved)) % 97 == 1


def _card_ok(value: str) -> bool:
    digits = _compact(value)
    return digits.isdigit() and 12 <= len(digits) <= 19 and _luhn(digits)


def _phone_ok(value: str) -> bool:
    text = value.strip()
    if not _PHONE.fullmatch(text) or _NOT_PHONE.fullmatch(text):
        return False
    digits = re.sub(r"\D", "", text)
    separated = text.startswith("+") or any(ch in text for ch in " ()-.")
    return separated and 7 <= len(digits) <= 15


def _postal_ok(value: str) -> bool:
    text = re.sub(r"\s+", " ", value.strip().upper())
    return any(p.fullmatch(text) for p in _POSTAL)


_CHECKS = {
    "email": lambda v: bool(_EMAIL.fullmatch(v.strip())),
    "iban": _iban_ok,
    "card": _card_ok,
    "phone": _phone_ok,
    "postal": _postal_ok,
}


def is_valid(kind: str, value: Any) -> bool:
    """Whether one value is a well-formed `kind`. A non-text value never is."""
    return isinstance(value, str) and bool(value.strip()) and _CHECKS[kind](value)


def infer_kind(values: Iterable[Any]) -> tuple[str | None, float]:
    """The kind at least 90% of the non-empty text values are, and that share; (None, 0.0) if none."""
    texts = [v for v in values if isinstance(v, str) and v.strip()]
    if not texts:
        return None, 0.0
    for kind in KINDS:
        rate = sum(1 for v in texts if _CHECKS[kind](v)) / len(texts)
        if rate >= MATCH_THRESHOLD:
            return kind, round(rate, 3)
    return None, 0.0


def normalize(kind: str, value: Any) -> Any:
    """One canonical spelling of a valid value; anything else is returned as it came."""
    if not is_valid(kind, value):
        return value
    text = value.strip()
    if kind == "email":
        local, _, domain = text.rpartition("@")
        return f"{local}@{domain.lower()}"
    if kind == "phone":
        digits = re.sub(r"\D", "", text)
        return f"+{digits}" if text.startswith("+") else digits
    if kind in ("iban", "card"):
        return _compact(text).upper()
    return re.sub(r"\s+", " ", text.upper())
