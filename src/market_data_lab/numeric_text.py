"""Canonical text helpers for Decimal values shared by producers and consumers.

A single canonical representation avoids mismatches when the same logical
notional is formatted differently by different modules (e.g. ``Decimal("1.00")``
vs. ``Decimal("1")``) but used as a mapping key or identity component.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation


def canonical_decimal_text(value: Decimal) -> str:
    """Return a fixed-point canonical text form of a finite Decimal.

    - Uses fixed-point notation (never scientific).
    - Trims trailing fractional zeros and a dangling decimal point.
    - Normalizes ``-0`` / ``-0.0`` to ``0``.
    - Raises ``ValueError`` for non-finite (NaN, Infinity) inputs.
    """

    if not isinstance(value, Decimal):
        raise TypeError("value must be a Decimal")
    if not value.is_finite():
        raise ValueError("value must be finite (not NaN or Infinity)")
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    # Normalize -0 and -0.0 to 0.
    if text in ("-0", "-0.0"):
        return "0"
    return text or "0"


def parse_canonical_decimal(text: str) -> Decimal:
    """Parse a canonical decimal text back into a Decimal, rejecting junk."""

    if not isinstance(text, str):
        raise TypeError("text must be a string")
    candidate = Decimal(text)
    if not candidate.is_finite():
        raise ValueError("value must be finite (not NaN or Infinity)")
    return candidate


# Backwards-compatible alias used by modules that previously had a private
# ``_decimal_text`` helper with identical fixed-point, trailing-zero-trimming
# semantics.  Modules should prefer ``canonical_decimal_text`` for new code.
def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return canonical_decimal_text(value)
