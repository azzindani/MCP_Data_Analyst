"""Report a p-value without flattening it to zero.

Seventeen call sites did `round(float(p), 6)`, so every p below 5e-7 came back
as `0.0`. A coverage sweep running a t-test on the reference dataset got

    "p_value": 0.0

where the real value is about 1.8e-58. A p-value is never exactly zero, so the
number is not merely imprecise -- it is a value the test cannot produce, and it
reads as a failed computation as readily as an extreme result. The gap between
p=1e-8 and p=1e-58 is also real information, and rounding threw it away.

Above 1e-4 the old six-decimal form is kept, because that is the range people
read directly and 0.032451 is friendlier than 0.0325. Below it, three
significant figures preserve the exponent.
"""

from __future__ import annotations

import math


def round_p(p: float | None) -> float | None:
    """Round a p-value for reporting, keeping small ones distinguishable."""
    if p is None:
        return None
    try:
        value = float(p)
    except (TypeError, ValueError):
        return None
    if math.isnan(value):
        return None
    if value >= 1e-4 or value <= 0.0:
        # <= 0 covers a genuine float underflow, where there is nothing left to
        # preserve; it stays 0.0 rather than pretending to a precision the
        # float never had.
        return round(value, 6)
    return float(f"{value:.3g}")


def format_p(p: float | None) -> str:
    """A p-value as text, for interpretation strings."""
    if p is None:
        return "n/a"
    value = float(p)
    if math.isnan(value):
        return "n/a"
    if value >= 1e-4:
        return f"{value:.4f}"
    if value <= 0.0:
        return "0.0"
    return f"{value:.3g}"
