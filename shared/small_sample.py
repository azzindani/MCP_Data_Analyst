"""Conclusions a sample is too small to support.

A statistical test that cannot run does not return an error. It returns NaN,
and `float("nan") < 0.05` is `False`, so the natural way to write a verdict --

    "significant": float(pval) < 0.05,
    "interpretation": "..." if float(pval) < 0.05 else "No significant difference (p>=0.05)"

-- turns a test that never ran into a confident negative. A sweep that handed
every tool in the fleet a valid one-row CSV got exactly that from
`statistical_tests`: `statistic: null, p_value: null` beside
`significant: false` and "No significant difference (p>=0.05)". The two null
fields are honest and the sentence contradicts them, and a caller reading the
sentence learns the opposite of the truth. Absence of evidence, sold as
evidence of absence.

The same shape recurs wherever a number becomes English:

  * a skewness that is NaN at n=1 falls through every `> 0.5` / `< -0.5`
    comparison to the `else` branch and is labelled "approximately symmetric";
  * `bool(normality_p >= 0.05)` on a NaN says the residuals are not normal;
  * an outlier scan whose IQR is zero reports "0 outliers", which was never a
    measurement -- no value can fall outside a bound it sits on.

So the helpers here do two jobs. `need_n` refuses up front, naming the test and
the sample it got, which is what a caller asking for one specific test wants.
The rest are backstops for the places where a number still arrives undefined:
they yield None rather than a default, because a missing verdict reads as
missing, while a defaulted one reads as a finding.
"""

from __future__ import annotations

import math
from typing import Any

# --- how small is too small -------------------------------------------------

# Below four values the 1.5*IQR fence cannot flag anything, whatever the data.
# With n <= 3 pandas interpolates q1 and q3 between the same two order
# statistics, so the fence always lands outside the sample: for [0, a, M] the
# upper fence is (a+M)/2 + 0.75*M, which exceeds M for every a < M. n=4 is the
# first size where a point can sit outside -- [0, 0, 0, 100] gives an upper
# fence of 62.5 and flags the 100.
MIN_N_IQR = 4

# Shapiro-Wilk is undefined below three values. scipy does not merely return
# NaN there: on the 1-element case it raises `'float' object has no attribute
# 'dtype'` from inside its own NaN-policy wrapper, which surfaced as
# regression_analysis's error message with nothing about sample size in it.
MIN_N_SHAPIRO = 3

# Any two points lie on a line, so a correlation over fewer than three pairs is
# +-1 by construction however unrelated the columns are -- a property of the
# count, not of the data. Applies equally to a chart that depicts a
# relationship: a scatter matrix or a correlation heatmap drawn from one row is
# a picture of nothing, and renders as a blank or zero-valued grid that reads
# like a measured absence of correlation.
MIN_N_CORRELATION = 3


def min_n_for_zscore(threshold: float = 3.0) -> int:
    """Smallest n where some point *could* exceed `threshold` standard deviations.

    With the sample standard deviation (ddof=1) the largest z-score attainable
    by any of n points is (n-1)/sqrt(n), regardless of the values. For the
    usual 3-sigma rule that first exceeds 3 at n=11, so a 3-sigma scan over ten
    rows or fewer is guaranteed to report zero outliers -- not because the data
    are clean, but because the arithmetic cannot say otherwise.
    """
    if threshold <= 0:
        return 2
    # (n-1)/sqrt(n) > t  <=>  u**2 - t*u - 1 > 0 for u = sqrt(n).
    u = (threshold + math.sqrt(threshold * threshold + 4.0)) / 2.0
    return int(math.floor(u * u)) + 1


# --- reporting numbers that may not exist -----------------------------------


def finite(value: Any) -> float | None:
    """A float for reporting, or None if it is NaN or infinite.

    `round(float("nan"), 4)` is still NaN, and json.dumps writes it as the bare
    token `NaN`, which is not valid JSON and which several clients read as a
    number anyway.
    """
    try:
        number = float(value)
    except TypeError, ValueError:
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def rounded(value: Any, digits: int = 4) -> float | None:
    """`finite`, then rounded. None stays None."""
    number = finite(value)
    return None if number is None else round(number, digits)


def is_missing(value: Any) -> bool:
    """True for None, NaN and NaT -- a scalar that is not a value.

    `finite()` cannot stand in for this where the value may legitimately be
    text: the mode of a string column is a string, and float("West") raises.
    NaN is the only thing not equal to itself, which covers numpy scalars and
    pandas' NaT without importing either.
    """
    if value is None:
        return True
    try:
        return bool(value != value)
    except Exception:  # noqa: BLE001 - an array compares elementwise; not a scalar, not missing
        return False


def is_significant(p: Any, alpha: float = 0.05) -> bool | None:
    """True/False at `alpha`, or None when there is no p-value to judge."""
    number = finite(p)
    return None if number is None else bool(number < alpha)


def undetermined_because(reason: str) -> str:
    """The interpretation text for a test that produced no p-value."""
    return (
        f"Undetermined: the test returned no p-value ({reason}). "
        "This is not a negative result -- nothing was measured either way."
    )


def settle_verdict(result: dict, reason: str, *, flag: str = "significant") -> dict:
    """Backstop: strip a verdict from a test result that has no p-value.

    Call it on the assembled per-test dict, after the branch that built it.
    Every branch writes `p_value` through `round_p`, which already yields None
    for NaN, so one check here covers all of them -- including the next test
    someone adds, which is the point of doing it at the choke point rather than
    inside each branch.
    """
    if result.get("p_value") is not None:
        return result
    result[flag] = None
    result["interpretation"] = undetermined_because(reason)
    result["undetermined"] = True
    return result


# --- refusing before the maths ----------------------------------------------


def need_n(
    op: str,
    test: str,
    sizes: dict[str, int],
    minimum: int,
    hint: str = "",
    demand: str = "",
) -> dict | None:
    """Refuse a test whose sample is too small, naming which sample was short.

    `sizes` maps a name the caller will recognise -- "group 1", "complete
    pairs", the column's own name -- to the count it had, so the message says
    which part of the input was short rather than only that something was.
    Returns None when every size is large enough, so callers read as
    `if err := need_n(...): return err`.
    """
    short = {name: n for name, n in sizes.items() if n < minimum}
    if not short:
        return None
    from shared.progress import fail

    detail = ", ".join(f"{name} = {n}" for name, n in short.items())
    error = f"{test} needs {demand or f'at least {minimum} values per sample'}; {detail}."
    return {
        "success": False,
        "op": op,
        "error": error,
        "hint": hint or f"Give {test} a sample of at least {minimum} non-null values, or pick a test that fits n.",
        "progress": [fail(f"Sample too small for {test}", detail)],
        "token_estimate": 30,
    }


def finite_split(values: Any) -> tuple[int, int]:
    """(finite values, non-finite values) in `values`.

    Callers use this to say *why* a test could not run. `shapiro_p` returning
    None has three causes and they need different sentences; without this the
    only message available was "needs at least 3 values", which produced
    "Shapiro-Wilk needs at least 3 residuals, this fit has 16834" -- a sentence
    that contradicts itself in nine words, because the count it printed was the
    one before the non-finite values were dropped.
    """
    try:
        import numpy as np

        array = np.asarray(values, dtype=float)
        good = int(np.isfinite(array).sum())
        return good, int(array.size) - good
    except Exception:  # noqa: BLE001 - a count is never worth an exception
        return 0, 0


def shapiro_p(values: Any, scipy_stats: Any, cap: int = 5000) -> float | None:
    """Shapiro-Wilk p-value, or None when the sample cannot support the test.

    Guards the three ways this goes wrong: fewer than three values (where scipy
    raises from inside its own wrapper rather than returning NaN), a degenerate
    sample where it returns NaN, and a sample containing an infinity.

    The third was found live. `~np.isnan` drops NaN and *keeps* inf, and scipy
    does not raise on inf -- it returns `W=nan, p=1.0`. `finite(1.0)` is 1.0,
    `1.0 > 0.05` is True, and the caller printed "likely normal (Shapiro
    p>1.00)": a p-value that cannot exist, asserting the opposite of the truth.

    Measured on a CTR column, 4 infinities in 16,834 rows (0.02%), from four
    campaign rows with zero impressions:

        as shipped       p = 1.0        -> "likely normal"
        with isfinite    p = 5.359e-65  -> "non-normal"

    Four rows in sixteen thousand decided the verdict. `np.isfinite` is the
    whole fix; `finite_split` above lets the caller say what was dropped.
    """
    try:
        import numpy as np

        array = np.asarray(values, dtype=float)
        array = array[np.isfinite(array)]
        if array.size < MIN_N_SHAPIRO:
            return None
        if array.size > cap:
            rng = np.random.default_rng(42)
            array = rng.choice(array, size=cap, replace=False)
        _stat, p = scipy_stats.shapiro(array)
        return finite(p)
    except Exception:  # noqa: BLE001 - a normality hint is never worth an exception
        return None
