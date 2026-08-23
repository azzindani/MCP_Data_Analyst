"""Ring-1 pure utility — column inference across all tiers. No I/O, no MCP imports."""

from __future__ import annotations

import re

import pandas as pd

# Keywords that suggest mean is the right aggregation
_AGG_MEAN = frozenset(
    {
        "rate",
        "ratio",
        "pct",
        "percent",
        "percentage",
        "score",
        "avg",
        "average",
        "mean",
        "index",
        "idx",
        "temperature",
        "temp",
        "speed",
        "density",
        "grade",
        "gpa",
        "weight",
        "proportion",
        "fraction",
        "growth",
        "margin",
        "efficiency",
        "utilization",
        "utilisation",
        "yield",
        "conversion",
        "accuracy",
        "precision",
        "recall",
        "f1",
        "satisfaction",
        "rating",
        "probability",
        "prob",
        "likelihood",
    }
)

# Keywords that suggest max
_AGG_MAX = frozenset(
    {
        "max",
        "maximum",
        "peak",
        "high",
        "highest",
        "ceiling",
        "top",
        "upper",
        "limit",
        "cap",
        "best",
    }
)

# Keywords that suggest min
_AGG_MIN = frozenset(
    {
        "min",
        "minimum",
        "low",
        "lowest",
        "floor",
        "bottom",
        "base",
        "lower",
        "worst",
    }
)


def is_numeric_col(series: pd.Series) -> bool:
    """True for numeric columns excluding boolean dtype.

    pd.api.types.is_numeric_dtype returns True for bool, which causes
    numpy boolean subtract errors in corr/std/skew/quantile operations.
    """
    return pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series)


def infer_agg(col: str, series: pd.Series | None = None) -> str:
    """
    Infer the best aggregation function for a numeric column.

    Returns one of: "sum", "mean", "max", "min".

    Priority order:
    1. Name-keyword match (most reliable)
    2. Distribution heuristic: values in [0, 1] → mean (likely a rate/ratio)
    3. Default: sum
    """
    lower = col.lower()
    words = set(re.split(r"[^a-zA-Z]+", lower))
    words.discard("")

    if words & _AGG_MEAN or any(k in lower for k in _AGG_MEAN):
        return "mean"
    if words & _AGG_MAX or any(k in lower for k in _AGG_MAX):
        return "max"
    if words & _AGG_MIN or any(k in lower for k in _AGG_MIN):
        return "min"

    # Distribution heuristic: proportion/rate columns sit in [0, 1]
    if series is not None:
        try:
            valid = series.dropna()
            if len(valid) > 0:
                mn, mx = float(valid.min()), float(valid.max())
                if mn >= 0.0 and mx <= 1.0:
                    return "mean"
        except Exception:
            pass

    return "sum"


def agg_label(agg: str) -> str:
    """Human-readable label prefix for an aggregation function."""
    return {"sum": "Total", "mean": "Avg", "max": "Max", "min": "Min"}.get(agg, "Total")


def parse_agg_overrides(overrides: list[str] | None) -> dict[str, str]:
    """
    Parse a list of "column:agg" strings into a dict.

    Example input: ["revenue:sum", "rate:mean", "temperature:mean"]
    """
    result: dict[str, str] = {}
    if not overrides:
        return result
    valid = {"sum", "mean", "max", "min"}
    for item in overrides:
        if ":" in item:
            col, agg = item.split(":", 1)
            col, agg = col.strip(), agg.strip().lower()
            if agg in valid:
                result[col] = agg
    return result


# ---------------------------------------------------------------------------
# Filter conditions
# ---------------------------------------------------------------------------

# `conditions` is a bare list[dict] in every schema that takes one, so the key
# names live nowhere a caller can read them. `op` has always accepted `operator`
# as an alias; the column key accepted exactly one spelling, and a caller who
# wrote `variable` got "Column '' not found" -- the empty string being the
# tool's own default, quoted back as if the caller had asked for a column named
# "". The same shape as delete_paragraph answering with an index nobody sent.
COLUMN_KEYS: tuple[str, ...] = ("column", "col", "field", "variable", "name", "column_name")


def condition_column(cond: dict) -> str:
    """The column a filter condition names, under any of its spellings."""
    for key in COLUMN_KEYS:
        value = cond.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return ""


def missing_column_error(cond: dict) -> tuple[str, str]:
    """(error, hint) for a condition that names no column at all.

    Kept separate from "names a column that does not exist" because the two
    need different advice and used to share one misleading message.
    """
    keys = ", ".join(str(k) for k in cond) or "none"
    return (
        f"This filter condition names no column. Its keys are: {keys}",
        f'Give the column under "column" — {", ".join(COLUMN_KEYS[1:])} are accepted too. '
        'A condition looks like {"column": "spends", "op": "gt", "value": 0}.',
    )


def paired_numeric(df: pd.DataFrame, col_a: str, col_b: str) -> tuple[pd.Series, pd.Series]:
    """Two numeric columns as aligned pairs, dropping rows either one is null in.

    A paired test compares row i of one column against row i of the other, so
    the two series have to come out of the same rows. Dropping the nulls from
    each column *separately* and then cutting both to the shorter length looks
    equivalent and is not: after the first null, every pair is offset by one,
    and the offset grows with each null after it.

        a = to_numeric(df[col_a]).dropna()
        b = to_numeric(df[col_b]).dropna()
        n = min(len(a), len(b))
        pearsonr(a.iloc[:n], b.iloc[:n])

    On the reference dataset, clicks against link_clicks -- 546 nulls out of
    16,834, the first at row 2,011:

        as written           r = 0.0015   p = 0.847257   "not significant"
        pairwise deletion    r = 0.9256   p < 1e-300     n = 16,288

    A near-perfect correlation reported as no correlation at all, deterministic,
    under success: true. Pairwise deletion is what every stats package means by
    dropping missing values from a paired test.
    """
    pair = pd.DataFrame(
        {
            "a": pd.to_numeric(df[col_a], errors="coerce"),
            "b": pd.to_numeric(df[col_b], errors="coerce"),
        }
    ).dropna()
    return pair["a"], pair["b"]
