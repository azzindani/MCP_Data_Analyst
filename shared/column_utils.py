"""Column inference utilities shared across all tiers. No MCP imports."""

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
