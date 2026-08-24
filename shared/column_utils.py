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


# The operand each filter op compares against, and the key(s) it may arrive
# under. Ops absent from this map compare against nothing (is_null, not_null).
#
# An earlier round taught `between`, `isin` and `regex` to name the key they
# were missing, because those three read unusually-named keys (min/max, values,
# pattern) and a caller could not guess them. The ten ops that read plain
# `value` were left alone -- they were not the ones being debugged -- and they
# are the ops everybody uses. So they kept doing this:
#
#     filter_dataset(f, [{"column": "spend", "op": "gt"}])
#     -> error: 'value'
#
# The entire error is one quoted word. And in data-medium's filter_rows, which
# reads the operand with cond.get("value") instead, it was worse than an
# unhelpful error -- there was no error:
#
#     filter_rows(f, [{"column": "region", "op": "equals"}])
#     -> success: true, rows_kept: 0
#
# Every value compared against None, nothing matched, and the tool wrote an
# empty CSV over the caller's filtered output and reported it as a filter that
# worked. A condition missing its operand is not a condition that excludes
# everything; it is a condition nobody finished writing.
FILTER_OPERANDS: dict[str, tuple[str, ...]] = {
    "equals": ("value",),
    "not_equals": ("value",),
    "contains": ("value",),
    "not_contains": ("value",),
    "starts_with": ("value",),
    "ends_with": ("value",),
    "gt": ("value",),
    "lt": ("value",),
    "gte": ("value",),
    "lte": ("value",),
    "isin": ("values", "value"),
    "not_isin": ("values", "value"),
    "regex": ("pattern", "value"),
    "between": ("min", "max", "value"),
    "quantile_between": ("min", "max", "min_q", "max_q", "value"),
    "date_range": ("start", "end"),
}

# Ops whose operand goes through float(); a non-numeric one raised
# "float() argument must be a string or a real number, not 'NoneType'",
# which names neither the column nor the condition it came from.
_NUMERIC_FILTER_OPS: frozenset[str] = frozenset({"gt", "lt", "gte", "lte", "between", "quantile_between"})
_RANGE_FILTER_OPS: frozenset[str] = frozenset({"between", "quantile_between"})


def _is_number(value) -> bool:
    if isinstance(value, bool):
        return False
    try:
        float(value)
    except (TypeError, ValueError):
        return False
    return True


def filter_operand_error(cond: dict, op: str, index: int = -1) -> str:
    """Why this condition cannot be evaluated, or "" if it can.

    Checked before dispatch so a half-written condition is refused by name
    rather than reaching float() or a bare subscript.
    """
    wanted = FILTER_OPERANDS.get(op)
    if not wanted:
        return ""  # is_null / not_null take no operand
    where = f"Condition {index}" if index >= 0 else "This condition"
    column = condition_column(cond) or "?"
    present = [k for k in wanted if cond.get(k) is not None]

    if op in _RANGE_FILTER_OPS:
        pair = [k for k in wanted if k != "value"]
        lo, hi = pair[0], pair[1]
        both = cond.get(lo) is not None and cond.get(hi) is not None
        value = cond.get("value")
        as_pair = isinstance(value, list | tuple) and len(value) == 2
        if not both and not as_pair:
            return (
                f"{where} ('{column}' {op}) needs both '{lo}' and '{hi}'. Its keys are: "
                f"{', '.join(str(k) for k in cond) or 'none'}. "
                f"Write it as {{'column': '{column}', 'op': '{op}', '{lo}': ..., '{hi}': ...}}, "
                "or give 'value' as a two-item list."
            )
        bounds = list(value) if as_pair and not both else [cond.get(lo), cond.get(hi)]
        bad = [b for b in bounds if not _is_number(b)]
        if bad:
            return f"{where} ('{column}' {op}) needs numeric bounds; got {bad!r}."
        return ""

    if op == "date_range":
        if not present:
            return (
                f"{where} ('{column}' date_range) names neither 'start' nor 'end', so it would keep every row. "
                f"Give at least one, as an ISO date."
            )
        return ""

    if not present:
        names = " or ".join(f"'{k}'" for k in wanted)
        return (
            f"{where} ('{column}' {op}) has no {names} to compare against. Its keys are: "
            f"{', '.join(str(k) for k in cond) or 'none'}. "
            f"Write it as {{'column': '{column}', 'op': '{op}', '{wanted[0]}': ...}}."
        )

    if op in _NUMERIC_FILTER_OPS and not _is_number(cond.get(present[0])):
        return f"{where} ('{column}' {op}) needs a number to compare against; got {cond.get(present[0])!r}."

    return ""


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
