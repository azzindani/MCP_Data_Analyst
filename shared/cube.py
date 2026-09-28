"""The rows a large dashboard embeds: one cell per combination of the values it groups and filters by.

Every number on the page is computed in the browser from the rows embedded
in it, so a 2-million-row file made a page that would not load, and capping
the rows turned every total into an estimate. Above CUBE_ROWS the page embeds
a cube instead: the rows grouped by every column the page can group or filter
by (a date kept by day), each measure carried as its sum, its count of
values, its minimum and its maximum, and the cell's row count. Sums, counts,
means, minimums, maximums and every metric built from them (a ratio of sums)
come out of a cube exactly, under any filter on its keys. A median or a
distinct count cannot, so a page that needs one keeps its rows. Panels that
plot rows -- a scatter, a histogram, a box -- draw from a sample, and say so.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

CUBE_ROWS = 100_000
SAMPLE_ROWS = 20_000
MAX_KEY_LEVELS = 1000
WORTHWHILE = 0.5  # a cube with more cells than half the rows saves too little to be worth the sample
EXACT = ("sum", "count", "mean", "min", "max")


def inexact(aggs: list[str]) -> list[str]:
    """The aggregates a cube cannot give exactly, from those the page uses."""
    return sorted({a for a in aggs if a and a not in EXACT})


def tree_aggs(tree: dict) -> list[str]:
    """Every aggregate a compiled metric (shared/metrics.py) takes."""
    if "agg" in tree:
        return [tree["agg"]]
    return [*tree_aggs(tree["a"]), *tree_aggs(tree["b"])] if "op" in tree and "b" in tree else tree_aggs(tree["a"]) if "op" in tree else []


def keys_for(df: pd.DataFrame, measures: list[str], named: set[str]) -> tuple[list[str], list[str]]:
    """The columns a cube groups by, and those it leaves out: identifiers and free text, unless a panel names one."""
    keys, dropped = [], []
    for col in df.columns:
        if col in measures and col not in named:
            continue
        if col in named or df[col].nunique(dropna=True) <= MAX_KEY_LEVELS:
            keys.append(col)
        else:
            dropped.append(col)
    return keys, dropped


def build(df: pd.DataFrame, keys: list[str], measures: list[str]) -> pd.DataFrame:
    """One row per combination of `keys`: each measure's sum, count, min and max, and the rows behind it."""
    grouped = df.groupby(keys, dropna=False, sort=False, observed=True)
    parts: dict[str, Any] = {"__n": grouped.size()}
    for m in measures:
        col = grouped[m]
        parts[m] = col.sum(min_count=1)
        parts[f"{m}#n"] = col.count()
        parts[f"{m}#lo"] = col.min()
        parts[f"{m}#hi"] = col.max()
    return pd.DataFrame(parts).reset_index()
