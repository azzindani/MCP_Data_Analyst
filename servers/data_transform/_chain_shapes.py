"""The reshapes a chain's pivot and resample steps make, shared with its pandas export.

The export carries these functions' own source (inspect.getsource), so the
script cannot drift from the chain: there is one definition of what a pivot
or a resample does. They import nothing but numpy and pandas for that reason.

One rule covers every cell a reshape invents: a pivot cell or a period that
no row falls in holds the formula's value over no rows -- a sum or a count of
nothing is 0, a mean of nothing is empty. A group that has rows keeps the
value they give, empty included.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

PERIODS = {"day": "D", "week": "W", "month": "M", "quarter": "Q", "year": "Y"}


def _groups(df, keys):
    """Every group of `keys` in df, as key columns: sorted, a missing value a group of its own."""
    return df.groupby(keys, dropna=False, sort=True).size().reset_index(name="_rows")


def _fill(grid, keys, work, results, empties):
    """`grid` (key columns, one row per group to report) with each result as a column.

    `results` maps a name to an aggregate by `keys` (a Series) or one value.
    A group of the grid that no row of `work` falls in holds `empties[name]`.
    """
    out = grid.merge(_groups(work, keys), on=keys, how="left")
    has = out["_rows"].notna().to_numpy()
    for name, value in results.items():
        if isinstance(value, pd.Series):
            got = out[keys].merge(value.rename("_value").reset_index(), on=keys, how="left")["_value"]
            column = got.where(has, empties[name])
            if pd.api.types.is_integer_dtype(value.dtype) and not column.isna().any():
                column = column.astype(value.dtype)
        else:
            column = pd.Series(np.where(has, value, empties[name]), index=out.index)
        out[name] = column.to_numpy()
    return out.drop(columns="_rows")


def _pivot_wide(df, rows, column, work, value, empty):
    """One row per group of `rows` in df, one column per value of `column` in `work`, each cell `value`."""
    heads = sorted(work[column].unique().tolist())
    row_groups = _groups(df, rows)[rows]
    grid = row_groups.merge(pd.DataFrame({column: heads}), how="cross")
    cells = _fill(grid, [*rows, column], work, {"_cell": value}, {"_cell": empty})["_cell"]
    wide = row_groups.reset_index(drop=True)
    for j, head in enumerate(heads):
        wide[str(head)] = cells.iloc[j :: len(heads)].to_numpy()
    return wide


def _period_starts(dates, every):
    """Each date as the first day of its period (a week starts on Monday)."""
    return dates.dt.to_period(PERIODS[every]).dt.start_time


def _every_period(df, by, date, every):
    """Every period from each group's first to its last, as key columns."""
    code = PERIODS[every]
    parts = []
    groups = df.groupby(by, dropna=False, sort=True) if by else [((), df)]
    for key, group in groups:
        span = pd.period_range(group[date].min(), group[date].max(), freq=code).start_time
        keys = dict(zip(by, key if isinstance(key, tuple) else (key,), strict=True))
        parts.append(pd.DataFrame({**keys, date: span}))
    return pd.concat(parts, ignore_index=True)[[*by, date]]


def _resampled(work, by, date, every, results, empties):
    """One row per group and period, every period between a group's first and last kept."""
    out = _fill(_every_period(work, by, date, every), [*by, date], work, results, empties)
    out[date] = out[date].dt.strftime("%Y-%m-%d")
    return out


def _periods_spanned(df, by, date, every):
    """How many rows a resample makes: each group's periods, first to last."""
    code = PERIODS[every]
    ordinals = df[date].dt.to_period(code).map(lambda p: p.ordinal)
    if not by:
        return int(ordinals.max() - ordinals.min() + 1)
    span = ordinals.groupby([df[c] for c in by], dropna=False).agg(["min", "max"])
    return int((span["max"] - span["min"] + 1).sum())
