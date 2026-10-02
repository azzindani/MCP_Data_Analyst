"""inspect_dataset and read_column_stats for a table too big to load: the same fields, read in chunks.

`shared/big_table.py` says whether a file is too big and answers the questions a pass at a time. These
two functions put those answers in the shape the pandas path gives, so a caller reads one response
whichever way the file was read. The pandas path stays the one for every file that fits; a test holds
the two to the same numbers.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from shared.big_table import BigTable
from shared.column_utils import date_like


def _label(kind: str, nulls: int, rows: int) -> str:
    """The dtype label the pandas path prints: dates and booleans are `object` there, as they are read, a
    column of whole numbers with a gap in it is float64 (NaN is a float), and so is a column with nothing in it."""
    if nulls == rows:
        return "float64"
    if kind == "integer":
        return "float64" if nulls else "int64"
    return "float64" if kind == "float" else "object"


def _stripped(table: BigTable) -> dict[str, str]:
    """{name as the pandas path shows it: name in the file}; pandas strips a header's surrounding spaces."""
    return {name.strip(): name for name in table.names()}


def inspect_stats(table: BigTable) -> dict[str, Any]:
    """The fields of `_inspect_df`, from a pass over the file per few columns."""
    shown = _stripped(table)
    kinds = {c["name"]: c["kind"] for c in table.columns()}
    counts = table.counts()
    rows = table.rows()
    numeric: list[str] = []
    categorical: list[str] = []
    datetimes: list[str] = []
    for name, raw in shown.items():
        kind = "float" if counts[raw]["non_null"] == 0 else kinds[raw]  # nothing in it: pandas reads float64
        if kind == "datetime" or (kind == "text" and date_like(pd.Series(table.sample(raw)))):
            datetimes.append(name)
        elif kind in ("integer", "float", "boolean"):
            numeric.append(name)
        else:
            categorical.append(name)
    null_counts = {name: rows - counts[raw]["non_null"] for name, raw in shown.items()}
    return {
        "rows": rows,
        "columns": len(shown),
        "column_names": list(shown),
        "dtypes": {name: _label(kinds[raw], null_counts[name], rows) for name, raw in shown.items()},
        "null_counts": null_counts,
        "null_pct": {n: round(c / rows * 100, 2) if rows > 0 else 0.0 for n, c in null_counts.items()},
        "unique_counts": {name: counts[raw]["distinct"] for name, raw in shown.items()},
        "numeric_columns": numeric,
        "categorical_columns": categorical,
        "datetime_columns": datetimes,
    }


def sample_rows(table: BigTable, n: int = 2) -> list[dict[str, Any]]:
    """The first rows, an empty cell as "" as the pandas path writes it."""
    strip = {raw: name for name, raw in _stripped(table).items()}
    return [{strip.get(k, k): ("" if v is None else v) for k, v in row.items()} for row in table.head(n)]


def column_stats(table: BigTable, column: str) -> dict[str, Any] | None:
    """The fields of `_stats_for_series` for one column, or None when the file has no such column."""
    raw = _stripped(table).get(column)
    if raw is None:
        return None
    kind = next(c["kind"] for c in table.columns() if c["name"] == raw)
    rows = table.rows()
    counted = table.counts([raw])[raw]
    count, distinct = counted["non_null"], counted["distinct"]
    nulls = rows - count
    out: dict[str, Any] = {
        "column": column,
        "dtype": _label(kind, nulls, rows),
        "count": count,
        "null_count": nulls,
        "null_pct": round(nulls / rows * 100, 2) if rows > 0 else 0.0,
    }
    if count == 0:  # pandas reads a column with nothing in it as float64, so it takes the numeric branch
        kind = "float"
    if kind not in ("integer", "float"):
        top = table.top_values(raw)
        if kind == "boolean":  # DuckDB writes true / false; pandas, and so every other answer here, True / False
            top = {k.capitalize(): v for k, v in top.items()}
        out.update({"unique_count": distinct, "top_values": top})
        return out
    s = (
        table.numeric(raw)
        if count
        else dict.fromkeys(("mean", "std", "lo", "hi", "q1", "median", "q3"), None)
        | {"zeros": 0, "non_finite": 0, "outliers_iqr": 0, "outliers_std": 0}
    )

    def r(value: Any) -> Any:
        return round(float(value), 4) if value is not None else None

    non_finite = int(s["non_finite"])
    q1, q3 = s["q1"], s["q3"]
    out.update(
        {
            "zero_count": int(s["zeros"]),
            "non_finite_count": non_finite,
            "not_computed": (
                f"{non_finite} value(s) are infinite, so mean, std and max are null here. "
                "median, min and the quartiles rank rather than sum and are unaffected."
                if non_finite
                else ""
            ),
            "mean": r(s["mean"]),
            "median": r(s["median"]),
            "std": r(s["std"]) if count > 1 else None,
            "min": r(s["lo"]),
            "max": r(s["hi"]),
            "q1": r(q1),
            "q3": r(q3),
            "iqr": r(q3 - q1) if q1 is not None and q3 is not None else None,
            "outlier_count_iqr": s["outliers_iqr"],
            "outlier_count_std": s["outliers_std"],
            "unique_count": distinct,
            "top_values": table.top_values(raw),
        }
    )
    return out
