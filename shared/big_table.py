"""What a first look at a table says, for a table too big to load.

`inspect_dataset` and a column's statistics read the whole file into pandas. A 2 GB CSV is a few
gigabytes in a frame, and a container with 1 GB is killed long before the answer. These questions do
not need the table in memory: a count, a distinct count, a mean, a quartile are each one pass over
the file, and DuckDB makes that pass in chunks inside a memory limit and spills what it cannot hold.

Every query goes through `run_query` (shared/sql_query.py), so the engine is the locked-down one: it
reads only the file it is given, with no network. A tool asks `reason()` first and keeps its own
pandas path when it returns "" -- the file fits, and the answer stays exactly the one it always was.
`MCP_BIG_TABLE` forces it: `always` (tests, and a caller who wants the chunked answer) or `never`.
"""

from __future__ import annotations

import math
import os
from pathlib import Path
from typing import Any

from shared.isolation import memory_budget_mb, worker_threads
from shared.sql_query import MAX_PREVIEW, run_query

# What loading a file whole costs in memory, as a multiple of its size on disk: a CSV's frame is two to
# three times the file and the parse and the tool's own copies take the rest. Only the text tables the
# first-look tools read are here; they do not read Parquet or JSON at all.
LOAD_FACTORS = {".csv": 6, ".tsv": 6, ".txt": 6}
# Columns counted in one pass over the file: a distinct count holds its values in a hash table, so a
# few hundred of them at once is the memory a small container does not have.
COLUMNS_PER_PASS = 12

_INTEGERS = ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT")


def _mode() -> str:
    chosen = os.environ.get("MCP_BIG_TABLE", "auto").strip().lower()
    return chosen if chosen in ("always", "never") else "auto"


def reason(path: Path) -> str:
    """Why this file is read in chunks, in a sentence for the response; "" when it is loaded whole."""
    mode = _mode()
    suffix = path.suffix.lower()
    if mode == "never" or suffix not in LOAD_FACTORS:
        return ""
    try:
        size = path.stat().st_size
    except OSError:
        return ""
    if mode == "always":
        return f"{size / 1048576:,.0f} MB file, read in chunks by DuckDB because MCP_BIG_TABLE=always."
    budget = memory_budget_mb(0)  # 0: no container limit is known, so nothing says it will not fit
    need = size * LOAD_FACTORS[suffix] / 1048576
    if budget and need > budget:
        return (
            f"{size / 1048576:,.0f} MB file: loaded whole it needs about {need:,.0f} MB and this call may hold "
            f"{budget:,} MB, so it is read in chunks by DuckDB instead of loaded."
        )
    return ""


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def kind_of(sql_type: str) -> str:
    """integer, float, boolean, datetime or text: what DuckDB's type for a column means to a caller."""
    upper = sql_type.upper()
    if upper in _INTEGERS:
        return "integer"
    if upper.startswith(("DECIMAL", "DOUBLE", "FLOAT", "REAL")):
        return "float"
    if upper == "BOOLEAN":
        return "boolean"
    if upper.startswith(("DATE", "TIMESTAMP", "TIME")):
        return "datetime"
    return "text"


def _number(value: Any) -> Any:
    """A count that came back as a float (DuckDB sums into HUGEINT, which crosses as one) is an int."""
    if isinstance(value, float) and math.isfinite(value) and value == int(value):
        return int(value)
    return value


class BigTable:
    """One file, asked questions one pass at a time."""

    def __init__(self, path: Path, memory_mb: int | None = None) -> None:
        self.path = path
        self.memory_mb = memory_mb or memory_budget_mb()
        self._schema: list[dict[str, str]] | None = None
        self._rows: int | None = None

    def _ask(self, sql: str, limit: int = MAX_PREVIEW) -> list[dict[str, Any]]:
        answer = run_query(
            sql,
            tables={"data": self.path},
            preview_rows=limit,
            memory_mb=self.memory_mb,
            threads=worker_threads(),
        )
        return answer["rows"]

    def columns(self) -> list[dict[str, str]]:
        """Each column's name, DuckDB type and kind, in file order."""
        if self._schema is None:
            rows = self._ask("SELECT column_name, column_type FROM (DESCRIBE data)", 100_000)
            self._schema = [
                {"name": r["column_name"], "sql_type": r["column_type"], "kind": kind_of(r["column_type"])}
                for r in rows
            ]
        return self._schema

    def names(self) -> list[str]:
        return [c["name"] for c in self.columns()]

    def rows(self) -> int:
        if self._rows is None:
            self._rows = int(self._ask("SELECT count(*) AS n FROM data")[0]["n"])
        return self._rows

    def counts(self, names: list[str] | None = None) -> dict[str, dict[str, int]]:
        """{column: {"non_null": n, "distinct": d}}, exact, a few columns per pass over the file."""
        wanted = list(names) if names is not None else self.names()
        found: dict[str, dict[str, int]] = {}
        for start in range(0, len(wanted), COLUMNS_PER_PASS):
            batch = wanted[start : start + COLUMNS_PER_PASS]
            parts = [f"count({_q(n)}) AS n{i}, count(DISTINCT {_q(n)}) AS d{i}" for i, n in enumerate(batch)]
            row = self._ask(f"SELECT {', '.join(parts)} FROM data")[0]
            for i, n in enumerate(batch):
                found[n] = {"non_null": int(row[f"n{i}"]), "distinct": int(row[f"d{i}"])}
        return found

    def head(self, n: int = 2) -> list[dict[str, Any]]:
        return self._ask(f"SELECT * FROM data LIMIT {int(n)}", int(n))

    def window(self, start: int, count: int) -> list[dict[str, Any]]:
        """Rows [start, start + count) in file order. An infinity is the text "inf" / "-inf", not a null."""
        floats = [c["name"] for c in self.columns() if c["kind"] == "float"]
        picked = [
            f"CASE WHEN isinf({_q(n)}) THEN NULL ELSE {_q(n)} END AS {_q(n)}" if n in floats else _q(n)
            for n in self.names()
        ]
        signs = [
            f"CASE WHEN isinf({_q(n)}) THEN CAST(sign({_q(n)}) AS INTEGER) END AS {_q('__inf_' + n)}" for n in floats
        ]
        rows = self._ask(
            f"SELECT {', '.join(picked + signs)} FROM data LIMIT {int(count)} OFFSET {int(start)}", int(count)
        )
        for row in rows:
            for n in floats:
                sign = row.pop("__inf_" + n)
                if sign:
                    row[n] = "inf" if sign > 0 else "-inf"
        return rows

    def sample(self, name: str, limit: int = 200) -> list[Any]:
        """A fixed random sample of a column's non-null values: what a text column is judged (date or not) from."""
        rows = self._ask(
            f"SELECT {_q(name)} AS v FROM data WHERE {_q(name)} IS NOT NULL "
            f"USING SAMPLE reservoir({int(limit)} ROWS) REPEATABLE (0)",
            limit,
        )
        return [r["v"] for r in rows]

    def top_values(self, name: str, limit: int = 10) -> dict[str, int]:
        """The most frequent values and their counts, most frequent first (ties in value order)."""
        rows = self._ask(
            f"SELECT CAST({_q(name)} AS VARCHAR) AS v, count(*) AS n FROM data WHERE {_q(name)} IS NOT NULL "
            f"GROUP BY {_q(name)} ORDER BY n DESC, v LIMIT {int(limit)}",
            limit,
        )
        return {str(r["v"]): int(r["n"]) for r in rows}

    def extent(self, name: str) -> dict[str, Any]:
        """A column's smallest and largest value, as text, for a column that is not a number."""
        row = self._ask(f"SELECT min({_q(name)}) AS lo, max({_q(name)}) AS hi FROM data")[0]
        return {"min": row["lo"], "max": row["hi"]}

    def distinct_values(self, name: str, limit: int = 3) -> list[Any]:
        """A few of a column's distinct non-null values (a column of two or three says what it holds)."""
        rows = self._ask(
            f"SELECT DISTINCT {_q(name)} AS v FROM data WHERE {_q(name)} IS NOT NULL LIMIT {int(limit)}", limit
        )
        return [r["v"] for r in rows]

    def true_count(self, name: str) -> int:
        """How many non-null values of a two-valued column are true (or 1)."""
        row = self._ask(f"SELECT sum(CAST({_q(name)} AS BIGINT)) AS n FROM data")[0]
        return int(row["n"] or 0)

    def numeric(self, name: str, finite: bool = False) -> dict[str, Any]:
        """Count, mean, sample std, skewness, extremes, quartiles, zeros and infinities of a numeric column,
        then the counts beyond the 1.5 x IQR fences and beyond three standard deviations: what pandas gives,
        in two passes. `finite` leaves infinities out of every statistic, as a profile that reports them
        separately does."""
        c = _q(name)
        keep = f" FILTER (WHERE isfinite({c}))" if finite else ""
        row = self._ask(
            f"SELECT count({c}){keep} AS n, avg({c}){keep} AS mean, stddev_samp({c}){keep} AS std, "
            f"min({c}){keep} AS lo, max({c}){keep} AS hi, skewness({c}){keep} AS skew, "
            f"quantile_cont({c}, 0.25){keep} AS q1, quantile_cont({c}, 0.5){keep} AS median, "
            f"quantile_cont({c}, 0.75){keep} AS q3, "
            f"count(*) FILTER (WHERE {c} = 0) AS zeros, count(*) FILTER (WHERE isinf({c})) AS non_finite FROM data"
        )[0]
        stats = {k: _number(v) for k, v in row.items()}
        if stats["n"] and stats["n"] >= 3 and stats["lo"] == stats["hi"]:
            # A constant column has no skew: pandas says 0.0, DuckDB says NaN or, on some CPUs, rounding noise.
            stats["skew"] = 0.0
        q1, q3, mean, std = stats["q1"], stats["q3"], stats["mean"], stats["std"]
        fences: list[str] = []
        if q1 is not None and q3 is not None and math.isfinite(q1) and math.isfinite(q3):
            spread = q3 - q1
            fences.append(f"count(*) FILTER (WHERE {c} < {q1 - 1.5 * spread!r} OR {c} > {q3 + 1.5 * spread!r}) AS iqr")
        if mean is not None and std is not None and math.isfinite(mean) and math.isfinite(std):
            fences.append(f"count(*) FILTER (WHERE {c} < {mean - 3 * std!r} OR {c} > {mean + 3 * std!r}) AS sd3")
        beyond = self._ask(f"SELECT {', '.join(fences)} FROM data")[0] if fences else {}
        stats["outliers_iqr"] = int(beyond.get("iqr", 0) or 0)
        stats["outliers_std"] = int(beyond.get("sd3", 0) or 0)
        return stats
