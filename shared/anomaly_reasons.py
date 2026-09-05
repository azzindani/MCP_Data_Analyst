"""Why a row was flagged, in words, and what to do about it.

A user review ran `detect_anomalies` over 38,576 rows and got an 8.5 MB CSV
back. 2,793 rows were flagged. The other 35,783 were in the file because the
tool writes every row with a set of boolean flag columns appended, so the
answer to "which rows are anomalous" arrived as a file that is 93% rows that
are not, and the reason each flagged row was flagged existed only as a `True`
under a column name.

Its prescription, from the roadmap: *anomalies-only + reasons*, and from the
detail: *anomalies-only CSV with plain-language `reason` + `suggested_fix`,
plus a full scored file*. Both files, because they answer different questions
-- the small one is what an agent reads, the big one is what a later pass
re-scores against.

The wording matters more than it looks. "iqr_flag: True" tells a reader that a
rule fired. "income 250,000 is above the IQR upper limit of 125,000" tells them
what to check, and carries the number they would otherwise call another tool to
find. A `suggested_fix` that says "drop the row" would be worse than nothing --
an outlier is not automatically an error, and the tool has no way to know which
this is -- so it says what to verify and names the two ordinary outcomes.
"""

from __future__ import annotations

from typing import Any


def _fmt(value: Any) -> str:
    """A number a person can read, without inventing precision."""
    if value is None:
        return "?"
    try:
        f = float(value)
    except (TypeError, ValueError):
        return str(value)
    if f == int(f) and abs(f) < 1e15:
        return f"{int(f):,}"
    return f"{f:,.4g}"


def row_reason(hits: list[dict[str, Any]]) -> str:
    """One sentence naming every rule that fired on this row.

    `hits` are `{"column", "method", "value", "limit", "side"}` dicts, where
    `side` is "above" or "below" for IQR and "" for z-score.
    """
    if not hits:
        return ""
    parts = []
    for h in hits:
        col = h["column"]
        value = _fmt(h.get("value"))
        if h["method"] == "iqr":
            side = h.get("side") or "outside"
            parts.append(f"{col} {value} is {side} the IQR {'upper' if side == 'above' else 'lower'} limit of {_fmt(h.get('limit'))}")
        else:
            parts.append(f"{col} {value} is {_fmt(h.get('limit'))} standard deviations from the mean")
    return "; ".join(parts)


def row_fix(hits: list[dict[str, Any]]) -> str:
    """What to do next, without pretending to know whether it is an error.

    Deliberately not "drop the row". An outlier is a value that is unusual, not
    a value that is wrong, and this tool cannot tell the difference. Naming the
    two ordinary outcomes is the honest advice, with the column to look at.
    """
    if not hits:
        return ""
    columns = sorted({h["column"] for h in hits})
    named = ", ".join(columns)
    if len(columns) == 1:
        return (
            f"Check {named} against the source. If the value is real, keep it and consider a "
            "log transform or a winsorised copy for modelling; if it is a data-entry or unit "
            "error, correct it with apply_patch()."
        )
    return (
        f"{len(columns)} columns flagged this row ({named}), which more often means the row "
        "itself is unusual than that any one value is wrong. Check the source record before "
        "changing anything; run_cleaning_pipeline() can quarantine rows rather than edit them."
    )


def collect_hits(row, per_column: dict[str, dict], threshold: float) -> list[dict[str, Any]]:
    """Which rules fired on one row, read off the flag columns beside it.

    Reads the same `_iqr_flag` / `_zscore_flag` columns the scored file
    carries, so the small file and the big one cannot disagree about which rows
    are anomalous.
    """
    hits: list[dict[str, Any]] = []
    for col, summary in per_column.items():
        value = row.get(col)
        if row.get(f"{col}_iqr_flag"):
            upper = summary.get("iqr_upper")
            lower = summary.get("iqr_lower")
            side, limit = "above", upper
            try:
                if lower is not None and value is not None and float(value) < float(lower):
                    side, limit = "below", lower
            except (TypeError, ValueError):
                pass
            hits.append({"column": col, "method": "iqr", "value": value, "limit": limit, "side": side})
        if row.get(f"{col}_zscore_flag"):
            hits.append(
                {"column": col, "method": "zscore", "value": value, "limit": threshold, "side": ""}
            )
    return hits
