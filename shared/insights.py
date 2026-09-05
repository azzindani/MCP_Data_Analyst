"""The findings, beside the matrix that contains them.

A user review's roadmap line: *`insights.json` beside every matrix (correlation,
cross-tab, outliers, EDA)*. It sits in the composability section rather than the
truth one, and the distinction matters: nothing these tools return is wrong. A
correlation matrix of 24 columns is 576 correct numbers, and an agent that wants
to know "is anything redundant here" has to read all of them and decide what
0.9936 means.

That is the whole gap. `id` and `member_id` correlated at 0.9936 in the review's
file. The matrix said so. Nothing in the response said *these two columns are
the same column twice, drop one before modelling* -- so the finding existed only
if the reader already knew to look for it, which is exactly the knowledge an
agent does not have.

**An insight is a claim with its evidence attached, not a restatement.** Each
one carries what was measured, the threshold it crossed, and the next call that
acts on it. `severity` is the same three-level vocabulary the alerts use, so a
reader does not learn a second scale.

**Written as a sidecar, and that is deliberate here.** The rule that artifacts
must stand alone applies to *deliverables* -- an HTML page that renders into an
empty box without its sibling is broken. `insights.json` is not a deliverable;
it is a second answer to the same call, and both the file path and the insights
themselves are returned in the response, so a caller who never opens the file
still has them. It exists on disk for the later pass that has the matrix and
wants the reading of it.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

# The same three levels the data-quality alerts use. A reader should not have to
# learn a second scale to combine them.
SEVERITIES: tuple[str, ...] = ("high", "medium", "low")

# Two columns above this are, for modelling purposes, one column twice.
REDUNDANT_R = 0.95

# Below this a correlation is not worth a caller's attention on its own.
NOTABLE_R = 0.70

# A categorical column with more distinct values than this share of the rows is
# an identifier wearing a category's clothes.
IDENTIFIER_RATIO = 0.9

_ORDER = {"high": 0, "medium": 1, "low": 2}


def insight(
    kind: str,
    severity: str,
    headline: str,
    *,
    evidence: dict[str, Any] | None = None,
    columns: list[str] | None = None,
    suggested_next: str = "",
) -> dict[str, Any]:
    """One finding. `headline` is the sentence; `evidence` is why it is true."""
    if severity not in SEVERITIES:
        raise ValueError(f"severity must be one of {', '.join(SEVERITIES)}; got {severity!r}")
    out: dict[str, Any] = {"kind": kind, "severity": severity, "headline": headline}
    if columns:
        out["columns"] = list(columns)
    if evidence:
        out["evidence"] = evidence
    if suggested_next:
        out["suggested_next"] = suggested_next
    return out


def rank(insights: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Worst first, so a caller reading only the head reads the worst."""
    return sorted(insights, key=lambda i: _ORDER.get(i.get("severity", "low"), 3))


# ---------------------------------------------------------------------------
# per-matrix readers
# ---------------------------------------------------------------------------


def from_correlations(pairs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """What a correlation matrix is trying to tell someone who reads it.

    `pairs` are the `{col_a, col_b, correlation}` dicts these tools already
    build, so this adds a reading rather than a second computation.
    """
    found: list[dict[str, Any]] = []
    for p in pairs:
        r = p.get("correlation")
        if r is None:
            continue
        a, b, mag = p.get("col_a"), p.get("col_b"), abs(float(r))
        if mag >= REDUNDANT_R:
            found.append(
                insight(
                    "redundant_pair",
                    "high",
                    f"'{a}' and '{b}' are {mag:.4f} correlated -- for modelling they are one column twice.",
                    evidence={"correlation": round(float(r), 4), "threshold": REDUNDANT_R},
                    columns=[a, b],
                    suggested_next=f"Drop one of them, or check whether '{a}' and '{b}' are the same field under two names.",
                )
            )
        elif mag >= NOTABLE_R:
            found.append(
                insight(
                    "strong_pair",
                    "medium",
                    f"'{a}' and '{b}' move together at {float(r):+.4f}.",
                    evidence={"correlation": round(float(r), 4), "threshold": NOTABLE_R},
                    columns=[a, b],
                    suggested_next="Worth a scatter before treating them as independent inputs.",
                )
            )
    return rank(found)


def from_alerts(alerts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Translate the existing data-quality alerts into the same shape.

    The alerts are already computed and already right. This gives them the
    `suggested_next` they lacked, and puts them on one severity scale with
    everything else in the file.
    """
    mapping = {"error": "high", "warning": "medium", "info": "low"}
    advice = {
        "ALL NULL": "Drop the column, or find out why it was never populated.",
        "CONSTANT": "Drop it -- a single value carries no signal and costs a column in every model.",
        "HIGH CARDINALITY": "Group it, hash it, or exclude it before encoding; one column per value is not a feature.",
        "DUPLICATES": "Call apply_patch() with a drop_duplicates op, or confirm the repeats are real events.",
    }
    found: list[dict[str, Any]] = []
    for a in alerts:
        kind = str(a.get("type", "alert")).lower().replace(" ", "_")
        sev = mapping.get(str(a.get("sev", "info")), "low")
        col = a.get("col")
        found.append(
            insight(
                kind,
                sev,
                str(a.get("msg", "")),
                columns=[col] if col else None,
                suggested_next=advice.get(str(a.get("type", "")), ""),
            )
        )
    return rank(found)


def from_outliers(outlier_cols: list[dict[str, Any]], rows: int) -> list[dict[str, Any]]:
    """Which columns the outlier scan actually found something in."""
    found: list[dict[str, Any]] = []
    for o in outlier_cols:
        count = int(o.get("outlier_count", 0) or 0)
        if not count or not rows:
            continue
        pct = count / rows * 100
        sev = "high" if pct >= 10 else "medium" if pct >= 1 else "low"
        found.append(
            insight(
                "outlier_column",
                sev,
                f"'{o.get('column')}' has {count:,} values outside its IQR fence ({pct:.2f}% of rows).",
                evidence={
                    "outlier_count": count,
                    "outlier_pct": round(pct, 2),
                    "lower_limit": o.get("lower_limit"),
                    "upper_limit": o.get("upper_limit"),
                },
                columns=[o.get("column")],
                suggested_next="detect_anomalies() writes the flagged rows with a reason each.",
            )
        )
    return rank(found)


def from_crosstab(table: dict[str, Any], row_column: str, col_column: str) -> list[dict[str, Any]]:
    """Cells that are far from what independence would predict.

    A cross-tab is a table of counts. The question a reader has is "which
    combination is unusual", and answering it means comparing each cell against
    the product of its margins -- arithmetic the table contains but does not do.
    """
    rows_ = {k: v for k, v in table.items() if isinstance(v, dict)}
    if not rows_:
        return []
    col_names: list[str] = []
    for cells in rows_.values():
        for c in cells:
            if c not in col_names:
                col_names.append(c)

    def _num(v: Any) -> float:
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    total = sum(_num(v) for cells in rows_.values() for v in cells.values())
    if total <= 0:
        return []
    row_tot = {r: sum(_num(v) for v in cells.values()) for r, cells in rows_.items()}
    col_tot = {c: sum(_num(cells.get(c, 0)) for cells in rows_.values()) for c in col_names}

    found: list[dict[str, Any]] = []
    for r, cells in rows_.items():
        for c in col_names:
            observed = _num(cells.get(c, 0))
            expected = row_tot[r] * col_tot[c] / total
            if expected < 5:
                continue  # too small for the comparison to mean anything
            ratio = observed / expected
            if ratio >= 2.0 or ratio <= 0.5:
                direction = "over" if ratio >= 2.0 else "under"
                found.append(
                    insight(
                        "crosstab_cell",
                        "medium" if 0.33 < ratio < 3.0 else "high",
                        f"{row_column}={r!r} with {col_column}={c!r} is {ratio:.1f}x {direction}-represented "
                        f"({observed:,.0f} observed, {expected:,.0f} expected if independent).",
                        evidence={"observed": observed, "expected": round(expected, 1), "ratio": round(ratio, 2)},
                        columns=[row_column, col_column],
                        suggested_next="statistical_tests() with chi_square tests whether the whole table is independent.",
                    )
                )
    return rank(found)[:20]


# ---------------------------------------------------------------------------
# the sidecar
# ---------------------------------------------------------------------------


def insights_path(artifact_path: str | Path) -> Path:
    """`x_correlation.html` -> `x_correlation_insights.json`, beside it."""
    p = Path(artifact_path)
    return p.with_name(f"{p.stem}_insights.json")


def write_insights(
    artifact_path: str | Path,
    insights: list[dict[str, Any]],
    *,
    op: str,
    source: str = "",
    extra: dict[str, Any] | None = None,
) -> str:
    """Write the sidecar and return its path. Empty string on failure.

    Never raises: a report that exists is worth more than a report that was
    abandoned because its sidecar could not be written.
    """
    try:
        out = insights_path(artifact_path)
        payload: dict[str, Any] = {
            "op": op,
            "source": source,
            "insight_count": len(insights),
            "counts_by_severity": {s: sum(1 for i in insights if i.get("severity") == s) for s in SEVERITIES},
            "insights": rank(insights),
        }
        if extra:
            payload.update(extra)
        out.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        return str(out.resolve())
    except Exception:
        return ""
