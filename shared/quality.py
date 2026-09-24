"""One quality score, with its parts shown.

Two servers scored the same file and disagreed. A user review recorded
`run_eda` returning 77 and `check_data_quality` returning 53 for one dataset,
both flagging the same four issues -- a constant column, a 28,000-value
identifier, income skew of 31.07, and a pair of columns correlated at 0.9936.
Nothing in either response said which denominator it had used, so an agent
handing one number to the next step could not reconcile them, and a reader had
no way to tell which was the pessimistic one.

Neither formula was wrong on its own terms. They were different terms:

    MCP_Data_Analyst   penalty = null_pct*2 + dup_pct*0.5 + 8|3 per alert
                       alert term uncapped, severities "error"/"warning"
    MCP_Machine_Learning   deductions = min(alerts,70) + min(miss*0.5,20)
                                      + min(dup*0.3,10)
                       severities "high"/"medium"/"low"

Both docstrings already recorded a disagreement with a sibling report -- one
says "the dashboard said 41, the EDA report said 98", the other "the sibling
report in MCP_Data_Analyst scored the same file 41". Each was fixed locally,
twice, and the two never converged. That is the shape this module ends.

**The score is now a breakdown, not a scalar.** A single number cannot say
whether 53 means "half the values are missing" or "one column is constant",
and those call for opposite next actions. Components are scored independently,
published with their weights, and the composite is their weighted mean.

`drift` is deliberately reported as `None` rather than invented. It needs a
baseline to compare against, which a single-file profile does not have; it
becomes computable when profilers take `compare_to`. Reporting a fourth
component as 100 because nothing was measured would be exactly the class of
falsehood this module exists to stop.
"""

from __future__ import annotations

from typing import Any

# Severity arrives under two key names and two vocabularies, because the two
# repos grew their alert dicts independently. Both are read; neither wins.
_SEVERITY_KEYS: tuple[str, ...] = ("sev", "severity")

_SEVERITY_ALIASES: dict[str, str] = {
    "error": "high",
    "critical": "high",
    "high": "high",
    "warning": "medium",
    "warn": "medium",
    "medium": "medium",
    "info": "low",
    "notice": "low",
    "low": "low",
}

# What one rule alert costs the `validity` component. Constant columns are
# priced by their share of the file instead, and distribution advice not at
# all (below).
#
# First calibrated against MCP_Machine_Learning's tests, which wanted an
# otherwise-clean frame with one constant column in 60-85. With validity
# weighted 0.40, a high alert costing half the component puts such a frame at
# 80, and three of them still leave the composite off the floor.
_SEVERITY_COST: dict[str, float] = {"high": 50.0, "medium": 25.0, "low": 10.0}

# How many points of missingness or duplication a percent costs.
_NULL_COST_PER_PCT = 2.0
_DUP_COST_PER_PCT = 2.0

# Published, because a score whose weights are private is a score a caller
# cannot argue with. They sum to 1 over the three measurable components.
WEIGHTS: dict[str, float] = {
    "completeness": 0.35,
    "validity": 0.40,
    "uniqueness": 0.25,
}

COMPONENTS: tuple[str, ...] = ("completeness", "validity", "uniqueness", "drift")

# Alerts about a fact another component already prices. Duplicate rows cost
# `uniqueness` through dup_pct and missing values cost `completeness` through
# null_pct; charging their alerts to `validity` as well priced one fact twice --
# Ad_Data.csv's 205 duplicate rows cost both components. A column with no value
# at all is missing values too. They stay in the alert list and in
# alert_counts; they do not cost validity.
_PRICED_ELSEWHERE: frozenset[str] = frozenset(
    {
        "duplicate_rows",
        "duplicates",
        "high_missing",
        "high_nulls",
        "missing_values",
        "all_null",
        "all_null_column",
    }
)

# Advice about a column's distribution, not a value that breaks a rule: mostly
# zeros, skewed, outliers, two columns that move together, one category that
# dominates, a great many distinct values. Each is worth reading before a model
# or a chart, and none makes a value wrong -- a revenue column is supposed to be
# skewed. Priced at 25 or 50 apiece they took validity to 0 on ordinary files:
# Ad_Data.csv scored 59.3 in ML and 99 in DA from the same alerts, because the
# two repos raise different advice. They are counted in `not_scored`.
_ADVICE: frozenset[str] = frozenset(
    {
        "zeros",
        "zero_inflated",
        "skewed",
        "extreme_skewness",
        "outliers",
        "high_corr",
        "multicollinearity",
        "imbalanced",
        "class_imbalance",
        "high_cardinality",
    }
)

# A column that holds one value carries nothing, and what that costs depends on
# how much of the file it is: one of three columns is a third of the file, two
# of sixteen an eighth. So a constant column costs validity its share of the
# columns (100 / columns). A flat 50 apiece put a file with two of them at
# validity 0 however wide it was.
_COLUMN_WIDE: frozenset[str] = frozenset({"constant", "constant_column"})

VALIDITY_NOTE = (
    "validity counts what breaks a rule. A constant column costs its share of the columns "
    "(100 / columns each); any other rule alert costs by severity (high 50, medium 25, low 10). "
    "Missing values and duplicate rows are priced by completeness and uniqueness. Distribution "
    "advice (zeros, skew, outliers, correlation, imbalance, cardinality) is reported but not "
    "scored: see not_scored."
)


def _alert_type(alert: dict[str, Any]) -> str:
    return str(alert.get("type", "")).strip().lower().replace(" ", "_")


def severity_of(alert: dict[str, Any]) -> str:
    """Normalised severity for an alert dict from either repo."""
    for key in _SEVERITY_KEYS:
        raw = alert.get(key)
        if raw:
            return _SEVERITY_ALIASES.get(str(raw).strip().lower(), "low")
    return "low"


def is_advice(alert: dict[str, Any]) -> bool:
    """Whether an alert is distribution advice, which the score reports and does not count."""
    return _alert_type(alert) in _ADVICE


def _component(penalty: float) -> float:
    """A component score: 100 down to a floor of 0."""
    return round(max(0.0, 100.0 - max(0.0, penalty)), 1)


def _validity_penalty(alerts: list[dict[str, Any]], columns: int | None) -> float:
    penalty = 0.0
    constant: set[str] = set()
    for a in alerts:
        kind = _alert_type(a)
        if kind in _PRICED_ELSEWHERE or kind in _ADVICE:
            continue
        if kind in _COLUMN_WIDE and columns:
            constant.add(str(a.get("col", a.get("column", len(constant)))))
            continue
        penalty += _SEVERITY_COST[severity_of(a)]
    return penalty + (100.0 * len(constant) / columns if columns else 0.0)


def quality_report(
    null_pct: float,
    dup_pct: float,
    alerts: list[dict[str, Any]] | None = None,
    *,
    columns: int | None = None,
    has_baseline: bool = False,
    drift_pct: float | None = None,
) -> dict[str, Any]:
    """The score, its components, and the weights that combined them.

        {"quality_score": 71.4,
         "components": {"completeness": 92.5, "validity": 55.0,
                        "uniqueness": 100.0, "drift": None},
         "weights": {...},
         "drift_note": "no baseline supplied; pass compare_to to measure drift"}

    `null_pct` and `dup_pct` are percentages, 0-100. `alerts` are the dicts
    either repo already builds; severity is read from `sev` or `severity`.
    `columns` is the frame's width, which a constant column's cost is a share
    of; without it a constant column costs by severity, as any rule alert does.
    """
    alerts = alerts or []

    completeness = _component(float(null_pct) * _NULL_COST_PER_PCT)
    uniqueness = _component(float(dup_pct) * _DUP_COST_PER_PCT)
    validity = _component(_validity_penalty(alerts, columns))

    components: dict[str, float | None] = {
        "completeness": completeness,
        "validity": validity,
        "uniqueness": uniqueness,
        "drift": None,
    }
    note = "no baseline supplied; pass compare_to to measure drift"
    if has_baseline and drift_pct is not None:
        components["drift"] = _component(float(drift_pct))
        note = ""

    # Only the measured components carry weight. A component that was not
    # measured must not quietly raise or lower the headline.
    total_weight = sum(WEIGHTS[c] for c in WEIGHTS)
    score = sum(float(components[c]) * WEIGHTS[c] for c in WEIGHTS) / total_weight

    report: dict[str, Any] = {
        "quality_score": round(score, 1),
        "components": components,
        "weights": dict(WEIGHTS),
        "alert_counts": {
            level: sum(1 for a in alerts if severity_of(a) == level) for level in ("high", "medium", "low")
        },
        "not_scored": {"advice": sum(1 for a in alerts if is_advice(a))},
        "validity_note": VALIDITY_NOTE,
    }
    if note:
        report["drift_note"] = note
    return report


def quality_score(
    null_pct: float, dup_pct: float, alerts: list[dict[str, Any]] | None = None, *, columns: int | None = None
) -> float:
    """Just the headline, for a caller that only shows one number."""
    return quality_report(null_pct, dup_pct, alerts, columns=columns)["quality_score"]
