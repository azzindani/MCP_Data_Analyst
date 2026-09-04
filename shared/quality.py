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

# What one alert costs the `validity` component.
#
# Calibrated against MCP_Machine_Learning's existing tests, which encode a
# product judgement rather than an arithmetic one: an otherwise-clean frame
# with a single constant column must land in 60-85, not "nearly perfect". With
# validity weighted 0.40, that requires one high-severity alert to cost about
# half the component -- 50 puts that frame at 80, comfortably inside the band,
# and still leaves three high alerts short of flooring the composite on their
# own. Those tests are the reason these numbers are not round: they were chosen
# to satisfy a constraint someone had already written down, not picked fresh.
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


def severity_of(alert: dict[str, Any]) -> str:
    """Normalised severity for an alert dict from either repo."""
    for key in _SEVERITY_KEYS:
        raw = alert.get(key)
        if raw:
            return _SEVERITY_ALIASES.get(str(raw).strip().lower(), "low")
    return "low"


def _component(penalty: float) -> float:
    """A component score: 100 down to a floor of 0."""
    return round(max(0.0, 100.0 - max(0.0, penalty)), 1)


def quality_report(
    null_pct: float,
    dup_pct: float,
    alerts: list[dict[str, Any]] | None = None,
    *,
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
    """
    alerts = alerts or []

    completeness = _component(float(null_pct) * _NULL_COST_PER_PCT)
    uniqueness = _component(float(dup_pct) * _DUP_COST_PER_PCT)
    validity = _component(sum(_SEVERITY_COST[severity_of(a)] for a in alerts))

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
    }
    if note:
        report["drift_note"] = note
    return report


def quality_score(null_pct: float, dup_pct: float, alerts: list[dict[str, Any]] | None = None) -> float:
    """Just the headline, for a caller that only shows one number."""
    return quality_report(null_pct, dup_pct, alerts)["quality_score"]
