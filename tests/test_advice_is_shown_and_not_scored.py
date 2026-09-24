"""Validity counts what breaks a rule; distribution advice is shown and not scored.

Ad_Data.csv scored 59.3 in MCP_Machine_Learning and 99 in MCP_Data_Analyst from
one shared formula, because each alert cost validity a flat 50/25/10 and the
two repos raise different advice about the same file: skew, zeros, correlated
columns. Its two constant columns took validity to 0 on their own, however wide
the file. Decision #22 (2026-09-24):

- advice (zeros, skew, outliers, correlation, imbalance, cardinality) costs
  nothing and is counted in `not_scored`;
- a constant column costs validity its share of the columns, 100 / columns;
- an all-null column is missing values, which completeness already prices;
- any other alert -- a rule the data breaks -- costs by severity, as before.

This file is byte-identical in both repos, like shared/quality.py.
"""

from __future__ import annotations

import pytest

from shared.quality import VALIDITY_NOTE, is_advice, quality_report

ADVICE = [
    {"type": "ZEROS", "sev": "warning"},
    {"type": "SKEWED", "sev": "warning"},
    {"type": "OUTLIERS", "sev": "warning"},
    {"type": "HIGH CORR", "sev": "warning"},
    {"type": "IMBALANCED", "sev": "warning"},
    {"type": "HIGH CARDINALITY", "sev": "warning"},
    {"type": "zero_inflated", "severity": "medium"},
    {"type": "extreme_skewness", "severity": "medium"},
    {"type": "multicollinearity", "severity": "medium"},
    {"type": "class_imbalance", "severity": "medium"},
    {"type": "high_cardinality", "severity": "medium"},
]


class TestAdviceIsNotScored:
    def test_every_kind_either_repo_raises(self):
        assert all(is_advice(a) for a in ADVICE)
        r = quality_report(0, 0, ADVICE, columns=10)
        assert r["components"]["validity"] == 100.0 and r["quality_score"] == 100.0
        assert r["not_scored"] == {"advice": len(ADVICE)}
        assert r["alert_counts"]["medium"] == len(ADVICE), "still counted as alerts"

    def test_the_answer_says_what_counts(self):
        r = quality_report(0, 0, [], columns=3)
        assert r["validity_note"] == VALIDITY_NOTE
        assert "not scored" in VALIDITY_NOTE and "100 / columns" in VALIDITY_NOTE


class TestAConstantColumnCostsItsShare:
    @pytest.mark.parametrize(("constant", "columns", "validity"), [(1, 3, 66.7), (2, 16, 87.5), (1, 100, 99.0)])
    def test_by_width(self, constant, columns, validity):
        alerts = [{"type": "CONSTANT", "sev": "error", "col": f"c{i}"} for i in range(constant)]
        assert quality_report(0, 0, alerts, columns=columns)["components"]["validity"] == validity

    def test_either_repos_spelling(self):
        from_da = quality_report(0, 0, [{"type": "CONSTANT", "sev": "error", "col": "p"}], columns=4)
        from_ml = quality_report(0, 0, [{"type": "constant_column", "severity": "high", "column": "p"}], columns=4)
        assert from_da["components"] == from_ml["components"] and from_da["components"]["validity"] == 75.0

    def test_one_column_alerted_twice_is_one_column(self):
        alerts = [{"type": "CONSTANT", "sev": "error", "col": "p"}] * 2
        assert quality_report(0, 0, alerts, columns=4)["components"]["validity"] == 75.0

    def test_without_a_width_it_costs_by_severity(self):
        r = quality_report(0, 0, [{"type": "CONSTANT", "sev": "error", "col": "p"}])
        assert r["components"]["validity"] == 50.0


class TestEachFactIsPricedOnce:
    def test_an_all_null_column_is_missing_values(self):
        for alert in ({"type": "ALL NULL", "sev": "error"}, {"type": "all_null_column", "severity": "high"}):
            assert quality_report(10, 0, [alert], columns=10)["components"]["validity"] == 100.0

    def test_a_rule_alert_still_costs_by_severity(self):
        r = quality_report(0, 0, [{"type": "mixed_types", "severity": "high"}], columns=10)
        assert r["components"]["validity"] == 50.0


def test_ad_data_scores_the_same_in_both_repos_shape():
    """Ad_Data's facts: 0.2% missing cells, 1.2% duplicate rows, 2 constant
    columns of 16 -- and DA raises 13 advice alerts where ML raises 8. The
    advice no longer decides the score, so both repos answer 94.3."""
    base = [
        {"type": "CONSTANT", "sev": "error", "col": "product"},
        {"type": "CONSTANT", "sev": "error", "col": "phase"},
    ]
    da = quality_report(0.2, 1.2, base + ADVICE[:6] * 2 + ADVICE[:1], columns=16)
    ml = quality_report(0.2, 1.2, base + ADVICE[6:] + ADVICE[6:9], columns=16)
    assert da["quality_score"] == ml["quality_score"]
    assert da["components"]["validity"] == 87.5
