"""A quality score prices each fact once, and shows how it was priced.

check_data_quality scored Ad_Data.csv 59.3 as a bare number. Its parts were
completeness 99.6, uniqueness 97.6 and validity 0.0 -- and nothing in the
answer said so, or that every lost point went to alerts. Two of those points
were priced twice: the duplicate-rows alert cost validity on top of the
uniqueness component that dup_pct already prices (missing values the same way,
through completeness), and generate_eda_report alerted every constant column
twice -- as constant, and again as "100% one class" imbalance.

The shared module is byte-identical with MCP_Data_Analyst's; this file is too.
"""

from __future__ import annotations

from shared.quality import quality_report


def _alert(kind: str, severity: str = "medium", column: str = "x") -> dict:
    return {"type": kind, "severity": severity, "column": column}


class TestEachFactIsPricedOnce:
    def test_duplicate_rows_cost_uniqueness_not_validity(self):
        report = quality_report(0.0, 10.0, [_alert("duplicate_rows")])
        assert report["components"]["validity"] == 100.0
        assert report["components"]["uniqueness"] == 80.0

    def test_missing_values_cost_completeness_not_validity(self):
        report = quality_report(30.0, 0.0, [_alert("high_missing", "high")])
        assert report["components"]["validity"] == 100.0
        assert report["components"]["completeness"] == 40.0

    def test_the_other_repos_names_for_them_too(self):
        alerts = [{"type": "DUPLICATES", "sev": "warning"}, {"type": "HIGH NULLS", "sev": "error"}]
        assert quality_report(0.0, 0.0, alerts)["components"]["validity"] == 100.0

    def test_they_are_still_counted_as_alerts(self):
        report = quality_report(0.0, 10.0, [_alert("duplicate_rows")])
        assert report["alert_counts"]["medium"] == 1

    def test_a_constant_column_still_costs_validity(self):
        assert quality_report(0.0, 0.0, [_alert("constant_column", "high")])["components"]["validity"] == 50.0
