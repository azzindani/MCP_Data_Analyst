"""Two tools failed the call their own schema documents.

Both found by calling all 225 tools with nothing but the arguments each schema
marks required -- the call a model reading tools/list actually makes.

generate_multi_chart marks file_path, chart_type and value_columns required.
With no category_column, multi_bar fell through to `x_vals = range(len(df))`,
and plotly refuses a range object:

    Invalid value of type 'builtins.range' received for the 'x' property of bar
        Received value: range(0, 400)

under the hint "Check file_path, column names, and chart_type" -- the three
arguments that were already correct. Its sibling multi_line has always required
its date_column and says so. generate_chart was given the same guard last time
round; this one was missed.

statistical_test marks file_path and test required. column_a defaults to "", so
the documented call reached `df[""]` and answered "Column '' not found.
Available: [...]", which reads as though the caller had named a column when it
had named none, under a hint listing the 17 valid tests -- and the test was
right.
"""

from __future__ import annotations

import csv as csvmod
import json
from pathlib import Path

import pytest

from servers.data_statistics.engine import statistical_test
from servers.data_visual.engine import generate_multi_chart

# plotly and pandas internals: none of these is a parameter of either tool.
LEAKS = ["builtins.range", "range(0,", "'x' property", "data_frame", "numpy array"]


@pytest.fixture()
def csv_path(tmp_path: Path) -> str:
    path = tmp_path / "campaigns.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csvmod.writer(fh)
        w.writerow(["campaign_platform", "spends", "impressions"])
        for i in range(20):
            w.writerow(["Google Ads" if i % 2 else "Facebook Ads", 100 + i, 1000 + i])
    return str(path)


class TestGenerateMultiChartWithoutACategory:
    def test_it_refuses_instead_of_raising_plotlys_error(self, csv_path: str, tmp_path: Path):
        r = generate_multi_chart(
            csv_path, "bar", ["spends", "impressions"], output_path=str(tmp_path / "c.html"), open_after=False
        )
        assert r["success"] is False
        assert "category_column" in r["error"], r["error"]

    def test_no_plotly_internal_reaches_the_caller(self, csv_path: str, tmp_path: Path):
        r = generate_multi_chart(
            csv_path, "bar", ["spends", "impressions"], output_path=str(tmp_path / "c.html"), open_after=False
        )
        blob = json.dumps(r)
        for leak in LEAKS:
            assert leak not in blob, f"leaked {leak!r}: {r['error']}"

    def test_the_hint_names_the_missing_argument(self, csv_path: str, tmp_path: Path):
        r = generate_multi_chart(
            csv_path, "bar", ["spends", "impressions"], output_path=str(tmp_path / "c.html"), open_after=False
        )
        assert "category_column" in r["hint"] and "inspect_dataset" in r["hint"], r["hint"]

    def test_it_suggests_a_column_that_exists(self, csv_path: str, tmp_path: Path):
        r = generate_multi_chart(
            csv_path, "bar", ["spends", "impressions"], output_path=str(tmp_path / "c.html"), open_after=False
        )
        assert "campaign_platform" in r["hint"], r["hint"]

    def test_no_chart_file_is_written(self, csv_path: str, tmp_path: Path):
        out = tmp_path / "c.html"
        generate_multi_chart(csv_path, "bar", ["spends", "impressions"], output_path=str(out), open_after=False)
        assert not out.exists()


class TestGenerateMultiChartStillDrawsWhenAsked:
    def test_bar_with_a_category(self, csv_path: str, tmp_path: Path):
        out = tmp_path / "c.html"
        r = generate_multi_chart(
            csv_path,
            "bar",
            ["spends", "impressions"],
            category_column="campaign_platform",
            output_path=str(out),
            open_after=False,
        )
        assert r["success"] is True, r.get("error")
        assert out.exists()

    def test_line_still_asks_for_its_own_argument(self, csv_path: str, tmp_path: Path):
        r = generate_multi_chart(csv_path, "line", ["spends"], output_path=str(tmp_path / "c.html"), open_after=False)
        assert r["success"] is False
        assert "date_column" in r["error"], r["error"]

    def test_the_docstring_says_what_each_type_needs(self):
        from servers.data_visual.server import generate_multi_chart as tool

        doc = getattr(tool, "description", None) or tool.__doc__ or ""
        assert "category_column" in doc and "date_column" in doc, doc


class TestStatisticalTestWithoutAColumn:
    def test_it_says_which_argument_is_missing(self, csv_path: str):
        r = statistical_test(csv_path, "shapiro_wilk")
        assert r["success"] is False
        assert "column_a" in r["error"], r["error"]

    def test_the_error_does_not_pretend_a_column_was_named(self, csv_path: str):
        r = statistical_test(csv_path, "shapiro_wilk")
        assert "Column ''" not in r["error"], r["error"]

    def test_it_suggests_a_numeric_column(self, csv_path: str):
        r = statistical_test(csv_path, "shapiro_wilk")
        assert "spends" in r["error"], r["error"]

    def test_a_two_sample_test_names_the_second_column(self, csv_path: str):
        r = statistical_test(csv_path, "t_test", column_a="spends")
        assert r["success"] is False
        assert "column_b" in r["error"], r["error"]


class TestStatisticalTestIsOtherwiseUnchanged:
    def test_a_named_column_still_runs(self, csv_path: str):
        r = statistical_test(csv_path, "shapiro_wilk", column_a="spends")
        assert r["success"] is True, r.get("error")

    def test_a_column_that_is_not_there_still_says_so(self, csv_path: str):
        r = statistical_test(csv_path, "shapiro_wilk", column_a="ghost")
        assert r["success"] is False
        assert "ghost" in r["error"], r["error"]

    def test_an_unknown_test_still_says_so(self, csv_path: str):
        r = statistical_test(csv_path, "vibes", column_a="spends")
        assert r["success"] is False
        assert "vibes" in r["error"], r["error"]
