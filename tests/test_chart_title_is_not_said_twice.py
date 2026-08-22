"""generate_chart appended its own breakdown to a title the caller wrote.

A coverage sweep asked for a bar chart with

    title="Total Ad Spend by Campaign Platform", category_column="campaign_platform"

and the page came back headed

    Total Ad Spend by Campaign Platform by campaign_platform

The suffix exists to finish the *generated* title -- "sum of spends" does not
say what it is broken down by, so "sum of spends by campaign_platform" is the
right default. It was appended unconditionally, so a caller-supplied title got
it too, and a title is the one string on the page a reader is guaranteed to
read. generate_geo_map, doing the same job a few hundred lines away, already
guards its generated title with `if not chart_title`.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from servers.data_visual.engine import generate_chart

WRITTEN = "Total Ad Spend by Campaign Platform"


def heading(path: Path) -> str:
    """The visible page heading -- save_chart hoists the title out of the figure."""
    m = re.search(r'<h1 class="chart-title">([^<]*)</h1>', path.read_text(encoding="utf-8"))
    return m.group(1) if m else ""


def tab(path: Path) -> str:
    m = re.search(r"<title>([^<]*)</title>", path.read_text(encoding="utf-8"))
    return m.group(1) if m else ""


@pytest.fixture()
def csv(simple_csv: Path) -> str:
    return str(simple_csv)


class TestATitleTheCallerWroteIsUsedVerbatim:
    def test_the_response_does_not_append_the_column(self, csv: str, tmp_path: Path):
        out = tmp_path / "c.html"
        r = generate_chart(
            csv,
            "bar",
            "Revenue",
            category_column="Region",
            title=WRITTEN,
            output_path=str(out),
            open_after=False,
        )
        assert r["success"] is True, r.get("error")
        assert heading(out) == WRITTEN, heading(out)

    def test_the_browser_tab_matches_the_heading(self, csv: str, tmp_path: Path):
        out = tmp_path / "c.html"
        generate_chart(
            csv,
            "bar",
            "Revenue",
            category_column="Region",
            title=WRITTEN,
            output_path=str(out),
            open_after=False,
        )
        assert tab(out) == heading(out) == WRITTEN

    def test_the_column_name_is_not_said_twice(self, csv: str, tmp_path: Path):
        out = tmp_path / "c.html"
        generate_chart(
            csv,
            "bar",
            "Revenue",
            category_column="Region",
            title=WRITTEN,
            output_path=str(out),
            open_after=False,
        )
        assert heading(out).lower().count(" by ") == 1, heading(out)

    @pytest.mark.parametrize("chart_type", ["bar", "pie", "line", "scatter", "funnel"])
    def test_every_type_that_takes_a_category_respects_the_title(self, chart_type: str, csv: str, tmp_path: Path):
        out = tmp_path / f"{chart_type}.html"
        r = generate_chart(
            csv,
            chart_type,
            "Revenue",
            category_column="Region",
            title=WRITTEN,
            output_path=str(out),
            open_after=False,
        )
        assert r["success"] is True, r.get("error")
        assert heading(out) == WRITTEN, f"{chart_type}: {heading(out)}"


class TestTheGeneratedTitleStillSaysWhatItIsBrokenDownBy:
    """Without the suffix an auto-titled chart reads "sum of Revenue" and the
    reader cannot tell what the bars are."""

    def test_no_title_still_names_the_category(self, csv: str, tmp_path: Path):
        out = tmp_path / "c.html"
        generate_chart(
            csv,
            "bar",
            "Revenue",
            category_column="Region",
            output_path=str(out),
            open_after=False,
        )
        assert heading(out) == "sum of Revenue by Region", heading(out)

    def test_no_title_and_no_category_is_unchanged(self, csv: str, tmp_path: Path):
        """waterfall is one of the types that does not need a category."""
        out = tmp_path / "c.html"
        generate_chart(
            csv,
            "waterfall",
            "Revenue",
            output_path=str(out),
            open_after=False,
        )
        assert heading(out) == "sum of Revenue", heading(out)

    def test_the_agg_func_is_still_named(self, csv: str, tmp_path: Path):
        out = tmp_path / "c.html"
        generate_chart(
            csv,
            "bar",
            "Revenue",
            category_column="Region",
            agg_func="mean",
            output_path=str(out),
            open_after=False,
        )
        assert heading(out) == "mean of Revenue by Region", heading(out)
