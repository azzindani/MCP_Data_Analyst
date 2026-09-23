"""Charts the dashboard drew that a person could not read, or need not see.

All three were found by rendering the page and looking at it. None of them
fails a structural check: every chart was valid HTML, every number correct.

1. Constant columns were charted. The real ad dataset has 'product' and 'phase'
   with one value each, and the dashboard's own alert panel says so -- "'product'
   has only 1 unique value -- constant, no predictive value". Two rows further
   down it then drew "Total spends by product" as a single bar, "Total
   impressions by product" as a single bar, the same pair for 'phase', and two
   donuts that were solid 100% rings. Because cat_cols[0] and cat_cols[1] also
   feed the grouped bar, the coloured scatter, the box plot and the aggregate
   heatmap, a 1x1 heatmap came with them: roughly half the charts on the page
   were the grand total, drawn as a rectangle, above the fold. The filter bar
   (1 < len(uniq)) and the numeric range inputs (mn < mx) had always made this
   exclusion; the chart builder was the one place that had not.

2. tickangle was pinned at -38 for every chart, so "Google Ads" and "Facebook
   Ads" were printed diagonally across an otherwise empty axis. 'auto' rotates
   only when labels would actually collide, which keeps short labels level and
   still tilts eighteen long ones.

3. Pie slice labels were 'label+percent' at any slice count. With eighteen
   categories plotly moves them outside on leader lines, where they overlapped
   each other, spilled past the card and were cut off at the bottom -- while the
   legend underneath already listed every name. Past six slices the percent now
   stays inside the slice and the legend carries the names.

The horizontal legends on the grouped bar and coloured scatter sat at y=-0.3,
directly on top of the rotated tick labels they shared the space with. They now
sit above the plot, which is where the scatter and time-series legends already
were.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from tests.dashboard_page import NODE, drawn


def _generate(*a, **kw):
    """Imported lazily so these fail on their own assertions rather than on a
    collection error if the module moves."""
    from servers.data_advanced.engine import generate_dashboard

    return generate_dashboard(*a, **kw)


@pytest.fixture()
def constant_col_csv(tmp_path: Path) -> Path:
    """Shaped like the real dataset: two constant columns first, real ones after."""
    rng = np.random.default_rng(0)
    n = 400
    p = tmp_path / "campaigns.csv"
    pd.DataFrame(
        {
            "product": ["Product 1"] * n,
            "phase": ["Performance"] * n,
            "campaign_platform": rng.choice(["Google Ads", "Facebook Ads"], n),
            "device": rng.choice(["Desktop", "Mobile", "Tablet"], n),
            "spends": rng.gamma(2, 300, n).round(2),
            "impressions": rng.integers(1, 9000, n),
        }
    ).to_csv(p, index=False)
    return p


@pytest.fixture()
def rendered(constant_col_csv: Path, tmp_path: Path) -> str:
    out = tmp_path / "dash.html"
    r = _generate(str(constant_col_csv), output_path=str(out), open_after=False)
    assert r["success"] is True, r.get("error")
    return out.read_text(encoding="utf-8")


class TestConstantColumnsAreNotCharted:
    def test_no_bar_chart_groups_by_a_constant_column(self, rendered: str):
        assert "by product" not in rendered
        assert "by phase" not in rendered

    def test_no_pie_chart_of_a_constant_column(self, rendered: str):
        assert "product Distribution" not in rendered
        assert "phase Distribution" not in rendered

    def test_the_columns_that_vary_are_still_charted(self, rendered: str):
        """The point is to drop the useless charts, not to draw fewer charts."""
        assert "by campaign_platform" in rendered
        assert "campaign_platform Distribution" in rendered

    def test_the_alert_panel_still_reports_them(self, rendered: str):
        """Not charting a constant column must not mean going quiet about it --
        the reader still needs to know the column is dead. (The panel escapes
        the quotes it puts around a column name, hence &#x27;.)"""
        assert "&#x27;product&#x27; has only 1 unique value" in rendered
        assert "constant" in rendered

    def test_a_frame_of_only_constants_charts_nothing_rather_than_nonsense(self, tmp_path: Path):
        p = tmp_path / "flat.csv"
        pd.DataFrame({"only": ["x"] * 50, "value": range(50)}).to_csv(p, index=False)
        out = tmp_path / "flat.html"
        r = _generate(str(p), output_path=str(out), open_after=False)
        assert r["success"] is True, r.get("error")
        assert "by only" not in out.read_text(encoding="utf-8")


# The classes below read the figures the page's renderer actually draws (run in
# node by tests/dashboard_page.py), not the text of the script that draws them:
# a string in the source says nothing about which chart, if any, uses it.
needs_node = pytest.mark.skipif(NODE is None, reason="node is not installed")


def _cartesian(figures: dict) -> dict:
    """The figures drawn on x/y axes -- not pies or maps, whatever axes am() gives their layout."""
    flat = ("pie", "scattergeo", "choropleth")
    return {cid: f for cid, f in figures.items() if f["data"] and f["data"][0]["type"] not in flat}


@needs_node
class TestAxisLabelsStayLevelUnlessCrowded:
    def test_rotation_is_left_to_plotly_on_every_axis_that_has_categories(self, rendered: str):
        figures = drawn(rendered)["figures"]
        bars = {cid: f for cid, f in figures.items() if f["data"] and f["data"][0]["type"] == "bar"}
        assert bars, "the fixture draws bar charts"
        for cid, f in bars.items():
            assert f["layout"]["xaxis"]["tickangle"] == "auto", cid

    def test_no_chart_pins_the_old_rotation(self, rendered: str):
        for cid, f in drawn(rendered)["figures"].items():
            assert f["layout"].get("xaxis", {}).get("tickangle") != -38, cid


def _pie(tmp_path: Path, categories: int) -> dict:
    p = tmp_path / f"pie{categories}.csv"
    pd.DataFrame({"kind": [f"k{i % categories}" for i in range(200)], "v": range(200)}).to_csv(p, index=False)
    out = tmp_path / f"pie{categories}.html"
    r = _generate(str(p), output_path=str(out), open_after=False, spec={"layout": [{"chart": "pie"}]})
    assert r["success"] is True, r.get("error")
    return drawn(out.read_text(encoding="utf-8"))["figures"]["p0_pie"]["data"][0]


@needs_node
class TestPieLabelsDegradeWithSliceCount:
    def test_a_few_slices_are_labelled(self, tmp_path: Path):
        assert _pie(tmp_path, 4)["textinfo"] == "label+percent"

    def test_many_slices_keep_only_the_percent(self, tmp_path: Path):
        assert _pie(tmp_path, 9)["textinfo"] == "percent"

    def test_slice_text_is_kept_inside_the_slice(self, tmp_path: Path):
        """Outside placement is what produced the overlapping leader lines."""
        trace = _pie(tmp_path, 9)
        assert trace["textposition"] == "inside"
        assert trace["insidetextorientation"] == "horizontal"


@needs_node
class TestLegendsDoNotSitOnTheTickLabels:
    def test_no_legend_of_a_chart_with_axes_is_placed_below_them(self, rendered: str):
        """Below the plot is the band rotated tick labels occupy."""
        for cid, f in _cartesian(drawn(rendered)["figures"]).items():
            legend = f["layout"].get("legend") or {}
            assert legend.get("y", 1) >= 0, cid

    def test_every_axis_grows_to_fit_its_labels(self, rendered: str):
        figures = _cartesian(drawn(rendered)["figures"])
        assert figures
        for cid, f in figures.items():
            axes = [k for k in f["layout"] if k.startswith(("xaxis", "yaxis"))]
            assert axes and all(f["layout"][k].get("automargin") is True for k in axes), cid
