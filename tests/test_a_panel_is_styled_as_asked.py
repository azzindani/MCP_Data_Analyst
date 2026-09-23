"""A caller's panel style, title and place reach the page -- or are refused by name.

A panel used to take {slot, chart, cols, agg} and nothing else; how it looked
was the template's business. Now a panel may carry a `title`, a `place` on a
12-column grid, and a `style` whose every field is one its chart draws, and the
page may carry a palette and a colour per category value that every panel
uses. What has to hold, read off the figures the page's own script draws:

- a field a chart does not draw, or a value it cannot, is refused by name;
- each style field changes the figure it names, and nothing else;
- a category is one colour on every panel that draws it;
- a sequential colour scale draws the largest value darkest -- Plotly.js runs
  its own dark-to-light, which drew the heatmap's and the map's peaks palest;
- title and place reach the card, and a page with no place keeps its grid.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from servers.data_advanced._adv_dashboard import customize_dashboard, generate_dashboard
from tests.dashboard_page import NODE, drawn

needs_node = pytest.mark.skipif(NODE is None, reason="node is not installed")


@pytest.fixture
def sales(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(11)
    n = 200
    path = tmp_path / "sales.csv"
    pd.DataFrame(
        {
            "day": pd.date_range("2024-01-01", periods=n).strftime("%Y-%m-%d"),
            "region": rng.choice(["North", "South", "East", "West"], n),
            "channel": rng.choice(["web", "store"], n),
            "country": rng.choice(["France", "Germany", "Japan"], n),
            "revenue": rng.normal(1000, 200, n).round(2),
            "units": rng.integers(1, 50, n).astype(float),
        }
    ).to_csv(path, index=False)
    return path


def _build(path: Path, **kw) -> dict:
    out = path.parent / f"d{abs(hash(json.dumps(kw, sort_keys=True)))}.html"
    return generate_dashboard(str(path), output_path=str(out), open_after=False, **kw)


def _page(path: Path, **kw) -> str:
    r = _build(path, **kw)
    assert r["success"] is True, r
    return Path(r["output_path"]).read_text(encoding="utf-8")


def _one(path: Path, panel: dict, **kw) -> dict:
    """The figure of a one-panel page."""
    out = drawn(_page(path, spec={"layout": [panel], **kw}))
    return next(iter(out["figures"].values()))


BAR = {"chart": "bar", "cols": {"category": "region", "value": "revenue"}}


class TestWhatCannotBeDrawnIsRefused:
    @pytest.mark.parametrize(
        ("panel", "says"),
        [
            ({**BAR, "style": {"bins": 10}}, "a bar chart does not draw 'bins'. Its style takes: color"),
            ({**BAR, "style": {"color": "blue-ish"}}, "'blue-ish' is not a colour"),
            ({**BAR, "style": {"sort": "random"}}, "sort='random'; valid: desc, asc, label"),
            ({**BAR, "style": {"top_n": 0}}, "top_n must be a whole number from 1 to 500"),
            ({**BAR, "style": {"prefix": "dollars and"}}, "prefix must be text of at most 8 characters"),
            ({**BAR, "place": {"span": 13}}, "place.span must be a whole number from 1 to 12 columns of 12"),
            ({**BAR, "place": {"row": 2}}, "place has unknown key 'row'. It takes: span, height"),
            ({**BAR, "title": ""}, "title must be text of 1 to 120 characters"),
            ({**BAR, "width": 2}, "unknown key(s): width"),
        ],
    )
    def test_a_panel(self, sales, panel, says):
        r = _build(sales, spec={"layout": [panel]})
        assert r["success"] is False and says in r["error"], r.get("error")

    def test_the_page_style(self, sales):
        r = _build(sales, spec={"style": {"font": "serif"}})
        assert r["success"] is False and "The page's style takes: palette, colors" in r["error"]
        r = _build(sales, spec={"style": {"colors": {"North": "not-a-colour"}}})
        assert r["success"] is False and "style.colors['North']" in r["error"]


@needs_node
class TestEachFieldChangesItsFigure:
    def test_bar_colour_sort_top_n_and_labels(self, sales):
        df = pd.read_csv(sales)
        f = _one(sales, {**BAR, "style": {"color": "#112233", "top_n": 3, "sort": "label", "value_labels": False}})
        trace = f["data"][0]
        top3 = df.groupby("region")["revenue"].sum().nlargest(3).index
        assert trace["x"] == sorted(top3), "the three largest, ordered by label"
        assert trace["marker"]["color"] == "#112233" and "text" not in trace

    def test_value_format_prefix_and_log_axis(self, sales):
        style = {"format": "integer", "prefix": "$", "y_scale": "log"}
        f = _one(sales, {**BAR, "style": style})
        assert all(t.startswith("$") and "." not in t for t in f["data"][0]["text"])
        assert f["layout"]["yaxis"]["tickformat"] == ",.0f" and f["layout"]["yaxis"]["tickprefix"] == "$"
        assert f["layout"]["yaxis"]["type"] == "log"

    def test_a_bar_without_style_is_the_default(self, sales):
        trace = _one(sales, BAR)["data"][0]
        assert trace["marker"]["color"] == "#58a6ff" and trace["textposition"] == "outside"
        assert trace["y"] == sorted(trace["y"], reverse=True)

    def test_line_without_its_moving_average_and_legend(self, sales):
        f = _one(
            sales, {"chart": "line", "cols": {"date": "day", "value": "revenue"}, "style": {"ma": 0, "legend": "none"}}
        )
        assert len(f["data"]) == 1 and f["layout"]["showlegend"] is False
        f = _one(sales, {"chart": "line", "cols": {"date": "day", "value": "revenue"}, "style": {"legend": "right"}})
        assert f["layout"]["legend"]["orientation"] == "v"

    def test_histogram_bins(self, sales):
        f = _one(sales, {"chart": "histogram", "cols": {"value": "units"}, "style": {"bins": 12}})
        assert f["data"][0]["nbinsx"] == 12


@needs_node
class TestACategoryIsOneColourEverywhere:
    def test_the_page_map_reaches_every_panel_that_draws_the_value(self, sales):
        spec = {
            "style": {"colors": {"North": "#ff0000"}},
            "layout": [
                BAR,
                {"chart": "pie", "cols": {"category": "region", "value": "revenue"}},
                {"chart": "box", "cols": {"value": "units", "category": "region"}},
            ],
        }
        figures = drawn(_page(sales, spec=spec))["figures"]
        bar = figures["p0_bar"]["data"][0]
        assert dict(zip(bar["x"], bar["marker"]["color"], strict=True))["North"] == "#ff0000"
        assert {c for x, c in zip(bar["x"], bar["marker"]["color"], strict=True) if x != "North"} == {"#58a6ff"}
        pie = figures["p1_pie"]["data"][0]
        assert dict(zip(pie["labels"], pie["marker"]["colors"], strict=True))["North"] == "#ff0000"
        box = {t["name"]: t["marker"]["color"] for t in figures["p2_box"]["data"]}
        assert box["North"] == "#ff0000"

    def test_a_panel_map_wins_over_the_page_map(self, sales):
        spec = {
            "style": {"colors": {"North": "#ff0000"}},
            "layout": [{**BAR, "style": {"colors": {"North": "#00ff00"}}}],
        }
        bar = drawn(_page(sales, spec=spec))["figures"]["p0_bar"]["data"][0]
        assert dict(zip(bar["x"], bar["marker"]["color"], strict=True))["North"] == "#00ff00"

    def test_the_page_palette_colours_the_detected_page(self, sales):
        palette = ["#010101", "#020202", "#030303", "#040404"]
        figures = drawn(_page(sales, spec={"style": {"palette": palette}}))["figures"]
        pie = next(f for cid, f in figures.items() if cid.startswith("pie_"))["data"][0]
        assert pie["marker"]["colors"] == palette[: len(pie["labels"])]


@needs_node
class TestDarkerMeansMore:
    def test_the_heatmap_and_the_map_reverse_plotlys_dark_first_scale(self, sales):
        figures = drawn(_page(sales))["figures"]
        hm = next(f for cid, f in figures.items() if cid.startswith("aghm_"))["data"][0]
        assert (hm["colorscale"], hm["reversescale"]) == ("YlOrRd", True)
        choro = _one(sales, {"chart": "choropleth", "cols": {"location": "country", "value": "revenue"}})
        assert choro["layout"]["coloraxis"]["reversescale"] is True

    def test_a_scale_that_already_runs_light_to_dark_is_left_alone(self, sales):
        panel = {
            "chart": "choropleth",
            "cols": {"location": "country", "value": "revenue"},
            "style": {"colorscale": "Viridis"},
        }
        axis = _one(sales, panel)["layout"]["coloraxis"]
        assert (axis["colorscale"], axis["reversescale"]) == ("Viridis", False)

    def test_the_correlation_scale_is_a_name_plotly_js_knows(self, sales):
        corr = drawn(_page(sales))["figures"]["corr_hm"]["data"][0]
        assert corr["colorscale"] == "RdBu" and corr["reversescale"] is False


class TestTitleAndPlace:
    def test_the_title_is_the_cards(self, sales):
        html = _page(sales, spec={"layout": [{**BAR, "title": "Revenue <by> region"}]})
        assert "<h3>Revenue &lt;by&gt; region</h3>" in html

    def test_a_placed_page_is_on_the_12_column_grid(self, sales):
        spec = {"layout": [{**BAR, "place": {"span": 8, "height": 420}}, {"chart": "histogram"}, {"chart": "line"}]}
        html = _page(sales, spec=spec)
        assert '<div class="cgrid g12">' in html
        spans = re.findall(r'<div class="cc[^"]*" style="grid-column:span (\d+)">', html)
        assert spans == ["8", "6", "12"], "unplaced panels keep their default width: half, and full for a line"
        assert 'style="height:420px"' in html

    def test_an_unplaced_page_keeps_its_grid(self, sales):
        html = _page(sales, spec={"layout": [BAR]})
        assert '<div class="cgrid">' in html and "grid-column:span" not in html

    def test_a_phone_puts_every_placed_card_full_width(self):
        from servers.data_advanced._adv_dashboard import _PLACE_CSS

        assert "@media(max-width:68.75rem)" in _PLACE_CSS and ".cgrid.g12>.cc{grid-column:1/-1!important}" in _PLACE_CSS


class TestItRoundTrips:
    def test_the_spec_returns_style_and_place_and_customize_keeps_them(self, sales):
        spec = {
            "style": {"colors": {"North": "#ff0000"}},
            "layout": [{**BAR, "title": "Mine", "style": {"top_n": 2}, "place": {"span": 12}}],
        }
        r = _build(sales, spec=spec)
        assert r["spec"]["style"] == spec["style"] and r["spec"]["layout"][0]["place"] == {"span": 12}
        again = customize_dashboard(r["output_path"], {"title": "Renamed"}, open_after=False)
        assert again["success"] is True, again
        html = Path(again["output_path"]).read_text(encoding="utf-8")
        assert "<h3>Mine</h3>" in html and "grid-column:span 12" in html
