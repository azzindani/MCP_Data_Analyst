"""A generated dashboard is something to work with: every chart has its toolbar, hovers, pans, zooms, expands.

The story page had been told `toolbar: false` ("a page for reading"), so no figure had a modebar and there was
no pan, zoom, autoscale or reset; the KPI sparklines were static pictures with hover skipped, and the ranked
lists were HTML lists nobody could hover or expand. These pin what the reader can now do, and each was run once
with its fix switched off.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from servers.data_advanced._adv_dashboard import generate_dashboard
from shared.dashboard_looks import BUILTIN, DEFAULT_LOOK, chart_theme
from tests.dashboard_page import NODE, drawn, run_js

needs_node = pytest.mark.skipif(NODE is None, reason="node is not installed")


@pytest.fixture
def bookings(tmp_path, monkeypatch) -> Path:
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(31)
    n = 400  # 2024-01-01 to 2025-02-03
    path = tmp_path / "bookings.csv"
    pd.DataFrame(
        {
            "booking_date": pd.date_range("2024-01-01", periods=n).strftime("%Y-%m-%d"),
            "is_canceled": rng.choice([0, 1], n, p=[0.7, 0.3]),
            "channel": rng.choice(["Direct", "Agent", "Online", "Corporate"], n),
            "lead_time": rng.integers(0, 200, n),
            "revenue": rng.normal(200, 15, n).round(2),
        }
    ).to_csv(path, index=False)
    return path


def _html(path: Path, name: str = "page", **spec) -> str:
    out = path.parent / f"{name}.html"
    result = generate_dashboard(str(path), output_path=str(out), open_after=False, spec=spec or None)
    assert result["success"] is True, result.get("error")
    return out.read_text(encoding="utf-8")


def _figures(html: str) -> dict:
    figures = drawn(html)["figures"]
    assert figures
    return figures


def _trend(figures: dict) -> dict:
    return next(f for cid, f in figures.items() if cid.endswith("time_series"))


@needs_node
class TestEveryChartHasItsToolbar:
    @pytest.mark.parametrize(
        "spec", [{}, {"style": {"look": "classic"}}, {"style": {"look": "nocturne"}}], ids=["studio", "classic", "dark"]
    )
    def test_the_modebar_is_on_for_every_figure(self, bookings, spec):
        for cid, f in _figures(_html(bookings, **spec)).items():
            assert f["config"]["displayModeBar"] is True, cid
            assert f["config"]["scrollZoom"] is True, cid

    def test_a_generated_page_no_longer_says_it_is_for_reading(self, bookings):
        result = generate_dashboard(str(bookings), output_path=str(bookings.parent / "r.html"), open_after=False)
        assert "toolbar" not in result["spec"]["style"]

    def test_the_page_can_still_ask_for_no_toolbar(self, bookings):
        figures = _figures(_html(bookings, name="quiet", style={"toolbar": False}))
        assert all(f["config"]["displayModeBar"] is False for f in figures.values())

    def test_the_toolbar_has_the_tools_that_mean_something_on_a_chart_of_totals(self, bookings):
        config = next(iter(_figures(_html(bookings)).values()))["config"]
        assert config["displaylogo"] is False, "no link out to a vendor from an offline page"
        assert set(config["modeBarButtonsToRemove"]) == {"select2d", "lasso2d"}  # zoom, pan, reset, camera stay

    def test_the_toolbar_and_the_tooltip_wear_the_pages_look(self, bookings):
        want = chart_theme(BUILTIN[DEFAULT_LOOK], "light")
        layout = _trend(_figures(_html(bookings)))["layout"]
        assert layout["modebar"]["bgcolor"] == "rgba(0,0,0,0)" and layout["modebar"]["activecolor"].startswith("#")
        assert layout["hoverlabel"]["font"]["family"] == want["family"]

    def test_fullscreen_opens_the_chart_with_the_same_toolbar(self, bookings):
        assert "{height:null,autosize:true}),PCFG);" in _html(bookings)


@needs_node
class TestEveryMarkSaysWhatItIsOnHover:
    def test_the_trend_reads_every_series_along_the_period_in_the_panels_own_numbers(self, bookings):
        figure = _trend(_figures(_html(bookings)))
        assert figure["layout"]["hovermode"] == "x unified"
        assert figure["layout"]["xaxis"]["hoverformat"] == "%b %Y"  # a month is "Nov 2024", not "Nov 1, 2024"
        main = figure["data"][0]
        assert "%{customdata}" in main["hovertemplate"] and len(main["customdata"]) == len(main["y"])
        assert all(isinstance(c, str) and c for c in main["customdata"])

    def test_the_note_about_an_unfinished_period_stays_clear_of_the_toolbar(self, bookings):
        notes = [
            a
            for a in _trend(_figures(_html(bookings)))["layout"]["annotations"]
            if a["text"] == "latest period incomplete"
        ]
        assert notes and notes[0]["yanchor"] == "top", "inside the plot: the toolbar sits above it"

    def test_a_ring_names_the_slice_and_its_count_and_share(self, bookings):
        ring = next(f for f in _figures(_html(bookings)).values() if f["data"][0]["type"] == "pie")
        template = ring["data"][0]["hovertemplate"]
        assert "%{label}" in template and "%{value" in template and "%{percent}" in template

    def test_a_kpi_sparkline_is_a_chart_you_can_read_not_a_picture(self, bookings):
        html = _html(bookings)
        drawn_in_the_tile = run_js(
            html,
            "(function(){window.Plotly=Plotly;var out=[],r=Plotly.react;"
            "globalThis.getComputedStyle=function(){return{getPropertyValue:function(){return '#0072b2';}};};"
            "Plotly.react=function(el,d,l,c){if(el&&el.id&&/-sp$/.test(el.id))out.push({id:el.id,data:d,layout:l,config:c});};"
            "renderAll(_RAW);Plotly.react=r;return out;})()",
        )
        assert drawn_in_the_tile, "the page draws a line in the tile"
        for tile in drawn_in_the_tile:
            trace = tile["data"][0]
            assert not tile["config"].get("staticPlot"), tile["id"]
            assert tile["layout"]["hovermode"] == "x" and tile["layout"]["xaxis"]["showspikes"] is True
            assert len(trace["x"]) == len(trace["y"]) == len(trace["customdata"]) and "%{x}" in trace["hovertemplate"]
            assert tile["layout"]["dragmode"] is False, "a 44-pixel line is read, not zoomed"
            assert tile["layout"]["xaxis"]["fixedrange"] is True and tile["config"]["displayModeBar"] is False

    def test_the_classic_pages_kpi_strip_sparkline_hovers_too(self, bookings):
        html = _html(bookings, name="det", story=False)
        strip = [s for s in html.split("<script>") if "Plotly.newPlot('ks-" in s]
        assert strip, "a detected page has the KPI strip"
        for script in strip:
            assert "staticPlot" not in script and "hovertemplate" in script and "dragmode:false" in script

    def test_a_click_on_a_ranked_bar_filters_the_page_by_its_name(self, bookings):
        assert "p.type==='ranking'?pt.y" in _html(bookings)
