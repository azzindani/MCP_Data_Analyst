"""A dashboard over tens of thousands of rows stays usable: it draws what is seen, and only a sample of marks.

A 73,100-row page spent 1.1 of every 1.6 seconds of a redraw on one scatter chart in a tab nobody had opened:
every filter click re-plotted all 73,100 points in SVG. On a phone or a tablet that is a page that freezes for
many seconds per click. These pin the two causes -- a hidden tab is not drawn, and a scatter draws a sample --
and each was run once with its fix switched off.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from servers.data_advanced._adv_dashboard import generate_dashboard
from tests.dashboard_page import NODE, drawn, run_js

needs_node = pytest.mark.skipif(NODE is None, reason="node is not installed")


def _csv(tmp_path: Path, monkeypatch, rows: int) -> Path:
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(7)
    x = rng.normal(0, 1, rows)
    path = tmp_path / f"points_{rows}.csv"
    pd.DataFrame(
        {
            "x": x.round(4),
            "y": (0.6 * x + rng.normal(0, 1, rows)).round(4),
            "kind": rng.choice(["a", "b", "c"], rows),
            "n": rng.integers(0, 100, rows),
        }
    ).to_csv(path, index=False)
    return path


def _html(path: Path, layout: list[dict]) -> str:
    out = path.parent / "p.html"
    result = generate_dashboard(str(path), output_path=str(out), open_after=False, spec={"layout": layout})
    assert result["success"] is True, result.get("error")
    return out.read_text(encoding="utf-8")


SCATTER = [{"chart": "scatter", "cols": {"x": "x", "y": "y"}}]
GROUPED = [{"chart": "scatter", "cols": {"x": "x", "y": "y", "group": "kind"}}]


@needs_node
class TestAScatterDrawsASampleAndFitsAll:
    def test_twenty_thousand_rows_draw_six_thousand_marks_and_say_so(self, tmp_path, monkeypatch):
        path = _csv(tmp_path, monkeypatch, 20_000)
        figure = drawn(_html(path, SCATTER))["figures"]["p0_scatter"]
        marks, line = figure["data"]
        assert len(marks["x"]) == len(marks["y"]) == 6000
        notes = [a["text"] for a in figure["layout"]["annotations"]]
        assert any("a sample of 6,000 of 20,000 points" in t for t in notes), notes

    def test_the_line_and_r_are_fitted_to_every_row_not_the_sample(self, tmp_path, monkeypatch):
        path = _csv(tmp_path, monkeypatch, 20_000)
        frame = pd.read_csv(path)
        line = drawn(_html(path, SCATTER))["figures"]["p0_scatter"]["data"][1]
        assert line["name"] == f"r={frame['x'].corr(frame['y']):.2f}"
        slope, intercept = np.polyfit(frame["x"], frame["y"], 1)
        assert line["y"][0] == pytest.approx(slope * line["x"][0] + intercept, abs=1e-6)
        assert line["x"] == [frame["x"].min(), frame["x"].max()], "it runs the whole range, not the sample's"

    def test_the_sample_is_spread_across_the_rows_not_the_first_of_them(self, tmp_path, monkeypatch):
        path = _csv(tmp_path, monkeypatch, 20_000)
        frame = pd.read_csv(path)
        marks = drawn(_html(path, SCATTER))["figures"]["p0_scatter"]["data"][0]
        assert marks["x"][0] == frame["x"].iloc[0] and marks["x"][-1] == frame["x"].iloc[int(5999 * 20_000 / 6000)]

    def test_a_chart_a_few_marks_can_hold_is_drawn_whole_with_no_note(self, tmp_path, monkeypatch):
        path = _csv(tmp_path, monkeypatch, 1_000)
        figure = drawn(_html(path, SCATTER))["figures"]["p0_scatter"]
        assert len(figure["data"][0]["x"]) == 1_000
        assert not figure["layout"].get("annotations")

    def test_groups_share_the_budget_and_each_keeps_its_own_line(self, tmp_path, monkeypatch):
        path = _csv(tmp_path, monkeypatch, 20_000)
        figure = drawn(_html(path, GROUPED))["figures"]["p0_scatter"]
        marks = [t for t in figure["data"] if t["mode"] == "markers"]
        lines = [t for t in figure["data"] if t["mode"] == "lines"]
        assert len(marks) == len(lines) == 3
        assert sum(len(t["x"]) for t in marks) <= 6000
        assert all(len(t["x"]) >= 1500 for t in marks)


HIDE = (
    "(function(){__figs={};var el=__el('p1_scatter');"
    "el.closest=function(){return{style:{display:'none'}};};renderAll(_RAW);"
    "var hiddenDrawn=Object.keys(__figs);var stale=Object.keys(_STALE);"
    "el.closest=function(){return{style:{display:''}};};renderStale();"
    "return{hiddenDrawn:hiddenDrawn,stale:stale,after:Object.keys(__figs),staleAfter:Object.keys(_STALE)};})()"
)


@needs_node
class TestAHiddenTabIsNotDrawnUntilItOpens:
    LAYOUT = [{"chart": "bar", "cols": {"category": "kind", "value": "n"}}, *SCATTER]

    def test_a_redraw_skips_what_is_hidden_and_marks_it_stale(self, tmp_path, monkeypatch):
        path = _csv(tmp_path, monkeypatch, 3_000)
        out = run_js(_html(path, self.LAYOUT), HIDE)
        assert out["hiddenDrawn"] == ["p0_bar"] and out["stale"] == ["p1_scatter"]

    def test_opening_the_tab_draws_it_once_with_the_filters_in_force(self, tmp_path, monkeypatch):
        path = _csv(tmp_path, monkeypatch, 3_000)
        out = run_js(_html(path, self.LAYOUT), HIDE)
        assert sorted(out["after"]) == ["p0_bar", "p1_scatter"] and out["staleAfter"] == []

    def test_a_page_with_no_tabs_draws_everything_at_once(self, tmp_path, monkeypatch):
        path = _csv(tmp_path, monkeypatch, 3_000)
        assert sorted(drawn(_html(path, self.LAYOUT))["figures"]) == ["p0_bar", "p1_scatter"]

    def test_the_tab_switch_asks_for_the_stale_charts(self, tmp_path, monkeypatch):
        html = _html(_csv(tmp_path, monkeypatch, 500), self.LAYOUT)
        assert "card.style.display=(!keep.length||keep.indexOf(id)>=0)?'':'none';\n    });\n    renderStale();" in html
