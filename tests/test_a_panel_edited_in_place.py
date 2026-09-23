"""A dashboard is edited one panel at a time, and whatever names a panel follows it.

customize_dashboard replaced top-level keys whole, so "make the second chart a
line" meant resending every panel -- and tabs and filter scopes name panels by
slot, so a panel moved or removed by hand left them pointing at the wrong
cards, or at none. `ops` edit the layout in place:

- set_panel {slot, <fields>}: replaces the fields named (null removes one);
  cols, style and place change key by key;
- add_panel {panel, at, tab}, remove_panel {slot}, move_panel {slot, to}.

What has to hold, read off the page that is drawn:

- an op changes what it names and nothing else;
- tabs and filter scopes follow their panels through adds, moves and removes,
  and an edit that would leave one naming nothing is refused by name;
- a detected page, whose slots are chart kinds rather than panels, says how
  to make its layout editable;
- a dry run checks the edit against the data and writes nothing;
- a panel in no tab is refused: a tab hides every card it does not list, so
  such a panel was drawn, reported as success, and never shown.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from servers.data_advanced._adv_dashboard import customize_dashboard, generate_dashboard
from shared.dashboard_spec import MAX_OPS
from tests.dashboard_page import NODE, drawn

needs_node = pytest.mark.skipif(NODE is None, reason="node is not installed")


@pytest.fixture
def sales(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(31)
    n = 240
    path = tmp_path / "sales.csv"
    pd.DataFrame(
        {
            "day": pd.date_range("2024-01-01", periods=n).strftime("%Y-%m-%d"),
            "region": rng.choice(["North", "South", "East"], n),
            "revenue": rng.normal(1000, 150, n).round(2),
            "units": rng.integers(1, 40, n).astype(float),
        }
    ).to_csv(path, index=False)
    return path


KPI = {"chart": "kpi", "cols": {"value": "revenue"}}
BAR = {"chart": "bar", "cols": {"category": "region", "value": "units"}, "style": {"top_n": 2, "color": "#112233"}}
LINE = {"chart": "line", "cols": {"date": "day", "value": "revenue"}}


@pytest.fixture
def page(sales):
    """A three-panel page with two tabs and a filter scoped to the bar."""
    spec = {
        "layout": [KPI, BAR, LINE],
        "tabs": [{"name": "Headline", "slots": [0, 1]}, {"name": "Trend", "slots": [2]}],
        "filters": [{"column": "units", "scope": [1]}, "region"],
    }
    r = generate_dashboard(str(sales), output_path=str(sales.parent / "page.html"), open_after=False, spec=spec)
    assert r["success"] is True, r
    return Path(r["output_path"])


def _edit(page: Path, ops: list, **kw) -> dict:
    return customize_dashboard(str(page), ops=ops, open_after=False, **kw)


def _ok(page: Path, ops: list, **kw) -> dict:
    r = _edit(page, ops, **kw)
    assert r["success"] is True, r
    return r


def _cards(html: str) -> list[str]:
    return re.findall(r'<div id="([^"]+)" style="width:100%;height:100%"></div>', html)


def _tabs(html: str) -> dict[str, list[str]]:
    found = re.findall(r'class="tab-btn" data-tab="\d+" data-cards="([^"]*)"[^>]*>([^<]+)</button>', html)
    return {name: cards.split(",") for cards, name in found}


def _scope(html: str, column: str) -> list[str] | None:
    doc = json.loads(re.search(r"const _FILTERS=(.*?);\n", html).group(1))
    return next(f.get("scope") for f in doc if f["col"] == column)


class TestAnOpChangesWhatItNames:
    def test_set_panel_changes_one_style_field_and_keeps_the_rest(self, page):
        r = _ok(page, [{"op": "set_panel", "slot": 1, "style": {"color": "#ff0000"}, "title": "Units"}])
        panel = r["spec"]["layout"][1]
        assert panel["style"] == {"top_n": 2, "color": "#ff0000"} and panel["title"] == "Units"
        assert r["spec"]["layout"][0] == KPI and r["spec"]["layout"][2] == LINE
        assert r["ops_applied"] == ["slot 1: set style, title"]

    def test_null_removes_a_field_or_a_key(self, page):
        r = _ok(page, [{"op": "set_panel", "slot": 1, "style": {"top_n": None}}])
        assert r["spec"]["layout"][1]["style"] == {"color": "#112233"}
        r = _ok(Path(r["output_path"]), [{"op": "set_panel", "slot": 1, "style": None}])
        assert "style" not in r["spec"]["layout"][1]

    @needs_node
    def test_a_bar_retyped_as_a_line_is_drawn_as_one(self, page, sales):
        ops = [
            {"op": "set_panel", "slot": 1, "chart": "line", "cols": {"category": None, "date": "day"}, "style": None}
        ]
        r = _ok(page, ops)
        assert r["ops_applied"] == ["slot 1: set chart, cols, style (bar -> line)"]
        html = Path(r["output_path"]).read_text(encoding="utf-8")
        assert _cards(html) == ["p1_line", "p2_line"]
        trace = drawn(html)["figures"]["p1_line"]["data"][0]
        df = pd.read_csv(sales)
        want = df.groupby(df["day"].str[:7])["units"].sum()
        assert dict(zip(trace["x"], trace["y"], strict=True)) == pytest.approx(want.to_dict())


class TestWhatNamesAPanelFollowsIt:
    def test_move(self, page):
        r = _ok(page, [{"op": "move_panel", "slot": 1, "to": 0}])
        html = Path(r["output_path"]).read_text(encoding="utf-8")
        assert [p["chart"] for p in r["spec"]["layout"]] == ["bar", "kpi", "line"]
        assert _tabs(html) == {"Headline": ["p0_bar", "p1_kpi"], "Trend": ["p2_line"]}
        assert _scope(html, "units") == ["p0_bar"]

    def test_add_into_a_tab(self, page):
        hist = {"chart": "histogram", "cols": {"value": "units"}}
        r = _ok(page, [{"op": "add_panel", "panel": hist, "at": 0, "tab": "Trend"}])
        html = Path(r["output_path"]).read_text(encoding="utf-8")
        assert _cards(html) == ["p0_histogram", "p2_bar", "p3_line"]
        assert _tabs(html) == {"Headline": ["p1_kpi", "p2_bar"], "Trend": ["p0_histogram", "p3_line"]}
        assert _scope(html, "units") == ["p2_bar"]
        assert r["ops_applied"] == ["added a histogram panel at slot 0 in tab 'Trend'"]

    def test_remove(self, page):
        r = _ok(page, [{"op": "remove_panel", "slot": 0}])
        html = Path(r["output_path"]).read_text(encoding="utf-8")
        assert _tabs(html) == {"Headline": ["p0_bar"], "Trend": ["p1_line"]}
        assert _scope(html, "units") == ["p0_bar"]

    @needs_node
    def test_the_scoped_filter_still_narrows_the_panel_it_named(self, page, sales):
        # `changes` land first, in the slots the caller sees; the ops then move them.
        filters = [{"column": "units", "scope": [1], "default": {"max": 10}}, "region"]
        r = customize_dashboard(
            str(page), {"filters": filters}, ops=[{"op": "move_panel", "slot": 1, "to": 2}], open_after=False
        )
        assert r["success"] is True, r
        assert r["spec"]["filters"][0]["scope"] == [2]
        bar = drawn(Path(r["output_path"]).read_text(encoding="utf-8"))["figures"]["p2_bar"]["data"][0]
        df = pd.read_csv(sales)
        want = df[df["units"] <= 10].groupby("region")["units"].sum().nlargest(2)
        assert dict(zip(bar["x"], bar["y"], strict=True)) == pytest.approx(want.to_dict())


class TestAnEditThatCannotWorkIsRefused:
    @pytest.mark.parametrize(
        ("ops", "says"),
        [
            ([{"op": "swap"}], "ops[0] op='swap' is not an edit. Valid: set_panel, add_panel, remove_panel"),
            ([{"op": "remove_panel", "slot": 3}], "ops[0].slot must be a slot from 0 to 2; got 3"),
            ([{"op": "move_panel", "slot": 0}], "ops[0].to must be a slot from 0 to 2; got None"),
            ([{"op": "set_panel", "slot": 0}], "set_panel names no field to set"),
            ([{"op": "set_panel", "slot": 0, "width": 3}], "set_panel has unknown key(s): width"),
            ([{"op": "add_panel", "panel": {"cols": {}}, "tab": "Trend"}], "add_panel needs a panel with a chart"),
            ([{"op": "add_panel", "panel": LINE}], "adds a panel to a page with tabs (Headline, Trend)"),
            ([{"op": "add_panel", "panel": LINE, "tab": "Other"}], "ops[0].tab='Other' is not a tab on this page"),
            ([{"op": "remove_panel", "slot": 2}], "the ops leave tab 'Trend' with no panels"),
            ([{"op": "remove_panel", "slot": 1}], "the ops remove every panel the filter on 'units' narrows"),
            ([], "ops must be a list of {op, ...}"),
            ([{"op": "move_panel", "slot": 0, "to": 1}] * (MAX_OPS + 1), f"one call takes at most {MAX_OPS}"),
        ],
    )
    def test_by_name(self, page, ops, says):
        before = page.read_bytes()
        r = _edit(page, ops)
        assert r["success"] is False and says in r["error"], r.get("error")
        assert page.read_bytes() == before, "a refused edit writes nothing"

    def test_a_panel_the_data_cannot_draw_is_named_where_it_ended_up(self, page):
        r = _edit(page, [{"op": "move_panel", "slot": 1, "to": 2}, {"op": "set_panel", "slot": 2, "chart": "line"}])
        assert r["success"] is False
        assert r["error"].startswith("after the ops, layout[2]"), r["error"]

    def test_the_last_panel_stays(self, sales):
        r = generate_dashboard(
            str(sales), output_path=str(sales.parent / "one.html"), open_after=False, spec={"layout": [KPI]}
        )
        assert _edit(Path(r["output_path"]), [{"op": "remove_panel", "slot": 0}])["error"].endswith(
            "a dashboard needs one"
        )


class TestADetectedPage:
    def test_says_how_to_make_its_layout_editable(self, sales):
        r = generate_dashboard(str(sales), output_path=str(sales.parent / "auto.html"), open_after=False)
        edited = _edit(Path(r["output_path"]), [{"op": "remove_panel", "slot": 0}])
        assert edited["success"] is False and "this page's layout is the detector's" in edited["error"]
        assert "changes={'layout': [...]}" in edited["error"]

    def test_the_layout_made_yours_in_the_same_call_takes_the_ops(self, sales):
        r = generate_dashboard(str(sales), output_path=str(sales.parent / "auto.html"), open_after=False)
        layout = r["spec"]["layout"]
        again = customize_dashboard(
            r["output_path"], {"layout": layout}, ops=[{"op": "remove_panel", "slot": 0}], open_after=False
        )
        assert again["success"] is True, again
        assert [p["chart"] for p in again["spec"]["layout"]] == [p["chart"] for p in layout[1:]]


class TestADryRun:
    def test_checks_and_writes_nothing(self, page):
        before = page.read_bytes()
        r = _ok(page, [{"op": "move_panel", "slot": 2, "to": 0}], dry_run=True)
        assert r["dry_run"] is True and page.read_bytes() == before
        assert [p["chart"] for p in r["spec"]["layout"]] == ["line", "kpi", "bar"]
        assert r["spec"]["tabs"] == [{"name": "Headline", "slots": [1, 2]}, {"name": "Trend", "slots": [0]}]

    def test_refuses_what_the_data_cannot_draw(self, page):
        r = _edit(page, [{"op": "set_panel", "slot": 0, "cols": {"value": "region"}}], dry_run=True)
        assert r["success"] is False and "value='region' is not numeric" in r["error"]


class TestAPanelNoTabShows:
    def test_is_refused(self, sales):
        spec = {"layout": [KPI, BAR], "tabs": [{"name": "A", "slots": [0]}]}
        r = generate_dashboard(str(sales), output_path=str(sales.parent / "t.html"), open_after=False, spec=spec)
        assert r["success"] is False and "layout slot(s) [1] are in no tab" in r["error"]
