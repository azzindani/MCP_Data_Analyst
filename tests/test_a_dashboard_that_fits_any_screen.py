"""A generated dashboard fits a desktop, a tablet and a phone, each as that screen would want it.

At 820 px the 12-column placement had collapsed to one column, so four KPI numbers took 700 px of scrolling;
on a phone the filters filled two screens before the first chart, the tiles stacked one per row, and a legend
as wide as the screen ran under the chart toolbar. These pin what each screen gets, and each was run once with
its fix switched off. How it looks is checked in a real browser at 1440, 820 and 390 px.
"""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from servers.data_advanced._adv_dashboard import _PLACE_CSS, generate_dashboard
from servers.data_advanced._dash_ext import EXT_CSS, EXT_JS
from tests.dashboard_page import NODE, drawn, run_js

needs_node = pytest.mark.skipif(NODE is None, reason="node is not installed")


@pytest.fixture
def bookings(tmp_path, monkeypatch) -> Path:
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(31)
    n = 400
    path = tmp_path / "bookings.csv"
    pd.DataFrame(
        {
            "booking_date": pd.date_range("2024-01-01", periods=n).strftime("%Y-%m-%d"),
            "is_canceled": rng.choice([0, 1], n, p=[0.7, 0.3]),
            "channel": rng.choice(["Direct", "Agent", "Online", "Corporate"], n),
            "revenue": rng.normal(200, 15, n).round(2),
        }
    ).to_csv(path, index=False)
    return path


def _html(path: Path, name: str = "page", **spec) -> str:
    out = path.parent / f"{name}.html"
    result = generate_dashboard(str(path), output_path=str(out), open_after=False, spec=spec or None)
    assert result["success"] is True, result.get("error")
    return out.read_text(encoding="utf-8")


class TestTheGridFollowsTheScreen:
    def test_a_card_says_its_span_to_the_rules_for_narrower_screens(self, bookings):
        html = _html(bookings)
        spans = re.findall(r'style="grid-column:span (\d+)[^"]*" data-span="(\d+)"', html)
        assert spans and all(a == b for a, b in spans)

    def test_a_tablet_halves_the_tiles_and_gives_a_wide_card_the_row(self):
        tablet = _PLACE_CSS[
            _PLACE_CSS.index("@media(max-width:68.75rem)") : _PLACE_CSS.index("@media(max-width:40rem)")
        ]
        assert ".cgrid.g12>.cc{grid-column:span 12!important" in tablet
        assert all(f'data-span="{n}"' in tablet for n in range(1, 7)) and "span 6!important" in tablet
        assert 'data-span="7"' not in tablet and 'data-span="8"' not in tablet

    def test_a_phone_keeps_only_the_small_tiles_two_across(self):
        phone = _PLACE_CSS[_PLACE_CSS.index("@media(max-width:40rem)") :]
        assert all(f'data-span="{n}"' in phone for n in (4, 5, 6)) and "span 12!important" in phone
        assert 'data-span="3"' not in phone and 'data-span="1"' not in phone

    def test_a_tile_reads_its_own_width_to_stack_its_number_and_line(self):
        assert ".cc-kpi{container-type:inline-size}" in EXT_CSS
        query = EXT_CSS[EXT_CSS.index("@container") :]
        assert "flex-direction:column" in query and ".kpi-spark{flex:none;width:100%" in query


class TestThePhoneFoldsItsFilters:
    def test_the_filter_bar_leads_with_a_button_that_is_hidden_until_a_phone(self, bookings):
        html = _html(bookings)
        bar = html[html.index('<div class="filter-bar">') :]
        assert bar.startswith(
            '<div class="filter-bar">\n<button type="button" class="ftoggle" aria-expanded="false" onclick="fbToggle(this)">'
        )
        assert ".ftoggle{display:none" in EXT_CSS
        assert (
            "@media(max-width:40rem){.ftoggle{display:inline-flex}.filter-bar:not(.open)>.fgrp{display:none}}"
            in EXT_CSS
        )

    @needs_node
    def test_the_button_opens_and_closes_the_bar_and_says_so(self, bookings):
        out = run_js(
            _html(bookings),
            "(function(){var open=false,aria='';var bar={classList:{toggle:function(){open=!open;return open;}}};"
            "var b={closest:function(){return bar;},setAttribute:function(k,v){aria=v;}};"
            "fbToggle(b);var a=[open,aria];fbToggle(b);return{opened:a,closed:[open,aria]};})()",
        )
        assert out == {"opened": [True, "true"], "closed": [False, "false"]}

    def test_the_filters_in_force_are_counted_on_the_button_and_named_as_the_bar_labels_them(self, bookings):
        html = _html(bookings)
        assert "document.querySelectorAll('.fcount').forEach" in html and "_lab(c)" in html


class TestAFingerHasRoom:
    def test_controls_and_the_chart_toolbar_grow_on_a_coarse_pointer(self):
        rules = EXT_CSS[EXT_CSS.index("@media(pointer:coarse)") :]
        for sel in (".pill", ".btn", ".tab-btn", ".cc-hdr .exp", ".ddbtn"):
            assert sel in rules.split("}")[0], sel
        assert "min-height:2.5rem" in rules and ".js-plotly-plot .modebar-btn{font-size:22px!important" in rules

    def test_a_phone_shows_the_toolbar_when_a_chart_is_touched(self):
        assert (
            "else if((typeof window!=='undefined'&&window.innerWidth||1024)<640)PCFG.displayModeBar='hover';" in EXT_JS
        )
        assert EXT_JS.index("PCFG.displayModeBar=false") < EXT_JS.index(
            "PCFG.displayModeBar='hover'"
        )  # an opt-out wins


@needs_node
class TestALegendMakesRoomForTheToolbar:
    def _legend(self, html: str, width: int, extra: str = "") -> tuple[str, bool]:
        out = run_js(
            html,
            f"(function(){{__figs={{}};__el('p0_line').clientWidth={width};{extra}renderAll(_RAW);"
            "var f=__figs['p0_line'];return[f.layout.legend.orientation,_NW['p0_line']];})()",
        )
        return out[0], out[1]

    LAYOUT = {"layout": [{"chart": "line", "cols": {"date": "booking_date", "value": "revenue"}}]}

    def test_a_chart_as_wide_as_a_phone_stacks_its_legend_down_the_left(self, bookings):
        html = _html(bookings, **self.LAYOUT)
        assert self._legend(html, 360) == ("v", True)

    def test_a_wide_chart_keeps_its_legend_across_the_top(self, bookings):
        html = _html(bookings, **self.LAYOUT)
        assert self._legend(html, 900) == ("h", False)

    def test_a_legend_the_panel_placed_itself_stays_where_it_was_put(self, bookings):
        layout = [{**self.LAYOUT["layout"][0], "style": {"legend": "top"}}]
        html = _html(bookings, name="placed", layout=layout)
        assert self._legend(html, 360)[0] == "h"

    def test_turning_the_phone_draws_the_chart_again_for_its_new_width(self, bookings):
        html = _html(bookings, **self.LAYOUT)
        out = run_js(
            html,
            "(function(){var el=__el('p0_line');el.clientWidth=900;renderAll(_RAW);var wide=_NW['p0_line'];"
            "el.clientWidth=360;globalThis.setTimeout=function(f){f();return 0;};"
            "(__lis.resize||[]).forEach(function(f){f();});return[wide,_NW['p0_line']];})()",
        )
        assert out == [False, True]


@needs_node
def test_print_draws_the_cards_of_tabs_nobody_opened(bookings):
    layout = [
        {"chart": "bar", "cols": {"category": "channel", "value": "revenue"}},
        {"chart": "pie", "cols": {"category": "channel"}},
    ]
    out = run_js(
        _html(bookings, layout=layout),
        "(function(){__figs={};var el=__el('p1_pie');el.closest=function(){return{style:{display:'none'}};};renderAll(_RAW);"
        "var before=Object.keys(__figs);(__lis.beforeprint||[]).forEach(function(f){f();});return[before,Object.keys(__figs).sort()];})()",
    )
    assert out == [["p0_bar"], ["p0_bar", "p1_pie"]], "print lays out every tab, so the hidden chart is drawn first"
