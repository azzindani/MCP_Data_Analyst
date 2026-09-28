"""The panels a professional page is made of draw the numbers their names promise.

A stacked bar that shares out a whole, a Pareto whose line reaches 100%, a
waterfall whose steps add up to the total, a variance against target coloured
by whether it is good news, one small chart per segment, a gauge against its
target, and words that stay words. Each is checked on the figure the page's
own renderer hands Plotly (tests/dashboard_page.py), against pandas.
"""

from __future__ import annotations

import re
import shutil
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_advanced")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _adv_dashboard import generate_dashboard  # noqa: E402

from tests.dashboard_page import drawn, run_js  # noqa: E402

pytestmark = pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")

ROWS = [
    {"region": "North", "channel": "web", "sales": 50.0, "target": 40.0, "cost": 9.0},
    {"region": "North", "channel": "store", "sales": 30.0, "target": 40.0, "cost": 4.0},
    {"region": "South", "channel": "web", "sales": 10.0, "target": 25.0, "cost": 2.0},
    {"region": "South", "channel": "store", "sales": 15.0, "target": 25.0, "cost": 6.0},
    {"region": "East", "channel": "web", "sales": 5.0, "target": 2.0, "cost": 1.0},
]
FRAME = pd.DataFrame(ROWS)


@pytest.fixture
def page(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    pd.DataFrame(ROWS).to_csv(tmp_path / "s.csv", index=False)

    def make(*panels, **spec):
        r = generate_dashboard(
            str(tmp_path / "s.csv"),
            spec={"layout": list(panels), **spec},
            output_path=str(tmp_path / "s.html"),
            open_after=False,
        )
        assert r["success"] is True, r.get("error")
        return Path(r["output_path"]).read_text(encoding="utf-8")

    return make


def _figure(html: str, prefix: str) -> dict:
    figures = drawn(html, rows=ROWS)["figures"]
    return next(f for cid, f in figures.items() if cid.startswith(prefix))


def test_a_stacked_bar_is_each_groups_part_of_its_category(page):
    f = _figure(
        page({"chart": "stacked_bar", "cols": {"category": "region", "group": "channel", "value": "sales"}}), "p0_"
    )
    got = {t["name"]: dict(zip(t["x"], t["y"], strict=True)) for t in f["data"]}
    want = FRAME.pivot_table(index="channel", columns="region", values="sales", aggfunc="sum", fill_value=0)
    for channel, row in want.iterrows():
        assert got[channel] == pytest.approx(row.to_dict())
    assert f["layout"]["barmode"] == "stack" and "barnorm" not in f["layout"]


def test_normalized_it_is_a_share_of_each_bar(page):
    panel = {"chart": "stacked_bar", "cols": {"category": "region", "group": "channel", "value": "sales"}}
    f = _figure(page({**panel, "style": {"normalize": True}}), "p0_")
    assert f["layout"]["barnorm"] == "percent"


def test_a_pareto_line_climbs_to_the_whole(page):
    f = _figure(page({"chart": "pareto", "cols": {"category": "region", "value": "sales"}}), "p0_")
    bars, line = f["data"]
    want = FRAME.groupby("region")["sales"].sum().sort_values(ascending=False)
    assert bars["x"] == list(want.index) and bars["y"] == pytest.approx(list(want))
    assert line["y"] == pytest.approx(list(want.cumsum() / want.sum() * 100))


def test_a_waterfall_adds_up_to_its_total(page):
    f = _figure(page({"chart": "waterfall", "cols": {"category": "region", "value": "sales"}}), "p0_")
    t = f["data"][0]
    assert t["x"] == ["North", "South", "East", "Total"], "every region once, and no Other of them all"
    assert t["measure"][-1] == "total"
    assert sum(t["y"][:-1]) == pytest.approx(t["y"][-1]) == pytest.approx(FRAME["sales"].sum())


def test_a_variance_is_actual_less_target_coloured_by_good_news(page):
    f = _figure(
        page({"chart": "variance", "cols": {"category": "region", "value": "sales", "target": "target"}}), "p0_"
    )
    t = f["data"][0]
    by = FRAME.groupby("region")[["sales", "target"]].sum()
    got = dict(zip(t["x"], t["y"], strict=True))
    assert got == pytest.approx((by["sales"] - by["target"]).to_dict())
    colours = dict(zip(t["x"], t["marker"]["color"], strict=True))
    assert colours["North"] == colours["East"] == "#2da44e" and colours["South"] == "#cf222e"


def test_small_multiples_are_one_chart_per_segment_on_their_own_axes(page):
    f = _figure(
        page({"chart": "small_multiples", "cols": {"facet": "region", "value": "sales", "category": "channel"}}), "p0_"
    )
    assert len(f["data"]) == 3
    assert [t["xaxis"] for t in f["data"]] == ["x", "x2", "x3"]
    assert [a["text"] for a in f["layout"]["annotations"]] == ["<b>North</b>", "<b>South</b>", "<b>East</b>"]


def test_a_gauge_reads_its_value_against_its_target(page):
    f = _figure(page({"chart": "gauge", "cols": {"value": "sales"}, "style": {"target": 120}}), "p0_")
    ind = f["data"][0]
    assert ind["value"] == pytest.approx(FRAME["sales"].sum())
    assert ind["delta"]["reference"] == 120 and ind["gauge"]["threshold"]["value"] == 120


def test_words_stay_words(page):
    html = page(
        {"chart": "bar", "cols": {"category": "region", "value": "sales"}},
        {
            "chart": "markdown",
            "text": "**Up** <img src=x onerror=alert(1)> [a](javascript:alert(3)) [b](https://example.org)",
        },
        {"chart": "callout", "title": "Note", "text": "<script>alert(2)</script>"},
    )
    # Inside a script block (the embedded spec) the text is inert JSON; outside one it would be markup.
    markup = re.sub(r"<script\b.*?</script>", "", html, flags=re.S | re.I)
    assert "<b>Up</b>" in markup
    assert "<img src=x onerror" not in markup and "<script>alert(2)" not in markup
    assert "&lt;script&gt;alert(2)&lt;/script&gt;" in markup, "shown as the text it is"
    assert 'href="javascript' not in markup and 'href="https://example.org"' in markup, "a link is http(s) or text"


def test_a_click_filters_every_other_panel_to_that_bar(page):
    html = page(
        {"chart": "bar", "cols": {"category": "region", "value": "sales"}},
        {"chart": "kpi", "cols": {"value": "sales"}},
    )
    kept = run_js(html, "(function(){_CLK['region']='North';return getFilt().map(function(r){return r.sales;});})()")
    assert sorted(kept) == [30.0, 50.0]


def test_a_drill_redraws_the_bar_by_its_child(page):
    html = page({"chart": "bar", "cols": {"category": "region", "value": "sales"}, "style": {"drill": "channel"}})
    got = run_js(
        html,
        "(function(){_DRILL['p0_bar']='North';renderAll(_RAW);var t=__figs['p0_bar'].data[0];"
        "var o={};t.x.forEach(function(k,i){o[k]=t.y[i];});return o;})()",
    )
    assert got == {"web": 50.0, "store": 30.0}
