"""A metric is defined once and computed one way: in the response, in the page, per group.

The sweep's dashboard summed each numeric column and had no CTR, CPC or CPM,
so "Facebook buys impressions 18x cheaper than Google" was on no page. And a
ratio averaged row by row is not the ratio a business means: a platform's CTR
is its clicks over its impressions, not the mean of every row's CTR. These
tests hold the metric to that -- a ratio of sums, whatever it is sliced by --
and hold the page's renderer to the same arithmetic as shared/metrics.py.
"""

from __future__ import annotations

import math
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

from shared.metrics import (  # noqa: E402
    MetricError,
    auto_ratios,
    better_of,
    by_group,
    evaluate_tree,
    match_role,
    parameters_of,
    spec_metrics,
    value,
)
from tests.dashboard_page import drawn  # noqa: E402

needs_node = pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")

# Row CTRs 10%, 1%, 30%, 0.5%: their mean is nowhere near either platform's
# clicks over impressions.
ROWS = [
    {"platform": "A", "device": "mobile", "day": "2024-01-05", "clicks": 1, "impressions": 10, "spend": 5.0},
    {"platform": "A", "device": "desktop", "day": "2024-01-20", "clicks": 10, "impressions": 1000, "spend": 20.0},
    {"platform": "B", "device": "mobile", "day": "2024-02-03", "clicks": 30, "impressions": 100, "spend": 9.0},
    {"platform": "B", "device": "desktop", "day": "2024-02-09", "clicks": 2, "impressions": 400, "spend": 4.0},
]
FRAME = pd.DataFrame(ROWS)
MEASURES = ["clicks", "impressions", "spend"]


def _ratios() -> dict:
    return {m.name: m for m in auto_ratios(FRAME, MEASURES)}


def test_the_ratios_the_data_has_parts_for_are_found():
    assert set(_ratios()) == {"CTR", "CPC", "CPM"}


def test_ctr_per_platform_is_its_clicks_over_its_impressions():
    per = by_group(_ratios()["CTR"], FRAME, ["platform"])
    assert per["A"] == pytest.approx(11 / 1010)
    assert per["B"] == pytest.approx(32 / 500)
    row_mean = (FRAME["clicks"] / FRAME["impressions"]).groupby(FRAME["platform"]).mean()
    assert per["A"] != pytest.approx(row_mean["A"])


def test_cpm_is_per_thousand():
    assert value(_ratios()["CPM"], FRAME) == pytest.approx(38.0 / 1510 * 1000)


def test_an_exact_name_is_chosen_over_one_that_contains_the_word():
    assert match_role("clicks", ["link_clicks", "clicks"]) == "clicks"
    assert match_role("clicks", ["link_clicks", "impressions"]) == "link_clicks"


def test_a_denominator_that_is_all_zero_gives_no_ratio():
    frame = FRAME.assign(impressions=0)
    assert "CTR" not in {m.name for m in auto_ratios(frame, MEASURES)}


def test_down_is_good_for_a_cost_per_and_neither_for_spend():
    assert better_of("CPC") == "down"
    assert better_of("spend") == ""
    assert better_of("clicks") == "up"


def test_a_row_formula_is_summed_after_it_is_computed():
    frame = pd.DataFrame({"price": [2.0, 3.0], "qty": [10, 1]})
    (m,) = spec_metrics(frame, {"Revenue": "sum(price * qty)"})
    assert value(m, frame) == pytest.approx(23.0)
    assert m.tree["agg"] == "sum" and m.tree["col"] in m.hidden


def test_a_parameter_is_read_where_the_metric_names_it():
    params = parameters_of({"growth": {"default": 0.1, "min": 0, "max": 0.5, "step": 0.01}})
    (m,) = spec_metrics(FRAME, {"Projected clicks": "sum(clicks) * (1 + $growth)"}, params)
    assert value(m, FRAME, params) == pytest.approx(43 * 1.1)
    assert evaluate_tree(m.tree, FRAME, {"growth": 0.5}) == pytest.approx(43 * 1.5)


def test_an_unknown_parameter_is_refused_naming_the_ones_there_are():
    with pytest.raises(MetricError, match=r"\$rate .* Parameters: growth"):
        spec_metrics(FRAME, {"X": "sum(clicks) * $rate"}, {"growth": 0.1})


def test_an_aggregate_the_page_cannot_recompute_is_refused():
    with pytest.raises(MetricError, match="recomputes for no filter"):
        spec_metrics(FRAME, {"P90": "percentile(clicks, 90)"})


def test_a_metric_named_like_a_column_is_refused():
    with pytest.raises(MetricError, match="also a column"):
        spec_metrics(FRAME, {"clicks": "sum(clicks)"})


def test_dividing_by_nothing_is_not_a_number():
    (m,) = spec_metrics(FRAME, {"Per nothing": "sum(clicks) / sum(spend * 0)"})
    assert math.isnan(value(m, FRAME))


# ---------------------------------------------------------------------------
# The page computes the same numbers
# ---------------------------------------------------------------------------


@pytest.fixture
def page(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    pd.DataFrame(ROWS * 3).to_csv(tmp_path / "a.csv", index=False)

    def make(spec):
        r = generate_dashboard(
            str(tmp_path / "a.csv"), spec=spec, output_path=str(tmp_path / "a.html"), open_after=False
        )
        assert r["success"] is True, r.get("error")
        return r, Path(r["output_path"]).read_text(encoding="utf-8")

    return make


@needs_node
def test_the_bar_draws_each_platforms_ratio_of_sums(page):
    _, html = page({"layout": [{"chart": "bar", "cols": {"category": "platform", "value": "CTR"}}]})
    trace = drawn(html, rows=ROWS)["figures"]["p0_bar"]["data"][0]
    got = dict(zip(trace["x"], trace["y"], strict=True))
    want = by_group(_ratios()["CTR"], FRAME, ["platform"])
    assert got == pytest.approx(want.to_dict())


@needs_node
def test_the_page_and_python_agree_on_a_filtered_subset(page):
    _, html = page(
        {
            "metrics": {"Cost per click": {"formula": "sum(spend) / sum(clicks)", "unit": "currency"}},
            "layout": [{"chart": "bar", "cols": {"category": "platform", "value": "Cost per click"}}],
        }
    )
    subset = [r for r in ROWS if r["device"] == "mobile"]
    trace = drawn(html, rows=subset)["figures"]["p0_bar"]["data"][0]
    (m,) = spec_metrics(FRAME, {"Cost per click": "sum(spend) / sum(clicks)"})
    want = by_group(m, pd.DataFrame(subset), ["platform"])
    assert dict(zip(trace["x"], trace["y"], strict=True)) == pytest.approx(want.to_dict())


@needs_node
def test_a_kpi_shows_the_ratio_in_its_unit(page):
    _, html = page({"layout": [{"chart": "kpi", "cols": {"value": "CTR"}}]})
    body = drawn(html, rows=ROWS)["html"]["p0_kpi"]
    assert f">{43 / 1510 * 100:.2f}%<" in body


def test_the_response_names_every_metric_with_its_formula(page):
    r, _ = page({"metrics": {"Cost per click": "sum(spend) / sum(clicks)"}, "layout": [{"chart": "kpi"}]})
    assert r["metrics"]["CTR"]["formula"] == "sum(clicks) / sum(impressions)"
    assert r["metrics"]["Cost per click"]["unit"] == "currency"


@pytest.mark.parametrize("chart", ["stacked_bar", "pareto", "waterfall"])
def test_a_ratio_is_refused_where_bars_add_up_to_a_whole(page, tmp_path, chart):
    cols = {"category": "platform", "value": "CTR"}
    if chart == "stacked_bar":
        cols["group"] = "device"
    r = generate_dashboard(
        str(tmp_path / "a.csv"),
        spec={"layout": [{"chart": chart, "cols": cols}]},
        output_path=str(tmp_path / "b.html"),
        open_after=False,
    )
    assert r["success"] is False
    assert "CTR is a ratio" in r["error"] and "draw CTR as a bar" in r["error"]


def test_a_sum_metric_is_drawn_where_bars_add_up(page):
    r, _ = page(
        {
            "metrics": {"Paid clicks": "sum(clicks)"},
            "layout": [{"chart": "pareto", "cols": {"category": "platform", "value": "Paid clicks"}}],
        }
    )
    assert r["success"] is True
