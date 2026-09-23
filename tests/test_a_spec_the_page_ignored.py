"""A dashboard spec must reach the page, not only the response.

generate_dashboard validated a spec, echoed it back as `spec`, and then drew the
page from auto-detection anyway. Reproduced with
`{"kpis": ["revenue"], "filters": ["region"], "layout": [{"chart": "bar",
"cols": {"category": "channel", "value": "units"}, "agg": "mean"}]}`: the page
drew four bar charts of other columns plus a box plot, four KPI cards and both
filters, under success: true. The tests that covered this asserted on the echo,
so they passed.

Every assertion here reads the rendered HTML: which cards exist, which KPI
labels and filter controls are on the page, what each chart's JavaScript reads.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_advanced")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _adv_dashboard import customize_dashboard, generate_dashboard  # noqa: E402

from shared.dashboard_spec import LAYOUT_SOURCE_KEY  # noqa: E402


@pytest.fixture
def sales(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(21)
    n = 150
    path = tmp_path / "sales.csv"
    pd.DataFrame(
        {
            "day": pd.date_range("2024-01-01", periods=n).strftime("%Y-%m-%d"),
            "region": rng.choice(["North", "South", "East"], n),
            "channel": rng.choice(["web", "store"], n),
            "order_ref": [f"ORD-{i:05d}" for i in range(n)],
            "constant": ["same"] * n,
            "revenue": rng.normal(1000, 200, n).round(2),
            "units": rng.integers(1, 50, n),
        }
    ).to_csv(path, index=False)
    return path


def page(r: dict) -> str:
    assert r["success"] is True, r
    return Path(r["output_path"]).read_text(encoding="utf-8")


def cards(html: str) -> list[str]:
    return re.findall(r'<div id="([^"]+)" style="width:100%;height:100%"></div>', html)


def kpi_labels(html: str) -> list[str]:
    return re.findall(r'<div class="kpi-lbl">([^<]+)</div>', html)


def make(sales, **kw) -> dict:
    return generate_dashboard(str(sales), open_after=False, **kw)


class TestKpisAndFilters:
    def test_kpis_reach_the_page(self, sales):
        html = page(make(sales, spec={"kpis": ["revenue"]}))
        assert kpi_labels(html) == ["Quality Score", "Total revenue"]

    def test_filters_reach_the_page(self, sales):
        html = page(make(sales, spec={"filters": ["region"]}))
        assert 'data-col="region"' in html
        assert 'data-col="channel"' not in html
        assert "numCh(" not in html.split('<div class="kpi-row">')[0], "no numeric range was asked for"

    def test_a_numeric_filter_becomes_a_range(self, sales):
        html = page(make(sales, spec={"filters": ["units"]}))
        assert "numCh('units'" in html
        assert 'class="pills"' not in html

    def test_the_defaults_describe_the_page_that_is_drawn(self, sales):
        r = make(sales)
        html = page(r)
        # Quality Score plus one card per resolved KPI -- the spec used to name
        # eight over a row of seven.
        assert len(kpi_labels(html)) == 1 + len(r["spec"]["kpis"])
        for col in r["spec"]["filters"]:
            assert f'data-col="{col}"' in html or f"numCh('{col}'" in html

    def test_a_text_kpi_is_refused(self, sales):
        r = make(sales, spec={"kpis": ["region"]})
        assert r["success"] is False
        assert "not numeric" in r["error"] and "revenue" in r["error"]

    @pytest.mark.parametrize(
        ("column", "why"),
        [("constant", "1 distinct"), ("order_ref", "150 distinct values")],
    )
    def test_a_filter_that_could_not_work_is_refused(self, sales, column, why):
        r = make(sales, spec={"filters": [column]})
        assert r["success"] is False
        assert why in r["error"]


class TestACallerLayout:
    def test_it_draws_exactly_its_panels(self, sales):
        spec = {"layout": [{"chart": "pie", "cols": {"category": "region", "value": "revenue"}}]}
        r = make(sales, spec=spec)
        html = page(r)
        assert cards(html) == ["p0_pie"], "one panel, one card -- no detected extras"
        assert "(+r['revenue']||0)" in html, "the pie sums the value it was given"
        assert r["charts_included"] == ["pie"]

    def test_columns_and_agg_are_the_panels_own(self, sales):
        spec = {"layout": [{"chart": "bar", "cols": {"category": "channel", "value": "units"}, "agg": "mean"}]}
        html = page(make(sales, spec=spec))
        assert cards(html) == ["p0_bar"]
        assert "Avg units by channel" in html
        rf = html.split("function rf_p0_bar(d)")[1].split("function rf_")[0]
        assert "r['channel']" in rf and "r['units']" in rf and "cnt[k]" in rf

    def test_an_empty_panel_is_filled_by_the_detector(self, sales):
        spec = {"layout": [{"chart": "scatter"}, {"chart": "histogram"}, {"chart": "box", "cols": {}}]}
        html = page(make(sales, spec=spec))
        assert cards(html) == ["p0_scatter", "p1_histogram", "p2_box"]

    def test_a_named_text_date_is_read_as_a_date(self, sales):
        spec = {"layout": [{"chart": "line", "cols": {"date": "day", "value": "revenue"}}]}
        html = page(make(sales, spec=spec))
        assert cards(html) == ["p0_line"]
        assert "Total revenue Over Time" in html

    def test_tabs_address_panels(self, sales):
        spec = {
            "layout": [
                {"chart": "bar", "cols": {"category": "region", "value": "revenue"}},
                {"chart": "pie", "cols": {"category": "channel", "value": "units"}},
            ],
            "tabs": [{"name": "Revenue", "slots": [0]}, {"name": "Mix", "slots": [1]}],
        }
        html = page(make(sales, spec=spec))
        assert 'data-cards="p0_bar"' in html and 'data-cards="p1_pie"' in html


class TestRefusals:
    @pytest.mark.parametrize(
        ("panel", "needle"),
        [
            ({"chart": "bar", "width": 2}, "unknown key(s): width"),
            ({"chart": "bar", "cols": {"categry": "region", "category": "region", "value": "units"}}, "categry"),
            ({"chart": "bar", "cols": {"category": "channel", "value": "region"}}, "value='region' is not numeric"),
            ({"chart": "scatter", "agg": "mean"}, "agg applies to"),
            ({"chart": "pie", "cols": {"category": "region", "value": "units"}, "agg": "mean"}, "only agg is sum"),
            ({"chart": "bar", "agg": "mode"}, "'mode' is not one of"),
            ({"chart": "line", "cols": {"date": "region", "value": "units"}}, "is not read as a date"),
        ],
    )
    def test_a_panel_that_cannot_be_drawn_as_written_is_refused(self, sales, panel, needle):
        r = make(sales, spec={"layout": [panel]})
        assert r["success"] is False
        assert needle in r["error"], r["error"]

    def test_a_role_nothing_can_fill_is_refused(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
        path = tmp_path / "nodates.csv"
        pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}).to_csv(path, index=False)
        r = generate_dashboard(str(path), open_after=False, spec={"layout": [{"chart": "line"}]})
        assert r["success"] is False
        assert "no date column" in r["error"] and "cols.date" in r["error"]


class TestTheRoundTrip:
    def test_the_default_page_is_unchanged(self, sales):
        html = page(make(sales))
        ids = cards(html)
        assert "corr_hm" in ids and any(i.startswith("dist_") for i in ids)
        assert not any(i.startswith("p0_") for i in ids)

    def test_customizing_a_detected_page_keeps_the_detection(self, sales):
        first = make(sales)
        assert first["spec"][LAYOUT_SOURCE_KEY] == "detected"
        r = customize_dashboard(first["output_path"], {"title": "Renamed"}, open_after=False)
        assert cards(page(r)) == cards(page(first))

    def test_customizing_the_layout_makes_it_the_callers(self, sales):
        first = make(sales)
        r = customize_dashboard(first["output_path"], {"layout": [{"chart": "histogram"}]}, open_after=False)
        html = page(r)
        assert cards(html) == ["p0_histogram"]
        assert LAYOUT_SOURCE_KEY not in r["spec"]

    def test_a_geo_page_can_be_customised(self, tmp_path, monkeypatch):
        # The detected layout said "geo_choropleth", which the validator refuses,
        # so handing a geo dashboard's own spec back failed.
        monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
        path = tmp_path / "geo.csv"
        pd.DataFrame({"country": ["France", "Germany", "Japan", "Brazil"] * 5, "sales": np.arange(20.0)}).to_csv(
            path, index=False
        )
        first = generate_dashboard(str(path), open_after=False)
        assert first["success"] is True, first
        kinds = [p["chart"] for p in first["spec"]["layout"]]
        assert "geo_choropleth" not in kinds
        r = customize_dashboard(first["output_path"], {"title": "Geo"}, open_after=False)
        assert r["success"] is True, r
