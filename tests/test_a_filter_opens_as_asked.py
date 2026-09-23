"""A dashboard filter is a control, a default and a scope -- and the page says what it is filtering.

`filters` was a list of column names: each got whatever control the column's
type implied, every page opened on every row, every filter narrowed every
panel, and a date column was refused because the bar had no control for one.
An entry may now be {column, control, default, scope}:

- `control`: pills or dropdown for a list, range for numbers, date_range for days;
- `default`: the values, or the {min, max}, selected when the page opens;
- `scope`: "page", or the layout slots the filter narrows -- the KPI row, the
  row count, the rows table and the export follow the page's filters only.

What has to hold, read off what the page's own script draws and writes:

- a filter that cannot work is refused by name, including a default that
  would open the page (or one of its panels) on no rows;
- the page opens on its defaults, and a line says what is filtered;
- a scoped filter narrows its panels and nothing else;
- a pill's value is the text the page compares. Python wrote a boolean as
  'True' and a whole float as '3.0' where the page reads 'true' and '3', so
  choosing one of those pills filtered every row away;
- the tab's saved filters are this page's own: one key served every
  dashboard, so a range set on one narrowed the next one opened in that tab,
  with its inputs empty.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from servers.data_advanced._adv_dashboard import generate_dashboard
from tests.dashboard_page import NODE, drawn, run_js

needs_node = pytest.mark.skipif(NODE is None, reason="node is not installed")


@pytest.fixture
def sales(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(23)
    n = 300
    path = tmp_path / "sales.csv"
    pd.DataFrame(
        {
            "day": pd.date_range("2024-01-01", periods=n).strftime("%Y-%m-%d"),
            "region": rng.choice(["North", "South", "East", "West"], n),
            "flag": rng.choice([True, False], n),
            "store_id": rng.choice([1.0, 2.0, 3.0], n),
            "revenue": rng.normal(1000, 200, n).round(2),
            "units": rng.integers(1, 50, n).astype(float),
        }
    ).to_csv(path, index=False)
    return path


LAYOUT = [
    {"chart": "kpi", "cols": {"value": "revenue"}},
    {"chart": "bar", "cols": {"category": "region", "value": "units"}},
    {"chart": "line", "cols": {"date": "day", "value": "revenue"}},
    {"chart": "text", "text": "A note."},
]


def _build(path: Path, **spec) -> dict:
    out = path.parent / f"f{abs(hash(json.dumps(spec, sort_keys=True)))}.html"
    return generate_dashboard(str(path), output_path=str(out), open_after=False, spec=spec)


def _page(path: Path, **spec) -> str:
    r = _build(path, **spec)
    assert r["success"] is True, r
    return Path(r["output_path"]).read_text(encoding="utf-8")


def _frame(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


OPENED = "{figs:__figs,rows:__el('row-ctr').textContent,said:__el('fsum').textContent,kpi:__el('p0_kpi').innerHTML}"


class TestWhatCannotWorkIsRefused:
    @pytest.mark.parametrize(
        ("entry", "says"),
        [
            ({"column": "region", "control": "range"}, "control='range' does not fit 'region', a text column"),
            ({"column": "day", "control": "pills"}, "a date column; valid: date_range"),
            ({"column": "revenue", "control": "pills"}, "distinct values; a list filter offers at most 100"),
            ({"column": "region", "default": ["Nowhere"]}, "names value(s) 'region' does not hold: 'Nowhere'"),
            ({"column": "region", "default": "North"}, "a list of values of 'region', e.g. ['East', 'North']"),
            ({"column": "units", "default": {"min": 60}}, "default keeps no rows of 'units'"),
            ({"column": "units", "default": {"min": 10, "max": 5}}, "has min 10 above max 5"),
            ({"column": "units", "default": {"low": 1}}, "default of a range is {'min': ..., 'max': ...}"),
            ({"column": "units", "default": {"min": "ten"}}, "default.min must be a number"),
            ({"column": "day", "default": {"min": "03/01/2024"}}, "must be a date written YYYY-MM-DD"),
            ({"column": "day", "default": {"min": "2024-02-30"}}, "must be a date written YYYY-MM-DD"),
            ({"column": "units", "scope": [9]}, "refers to slot(s) [9] but layout has 4 panel(s)"),
            ({"column": "units", "scope": "all"}, "scope is 'page' or a list of the layout slots"),
            ({"column": "units", "scope": [3]}, "names slot 3, a text panel, which draws no rows"),
            ({"column": "units", "width": 2}, "unknown key(s): width. A filter takes: column, control, default"),
            ({"control": "pills"}, "filters[0] needs a column"),
            (7, "filters[0] must be a column name or a dict"),
        ],
    )
    def test_an_entry(self, sales, entry, says):
        r = _build(sales, layout=LAYOUT, filters=[entry])
        assert r["success"] is False and says in r["error"], r.get("error")

    def test_a_column_named_twice(self, sales):
        r = _build(sales, filters=["region", {"column": "region", "control": "dropdown"}])
        assert r["success"] is False and "filters name 'region' twice" in r["error"]

    def test_a_scope_on_a_page_with_no_layout_of_its_own(self, sales):
        r = _build(sales, filters=[{"column": "units", "scope": [0]}])
        assert r["success"] is False and "this spec has no layout of its own" in r["error"]

    def test_defaults_that_together_keep_no_rows(self, sales):
        df = _frame(sales)
        first = df.iloc[0]
        other = next(r for r in ("North", "South", "East", "West") if r != first["region"])
        filters = [{"column": "region", "default": [other]}, {"column": "day", "default": {"max": first["day"]}}]
        r = _build(sales, layout=LAYOUT, filters=filters)
        assert r["success"] is False and "the defaults together keep no rows" in r["error"]
        scoped = [dict(f, scope=[1]) for f in filters]
        r = _build(sales, layout=LAYOUT, filters=scoped)
        assert r["success"] is False and "the defaults on slot 1 together keep no rows" in r["error"]


@needs_node
class TestThePageOpensOnItsDefaults:
    FILTERS = [
        {"column": "region", "default": ["North", "South"]},
        {"column": "day", "default": {"min": "2024-03-01"}},
        {"column": "units", "default": {"max": 10}, "scope": [1]},
    ]

    def test_the_rows_and_every_figure(self, sales):
        opened = run_js(_page(sales, layout=LAYOUT, filters=self.FILTERS), OPENED)
        df = _frame(sales)
        page = df[df["region"].isin(["North", "South"]) & (df["day"] >= "2024-03-01")]
        assert opened["rows"] == f"{len(page)} of {len(df)} rows"
        assert f"over {len(page)} rows" in opened["kpi"]
        line = opened["figs"]["p2_line"]["data"][0]
        want = page.groupby(page["day"].str[:7])["revenue"].sum()
        assert dict(zip(line["x"], line["y"], strict=True)) == pytest.approx(want.to_dict())

    def test_a_scoped_filter_narrows_its_panel_and_nothing_else(self, sales):
        opened = run_js(_page(sales, layout=LAYOUT, filters=self.FILTERS), OPENED)
        df = _frame(sales)
        page = df[df["region"].isin(["North", "South"]) & (df["day"] >= "2024-03-01")]
        bar = opened["figs"]["p1_bar"]["data"][0]
        want = page[page["units"] <= 10].groupby("region")["units"].sum()
        assert dict(zip(bar["x"], bar["y"], strict=True)) == pytest.approx(want.to_dict())
        # The KPI panel and the line are the page's rows, not the bar's.
        assert f"over {len(page)} rows" in opened["kpi"]

    def test_a_line_says_what_is_filtered(self, sales):
        opened = run_js(_page(sales, layout=LAYOUT, filters=self.FILTERS), OPENED)
        assert opened["said"] == (
            "Filtered: region: North, South · day ≥ 2024-03-01 · units ≤ 10 (on Total units by region)"
        )

    def test_a_page_with_no_defaults_says_nothing_and_shows_everything(self, sales):
        opened = run_js(_page(sales, layout=LAYOUT, filters=["region", "day", "units"]), OPENED)
        assert opened["said"] == "" and opened["rows"] == "300 of 300 rows"

    def test_a_default_of_every_value_is_no_filter(self, sales):
        filters = [{"column": "region", "default": ["North", "South", "East", "West"]}]
        opened = run_js(_page(sales, layout=LAYOUT, filters=filters), OPENED)
        assert opened["said"] == "" and opened["rows"] == "300 of 300 rows"


class TestTheControls:
    def test_a_date_column_gets_a_date_range_over_its_days(self, sales):
        html = _page(sales, filters=["day"])
        assert '<div class="nrng" data-col="day">' in html
        dates = re.findall(r'<input type="date" class="ninp dinp" min="([^"]+)" max="([^"]+)"', html)
        assert dates == [("2024-01-01", "2024-10-26")] * 2

    def test_a_control_is_the_one_asked_for(self, sales):
        html = _page(
            sales, filters=[{"column": "region", "control": "dropdown"}, {"column": "units", "control": "pills"}]
        )
        assert '<div class="ddw" data-col="region">' in html and '<div class="pills" data-col="units">' in html

    def test_a_numeric_id_with_too_many_values_for_a_list_gets_a_range(self, tmp_path, monkeypatch):
        # It used to get a list control with no values at all, dropped silently.
        monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
        path = tmp_path / "orders.csv"
        pd.DataFrame({"order_id": range(1000, 1300), "amount": np.arange(300) % 7 + 1.0}).to_csv(path, index=False)
        html = _page(path, filters=["order_id"])
        assert '<div class="nrng" data-col="order_id">' in html

    @needs_node
    def test_every_pill_is_text_the_page_compares(self, sales):
        html = _page(sales, filters=["flag", "store_id", "region"])
        df = _frame(sales)
        for col in ("flag", "store_id", "region"):
            block = re.search(rf'<div class="pills" data-col="{col}">(.*?)</div>', html, flags=re.S)
            assert block, col
            values = re.findall(r'data-val="([^"]*)"', block.group(1))
            # The rows each pill keeps, by the page's own test of a list filter.
            count = "function(v){_CF[" + json.dumps(col) + "]=new Set([v]);return getFilt().length;}"
            counts = run_js(html, f"{json.dumps(values)}.map({count})")
            assert all(counts) and sum(counts) == len(df), (col, values, counts)


@needs_node
class TestTheTabsSavedFiltersAreThisPagesOwn:
    def _key(self, html: str) -> str:
        return json.loads(re.search(r"const _FKEY=(.*?);\n", html).group(1))

    def test_a_saved_range_is_restored_and_said(self, sales):
        html = _page(sales, filters=["region", "units"])
        saved = {self._key(html): json.dumps({"units": {"min": None, "max": 10}, "region": ["East"]})}
        opened = run_js(html, OPENED.replace(",kpi:__el('p0_kpi').innerHTML", ""), storage=saved)
        df = _frame(sales)
        keep = df[(df["units"] <= 10) & (df["region"] == "East")]
        assert opened["rows"] == f"{len(keep)} of {len(df)} rows"
        assert opened["said"] == "Filtered: region: East · units ≤ 10"

    def test_the_session_wins_over_the_defaults(self, sales):
        html = _page(sales, filters=[{"column": "region", "default": ["North"]}])
        opened = run_js(html, OPENED.replace(",kpi:__el('p0_kpi').innerHTML", ""), storage={self._key(html): "{}"})
        assert opened["rows"] == "300 of 300 rows", "the reader cleared the default this session"

    def test_another_pages_filters_are_not_this_ones(self, sales):
        html = _page(sales, filters=["units"])
        other = {"dash-filters": json.dumps({"cf": {}, "nf": {"units": {"min": None, "max": 10}}})}
        other["dash-filters:0000000000000000"] = json.dumps({"units": {"min": None, "max": 10}})
        opened = run_js(html, "__el('row-ctr').textContent", storage=other)
        assert opened == "300 of 300 rows"

    def test_a_saved_filter_with_no_control_on_the_page_is_dropped(self, sales):
        html = _page(sales, filters=["units"])
        saved = {self._key(html): json.dumps({"region": ["East"], "revenue": {"min": 0, "max": 1}})}
        assert run_js(html, "__el('row-ctr').textContent", storage=saved) == "300 of 300 rows"

    def test_the_key_is_the_data_and_the_filters(self, sales):
        a = self._key(_page(sales, filters=["units"]))
        b = self._key(_page(sales, filters=["units", "region"]))
        assert a != b and a.startswith("dash-filters:")


class TestItRoundTrips:
    def test_the_spec_returns_the_entries_and_customize_keeps_them(self, sales):
        from servers.data_advanced._adv_dashboard import customize_dashboard

        filters = [{"column": "region", "default": ["North"], "control": "dropdown"}, "day"]
        r = _build(sales, layout=LAYOUT, filters=filters)
        assert r["success"] is True, r
        assert r["spec"]["filters"] == filters and r["filter_columns"] == ["region", "day"]
        again = customize_dashboard(r["output_path"], {"title": "Renamed"}, open_after=False)
        assert again["success"] is True, again
        assert again["spec"]["filters"] == filters

    @needs_node
    def test_the_harness_draws_a_scoped_panel_from_the_subset_too(self, sales):
        # drawn() hands renderAll the rows directly, as the filter bar does.
        html = _page(sales, layout=LAYOUT, filters=[{"column": "units", "default": {"max": 10}, "scope": [1]}])
        bar = drawn(html)["figures"]["p1_bar"]["data"][0]
        df = _frame(sales)
        want = df[df["units"] <= 10].groupby("region")["units"].sum()
        assert dict(zip(bar["x"], bar["y"], strict=True)) == pytest.approx(want.to_dict())
