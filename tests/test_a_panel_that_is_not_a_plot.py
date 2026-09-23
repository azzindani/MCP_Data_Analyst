"""A dashboard is more than plots: headings, notes, headline numbers and small tables.

Every card used to be a Plotly figure. A person building a dashboard for
someone else also needs a heading that splits the page, a sentence saying what
the numbers mean, one big number, and a short ranked table -- and without them
a model wrote the HTML by hand. Four panel kinds, drawn by the same renderer:

- `section`: a heading across the grid;
- `text`: a note, escaped, in its own words;
- `kpi`: one number, computed in the page from the filtered rows;
- `table`: the top_n groups and their aggregate, also from the filtered rows.

A value from the data reaches the page as text, never as markup.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from servers.data_advanced._adv_dashboard import generate_dashboard
from tests.dashboard_page import NODE, drawn

needs_node = pytest.mark.skipif(NODE is None, reason="node is not installed")


@pytest.fixture
def sales(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(5)
    n = 160
    path = tmp_path / "sales.csv"
    pd.DataFrame(
        {
            "region": rng.choice(["North", "South", "East", "<img src=x onerror=alert(1)>"], n),
            "revenue": rng.normal(1000, 150, n).round(2),
            "units": rng.integers(1, 40, n).astype(float),
        }
    ).to_csv(path, index=False)
    return path


def _build(path: Path, layout: list, **spec) -> dict:
    out = path.parent / f"k{abs(hash(json.dumps(layout, sort_keys=True)))}.html"
    return generate_dashboard(str(path), output_path=str(out), open_after=False, spec={"layout": layout, **spec})


def _html(path: Path, layout: list, **spec) -> str:
    r = _build(path, layout, **spec)
    assert r["success"] is True, r
    return Path(r["output_path"]).read_text(encoding="utf-8")


class TestSectionAndText:
    def test_a_section_is_a_heading_across_the_grid(self, sales):
        html = _html(sales, [{"chart": "section", "title": "Headline <numbers>"}, {"chart": "bar"}])
        assert '<div class="cc-sec" id="p0_section">Headline &lt;numbers&gt;</div>' in html

    def test_a_note_is_its_own_words_escaped(self, sales):
        html = _html(sales, [{"chart": "text", "title": "Read me", "text": "Net of refunds.\n<b>not bold</b>"}])
        assert '<p class="ptext">Net of refunds.\n&lt;b&gt;not bold&lt;/b&gt;</p>' in html
        assert "<h3>Read me</h3>" in html


@needs_node
class TestKpi:
    def test_the_number_is_the_aggregate_of_the_rows(self, sales):
        layout = [{"chart": "kpi", "cols": {"value": "revenue"}, "style": {"format": "integer", "prefix": "$"}}]
        body = drawn(_html(sales, layout))["html"]["p0_kpi"]
        total = pd.read_csv(sales)["revenue"].sum()
        assert f'<div class="kpi-big">${round(total):,}</div>' in body

    def test_it_follows_the_filter(self, sales):
        layout = [{"chart": "kpi", "cols": {"value": "units"}, "agg": "max", "style": {"color": "#123456"}}]
        rows = [r for r in pd.read_csv(sales).to_dict(orient="records") if r["region"] == "North"]
        body = drawn(_html(sales, layout), rows=rows)["html"]["p0_kpi"]
        assert f'<div class="kpi-big" style="color:#123456">{int(max(r["units"] for r in rows))}</div>' in body
        assert f"over {len(rows)} rows" in body


@needs_node
class TestTable:
    def test_the_top_groups_and_their_totals(self, sales):
        layout = [
            {
                "chart": "table",
                "cols": {"category": "region", "value": "revenue"},
                "style": {"top_n": 2, "format": "decimal"},
            }
        ]
        body = drawn(_html(sales, layout))["html"]["p0_table"]
        want = pd.read_csv(sales).groupby("region")["revenue"].sum().nlargest(2)
        cells = re.findall(r'<td>(.*?)</td><td class="num">(.*?)</td>', body)
        assert [name for name, _ in cells] == [k.replace("<", "&lt;").replace(">", "&gt;") for k in want.index]
        assert [float(v.replace(",", "")) for _, v in cells] == pytest.approx([round(v, 2) for v in want])
        assert "<th>region</th>" in body and "Total revenue" in body

    def test_a_value_from_the_data_is_text_not_markup(self, sales):
        layout = [{"chart": "table", "cols": {"category": "region", "value": "units"}, "style": {"top_n": 10}}]
        body = drawn(_html(sales, layout))["html"]["p0_table"]
        assert "<img" not in body and "&lt;img src=x onerror=alert(1)&gt;" in body


class TestWhatTheyTakeIsChecked:
    @pytest.mark.parametrize(
        ("panel", "says"),
        [
            ({"chart": "text"}, "is a text panel and needs text"),
            ({"chart": "bar", "text": "hi"}, "only a text panel takes text"),
            ({"chart": "section"}, "is a section, a heading across the grid, and needs a title"),
            (
                {"chart": "section", "title": "S", "cols": {"value": "units"}},
                "section chart, which has no role(s) value",
            ),
            ({"chart": "text", "text": "x", "agg": "sum"}, "draws values as they are; agg applies to"),
            ({"chart": "kpi", "cols": {"value": "region"}}, "value='region' is not numeric"),
            ({"chart": "kpi", "style": {"bins": 4}}, "a kpi chart does not draw 'bins'"),
        ],
    )
    def test_refused_by_name(self, sales, panel, says):
        r = _build(sales, [panel])
        assert r["success"] is False and says in r["error"], r.get("error")


class TestTheyLiveOnThePageLikeAnyCard:
    def test_tabs_and_place_address_them(self, sales):
        layout = [
            {"chart": "section", "title": "Top"},
            {"chart": "kpi", "place": {"span": 4}},
            {"chart": "text", "text": "note", "place": {"span": 8}},
            {"chart": "table"},
        ]
        html = _html(sales, layout, tabs=[{"name": "A", "slots": [0, 1, 2]}, {"name": "B", "slots": [3]}])
        assert 'data-cards="p0_section,p1_kpi,p2_text"' in html and 'data-cards="p3_table"' in html
        assert re.findall(r"grid-column:span (\d+)", html) == ["4", "8", "6"]

    @needs_node
    def test_nothing_is_drawn_twice_and_nothing_warns(self, sales):
        out = drawn(
            _html(sales, [{"chart": "section", "title": "S"}, {"chart": "kpi"}, {"chart": "table"}, {"chart": "bar"}])
        )
        assert out["warnings"] == []
        assert set(out["figures"]) == {"p3_bar"} and set(out["html"]) >= {"p1_kpi", "p2_table"}
