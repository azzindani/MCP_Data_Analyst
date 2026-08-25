"""Four of six panels in a distribution report were captioned with another
panel's name.

Read from the DOM of a sweep artifact, subplot titles beside the trace actually
drawn in each cell:

    1  spends — Histogram        <- histogram of spends        OK
    2  impressions — Histogram   <- box plot  of spends        WRONG
    3  clicks — Histogram        <- histogram of impressions   WRONG
    4  spends — Box Plot         <- box plot  of impressions   WRONG
    5  impressions — Box Plot    <- histogram of clicks        WRONG
    6  clicks — Box Plot         <- box plot  of clicks        OK

make_subplots consumes `subplot_titles` row-major -- (r1c1, r1c2, r2c1, r2c2...)
-- and it was handed every histogram title followed by every box-plot title,
which is column-major. The traces were always in the right cells; only the
captions moved. So a reader comparing distributions saw the spread of spends
labelled "impressions", and nothing in the file was structurally wrong: the page
renders, every figure draws, visual_check passes it.

With a single column the two orderings are identical, which is why this survived
every test that plotted one column.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from servers.data_visual import engine as dv
from shared.plotly_payload import load_figure

COLUMNS = ["spends", "impressions", "clicks"]


def figure_of(html_path: str) -> dict:
    """The Plotly figure as the page hands it to the browser.

    This used to walk the page itself, taking the *first* `Plotly.newPlot(` it
    found. That was a second copy of `shared/plotly_payload.py`, and it broke
    the moment pages went back to carrying their own 4.85 MB of Plotly: the
    library's own source contains that call long before the page's real one, so
    the walk decoded minified JavaScript and raised JSONDecodeError. One
    extractor, in the module that owns it.
    """
    text = Path(html_path).read_text(encoding="utf-8")
    data, layout = load_figure(text)
    return {"data": data, "layout": layout}


def titles_and_traces(html_path: str) -> list[tuple[str, str, str]]:
    """(subplot title, trace type, trace name) in subplot order."""
    fig = figure_of(html_path)
    titles = [a["text"] for a in fig["layout"].get("annotations", [])]
    traces = [(t.get("type", ""), t.get("name", "")) for t in fig["data"]]
    assert len(titles) == len(traces), (len(titles), len(traces))
    return [(title, kind, name) for title, (kind, name) in zip(titles, traces)]


@pytest.fixture()
def three_column_plot(ad_data_full_csv: Path, tmp_path: Path) -> str:
    out = str(tmp_path / "dist.html")
    r = dv.generate_distribution_plot(str(ad_data_full_csv), COLUMNS, output_path=out, open_after=False)
    assert r["success"] is True, r.get("error")
    return out


class TestEveryPanelIsCaptionedWithItsOwnColumn:
    def test_the_title_names_the_column_the_trace_holds(self, three_column_plot: str):
        for title, _kind, name in titles_and_traces(three_column_plot):
            assert name and name in title, f"{title!r} holds a trace named {name!r}"

    def test_the_title_names_the_kind_of_chart_actually_drawn(self, three_column_plot: str):
        for title, kind, _name in titles_and_traces(three_column_plot):
            expected = "Histogram" if kind == "histogram" else "Box Plot"
            assert expected in title, f"{title!r} holds a {kind}"

    def test_the_rows_are_column_pairs(self, three_column_plot: str):
        """Each row is one column's histogram beside its own box plot."""
        pairs = titles_and_traces(three_column_plot)
        for i in range(0, len(pairs), 2):
            assert pairs[i][2] == pairs[i + 1][2], (pairs[i], pairs[i + 1])
            assert pairs[i][1] == "histogram" and pairs[i + 1][1] == "box"

    def test_every_requested_column_appears_exactly_twice(self, three_column_plot: str):
        names = [name for _t, _k, name in titles_and_traces(three_column_plot)]
        for column in COLUMNS:
            assert names.count(column) == 2, (column, names)

    def test_the_columns_keep_the_order_they_were_asked_for(self, three_column_plot: str):
        names = [name for _t, _k, name in titles_and_traces(three_column_plot)]
        assert names == [c for c in COLUMNS for _ in range(2)], names


class TestOtherColumnCounts:
    @pytest.mark.parametrize("count", [1, 2, 4])
    def test_the_pairing_holds(self, ad_data_full_csv: Path, tmp_path: Path, count: int):
        columns = ["spends", "impressions", "clicks", "link_clicks"][:count]
        out = str(tmp_path / f"dist{count}.html")
        r = dv.generate_distribution_plot(str(ad_data_full_csv), columns, output_path=out, open_after=False)
        assert r["success"] is True, r.get("error")
        for title, kind, name in titles_and_traces(out):
            assert name in title, f"{title!r} holds a trace named {name!r}"
            assert ("Histogram" if kind == "histogram" else "Box Plot") in title

    def test_one_column_is_still_two_panels(self, ad_data_full_csv: Path, tmp_path: Path):
        out = str(tmp_path / "one.html")
        r = dv.generate_distribution_plot(str(ad_data_full_csv), ["spends"], output_path=out, open_after=False)
        assert r["success"] is True, r.get("error")
        assert r["chart_count"] == 2
        assert len(titles_and_traces(out)) == 2


class TestTheReportStillDescribesItself:
    def test_the_page_heading_lists_the_columns(self, three_column_plot: str):
        """save_chart hoists the figure title into the page <h1> and clears it
        from the layout, so the heading is where the reader sees it."""
        page = Path(three_column_plot).read_text(encoding="utf-8")
        heading = page.split('class="chart-title">', 1)[1].split("</h1>", 1)[0]
        for column in COLUMNS:
            assert column in heading, heading

    def test_the_result_reports_the_columns_it_plotted(self, ad_data_full_csv: Path, tmp_path: Path):
        r = dv.generate_distribution_plot(
            str(ad_data_full_csv), COLUMNS, output_path=str(tmp_path / "d.html"), open_after=False
        )
        assert r["columns_plotted"] == COLUMNS
