"""Tests for servers/data_visual/_adv_customize.py's customize_chart().

customize_chart was the one data_visual tool with zero test coverage,
found via a full tool-inventory cross-check against tests/*.py. Fixing it
surfaced two real bugs: sort_bars/highlight were accepted parameters that
were never wired to any logic, and the pre-existing (unused) helper
_extract_plotly_json's regex didn't tolerate the whitespace real Plotly
output has between `Plotly.newPlot(` and the quoted chart id, so it never
actually matched real chart HTML.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from servers.data_visual.engine import customize_chart, generate_chart


@pytest.fixture()
def bar_chart(simple_csv: Path, tmp_path: Path) -> Path:
    out = tmp_path / "chart.html"
    result = generate_chart(
        str(simple_csv),
        "bar",
        "Revenue",
        category_column="Region",
        agg_func="sum",
        output_path=str(out),
        open_after=False,
    )
    assert result["success"] is True
    return out


class TestCustomizeChartBasic:
    def test_title_change(self, bar_chart: Path, tmp_path: Path):
        out = tmp_path / "out.html"
        result = customize_chart(str(bar_chart), title="Revenue by Region", output_path=str(out))
        assert result["success"] is True
        assert "title → 'Revenue by Region'" in result["changes_applied"]
        assert "Revenue by Region" in out.read_text(encoding="utf-8")

    def test_no_params_is_an_error(self, bar_chart: Path):
        result = customize_chart(str(bar_chart))
        assert result["success"] is False
        assert "hint" in result

    def test_file_not_found(self, tmp_path: Path):
        result = customize_chart(str(tmp_path / "ghost.html"))
        assert result["success"] is False
        assert "not found" in result["error"].lower()

    def test_wrong_file_type(self, tmp_path: Path):
        bad = tmp_path / "notes.txt"
        bad.write_text("hello", encoding="utf-8")
        result = customize_chart(str(bad), title="X")
        assert result["success"] is False


def _parsed(html: str) -> tuple[list, dict]:
    """Return the chart's traces and layout, as the browser would parse them."""
    import json

    from servers.data_visual._adv_customize import _split_newplot

    _, _, data_str, _, layout_str, _ = _split_newplot(html)
    return json.loads(data_str), json.loads(layout_str)


def _heading(html: str) -> str | None:
    """The caption the reader actually sees.

    A customized title used to be written into layout["title"], inside the SVG,
    where plotly clips it and the page ends up captioned twice: the stale <h1>
    above the chart and the new title cut off within it. It now goes to the
    heading, which is where save_chart() puts every other chart's title.
    See test_customize_chart_retitles_the_page.py.
    """
    import re

    m = re.search(r'<h1 class="chart-title">([^<]*)</h1>', html)
    return m.group(1) if m else None


class TestCustomizedChartStillRenders:
    """A chart whose JSON no longer parses is a blank page in the browser, but
    still a structurally valid HTML file on disk — so only parsing the payload
    back out catches it. Text-substituting the JSON used to append an unbalanced
    brace per replacement, and every customized chart rendered blank."""

    def test_title_change_leaves_payload_parseable(self, bar_chart: Path, tmp_path: Path):
        out = tmp_path / "out.html"
        result = customize_chart(str(bar_chart), title="Revenue by Region", output_path=str(out))
        assert result["success"] is True
        traces, layout = _parsed(out.read_text(encoding="utf-8"))
        assert traces
        assert "title" not in layout
        assert _heading(out.read_text(encoding="utf-8")) == "Revenue by Region"

    def test_title_does_not_leak_into_axis_titles(self, bar_chart: Path, tmp_path: Path):
        out = tmp_path / "out.html"
        customize_chart(str(bar_chart), title="Only The Chart", output_path=str(out))
        _, layout = _parsed(out.read_text(encoding="utf-8"))
        for axis in ("xaxis", "yaxis"):
            axis_title = layout.get(axis, {}).get("title")
            text = axis_title.get("text") if isinstance(axis_title, dict) else axis_title
            assert text != "Only The Chart"

    def test_axis_labels_are_independent(self, bar_chart: Path, tmp_path: Path):
        out = tmp_path / "out.html"
        result = customize_chart(str(bar_chart), title="T", x_label="Region", y_label="Revenue", output_path=str(out))
        assert result["success"] is True
        _, layout = _parsed(out.read_text(encoding="utf-8"))
        assert _heading(out.read_text(encoding="utf-8")) == "T"
        assert layout["xaxis"]["title"]["text"] == "Region"
        assert layout["yaxis"]["title"]["text"] == "Revenue"

    def test_every_option_at_once_still_parses(self, bar_chart: Path, tmp_path: Path):
        out = tmp_path / "out.html"
        result = customize_chart(
            str(bar_chart),
            title="All Options",
            x_label="X",
            y_label="Y",
            color_scheme=["#111111", "#222222"],
            show_value_labels=True,
            annotations=[{"x": 0, "y": 1, "text": "peak"}],
            width=900,
            height=600,
            output_path=str(out),
        )
        assert result["success"] is True
        traces, layout = _parsed(out.read_text(encoding="utf-8"))
        assert traces
        assert layout["width"] == 900 and layout["height"] == 600
        assert layout["annotations"][0]["text"] == "peak"
        assert layout["colorway"] == ["#111111", "#222222"]

    def test_customizing_a_customized_chart_still_parses(self, bar_chart: Path, tmp_path: Path):
        once = tmp_path / "once.html"
        twice = tmp_path / "twice.html"
        customize_chart(str(bar_chart), title="First", output_path=str(once))
        result = customize_chart(str(once), title="Second", output_path=str(twice))
        assert result["success"] is True
        _, layout = _parsed(twice.read_text(encoding="utf-8"))
        assert layout
        assert _heading(twice.read_text(encoding="utf-8")) == "Second"

    def test_colors_reach_the_bars(self, bar_chart: Path, tmp_path: Path):
        out = tmp_path / "out.html"
        result = customize_chart(str(bar_chart), color_scheme=["#abcdef", "#fedcba"], output_path=str(out))
        assert result["success"] is True
        traces, _ = _parsed(out.read_text(encoding="utf-8"))
        assert traces[0]["marker"]["color"][0] == "#abcdef"

    def test_sorting_an_integer_valued_chart_works(self, tmp_path: Path):
        """Plotly encodes small integers as i1/i2, not f8. Assuming float64 read
        zero values out of the buffer and sorting the chart failed."""
        from servers.data_visual._adv_customize import _decode_plotly_y

        csv = tmp_path / "ints.csv"
        csv.write_text("Region,Revenue\nNorth,3\nSouth,9\nEast,1\nWest,7\n", encoding="utf-8")
        chart = tmp_path / "ints.html"
        generate_chart(str(csv), "bar", "Revenue", category_column="Region", output_path=str(chart), open_after=False)
        out = tmp_path / "sorted.html"
        result = customize_chart(str(chart), sort_bars="asc", output_path=str(out))
        assert result["success"] is True
        traces, _ = _parsed(out.read_text(encoding="utf-8"))
        assert _decode_plotly_y(traces[0]["y"]) == [1, 3, 7, 9]

    def test_corrupt_chart_is_rejected_not_silently_written(self, tmp_path: Path):
        broken = tmp_path / "broken.html"
        broken.write_text("<html><body><script>Plotly.newPlot('c', [{'x':</script></body></html>")
        result = customize_chart(str(broken), title="X")
        assert result["success"] is False
        assert "hint" in result


class TestCustomizeChartSortBars:
    def test_sort_desc_reorders_categories_by_real_value(self, bar_chart: Path, tmp_path: Path):
        out = tmp_path / "sorted.html"
        result = customize_chart(str(bar_chart), sort_bars="desc", output_path=str(out))
        assert result["success"] is True
        assert "bars sorted desc" in result["changes_applied"]

        import json

        from servers.data_visual._adv_customize import _decode_plotly_y, _extract_plotly_json

        html = out.read_text(encoding="utf-8")
        _, _, data_str, _, _ = _extract_plotly_json(html)
        trace = json.loads(data_str)[0]
        values = _decode_plotly_y(trace["y"])
        assert values == sorted(values, reverse=True)

    def test_sort_asc(self, bar_chart: Path, tmp_path: Path):
        out = tmp_path / "sorted.html"
        result = customize_chart(str(bar_chart), sort_bars="asc", output_path=str(out))
        assert result["success"] is True

        import json

        from servers.data_visual._adv_customize import _decode_plotly_y, _extract_plotly_json

        html = out.read_text(encoding="utf-8")
        _, _, data_str, _, _ = _extract_plotly_json(html)
        trace = json.loads(data_str)[0]
        values = _decode_plotly_y(trace["y"])
        assert values == sorted(values)

    def test_invalid_direction_is_a_clear_error(self, bar_chart: Path):
        result = customize_chart(str(bar_chart), sort_bars="sideways")
        assert result["success"] is False
        assert "asc" in result["hint"] and "desc" in result["hint"]


class TestCustomizeChartHighlight:
    def test_highlighted_category_gets_distinct_color(self, bar_chart: Path, tmp_path: Path):
        out = tmp_path / "highlighted.html"
        result = customize_chart(str(bar_chart), highlight=["West"], output_path=str(out))
        assert result["success"] is True
        assert "1 category highlighted" in result["changes_applied"]

        import json

        from servers.data_visual._adv_customize import _extract_plotly_json

        html = out.read_text(encoding="utf-8")
        _, _, data_str, _, _ = _extract_plotly_json(html)
        trace = json.loads(data_str)[0]
        colors = trace["marker"]["color"]
        west_idx = trace["x"].index("West")
        other_idx = 1 - west_idx
        assert colors[west_idx] == "#EF553B"
        assert colors[other_idx] != "#EF553B"

    def test_sort_and_highlight_together(self, bar_chart: Path, tmp_path: Path):
        out = tmp_path / "both.html"
        result = customize_chart(str(bar_chart), sort_bars="desc", highlight=["West"], output_path=str(out))
        assert result["success"] is True
        assert len(result["changes_applied"]) == 2
