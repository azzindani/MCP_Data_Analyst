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
        assert "Revenue by Region" in out.read_text()

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
        bad.write_text("hello")
        result = customize_chart(str(bad), title="X")
        assert result["success"] is False


class TestCustomizeChartSortBars:
    def test_sort_desc_reorders_categories_by_real_value(self, bar_chart: Path, tmp_path: Path):
        out = tmp_path / "sorted.html"
        result = customize_chart(str(bar_chart), sort_bars="desc", output_path=str(out))
        assert result["success"] is True
        assert "bars sorted desc" in result["changes_applied"]

        import json

        from servers.data_visual._adv_customize import _decode_plotly_y, _extract_plotly_json

        html = out.read_text()
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

        html = out.read_text()
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

        html = out.read_text()
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
