"""Content returned inline has to render for the caller who received it.

`return_content=True` used to hand back HTML that loaded `plotly.min.js` from
beside itself. In the shared folder that works; anywhere else — a chat client, an
email, a temp directory — it is a blank page reading "Plotly is not defined",
proven with a headless browser before this module existed. The file on disk is
still the interactive chart; only the copy travelling in the response is drawn
here instead.
"""

from __future__ import annotations

import base64
from pathlib import Path

import pytest

try:
    from servers.data_advanced.engine import generate_chart

    HAS_ADVANCED = True
except ImportError:
    HAS_ADVANCED = False

from shared.svg_chart import figure_to_svg, standalone_html

pytestmark = pytest.mark.skipif(not HAS_ADVANCED, reason="data_advanced deps unavailable")


def _inline_html(result: dict) -> str:
    return base64.b64decode(result["content_base64"]).decode("utf-8")


@pytest.fixture()
def chart_csv(tmp_path: Path) -> Path:
    csv = tmp_path / "sales.csv"
    csv.write_text(
        "region,month,revenue\n"
        "West,2024-01,5000\nEast,2024-01,7500\nNorth,2024-01,3200\n"
        "West,2024-02,6100\nEast,2024-02,8200\nNorth,2024-02,2900\n"
    )
    return csv


class TestInlineContentIsSelfContained:
    @pytest.mark.parametrize("chart_type,category", [("bar", "region"), ("pie", "region"), ("line", "month")])
    def test_returned_content_needs_no_sibling_file(self, chart_csv, tmp_path, chart_type, category):
        result = generate_chart(
            str(chart_csv),
            chart_type,
            "revenue",
            category_column=category,
            output_path=str(tmp_path / f"{chart_type}.html"),
            open_after=False,
            return_content=True,
        )
        assert result["success"] is True
        html = _inline_html(result)
        assert "plotly.min.js" not in html
        assert "<svg" in html

    def test_the_file_on_disk_is_still_the_interactive_chart(self, chart_csv, tmp_path):
        out = tmp_path / "chart.html"
        generate_chart(
            str(chart_csv),
            "bar",
            "revenue",
            category_column="region",
            output_path=str(out),
            open_after=False,
            return_content=True,
        )
        assert "Plotly.newPlot" in out.read_text(encoding="utf-8")

    def test_inline_content_is_small_enough_to_return(self, chart_csv, tmp_path):
        """Inlining the library would be 4.85 MB — 6.5 MB base64."""
        result = generate_chart(
            str(chart_csv),
            "bar",
            "revenue",
            category_column="region",
            output_path=str(tmp_path / "c.html"),
            open_after=False,
            return_content=True,
        )
        assert len(result["content_base64"]) < 200_000

    def test_the_caller_is_told_what_it_received(self, chart_csv, tmp_path):
        result = generate_chart(
            str(chart_csv),
            "bar",
            "revenue",
            category_column="region",
            output_path=str(tmp_path / "c.html"),
            open_after=False,
            return_content=True,
        )
        assert "public_url" in result["content_note"]

    def test_nothing_is_embedded_without_return_content(self, chart_csv, tmp_path):
        result = generate_chart(
            str(chart_csv),
            "bar",
            "revenue",
            category_column="region",
            output_path=str(tmp_path / "c.html"),
            open_after=False,
        )
        assert "content_base64" not in result

    def test_values_survive_into_the_drawing(self, chart_csv, tmp_path):
        """A chart that renders but shows the wrong numbers is worse than none."""
        result = generate_chart(
            str(chart_csv),
            "bar",
            "revenue",
            category_column="region",
            output_path=str(tmp_path / "c.html"),
            open_after=False,
            return_content=True,
        )
        html = _inline_html(result)
        for region in ("West", "East", "North"):
            assert region in html


class TestRendererDirectly:
    def test_bar_traces_draw_rects(self):
        svg = figure_to_svg([{"type": "bar", "x": ["a", "b"], "y": [1, 2]}], {"title": {"text": "T"}})
        assert svg and "<rect" in svg and "T" in svg

    def test_scatter_markers_draw_circles(self):
        svg = figure_to_svg([{"type": "scatter", "mode": "markers", "x": [1, 2], "y": [3, 4]}], {})
        assert svg and "<circle" in svg

    def test_lines_draw_a_polyline(self):
        svg = figure_to_svg([{"type": "scatter", "mode": "lines", "x": [1, 2, 3], "y": [3, 1, 4]}], {})
        assert svg and "<polyline" in svg

    def test_pie_draws_arcs(self):
        svg = figure_to_svg([{"type": "pie", "labels": ["a", "b"], "values": [3, 1]}], {})
        assert svg and "<path" in svg and "75%" in svg

    def test_an_unsupported_trace_returns_none_rather_than_a_wrong_picture(self):
        assert figure_to_svg([{"type": "surface", "z": [[1, 2], [3, 4]]}], {}) is None

    def test_empty_traces_return_none(self):
        assert figure_to_svg([], {}) is None

    def test_mismatched_axes_return_none(self):
        assert figure_to_svg([{"type": "bar", "x": ["a", "b"], "y": [1]}], {}) is None

    def test_titles_are_escaped(self):
        svg = figure_to_svg([{"type": "bar", "x": ["a"], "y": [1]}], {"title": {"text": "<script>x</script>"}})
        assert svg and "<script>" not in svg

    def test_axis_title_does_not_sit_on_the_tick_labels(self):
        """A fixed left gutter let a rotated y title overlap a 2,000,000 tick."""
        svg = figure_to_svg(
            [{"type": "bar", "x": ["a"], "y": [2_000_000]}],
            {"yaxis": {"title": {"text": "revenue"}}},
        )
        assert svg
        import re

        ticks = [float(m) for m in re.findall(r'<text x="([\d.]+)" y="[\d.]+" font-size="11"', svg)]
        assert ticks and min(ticks) > 40

    def test_non_chart_html_is_not_mangled(self):
        assert standalone_html("<html><body>just a page</body></html>") is None
