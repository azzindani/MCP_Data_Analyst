"""Regressions for defects that only showed up when the artifacts were rendered.

Every tool in this repo returned success:true for the charts below, and each
file passed structural validation — valid HTML, a Plotly payload, non-zero
bytes. The defects were only visible once the pages were opened in a headless
browser and looked at: a line chart drawn as a zigzag, a correlation heatmap
whose palette inverted the sign of a result, and a world map plotted from a
spend column. These tests assert on the figure data instead of the file, which
is the level the bugs actually lived at.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

try:
    from servers.data_advanced.engine import generate_chart, generate_correlation_heatmap, generate_geo_map

    HAS_ADVANCED = True
except ImportError:
    HAS_ADVANCED = False

pytestmark = pytest.mark.skipif(not HAS_ADVANCED, reason="data_advanced deps unavailable")


def _figure(html_path: Path) -> tuple[list, dict]:
    """Return the chart's traces and layout exactly as the browser parses them."""
    from servers.data_visual._adv_customize import _split_newplot

    _, _, data_str, _, layout_str, _ = _split_newplot(html_path.read_text(encoding="utf-8"))
    return json.loads(data_str), json.loads(layout_str)


@pytest.fixture()
def dated_csv(tmp_path: Path) -> Path:
    """Rows deliberately out of date order, with the largest value in the middle,
    so a value-sort and a date-sort produce visibly different charts."""
    csv = tmp_path / "dated.csv"
    csv.write_text(
        "day,amount\n"
        "2024-03-01,10\n"
        "2024-01-01,50\n"
        "2024-05-01,20\n"
        "2024-02-01,90\n"
        "2024-04-01,30\n"
    )
    return csv


class TestLineChartsFollowTheXAxis:
    """A line joins its points in row order. Sorting by value — right for bars —
    turns a trend into a zigzag; the real Ad_Data line chart came out as a mass
    of horizontal strokes rather than a trend over time."""

    def test_line_points_are_ordered_by_x(self, dated_csv: Path, tmp_path: Path):
        out = tmp_path / "line.html"
        result = generate_chart(
            str(dated_csv), "line", "amount", category_column="day", output_path=str(out), open_after=False
        )
        assert result["success"] is True
        traces, _ = _figure(out)
        assert traces[0]["x"] == ["2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01", "2024-05-01"]

    def test_line_values_follow_their_own_x(self, dated_csv: Path, tmp_path: Path):
        out = tmp_path / "line.html"
        generate_chart(
            str(dated_csv), "line", "amount", category_column="day", output_path=str(out), open_after=False
        )
        from servers.data_visual._adv_customize import _decode_plotly_y

        traces, _ = _figure(out)
        assert _decode_plotly_y(traces[0]["y"]) == [50, 90, 10, 30, 20]

    def test_bars_are_still_ranked_by_value(self, dated_csv: Path, tmp_path: Path):
        out = tmp_path / "bar.html"
        generate_chart(
            str(dated_csv), "bar", "amount", category_column="day", output_path=str(out), open_after=False
        )
        traces, _ = _figure(out)
        values = list(traces[0]["y"])
        assert values == sorted(values, reverse=True)


class TestCorrelationHeatmapScale:
    """Auto-ranging the colour scale over the observed values made r=+0.70 render
    in the palette's strong-negative colour, reading as the opposite result."""

    def test_scale_is_pinned_to_the_full_correlation_range(self, simple_csv: Path, tmp_path: Path):
        out = tmp_path / "heat.html"
        result = generate_correlation_heatmap(str(simple_csv), output_path=str(out), open_after=False)
        assert result["success"] is True
        _, layout = _figure(out)
        assert layout["coloraxis"]["cmin"] == -1
        assert layout["coloraxis"]["cmax"] == 1

    def test_midpoint_is_zero_not_the_data_mean(self, simple_csv: Path, tmp_path: Path):
        out = tmp_path / "heat.html"
        generate_correlation_heatmap(str(simple_csv), output_path=str(out), open_after=False)
        _, layout = _figure(out)
        assert layout["coloraxis"]["cmid"] == 0


class TestGeoMapRefusesNonCoordinates:
    """Plotly wraps out-of-range coordinates onto the globe rather than rejecting
    them, so plotting a spend column as latitude produced a convincing world map
    of nothing. The real run passed lat_column=spends, lon_column=impressions."""

    def test_latitude_outside_90_is_rejected(self, simple_csv: Path, tmp_path: Path):
        result = generate_geo_map(
            str(simple_csv),
            lat_column="Revenue",
            lon_column="Units Sold",
            output_path=str(tmp_path / "map.html"),
            open_after=False,
        )
        assert result["success"] is False
        assert "latitude" in result["error"]
        assert "Revenue" in result["error"]

    def test_the_error_names_a_real_alternative(self, simple_csv: Path, tmp_path: Path):
        result = generate_geo_map(
            str(simple_csv), lat_column="Revenue", lon_column="Units Sold", open_after=False
        )
        assert "location_column" in result["hint"]

    def test_nothing_is_written_when_the_columns_are_refused(self, simple_csv: Path, tmp_path: Path):
        out = tmp_path / "map.html"
        generate_geo_map(
            str(simple_csv), lat_column="Revenue", lon_column="Units Sold", output_path=str(out), open_after=False
        )
        assert not out.exists()

    def test_real_coordinates_still_plot(self, tmp_path: Path):
        csv = tmp_path / "cities.csv"
        csv.write_text("name,lat,lon,pop\nOslo,59.91,10.75,700000\nLima,-12.05,-77.04,9700000\n")
        out = tmp_path / "map.html"
        result = generate_geo_map(str(csv), output_path=str(out), open_after=False)
        assert result["success"] is True
        assert out.exists()

    def test_longitude_outside_180_is_rejected(self, tmp_path: Path):
        csv = tmp_path / "bad.csv"
        csv.write_text("lat,lon\n45.0,5000.0\n12.0,7000.0\n")
        result = generate_geo_map(str(csv), open_after=False)
        assert result["success"] is False
        assert "longitude" in result["error"]
