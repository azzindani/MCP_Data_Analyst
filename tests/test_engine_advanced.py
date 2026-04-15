"""Tests for servers/data_advanced/engine.py."""

from __future__ import annotations

import py_compile
from pathlib import Path

import pytest

try:
    from servers.data_advanced.engine import (
        export_data,
        generate_3d_chart,
        generate_auto_profile,
        generate_chart,
        generate_correlation_heatmap,
        generate_dashboard,
        generate_distribution_plot,
        generate_geo_map,
        generate_multi_chart,
        generate_pairwise_plot,
        run_eda,
    )

    HAS_ADVANCED = True
except ImportError:
    HAS_ADVANCED = False


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def rich_csv(tmp_path) -> Path:
    f = tmp_path / "rich_data.csv"
    f.write_text("""Region,Product,Revenue,Units_Sold,Order_Date,Discount,Customer_Score
West,Widget A,5000,10,2024-01-15,0.1,85
West,Widget B,3200,8,2024-02-20,0.12,72
East,Widget A,7500,15,2024-03-10,0.05,91
South,Widget C,2100,5,2024-04-05,0.2,60
North,Widget A,4800,12,2024-05-12,0.08,78
West,Widget A,6000,12,2024-06-18,0.07,88
East,Widget B,3000,7,2024-07-22,0.15,65
South,Widget A,2500,6,2024-08-30,0.1,70
North,Widget C,1800,4,2024-09-14,0.25,55
West,Widget C,4200,9,2024-10-01,0.09,80
East,Widget A,8000,16,2024-11-05,0.03,95
South,Widget B,1500,3,2024-12-10,0.3,45
North,Widget A,5500,14,2024-01-25,0.06,82
West,Widget B,2800,7,2024-02-14,0.11,68
East,Widget C,3500,8,2024-03-28,0.13,75
""")
    return f


# ---------------------------------------------------------------------------
# generate_auto_profile
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_ADVANCED, reason="advanced deps not installed")
class TestGenerateAutoProfile:
    def test_minimal(self, rich_csv):
        r = generate_auto_profile(str(rich_csv), open_after=False)
        assert r["success"] is True
        assert r["rows"] == 15
        assert r["columns"] == 7
        assert r["numeric_columns"] >= 4
        assert r["categorical_columns"] >= 2
        assert r["correlation_pairs"] > 0
        assert Path(rich_csv.parent / r["output_path"]).exists()

    def test_file_not_found(self, tmp_path):
        r = generate_auto_profile(str(tmp_path / "missing.csv"))
        assert r["success"] is False
        assert "hint" in r


# ---------------------------------------------------------------------------
# generate_distribution_plot
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_ADVANCED, reason="advanced deps not installed")
class TestGenerateDistributionPlot:
    def test_basic(self, rich_csv):
        r = generate_distribution_plot(str(rich_csv), open_after=False)
        assert r["success"] is True
        assert len(r["columns_plotted"]) >= 2
        assert Path(rich_csv.parent / r["output_path"]).exists()

    def test_file_not_found(self, tmp_path):
        r = generate_distribution_plot(str(tmp_path / "missing.csv"))
        assert r["success"] is False


# ---------------------------------------------------------------------------
# generate_correlation_heatmap
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_ADVANCED, reason="advanced deps not installed")
class TestGenerateCorrelationHeatmap:
    def test_basic(self, rich_csv):
        r = generate_correlation_heatmap(str(rich_csv), open_after=False)
        assert r["success"] is True
        assert len(r["columns"]) >= 4
        assert Path(rich_csv.parent / r["output_path"]).exists()

    def test_file_not_found(self, tmp_path):
        r = generate_correlation_heatmap(str(tmp_path / "missing.csv"))
        assert r["success"] is False


# ---------------------------------------------------------------------------
# generate_pairwise_plot
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_ADVANCED, reason="advanced deps not installed")
class TestGeneratePairwisePlot:
    def test_basic(self, rich_csv):
        r = generate_pairwise_plot(str(rich_csv), open_after=False)
        assert r["success"] is True
        assert len(r["columns_plotted"]) >= 2
        assert Path(rich_csv.parent / r["output_path"]).exists()

    def test_file_not_found(self, tmp_path):
        r = generate_pairwise_plot(str(tmp_path / "missing.csv"))
        assert r["success"] is False


# ---------------------------------------------------------------------------
# generate_multi_chart
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_ADVANCED, reason="advanced deps not installed")
class TestGenerateMultiChart:
    def test_multi_bar(self, rich_csv):
        r = generate_multi_chart(
            str(rich_csv),
            chart_type="multi_bar",
            value_columns=["Revenue", "Units_Sold"],
            category_column="Region",
            open_after=False,
        )
        assert r["success"] is True
        assert r["chart_type"] == "multi_bar"
        assert Path(rich_csv.parent / r["output_path"]).exists()

    def test_file_not_found(self, tmp_path):
        r = generate_multi_chart(
            str(tmp_path / "missing.csv"),
            chart_type="multi_bar",
            value_columns=["Revenue"],
        )
        assert r["success"] is False


# ---------------------------------------------------------------------------
# generate_chart
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_ADVANCED, reason="advanced deps not installed")
class TestGenerateChart:
    def test_bar(self, rich_csv):
        r = generate_chart(
            str(rich_csv),
            chart_type="bar",
            value_column="Revenue",
            category_column="Region",
            open_after=False,
        )
        assert r["success"] is True
        assert r["chart_type"] == "bar"
        assert Path(rich_csv.parent / r["output_path"]).exists()

    def test_pie(self, rich_csv):
        r = generate_chart(
            str(rich_csv),
            chart_type="pie",
            value_column="Revenue",
            category_column="Region",
            open_after=False,
        )
        assert r["success"] is True
        assert r["chart_type"] == "pie"

    def test_time_series(self, rich_csv):
        r = generate_chart(
            str(rich_csv),
            chart_type="time_series",
            value_column="Revenue",
            date_column="Order_Date",
            open_after=False,
        )
        assert r["success"] is True
        assert r["chart_type"] == "time_series"

    def test_treemap(self, rich_csv):
        r = generate_chart(
            str(rich_csv),
            chart_type="treemap",
            value_column="Revenue",
            hierarchy_columns=["Region", "Product"],
            open_after=False,
        )
        assert r["success"] is True
        assert r["chart_type"] == "treemap"

    def test_scatter(self, rich_csv):
        r = generate_chart(
            str(rich_csv),
            chart_type="scatter",
            value_column="Revenue",
            category_column="Units_Sold",
            open_after=False,
        )
        assert r["success"] is True
        assert r["chart_type"] == "scatter"

    def test_invalid_type(self, rich_csv):
        r = generate_chart(
            str(rich_csv),
            chart_type="heatmap",
            value_column="Revenue",
        )
        assert r["success"] is False
        assert "hint" in r

    def test_file_not_found(self, tmp_path):
        r = generate_chart(
            str(tmp_path / "missing.csv"),
            chart_type="bar",
            value_column="Revenue",
            category_column="Region",
        )
        assert r["success"] is False


# ---------------------------------------------------------------------------
# generate_dashboard
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_ADVANCED, reason="advanced deps not installed")
class TestGenerateDashboard:
    def test_dry_run(self, rich_csv):
        r = generate_dashboard(str(rich_csv), dry_run=True)
        assert r["success"] is True
        assert r["dry_run"] is True
        assert "would_generate" in r

    def test_full_generation(self, rich_csv):
        r = generate_dashboard(str(rich_csv), title="Test Dashboard", open_after=False)
        assert r["success"] is True
        assert r["output_name"].endswith(".html")
        assert Path(rich_csv.parent / r["output_path"]).exists()
        html = Path(rich_csv.parent / r["output_path"]).read_text(encoding="utf-8")
        assert "<!DOCTYPE html>" in html
        assert "Plotly.newPlot" in html

    def test_file_not_found(self, tmp_path):
        r = generate_dashboard(str(tmp_path / "missing.csv"))
        assert r["success"] is False


# ---------------------------------------------------------------------------
# export_data
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_ADVANCED, reason="advanced deps not installed")
class TestExportData:
    def test_csv(self, rich_csv):
        r = export_data(str(rich_csv), format="csv", open_after=False)
        assert r["success"] is True
        assert r["format"] == "csv"
        assert Path(rich_csv.parent / r["output_path"]).exists()

    def test_json(self, rich_csv):
        r = export_data(str(rich_csv), format="json", open_after=False)
        assert r["success"] is True
        assert r["format"] == "json"
        assert Path(rich_csv.parent / r["output_path"]).exists()

    def test_file_not_found(self, tmp_path):
        r = export_data(str(tmp_path / "missing.csv"))
        assert r["success"] is False


# ---------------------------------------------------------------------------
# generate_3d_chart
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_ADVANCED, reason="advanced deps not installed")
class TestGenerate3DChart:
    def test_scatter_3d(self, rich_csv):
        r = generate_3d_chart(
            str(rich_csv),
            chart_type="scatter_3d",
            x_column="Revenue",
            y_column="Units_Sold",
            z_column="Discount",
            open_after=False,
        )
        assert r["success"] is True
        assert r["chart_type"] == "scatter_3d"
        assert Path(rich_csv.parent / r["output_path"]).exists()

    def test_bad_type(self, rich_csv):
        r = generate_3d_chart(
            str(rich_csv),
            chart_type="invalid",
            x_column="Revenue",
            y_column="Units_Sold",
            z_column="Discount",
        )
        assert r["success"] is False
        assert "hint" in r

    def test_missing_column(self, rich_csv):
        r = generate_3d_chart(
            str(rich_csv),
            chart_type="scatter_3d",
            x_column="Revenue",
            y_column="Units_Sold",
            z_column="NonExistentColumn",
        )
        assert r["success"] is False
        assert "hint" in r


# ---------------------------------------------------------------------------
# New chart types in generate_chart
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_ADVANCED, reason="advanced deps not installed")
class TestGenerateChartNewTypes:
    def test_sunburst(self, rich_csv):
        r = generate_chart(
            str(rich_csv),
            chart_type="sunburst",
            value_column="Revenue",
            hierarchy_columns=["Region", "Product"],
            open_after=False,
        )
        assert r["success"] is True
        assert r["chart_type"] == "sunburst"

    def test_waterfall(self, rich_csv):
        r = generate_chart(
            str(rich_csv),
            chart_type="waterfall",
            value_column="Revenue",
            category_column="Region",
            open_after=False,
        )
        assert r["success"] is True
        assert r["chart_type"] == "waterfall"

    def test_funnel(self, rich_csv):
        r = generate_chart(
            str(rich_csv),
            chart_type="funnel",
            value_column="Revenue",
            category_column="Region",
            open_after=False,
        )
        assert r["success"] is True
        assert r["chart_type"] == "funnel"

    def test_parallel_coords(self, rich_csv):
        r = generate_chart(
            str(rich_csv),
            chart_type="parallel_coords",
            value_column="Revenue",
            open_after=False,
        )
        assert r["success"] is True
        assert r["chart_type"] == "parallel_coords"

    def test_sankey(self, rich_csv):
        r = generate_chart(
            str(rich_csv),
            chart_type="sankey",
            value_column="Revenue",
            category_column="Region",
            color_column="Product",
            open_after=False,
        )
        assert r["success"] is True
        assert r["chart_type"] == "sankey"


# ---------------------------------------------------------------------------
# run_eda
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_ADVANCED, reason="advanced deps not installed")
class TestRunEda:
    def test_basic(self, rich_csv):
        r = run_eda(str(rich_csv), open_after=False)
        assert r["success"] is True
        assert r["rows"] == 15
        assert r["columns"] == 7
        assert r["numeric_columns"] >= 4
        assert r["categorical_columns"] >= 2
        assert r["quality_score"] >= 0
        assert r["quality_score"] <= 100
        assert "column_summaries" in r
        assert len(r["column_summaries"]) == 7

    def test_html_file_created(self, rich_csv):
        r = run_eda(str(rich_csv), open_after=False)
        assert r["success"] is True
        out = Path(r["output_path"])
        assert out.exists()
        assert out.suffix == ".html"
        html = out.read_text(encoding="utf-8")
        assert "<!DOCTYPE html>" in html
        assert "EDA Report" in html

    def test_custom_output_path(self, rich_csv, tmp_path):
        out = tmp_path / "my_eda.html"
        r = run_eda(str(rich_csv), output_path=str(out), open_after=False)
        assert r["success"] is True
        assert out.exists()

    def test_top_correlations(self, rich_csv):
        r = run_eda(str(rich_csv), open_after=False)
        assert r["success"] is True
        # Revenue and Units_Sold should correlate
        assert "top_correlations" in r
        assert isinstance(r["top_correlations"], list)
        if r["top_correlations"]:
            first = r["top_correlations"][0]
            assert "col_a" in first
            assert "col_b" in first
            assert "correlation" in first

    def test_outlier_columns(self, rich_csv):
        r = run_eda(str(rich_csv), open_after=False)
        assert r["success"] is True
        assert "outlier_columns" in r

    def test_duplicate_rows_reported(self, tmp_path):
        f = tmp_path / "dups.csv"
        f.write_text("A,B\n1,2\n1,2\n3,4\n")
        r = run_eda(str(f), open_after=False)
        assert r["success"] is True
        assert r["duplicate_rows"] == 1

    def test_token_estimate(self, rich_csv):
        r = run_eda(str(rich_csv), open_after=False)
        assert "token_estimate" in r
        assert r["token_estimate"] > 0

    def test_file_not_found(self, tmp_path):
        r = run_eda(str(tmp_path / "missing.csv"))
        assert r["success"] is False
        assert "hint" in r


# ---------------------------------------------------------------------------
# generate_geo_map
# ---------------------------------------------------------------------------


@pytest.fixture()
def latlon_csv(tmp_path) -> Path:
    f = tmp_path / "geo_points.csv"
    f.write_text(
        "City,lat,lon,Population\n"
        "New York,40.7128,-74.0060,8336817\n"
        "Los Angeles,34.0522,-118.2437,3979576\n"
        "Chicago,41.8781,-87.6298,2693976\n"
        "Houston,29.7604,-95.3698,2320268\n"
        "Phoenix,33.4484,-112.0740,1680992\n"
    )
    return f


@pytest.fixture()
def country_csv(tmp_path) -> Path:
    f = tmp_path / "country_data.csv"
    f.write_text("country,Revenue\nUnited States,50000\nGermany,30000\nFrance,25000\nJapan,20000\nBrazil,15000\n")
    return f


@pytest.mark.skipif(not HAS_ADVANCED, reason="advanced deps not installed")
class TestGenerateGeoMap:
    def test_scatter_geo_explicit_cols(self, latlon_csv):
        r = generate_geo_map(
            str(latlon_csv),
            lat_column="lat",
            lon_column="lon",
            open_after=False,
        )
        assert r["success"] is True
        assert r["map_type"] == "scatter_geo"
        out = Path(r["output_path"])
        assert out.exists()
        html = out.read_text(encoding="utf-8")
        assert "Plotly.newPlot" in html

    def test_scatter_geo_auto_detect(self, latlon_csv):
        # Column names 'lat' and 'lon' are in _GEO_LAT/_GEO_LON → auto-detected
        r = generate_geo_map(str(latlon_csv), open_after=False)
        assert r["success"] is True
        assert r["map_type"] == "scatter_geo"

    def test_scatter_geo_with_value(self, latlon_csv):
        r = generate_geo_map(
            str(latlon_csv),
            lat_column="lat",
            lon_column="lon",
            value_column="Population",
            open_after=False,
        )
        assert r["success"] is True
        assert r["map_type"] == "scatter_geo"

    def test_choropleth_auto_detect(self, country_csv):
        # 'country' column is in _GEO_COUNTRY → auto-detected as choropleth
        r = generate_geo_map(str(country_csv), value_column="Revenue", open_after=False)
        assert r["success"] is True
        assert str(r["map_type"]).startswith("choropleth")

    def test_custom_output_path(self, latlon_csv, tmp_path):
        out = tmp_path / "map.html"
        r = generate_geo_map(
            str(latlon_csv),
            lat_column="lat",
            lon_column="lon",
            output_path=str(out),
            open_after=False,
        )
        assert r["success"] is True
        assert out.exists()

    def test_no_geo_columns_fails(self, tmp_path):
        f = tmp_path / "nongeo.csv"
        f.write_text("Name,Score\nAlice,90\nBob,85\n")
        r = generate_geo_map(str(f))
        assert r["success"] is False
        assert "hint" in r

    def test_value_column_not_found(self, latlon_csv):
        r = generate_geo_map(
            str(latlon_csv),
            lat_column="lat",
            lon_column="lon",
            value_column="NonExistent",
        )
        assert r["success"] is False
        assert "hint" in r

    def test_file_not_found(self, tmp_path):
        r = generate_geo_map(str(tmp_path / "missing.csv"))
        assert r["success"] is False
        assert "hint" in r


# ---------------------------------------------------------------------------
# Docstring length CI check
# ---------------------------------------------------------------------------


def test_advanced_server_docstrings_lte_80_chars():
    """All @mcp.tool() docstrings in data_advanced/server.py must be ≤ 80 chars."""
    from servers.data_advanced import server

    tool_funcs = [
        server.run_eda,
        server.generate_distribution_plot,
        server.generate_multi_chart,
        server.generate_chart,
        server.generate_geo_map,
        server.generate_dashboard,
        server.generate_correlation_heatmap,
        server.generate_pairwise_plot,
        server.generate_auto_profile,
        server.export_data,
        server.generate_3d_chart,
    ]
    for fn in tool_funcs:
        doc = fn.__doc__ or ""
        assert len(doc) <= 80, f"{fn.__name__} docstring too long ({len(doc)} chars): {doc!r}"
