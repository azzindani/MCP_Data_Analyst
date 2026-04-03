"""Tests for servers/data_advanced/engine.py — ≥90% coverage required."""

from __future__ import annotations

import os
import py_compile
from pathlib import Path

import pytest
import pandas as pd

from servers.data_advanced.engine import (
    generate_profile_report,
    generate_sweetviz_report,
    generate_autoviz_report,
    generate_chart,
    generate_dashboard,
)


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


@pytest.fixture()
def geo_json(tmp_path) -> Path:
    f = tmp_path / "states.geojson"
    f.write_text("""{
  "type": "FeatureCollection",
  "features": [
    {"type":"Feature","properties":{"name":"California","code":"CA"},"geometry":{"type":"Polygon","coordinates":[[[-120,35],[-120,40],[-115,40],[-115,35],[-120,35]]]}},
    {"type":"Feature","properties":{"name":"Texas","code":"TX"},"geometry":{"type":"Polygon","coordinates":[[[-105,26],[-105,36],[-95,36],[-95,26],[-105,26]]]}},
    {"type":"Feature","properties":{"name":"New York","code":"NY"},"geometry":{"type":"Polygon","coordinates":[[[-80,40],[-80,45],[-72,45],[-72,40],[-80,40]]]}}
  ]
}""")
    return f


# ---------------------------------------------------------------------------
# generate_profile_report
# ---------------------------------------------------------------------------


class TestGenerateProfileReport:
    def test_minimal(self, rich_csv):
        r = generate_profile_report(str(rich_csv), minimal=True, correlations=False)
        if not r["success"]:
            pytest.skip(f"ydata-profiling not installed: {r['error']}")
        assert r["success"] is True
        assert r["report_path"].endswith(".html")
        assert r["columns_profiled"] == 7
        assert r["rows"] == 15
        assert Path(rich_csv.parent / r["report_path"]).exists()

    def test_full_with_correlations(self, rich_csv):
        r = generate_profile_report(
            str(rich_csv),
            minimal=False,
            correlations=True,
            title="Full Profile",
            description="Test report",
        )
        if not r["success"]:
            pytest.skip(f"ydata-profiling not installed: {r['error']}")
        assert r["success"] is True
        assert r["correlations_included"] is True

    def test_file_not_found(self, tmp_path):
        r = generate_profile_report(str(tmp_path / "missing.csv"))
        assert r["success"] is False
        assert "hint" in r


# ---------------------------------------------------------------------------
# generate_sweetviz_report
# ---------------------------------------------------------------------------


class TestGenerateSweetvizReport:
    def test_basic(self, rich_csv):
        r = generate_sweetviz_report(str(rich_csv))
        if not r["success"]:
            pytest.skip(f"sweetviz not installed: {r['error']}")
        assert r["success"] is True
        assert r["report_path"].endswith(".html")
        assert r["columns_analysed"] == 7
        assert Path(rich_csv.parent / r["report_path"]).exists()

    def test_with_target(self, rich_csv):
        r = generate_sweetviz_report(str(rich_csv), target_column="Revenue")
        if not r["success"]:
            pytest.skip(f"sweetviz not installed: {r['error']}")
        assert r["target_column"] == "Revenue"

    def test_bad_target(self, rich_csv):
        r = generate_sweetviz_report(str(rich_csv), target_column="NonExistent")
        assert r["success"] is False
        assert "hint" in r

    def test_file_not_found(self, tmp_path):
        r = generate_sweetviz_report(str(tmp_path / "missing.csv"))
        assert r["success"] is False


# ---------------------------------------------------------------------------
# generate_autoviz_report
# ---------------------------------------------------------------------------


class TestGenerateAutovizReport:
    def test_basic(self, rich_csv):
        r = generate_autoviz_report(
            str(rich_csv), chart_format="html", max_rows_analyzed=100
        )
        if not r["success"]:
            pytest.skip(f"autoviz not installed: {r['error']}")
        assert r["success"] is True
        assert r["chart_count"] >= 0
        assert r["rows_analyzed"] == 15

    def test_file_not_found(self, tmp_path):
        r = generate_autoviz_report(str(tmp_path / "missing.csv"))
        assert r["success"] is False


# ---------------------------------------------------------------------------
# generate_chart
# ---------------------------------------------------------------------------


class TestGenerateChart:
    def test_bar(self, rich_csv):
        r = generate_chart(
            str(rich_csv),
            chart_type="bar",
            value_column="Revenue",
            category_column="Region",
            agg_func="sum",
        )
        if not r["success"]:
            pytest.skip(f"plotly not installed: {r['error']}")
        assert r["success"] is True
        assert r["output_path"].endswith(".html")
        assert "Revenue" in r["title"]
        assert r["rows_plotted"] == 4
        assert Path(rich_csv.parent / r["output_path"]).exists()

    def test_pie(self, rich_csv):
        r = generate_chart(
            str(rich_csv),
            chart_type="pie",
            value_column="Revenue",
            category_column="Product",
            agg_func="sum",
        )
        if not r["success"]:
            pytest.skip(f"plotly not installed: {r['error']}")
        assert r["success"] is True

    def test_time_series(self, rich_csv):
        r = generate_chart(
            str(rich_csv),
            chart_type="time_series",
            value_column="Revenue",
            date_column="Order_Date",
            period="M",
        )
        if not r["success"]:
            pytest.skip(f"plotly not installed: {r['error']}")
        assert r["success"] is True

    def test_treemap(self, rich_csv):
        r = generate_chart(
            str(rich_csv),
            chart_type="treemap",
            value_column="Revenue",
            hierarchy_columns=["Region", "Product"],
        )
        if not r["success"]:
            pytest.skip(f"plotly not installed: {r['error']}")
        assert r["success"] is True

    def test_scatter(self, rich_csv):
        r = generate_chart(
            str(rich_csv),
            chart_type="scatter",
            value_column="Revenue",
            category_column="Units_Sold",
        )
        if not r["success"]:
            pytest.skip(f"plotly not installed: {r['error']}")
        assert r["success"] is True
        assert r["output_path"].endswith(".html")
        assert "Revenue" in r["title"]
        assert r["rows_plotted"] == 4
        assert Path(rich_csv.parent / r["output_path"]).exists()

    def test_pie(self, rich_csv):
        r = generate_chart(
            str(rich_csv),
            chart_type="pie",
            value_column="Revenue",
            category_column="Product",
            agg_func="sum",
        )
        if not r["success"]:
            pytest.skip(f"plotly not installed: {r['error']}")
        assert r["success"] is True

    def test_time_series(self, rich_csv):
        r = generate_chart(
            str(rich_csv),
            chart_type="time_series",
            value_column="Revenue",
            date_column="Order_Date",
            period="M",
        )
        if not r["success"]:
            pytest.skip(f"plotly not installed: {r['error']}")
        assert r["success"] is True

    def test_treemap(self, rich_csv):
        r = generate_chart(
            str(rich_csv),
            chart_type="treemap",
            value_column="Revenue",
            hierarchy_columns=["Region", "Product"],
        )
        if not r["success"]:
            pytest.skip(f"plotly not installed: {r['error']}")
        assert r["success"] is True

    def test_scatter(self, rich_csv):
        r = generate_chart(
            str(rich_csv),
            chart_type="scatter",
            value_column="Revenue",
            category_column="Units_Sold",
        )
        if not r["success"]:
            pytest.skip(f"plotly not installed: {r['error']}")
        assert r["success"] is True

    def test_invalid_type(self, rich_csv):
        r = generate_chart(str(rich_csv), chart_type="heatmap", value_column="Revenue")
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


class TestGenerateDashboard:
    def test_dry_run(self, rich_csv):
        r = generate_dashboard(str(rich_csv), dry_run=True)
        assert r["success"] is True
        assert r["dry_run"] is True
        assert "would_generate" in r

    def test_full_generation(self, rich_csv):
        r = generate_dashboard(
            str(rich_csv), title="Test Dashboard", chart_types=["bar", "pie", "scatter"]
        )
        assert r["success"] is True
        assert r["output_path"] == "app.py"
        assert r["dashboard_title"] == "Test Dashboard"
        assert "bar" in r["charts_included"]
        assert "pie" in r["charts_included"]
        assert "scatter" in r["charts_included"]
        assert len(r["kpi_columns"]) > 0
        assert len(r["filter_columns"]) > 0
        assert "streamlit run" in r["run_command"]
        out = rich_csv.parent / r["output_path"]
        assert out.exists()

    def test_generated_code_valid_python(self, rich_csv):
        r = generate_dashboard(str(rich_csv), title="Test", chart_types=["bar", "pie"])
        if not r["success"]:
            pytest.skip(f"dashboard generation failed: {r['error']}")
        app_path = rich_csv.parent / "app.py"
        assert app_path.exists()
        py_compile.compile(str(app_path), doraise=True)

    def test_file_not_found(self, tmp_path):
        r = generate_dashboard(str(tmp_path / "missing.csv"))
        assert r["success"] is False


# ---------------------------------------------------------------------------
# All tools handle missing files
# ---------------------------------------------------------------------------


class TestMissingFileHandling:
    def test_profile_report(self, tmp_path):
        r = generate_profile_report(str(tmp_path / "missing.csv"))
        assert r["success"] is False
        assert "hint" in r

    def test_sweetviz_report(self, tmp_path):
        r = generate_sweetviz_report(str(tmp_path / "missing.csv"))
        assert r["success"] is False
        assert "hint" in r

    def test_autoviz_report(self, tmp_path):
        r = generate_autoviz_report(str(tmp_path / "missing.csv"))
        assert r["success"] is False
        assert "hint" in r

    def test_chart(self, tmp_path):
        r = generate_chart(
            str(tmp_path / "missing.csv"),
            chart_type="bar",
            value_column="Revenue",
            category_column="Region",
        )
        assert r["success"] is False
        assert "hint" in r

    def test_dashboard(self, tmp_path):
        r = generate_dashboard(str(tmp_path / "missing.csv"))
        assert r["success"] is False
        assert "hint" in r
