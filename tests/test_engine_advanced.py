"""Tests for servers/data_advanced/engine.py."""

from __future__ import annotations

import py_compile
from pathlib import Path

import pytest

try:
    from servers.data_advanced.engine import (
        generate_auto_profile,
        generate_distribution_plot,
        generate_correlation_heatmap,
        generate_pairwise_plot,
        generate_multi_chart,
        generate_chart,
        generate_dashboard,
        export_data,
    )
    HAS_ADVANCED = True
except ImportError:
    HAS_ADVANCED = False


pytestmark = pytest.mark.skipif(not HAS_ADVANCED, reason="advanced engine dependencies not installed")
        assert "hint" in r


# ---------------------------------------------------------------------------
# generate_distribution_plot
# ---------------------------------------------------------------------------


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


class TestGenerateCorrelationHeatmap:
    def test_basic(self, rich_csv):
        r = generate_correlation_heatmap(str(rich_csv), open_after=False)
        assert r["success"] is True
        assert r["columns"] >= 4
        assert Path(rich_csv.parent / r["output_path"]).exists()

    def test_file_not_found(self, tmp_path):
        r = generate_correlation_heatmap(str(tmp_path / "missing.csv"))
        assert r["success"] is False


# ---------------------------------------------------------------------------
# generate_pairwise_plot
# ---------------------------------------------------------------------------


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
