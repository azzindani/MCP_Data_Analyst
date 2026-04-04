"""Tier 3 MCP server — thin wrapper only. Zero domain logic."""

from __future__ import annotations

import sys
import logging
from pathlib import Path

logging.basicConfig(stream=sys.stderr, level=logging.WARNING)

from fastmcp import FastMCP

try:
    from . import engine
except ImportError:
    _root = str(Path(__file__).resolve().parents[2])
    if _root not in sys.path:
        sys.path.insert(0, _root)
    import engine

mcp = FastMCP("data_advanced")


@mcp.tool()
def run_eda(
    file_path: str,
    output_path: str = "",
    open_after: bool = True,
) -> dict:
    """Fast EDA summary. Stats, nulls, correlations, outliers. Opens HTML."""
    return engine.run_eda(file_path, output_path, open_after)


@mcp.tool()
def generate_distribution_plot(
    file_path: str,
    columns: list[str] = None,
    output_path: str = "",
    open_after: bool = True,
) -> dict:
    """Histogram + box plot for numeric columns. Opens HTML file."""
    return engine.generate_distribution_plot(
        file_path, columns, output_path, open_after
    )


@mcp.tool()
def generate_multi_chart(
    file_path: str,
    chart_type: str,
    value_columns: list[str],
    category_column: str = "",
    date_column: str = "",
    agg_func: str = "sum",
    output_path: str = "",
    title: str = "",
    open_after: bool = True,
) -> dict:
    """Multi-variable bar/line chart. Compares 2+ metrics. Opens HTML."""
    return engine.generate_multi_chart(
        file_path,
        chart_type,
        value_columns,
        category_column,
        date_column,
        agg_func,
        output_path,
        title,
        open_after,
    )


@mcp.tool()
def generate_chart(
    file_path: str,
    chart_type: str,
    value_column: str,
    category_column: str = "",
    agg_func: str = "sum",
    color_column: str = "",
    date_column: str = "",
    period: str = "M",
    hierarchy_columns: list[str] = None,
    geo_file_path: str = "",
    geo_join_column: str = "",
    output_path: str = "",
    title: str = "",
    theme: str = "plotly_dark",
    open_after: bool = True,
) -> dict:
    """Generate Plotly chart. type: bar pie line scatter geo treemap radius."""
    return engine.generate_chart(
        file_path,
        chart_type,
        value_column,
        category_column,
        agg_func,
        color_column,
        date_column,
        period,
        hierarchy_columns,
        geo_file_path,
        geo_join_column,
        output_path,
        title,
        theme,
        open_after,
    )


@mcp.tool()
def generate_dashboard(
    file_path: str,
    output_path: str = "",
    title: str = "",
    chart_types: list[str] = None,
    geo_file_path: str = "",
    theme: str = "plotly_dark",
    dry_run: bool = False,
) -> dict:
    """Generate Streamlit dashboard app.py from dataset. Run separately."""
    return engine.generate_dashboard(
        file_path, output_path, title, chart_types, geo_file_path, theme, dry_run
    )


@mcp.tool()
def generate_correlation_heatmap(
    file_path: str,
    method: str = "pearson",
    output_path: str = "",
    open_after: bool = True,
) -> dict:
    """Interactive correlation heatmap for numeric columns. Opens HTML."""
    return engine.generate_correlation_heatmap(
        file_path, method, output_path, open_after
    )


@mcp.tool()
def generate_pairwise_plot(
    file_path: str,
    columns: list[str] = None,
    max_cols: int = 6,
    output_path: str = "",
    open_after: bool = True,
) -> dict:
    """Pairwise scatter + histogram matrix for numeric columns. Opens HTML."""
    return engine.generate_pairwise_plot(
        file_path, columns, max_cols, output_path, open_after
    )


@mcp.tool()
def export_data(
    file_path: str,
    output_path: str = "",
    format: str = "csv",
    encoding: str = "utf-8",
    separator: str = ",",
    open_after: bool = True,
) -> dict:
    """Export dataset to CSV, Excel, or JSON format."""
    return engine.export_data(
        file_path, output_path, format, encoding, separator, open_after
    )


@mcp.tool()
def rfm_analysis(
    file_path: str,
    customer_column: str = "",
    date_column: str = "",
    monetary_column: str = "",
    reference_date: str = "",
    n_segments: int = 5,
    output_path: str = "",
) -> dict:
    """RFM segmentation with auto-detect columns. Saves enriched CSV."""
    return engine.rfm_analysis(
        file_path,
        customer_column,
        date_column,
        monetary_column,
        reference_date,
        n_segments,
        output_path,
    )


@mcp.tool()
def auto_chart_recommendation(
    file_path: str,
    column_a: str = "",
    column_b: str = "",
) -> dict:
    """Auto-recommend best chart type based on column types and data patterns."""
    return engine.auto_chart_recommendation(file_path, column_a, column_b)


@mcp.tool()
def generate_insights_report(
    file_path: str,
    output_path: str = "",
    open_after: bool = True,
) -> dict:
    """Auto-generate text insights from data patterns. Saves HTML report."""
    return engine.generate_insights_report(file_path, output_path, open_after)


@mcp.tool()
def anomaly_detection(
    file_path: str,
    columns: list[str] = None,
    method: str = "zscore",
    threshold: float = 3.0,
) -> dict:
    """Detect anomalies using Z-score or IQR method. Returns flagged rows."""
    return engine.anomaly_detection(file_path, columns, method, threshold)


@mcp.tool()
def segmentation_analysis(
    file_path: str,
    features: list[str] = None,
    n_clusters: int = 4,
    output_path: str = "",
    open_after: bool = True,
) -> dict:
    """K-means clustering for customer/data segmentation."""
    return engine.segmentation_analysis(
        file_path, features, n_clusters, output_path, open_after
    )


@mcp.tool()
def generate_auto_profile(
    file_path: str,
    output_path: str = "",
    open_after: bool = True,
) -> dict:
    """Fast auto-profile: overview, distributions, correlations, outliers. Opens HTML."""
    return engine.generate_auto_profile(file_path, output_path, open_after)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
