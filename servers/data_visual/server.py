"""T4 data_visual MCP server — thin wrapper only. Zero domain logic."""

from __future__ import annotations

import logging
import sys
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

mcp = FastMCP("data_visual")


@mcp.tool()
def run_eda(
    file_path: str,
    output_path: str = "",
    open_after: bool = False,
    theme: str = "dark",
) -> dict:
    """Fast EDA summary. Stats, nulls, correlations, outliers. Saves HTML."""
    return engine.run_eda(file_path, output_path, open_after, theme)


@mcp.tool()
def generate_auto_profile(
    file_path: str,
    output_path: str = "",
    open_after: bool = False,
    theme: str = "dark",
) -> dict:
    """Full column profile: stats charts correlations outliers insights."""
    return engine.generate_auto_profile(file_path, output_path, open_after, theme)


@mcp.tool()
def generate_distribution_plot(
    file_path: str,
    columns: list[str] = None,
    output_path: str = "",
    open_after: bool = False,
    theme: str = "dark",
) -> dict:
    """Histogram + box plot for numeric columns. Saves HTML."""
    return engine.generate_distribution_plot(file_path, columns, output_path, open_after, theme)


@mcp.tool()
def generate_correlation_heatmap(
    file_path: str,
    method: str = "pearson",
    output_path: str = "",
    open_after: bool = False,
    theme: str = "dark",
) -> dict:
    """Interactive correlation heatmap for numeric columns. Saves HTML."""
    return engine.generate_correlation_heatmap(file_path, method, output_path, open_after, theme)


@mcp.tool()
def generate_pairwise_plot(
    file_path: str,
    columns: list[str] = None,
    max_cols: int = 6,
    output_path: str = "",
    open_after: bool = False,
    theme: str = "dark",
) -> dict:
    """Pairwise scatter + histogram matrix for numeric columns. Saves HTML."""
    return engine.generate_pairwise_plot(file_path, columns, max_cols, output_path, open_after, theme)


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
    open_after: bool = False,
    theme: str = "dark",
) -> dict:
    """Multi-variable bar/line chart. Compares 2+ metrics. Saves HTML."""
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
        theme,
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
    theme: str = "dark",
    open_after: bool = False,
) -> dict:
    """Generate chart. type: bar pie line scatter geo treemap radius time_series."""
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
def generate_geo_map(
    file_path: str,
    lat_column: str = "",
    lon_column: str = "",
    location_column: str = "",
    value_column: str = "",
    location_mode: str = "",
    color_column: str = "",
    title: str = "",
    output_path: str = "",
    theme: str = "dark",
    open_after: bool = False,
) -> dict:
    """Geo map: scatter (lat/lon) or choropleth (country/state). Auto-detects."""
    return engine.generate_geo_map(
        file_path,
        lat_column,
        lon_column,
        location_column,
        value_column,
        location_mode,
        color_column,
        title,
        output_path,
        theme,
        open_after,
    )


@mcp.tool()
def generate_3d_chart(
    file_path: str,
    chart_type: str,
    x_column: str,
    y_column: str,
    z_column: str,
    color_column: str = "",
    title: str = "",
    output_path: str = "",
    theme: str = "dark",
    open_after: bool = False,
) -> dict:
    """3D scatter or surface chart. type: scatter_3d surface. Saves HTML."""
    return engine.generate_3d_chart(
        file_path,
        chart_type,
        x_column,
        y_column,
        z_column,
        color_column,
        title,
        output_path,
        theme,
        open_after,
    )


@mcp.tool()
def generate_dashboard(
    file_path: str,
    output_path: str = "",
    title: str = "",
    chart_types: list[str] = None,
    agg_overrides: list[str] = None,
    geo_file_path: str = "",
    theme: str = "dark",
    dry_run: bool = False,
    open_after: bool = False,
) -> dict:
    """Interactive HTML dashboard with auto-detected charts. Saves HTML."""
    return engine.generate_dashboard(
        file_path,
        output_path,
        title,
        chart_types,
        agg_overrides,
        geo_file_path,
        theme,
        dry_run,
        open_after,
    )


@mcp.tool()
def export_data(
    file_path: str,
    output_path: str = "",
    format: str = "csv",
    encoding: str = "utf-8",
    separator: str = ",",
    open_after: bool = False,
) -> dict:
    """Export dataset to CSV, Excel, or JSON format."""
    return engine.export_data(file_path, output_path, format, encoding, separator, open_after)


@mcp.tool()
def customize_chart(
    chart_path: str,
    title: str = "",
    x_label: str = "",
    y_label: str = "",
    color_scheme: list[str] = None,
    sort_bars: str = "",
    highlight: list[str] = None,
    annotations: list[dict] = None,
    show_value_labels: bool = False,
    width: int = 0,
    height: int = 0,
    output_path: str = "",
) -> dict:
    """Customize existing chart. changes: title labels colors annotations."""
    return engine.customize_chart(
        chart_path,
        title,
        x_label,
        y_label,
        color_scheme,
        sort_bars,
        highlight,
        annotations,
        show_value_labels,
        width,
        height,
        output_path,
    )


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
