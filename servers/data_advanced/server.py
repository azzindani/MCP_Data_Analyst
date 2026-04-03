"""Tier 3 MCP server — thin wrapper only. Zero domain logic."""

from __future__ import annotations

import sys
import logging

logging.basicConfig(stream=sys.stderr, level=logging.WARNING)

from fastmcp import FastMCP

try:
    from . import engine
except ImportError:
    import engine

mcp = FastMCP("data_advanced")


@mcp.tool()
def generate_profile_report(
    file_path: str,
    output_path: str = "",
    title: str = "",
    description: str = "",
    correlations: bool = True,
    minimal: bool = False,
) -> dict:
    """Run ydata-profiling on dataset. Saves HTML report to disk."""
    return engine.generate_profile_report(
        file_path, output_path, title, description, correlations, minimal
    )


@mcp.tool()
def generate_sweetviz_report(
    file_path: str,
    output_path: str = "",
    target_column: str = "",
) -> dict:
    """Run SweetViz EDA on dataset. Saves HTML report to disk."""
    return engine.generate_sweetviz_report(file_path, output_path, target_column)


@mcp.tool()
def generate_autoviz_report(
    file_path: str,
    output_dir: str = "",
    chart_format: str = "html",
    max_rows_analyzed: int = 0,
) -> dict:
    """Run AutoViz auto-EDA on dataset. Saves charts to output dir."""
    return engine.generate_autoviz_report(
        file_path, output_dir, chart_format, max_rows_analyzed
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


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
