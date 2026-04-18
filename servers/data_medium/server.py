"""Tier 2 MCP server — thin wrapper only. Zero domain logic."""

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

mcp = FastMCP("data_medium")


@mcp.tool()
def compute_aggregations(
    file_path: str,
    group_by: list[str],
    agg_column: str,
    agg_func: str = "sum",
    sort_desc: bool = True,
    top_n: int = 0,
) -> dict:
    """Group by columns and aggregate. agg: sum mean count min max."""
    return engine.compute_aggregations(file_path, group_by, agg_column, agg_func, sort_desc, top_n)


@mcp.tool()
def cross_tabulate(
    file_path: str,
    row_column: str,
    col_column: str,
    values_column: str = "",
    agg_func: str = "count",
    normalize: str = "",
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
) -> dict:
    """Contingency table between two categorical columns."""
    return engine.cross_tabulate(
        file_path,
        row_column,
        col_column,
        values_column,
        agg_func,
        normalize,
        output_path,
        open_after,
        theme,
    )


@mcp.tool()
def pivot_table(
    file_path: str,
    index: list[str],
    columns: list[str] = None,
    values: list[str] = None,
    agg_func: str = "sum",
    fill_value: float = 0,
) -> dict:
    """Multi-dimensional pivot/aggregation table."""
    return engine.pivot_table(file_path, index, columns, values, agg_func, fill_value)


@mcp.tool()
def value_counts(
    file_path: str,
    columns: list[str],
    top_n: int = 20,
    include_pct: bool = True,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
) -> dict:
    """Frequency tables with percentages for categorical columns."""
    return engine.value_counts(file_path, columns, top_n, include_pct, output_path, open_after, theme)


@mcp.tool()
def filter_rows(
    file_path: str,
    conditions: list[dict],
    output_path: str = "",
    dry_run: bool = False,
    open_after: bool = True,
    sort_by: list[str] = None,
    sort_ascending: list[bool] = None,
) -> dict:
    """Filter rows by conditions. ops: equals contains gt lt gte lte not_null is_null."""
    return engine.filter_rows(file_path, conditions, output_path, dry_run, open_after, sort_by, sort_ascending)


@mcp.tool()
def sample_data(
    file_path: str,
    method: str = "random",
    n: int = 100,
    random_state: int = 42,
    output_path: str = "",
    open_after: bool = True,
) -> dict:
    """Sample rows from dataset. methods: random head tail."""
    return engine.sample_data(file_path, method, n, random_state, output_path, open_after)


@mcp.tool()
def statistical_tests(
    file_path: str,
    test_type: str = "",
    column_a: str = "",
    column_b: str = "",
    group_column: str = "",
) -> dict:
    """Auto-select and run statistical tests: t-test ANOVA chi-square correlation."""
    return engine.statistical_tests(file_path, test_type, column_a, column_b, group_column)


@mcp.tool()
def analyze_text_column(file_path: str, column: str, top_n: int = 20) -> dict:
    """Analyze text column: length stats, word freq, pattern detection."""
    return engine.analyze_text_column(file_path, column, top_n)


@mcp.tool()
def detect_anomalies(
    file_path: str,
    columns: list[str] = None,
    method: str = "both",
    output_path: str = "",
    threshold: float = 3.0,
) -> dict:
    """Flag anomalous rows using IQR and/or z-score. Saves flagged CSV."""
    return engine.detect_anomalies(file_path, columns, method, output_path, threshold)


@mcp.tool()
def compare_datasets(
    file_path_a: str,
    file_path_b: str,
    key_columns: list[str] = None,
) -> dict:
    """Compare two CSVs: schema diff, row counts, value changes."""
    return engine.compare_datasets(file_path_a, file_path_b, key_columns)


@mcp.tool()
def extended_stats(
    file_path: str,
    columns: list[str] = None,
    percentiles: list[float] = None,
    compute_ci: bool = True,
    ci_level: float = 0.95,
) -> dict:
    """Deep numeric stats: skewness kurtosis percentiles CI MAD CV distribution."""
    return engine.extended_stats(file_path, columns, percentiles, compute_ci, ci_level)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
