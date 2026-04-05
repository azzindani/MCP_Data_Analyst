"""Tier 2 MCP server — thin wrapper only. Zero domain logic."""

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

mcp = FastMCP("data_medium")


@mcp.tool()
def check_outliers(
    file_path: str,
    columns: list[str] = None,
    method: str = "both",
    th1: float = 0.25,
    th3: float = 0.75,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
) -> dict:
    """Scan numeric columns for outliers. method: iqr std both."""
    return engine.check_outliers(file_path, columns, method, th1, th3, output_path, open_after, theme)


@mcp.tool()
def scan_nulls_zeros(
    file_path: str,
    include_zeros: bool = True,
    min_count: int = 1,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
) -> dict:
    """Scan all columns for nulls and zeros. Returns counts and pcts."""
    return engine.scan_nulls_zeros(file_path, include_zeros, min_count, output_path, open_after, theme)


@mcp.tool()
def enrich_with_geo(
    file_path: str,
    geo_file_path: str,
    join_column: str,
    geo_join_column: str,
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    """Merge dataset with geo data on a location key. Saves result."""
    return engine.enrich_with_geo(
        file_path, geo_file_path, join_column, geo_join_column, output_path, dry_run
    )


@mcp.tool()
def validate_dataset(
    file_path: str,
    expected_dtypes: dict = None,
    max_null_pct: float = 5.0,
    check_duplicates: bool = True,
) -> dict:
    """Validate dataset quality: types nulls duplicates ranges. Report."""
    return engine.validate_dataset(
        file_path, expected_dtypes, max_null_pct, check_duplicates
    )


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
    return engine.compute_aggregations(
        file_path, group_by, agg_column, agg_func, sort_desc, top_n
    )


@mcp.tool()
def run_cleaning_pipeline(
    file_path: str,
    ops: list[dict],
    dry_run: bool = False,
) -> dict:
    """Run ordered cleaning ops in one call. Single snapshot taken."""
    return engine.run_cleaning_pipeline(file_path, ops, dry_run)


@mcp.tool()
def correlation_analysis(
    file_path: str,
    method: str = "pearson",
    top_n: int = 10,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
) -> dict:
    """Correlation matrix + top N strongest pairs for numeric columns."""
    return engine.correlation_analysis(file_path, method, top_n, output_path, open_after, theme)


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
        file_path, row_column, col_column, values_column, agg_func, normalize, output_path, open_after, theme
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
) -> dict:
    """Filter rows by conditions. ops: equals contains gt lt gte lte not_null is_null."""
    return engine.filter_rows(file_path, conditions, output_path, dry_run, open_after)


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
    return engine.sample_data(
        file_path, method, n, random_state, output_path, open_after
    )


@mcp.tool()
def auto_detect_schema(
    file_path: str,
    max_rows: int = 1000,
) -> dict:
    """Auto-detect column types, dates, IDs, categories with cleaning suggestions."""
    return engine.auto_detect_schema(file_path, max_rows)


@mcp.tool()
def smart_impute(
    file_path: str,
    columns: list[str] = None,
    output_path: str = "",
    dry_run: bool = False,
    open_after: bool = True,
) -> dict:
    """Smart impute missing values using column-type-appropriate strategies."""
    return engine.smart_impute(file_path, columns, output_path, dry_run, open_after)


@mcp.tool()
def merge_datasets(
    file_path: str,
    right_file_path: str,
    left_on: str = "",
    right_on: str = "",
    how: str = "left",
    output_path: str = "",
    dry_run: bool = False,
    open_after: bool = True,
) -> dict:
    """Merge two datasets with auto-detect join keys and mismatch detection."""
    return engine.merge_datasets(
        file_path,
        right_file_path,
        left_on,
        right_on,
        how,
        output_path,
        dry_run,
        open_after,
    )


@mcp.tool()
def feature_engineering(
    file_path: str,
    features: list[str] = None,
    output_path: str = "",
    dry_run: bool = False,
    open_after: bool = True,
) -> dict:
    """Auto-create features: date parts, numeric bins, text length, one-hot encoding."""
    return engine.feature_engineering(
        file_path, features, output_path, dry_run, open_after
    )


@mcp.tool()
def statistical_tests(
    file_path: str,
    test_type: str = "",
    column_a: str = "",
    column_b: str = "",
    group_column: str = "",
) -> dict:
    """Auto-select and run statistical tests: t-test, ANOVA, chi-square, correlation."""
    return engine.statistical_tests(
        file_path, test_type, column_a, column_b, group_column
    )


@mcp.tool()
def time_series_analysis(
    file_path: str,
    date_column: str = "",
    value_columns: list[str] = None,
    period: str = "M",
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
) -> dict:
    """Auto-detect date column, compute trend, seasonality, rolling stats."""
    return engine.time_series_analysis(
        file_path, date_column, value_columns, period, output_path, open_after, theme
    )


@mcp.tool()
def cohort_analysis(
    file_path: str,
    cohort_column: str = "",
    date_column: str = "",
    value_column: str = "",
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
) -> dict:
    """Cohort retention analysis with auto-detection of cohort identifiers."""
    return engine.cohort_analysis(
        file_path, cohort_column, date_column, value_column, output_path, open_after, theme
    )


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
