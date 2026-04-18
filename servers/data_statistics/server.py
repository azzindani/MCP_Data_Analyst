"""T3 data_statistics MCP server — thin wrapper only. Zero domain logic."""

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

mcp = FastMCP("data_statistics")


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


@mcp.tool()
def validate_dataset(
    file_path: str,
    expected_dtypes: dict = None,
    max_null_pct: float = 5.0,
    check_duplicates: bool = True,
) -> dict:
    """Validate dataset quality: types nulls duplicates ranges. Score 0-100."""
    return engine.validate_dataset(file_path, expected_dtypes, max_null_pct, check_duplicates)


@mcp.tool()
def auto_detect_schema(
    file_path: str,
    max_rows: int = 1000,
) -> dict:
    """Auto-detect column types, dates, IDs, categories with cleaning suggestions."""
    return engine.auto_detect_schema(file_path, max_rows)


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
    """Scan for outliers + anomalies. method: iqr std both. Flags anomalous rows."""
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
    """Scan all columns for nulls and zeros. Returns counts, pcts, patterns."""
    return engine.scan_nulls_zeros(file_path, include_zeros, min_count, output_path, open_after, theme)


@mcp.tool()
def correlation_analysis(
    file_path: str,
    method: str = "pearson",
    top_n: int = 10,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
) -> dict:
    """Correlation matrix + top pairs. method: pearson spearman kendall."""
    return engine.correlation_analysis(file_path, method, top_n, output_path, open_after, theme)


@mcp.tool()
def statistical_test(
    file_path: str,
    test: str,
    column_a: str = "",
    column_b: str = "",
    group_column: str = "",
    alpha: float = 0.05,
    alternative: str = "two-sided",
    compute_effect_size: bool = True,
    posthoc: bool = False,
    correction: str = "",
    hypothesized_mean: float = 0.0,
) -> dict:
    """Run stat test. test: shapiro_wilk t_test anova chi_square mann_whitney kruskal."""
    return engine.statistical_test(
        file_path,
        test,
        column_a,
        column_b,
        group_column,
        alpha,
        alternative,
        compute_effect_size,
        posthoc,
        correction,
        hypothesized_mean,
    )


@mcp.tool()
def regression_analysis(
    file_path: str,
    y_col: str,
    x_cols: list[str],
    model_type: str = "ols",
    interaction_terms: list[str] = None,
    output_path: str = "",
) -> dict:
    """OLS or logistic regression. Returns coefs p-values R2 RMSE diagnostics."""
    return engine.regression_analysis(file_path, y_col, x_cols, model_type, interaction_terms, output_path)


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
    """Auto-detect dates, compute trend seasonality rolling stats. Saves HTML."""
    return engine.time_series_analysis(file_path, date_column, value_columns, period, output_path, open_after, theme)


@mcp.tool()
def period_comparison(
    file_path: str,
    date_col: str,
    metrics: list[str],
    period_unit: str,
    current_period: str = "",
    compare_to: str = "previous",
    group_by: str = "",
    output_path: str = "",
) -> dict:
    """Compare periods: MoM QoQ YoY. Returns delta pct_change direction."""
    return engine.period_comparison(
        file_path,
        date_col,
        metrics,
        period_unit,
        current_period,
        compare_to,
        group_by,
        output_path,
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
    """Cohort retention matrix. Auto-detect cohort + date + value columns."""
    return engine.cohort_analysis(
        file_path,
        cohort_column,
        date_column,
        value_column,
        output_path,
        open_after,
        theme,
    )


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
