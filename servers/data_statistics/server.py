"""T3 data_statistics MCP server — thin wrapper only. Zero domain logic."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(stream=sys.stderr, level=logging.WARNING)

_root = str(Path(__file__).resolve().parents[2])
if _root not in sys.path:
    sys.path.insert(0, _root)

from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from shared.arg_errors import contract_errors
from shared.deploy_auth import build_auth, build_oauth_bridge
from shared.json_safe import sanitize_responses
from shared.strict_args import enforce_known_arguments
from shared.token_estimate import measure_responses
from shared.tool_annotations import CREATES, READS

try:
    from . import engine
except ImportError:
    import engine

_VERSION = "0.2.2"  # keep in sync with pyproject.toml [project].version

_oauth_bridge = build_oauth_bridge(
    "DA", state_dir=os.environ.get("DA_STATISTICS_OAUTH_STATE_DIR", "/tmp/data-statistics-oauth-state")
)
_public_origin = os.environ.get("DA_PUBLIC_URL", "").rstrip("/")
_base_url = f"{_public_origin}/statistics" if _public_origin else None
_HOST = os.environ.get("DATA_STATISTICS_HOST", "127.0.0.1")
_PORT = int(os.environ.get("DATA_STATISTICS_PORT", "8813"))
_token_verifier, _auth_settings = build_auth("DA", _base_url, _oauth_bridge)

mcp = FastMCP(
    "data_statistics",
    host=_HOST,
    port=_PORT,
    token_verifier=_token_verifier,
    auth=_auth_settings,
)
if _oauth_bridge is not None:
    _oauth_bridge.register_routes(mcp)


@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request) -> JSONResponse:
    """Liveness check. Unauthenticated."""
    return JSONResponse({"status": "ok", "version": _VERSION})


@mcp.custom_route("/version", methods=["GET"])
async def version(request: Request) -> JSONResponse:
    """Report running version. Unauthenticated."""
    return JSONResponse({"current": _VERSION})


@mcp.tool(annotations=READS)
def extended_stats(
    file_path: str,
    columns: list[str] = None,
    percentiles: list[float] = None,
    compute_ci: bool = True,
    ci_level: float = 0.95,
) -> dict:
    """Deep numeric stats: skewness kurtosis percentiles CI MAD CV distribution."""
    return engine.extended_stats(file_path, columns, percentiles, compute_ci, ci_level)


@mcp.tool(annotations=READS)
def validate_dataset(
    file_path: str,
    expected_dtypes: dict = None,
    max_null_pct: float = 5.0,
    check_duplicates: bool = True,
) -> dict:
    """Validate dataset quality: types nulls duplicates ranges. Score 0-100."""
    return engine.validate_dataset(file_path, expected_dtypes, max_null_pct, check_duplicates)


@mcp.tool(annotations=READS)
def auto_detect_schema(
    file_path: str,
    max_rows: int = 1000,
) -> dict:
    """Auto-detect column types, dates, IDs, categories with cleaning suggestions."""
    return engine.auto_detect_schema(file_path, max_rows)


@mcp.tool(annotations=CREATES)
def check_outliers(
    file_path: str,
    columns: list[str] = None,
    method: str = "both",
    th1: float = 0.25,
    th3: float = 0.75,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "device",
) -> dict:
    """Scan for outliers + anomalies. method: iqr std both. Flags anomalous rows."""
    return engine.check_outliers(file_path, columns, method, th1, th3, output_path, open_after, theme)


@mcp.tool(annotations=CREATES)
def scan_nulls_zeros(
    file_path: str,
    include_zeros: bool = True,
    min_count: int = 1,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "device",
) -> dict:
    """Scan all columns for nulls and zeros. Returns counts, pcts, patterns."""
    return engine.scan_nulls_zeros(file_path, include_zeros, min_count, output_path, open_after, theme)


@mcp.tool(annotations=CREATES)
def correlation_analysis(
    file_path: str,
    method: str = "pearson",
    top_n: int = 10,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "device",
) -> dict:
    """Correlation matrix + top pairs. method: pearson spearman kendall."""
    return engine.correlation_analysis(file_path, method, top_n, output_path, open_after, theme)


@mcp.tool(annotations=READS)
def lag_correlation(
    file_path: str,
    date_column: str = "",
    x_column: str = "",
    y_column: str = "",
    max_lag: int = 10,
    period_unit: str = "D",
    method: str = "pearson",
    x_agg: str = "",
    y_agg: str = "",
    min_overlap: int = 8,
    date_col: str = "",
    dayfirst: str = "auto",
) -> dict:
    """Cross-correlate two columns across lags. Returns the curve and its peak."""
    return engine.lag_correlation(
        file_path,
        date_column,
        x_column,
        y_column,
        max_lag,
        period_unit,
        method,
        x_agg,
        y_agg,
        min_overlap,
        date_col,
        dayfirst,
    )


@mcp.tool(annotations=READS)
def statistical_test(
    file_path: str,
    test: str = "",
    column_a: str = "",
    column_b: str = "",
    group_column: str = "",
    alpha: float = 0.05,
    alternative: str = "two-sided",
    compute_effect_size: bool = True,
    posthoc: bool = False,
    correction: str = "",
    hypothesized_mean: float = 0.0,
    test_type: str = "",
) -> dict:
    # Six of the seventeen used to be listed here as if that were the set,
    # so eleven working tests were invisible to every caller who reads the
    # tool list -- which is every caller. The vocabulary outgrew 80
    # characters, so the description points at the error hint that already
    # enumerates it rather than carrying a list that will drift again.
    """Run a stat test. 17 available: an unknown 'test' lists them all."""
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
        test_type,
    )


@mcp.tool(annotations=CREATES)
def regression_analysis(
    file_path: str,
    y_col: str = "",
    x_cols: list[str] = None,
    model_type: str = "ols",
    interaction_terms: list[str] = None,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "device",
    y_column: str = "",
    x_columns: list[str] = None,
) -> dict:
    """OLS or logistic regression. Returns coefs p-values R2 RMSE diagnostics."""
    return engine.regression_analysis(
        file_path, y_col, x_cols, model_type, interaction_terms, output_path, theme, open_after, y_column, x_columns
    )


@mcp.tool(annotations=CREATES)
def time_series_analysis(
    file_path: str,
    date_column: str = "",
    value_columns: list[str] = None,
    period: str = "M",
    output_path: str = "",
    open_after: bool = True,
    theme: str = "device",
    dayfirst: str = "auto",
) -> dict:
    """Trend, seasonality and rolling stats. dayfirst: auto true false."""
    return engine.time_series_analysis(
        file_path, date_column, value_columns, period, output_path, open_after, theme, dayfirst
    )


@mcp.tool(annotations=CREATES)
def period_comparison(
    file_path: str,
    date_col: str = "",
    metrics: list[str] = None,
    period_unit: str = "",
    current_period: str = "",
    compare_to: str = "previous",
    group_by: str = "",
    output_path: str = "",
    open_after: bool = True,
    theme: str = "device",
    date_column: str = "",
    dayfirst: str = "auto",
) -> dict:
    """Compare periods: MoM QoQ YoY. dayfirst: auto true false."""
    return engine.period_comparison(
        file_path,
        date_col,
        metrics,
        period_unit,
        current_period,
        compare_to,
        group_by,
        output_path,
        theme,
        open_after,
        date_column,
        dayfirst,
    )


@mcp.tool(annotations=CREATES)
def cohort_analysis(
    file_path: str,
    cohort_column: str = "",
    date_column: str = "",
    value_column: str = "",
    output_path: str = "",
    open_after: bool = True,
    theme: str = "device",
    dayfirst: str = "auto",
) -> dict:
    """Cohort retention matrix. dayfirst: auto true false."""
    return engine.cohort_analysis(
        file_path,
        cohort_column,
        date_column,
        value_column,
        output_path,
        open_after,
        theme,
        dayfirst,
    )


# Every tool above reports what its response actually costs; see
# shared/token_estimate.py for why this is a choke point and not 325 edits.
# Infinity and NaN are not JSON; strip them before the estimate is taken
# so the number describes what actually goes on the wire.
sanitize_responses(mcp)
measure_responses(mcp)
# A known argument with the WRONG TYPE is rejected by pydantic before any of
# this runs, and used to escape as a raw dump with no success/hint/token_estimate
# and a pydantic.dev URL. Give it the fleet's failure shape instead.
contract_errors(mcp)

# An argument name no tool declares is dropped by the bundled FastMCP's
# pydantic model (extra="ignore") and the call succeeds anyway. Installed
# last so it wraps the guards above and answers first.
enforce_known_arguments(mcp)


def main() -> None:
    parser = argparse.ArgumentParser(description="data_statistics MCP Server")
    parser.add_argument(
        "--transport", choices=["stdio", "http"], default=os.environ.get("DA_STATISTICS_TRANSPORT", "stdio")
    )
    args = parser.parse_args()

    if args.transport == "http":
        mcp.run(transport="streamable-http")
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
