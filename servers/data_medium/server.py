"""Tier 2 MCP server — thin wrapper only. Zero domain logic."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

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
from shared.schema_enum import one_of
from shared.strict_args import enforce_known_arguments
from shared.token_estimate import measure_responses
from shared.tool_annotations import CREATES, EDITS, READS

try:
    from . import engine
except ImportError:
    import engine

_VERSION = "0.2.2"  # keep in sync with pyproject.toml [project].version

_oauth_bridge = build_oauth_bridge(
    "DA", state_dir=os.environ.get("DA_MEDIUM_OAUTH_STATE_DIR", "/tmp/data-medium-oauth-state")
)

# The legal values each dispatch parameter names in its schema. Rendered
# from the table the runtime switches on -- never a second copy -- and split
# on TYPE_CHECKING because a call expression is not a type expression to a
# static checker, while a checker only needs to know these are strings.
if TYPE_CHECKING:
    StatTest = str
    AggFunc = str
    Normalize = str
    SampleDataMethod = str
    DetectAnomaliesMethod = str
else:
    StatTest = one_of(
        "ttest",
        "anova",
        "chi_square",
        "correlation",
        "shapiro_wilk",
        "ks",
        "mann_whitney",
        "kruskal",
        "wilcoxon",
        "levene",
        "fisher",
    )
    AggFunc = one_of("count", "first", "last", "max", "mean", "median", "min", "nunique", "std", "sum", "var")
    Normalize = one_of("index", "columns", "all")
    SampleDataMethod = one_of("head", "random", "tail")
    DetectAnomaliesMethod = one_of("iqr", "zscore", "both")
_public_origin = os.environ.get("DA_PUBLIC_URL", "").rstrip("/")
_base_url = f"{_public_origin}/medium" if _public_origin else None
_HOST = os.environ.get("DATA_MEDIUM_HOST", "127.0.0.1")
_PORT = int(os.environ.get("DATA_MEDIUM_PORT", "8812"))
_token_verifier, _auth_settings = build_auth("DA", _base_url, _oauth_bridge)

mcp = FastMCP(
    "data_medium",
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
def compute_aggregations(
    file_path: str,
    group_by: list[str],
    agg_column: str,
    agg_func: AggFunc = "sum",
    sort_desc: bool = True,
    top_n: int = 0,
) -> dict:
    """Group by columns and aggregate. agg: sum mean count min max."""
    return engine.compute_aggregations(file_path, group_by, agg_column, agg_func, sort_desc, top_n)


@mcp.tool(annotations=CREATES)
def cross_tabulate(
    file_path: str,
    row_column: str,
    col_column: str,
    values_column: str = "",
    agg_func: AggFunc = "count",
    normalize: Normalize = "",
    output_path: str = "",
    open_after: bool = True,
    theme: str = "device",
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


@mcp.tool(annotations=READS)
def pivot_table(
    file_path: str,
    index: list[str],
    columns: list[str] = None,
    values: list[str] = None,
    agg_func: AggFunc = "sum",
    fill_value: float = 0,
) -> dict:
    """Multi-dimensional pivot/aggregation table."""
    return engine.pivot_table(file_path, index, columns, values, agg_func, fill_value)


@mcp.tool(annotations=CREATES)
def value_counts(
    file_path: str,
    columns: list[str],
    top_n: int = 20,
    include_pct: bool = True,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "device",
) -> dict:
    """Frequency tables with percentages for categorical columns."""
    return engine.value_counts(file_path, columns, top_n, include_pct, output_path, open_after, theme)


@mcp.tool(annotations=EDITS)
def filter_rows(
    file_path: str,
    conditions: list[dict],
    output_path: str = "",
    dry_run: bool = False,
    open_after: bool = True,
    sort_by: list[str] = None,
    sort_ascending: list[bool] = None,
) -> dict:
    """Filter rows. Compare a column to 'value' or to 'other_column'."""
    return engine.filter_rows(file_path, conditions, output_path, dry_run, open_after, sort_by, sort_ascending)


@mcp.tool(annotations=CREATES)
def sample_data(
    file_path: str,
    method: SampleDataMethod = "random",
    n: int = 100,
    random_state: int = 42,
    output_path: str = "",
    open_after: bool = True,
    top_n: int = 0,
) -> dict:
    """Sample rows from dataset. methods: random head tail."""
    return engine.sample_data(file_path, method, n, random_state, output_path, open_after, top_n)


@mcp.tool(annotations=READS)
def statistical_tests(
    file_path: str,
    test_type: StatTest = "",
    column_a: str = "",
    column_b: str = "",
    group_column: str = "",
    test: StatTest = "",
) -> dict:
    """Auto-select and run statistical tests: t-test ANOVA chi-square correlation."""
    return engine.statistical_tests(file_path, test_type, column_a, column_b, group_column, test)


@mcp.tool(annotations=READS)
def analyze_text_column(file_path: str, column: str, top_n: int = 20) -> dict:
    """Analyze text column: length stats, word freq, pattern detection."""
    return engine.analyze_text_column(file_path, column, top_n)


@mcp.tool(annotations=CREATES)
def detect_anomalies(
    file_path: str,
    columns: list[str] = None,
    method: DetectAnomaliesMethod = "both",
    output_path: str = "",
    threshold: float = 3.0,
) -> dict:
    """Flag anomalous rows using IQR and/or z-score. Saves flagged CSV."""
    return engine.detect_anomalies(file_path, columns, method, output_path, threshold)


@mcp.tool(annotations=READS)
def compare_datasets(
    file_path_a: str = "",
    file_path_b: str = "",
    key_columns: list[str] = None,
    file_path: str = "",
    right_file_path: str = "",
) -> dict:
    """Compare two CSVs: schema diff, row counts, value changes."""
    return engine.compare_datasets(file_path_a, file_path_b, key_columns, file_path, right_file_path)


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
    parser = argparse.ArgumentParser(description="data_medium MCP Server")
    parser.add_argument(
        "--transport", choices=["stdio", "http"], default=os.environ.get("DA_MEDIUM_TRANSPORT", "stdio")
    )
    args = parser.parse_args()

    if args.transport == "http":
        mcp.run(transport="streamable-http")
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
