"""T2 data_transform MCP server — thin wrapper only. Zero domain logic."""

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
from shared.token_estimate import measure_responses
from shared.tool_annotations import CREATES, EDITS

try:
    from . import engine
except ImportError:
    import engine

_VERSION = "0.2.2"  # keep in sync with pyproject.toml [project].version

_oauth_bridge = build_oauth_bridge(
    "DA", state_dir=os.environ.get("DA_TRANSFORM_OAUTH_STATE_DIR", "/tmp/data-transform-oauth-state")
)
_public_origin = os.environ.get("DA_PUBLIC_URL", "").rstrip("/")
_base_url = f"{_public_origin}/transform" if _public_origin else None
_HOST = os.environ.get("DATA_TRANSFORM_HOST", "127.0.0.1")
_PORT = int(os.environ.get("DATA_TRANSFORM_PORT", "8814"))
_token_verifier, _auth_settings = build_auth("DA", _base_url, _oauth_bridge)

mcp = FastMCP(
    "data_transform",
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


@mcp.tool(annotations=EDITS)
def filter_dataset(
    file_path: str,
    conditions: list[dict],
    sort_by: list[str] = None,
    sort_ascending: list[bool] = None,
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    """Filter rows by conditions + sort. ops: equals isin between regex date_range."""
    return engine.filter_dataset(file_path, conditions, sort_by, sort_ascending, output_path, dry_run)


@mcp.tool(annotations=EDITS)
def reshape_dataset(
    file_path: str,
    mode: str,
    index: list[str] = None,
    columns: list[str] = None,
    values: list[str] = None,
    agg_func: str = "sum",
    id_vars: list[str] = None,
    value_vars: list[str] = None,
    var_name: str = "variable",
    value_name: str = "value",
    split_column: str = "",
    delimiter: str = ",",
    new_columns: list[str] = None,
    drop_original: bool = False,
    combine_columns: list[str] = None,
    combine_delimiter: str = "_",
    new_column: str = "combined",
    drop_originals: bool = False,
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    """Reshape data. mode: pivot melt split_column combine_columns transpose."""
    return engine.reshape_dataset(
        file_path,
        mode,
        index,
        columns,
        values,
        agg_func,
        id_vars,
        value_vars,
        var_name,
        value_name,
        split_column,
        delimiter,
        new_columns,
        drop_original,
        combine_columns,
        combine_delimiter,
        new_column,
        drop_originals,
        output_path,
        dry_run,
    )


@mcp.tool(annotations=EDITS)
def aggregate_dataset(
    file_path: str,
    mode: str,
    group_by: list[str] = None,
    agg: dict = None,
    sort_desc: bool = True,
    top_n: int = 0,
    row_col: str = "",
    col_col: str = "",
    values_col: str = "",
    normalize: str = "",
    columns: list[str] = None,
    include_pct: bool = True,
    order_by: str = "",
    window: int = 3,
    window_agg: str = "mean",
    output_path: str = "",
    dry_run: bool = False,
    row_column: str = "",
    col_column: str = "",
    values_column: str = "",
) -> dict:
    """Aggregate data. mode: groupby crosstab value_counts describe window."""
    return engine.aggregate_dataset(
        file_path,
        mode,
        group_by,
        agg,
        sort_desc,
        top_n,
        row_col,
        col_col,
        values_col,
        normalize,
        columns,
        include_pct,
        order_by,
        window,
        window_agg,
        output_path,
        dry_run,
        row_column,
        col_column,
        values_column,
    )


@mcp.tool(annotations=CREATES)
def resample_timeseries(
    file_path: str,
    date_col: str = "",
    freq: str = "M",
    agg_func: str = "sum",
    value_cols: list[str] = None,
    group_by: str = None,
    output_path: str = "",
    dry_run: bool = False,
    date_column: str = "",
    value_columns: list[str] = None,
) -> dict:
    """Resample time series by freq: D W M Q Y H. agg: sum mean count min max."""
    return engine.resample_timeseries(
        file_path, date_col, freq, agg_func, value_cols, group_by, output_path, dry_run, date_column, value_columns
    )


@mcp.tool(annotations=EDITS)
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
    """Merge two datasets. how: inner left right outer. Auto-detect join keys."""
    return engine.merge_datasets(file_path, right_file_path, left_on, right_on, how, output_path, dry_run, open_after)


@mcp.tool(annotations=CREATES)
def concat_datasets(
    file_paths: list[str],
    direction: str = "rows",
    fill_missing: str = "null",
    add_source_column: bool = True,
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    """Stack multiple CSVs vertically (rows) or horizontally (columns)."""
    return engine.concat_datasets(file_paths, direction, fill_missing, add_source_column, output_path, dry_run)


@mcp.tool(annotations=EDITS)
def smart_impute(
    file_path: str,
    columns: list[str] = None,
    output_path: str = "",
    dry_run: bool = False,
    open_after: bool = True,
) -> dict:
    """Smart impute missing values using column-type-appropriate strategies."""
    return engine.smart_impute(file_path, columns, output_path, dry_run, open_after)


@mcp.tool(annotations=EDITS)
def run_cleaning_pipeline(
    file_path: str,
    ops: list[dict],
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    """Run ordered cleaning ops in one call. Single snapshot taken."""
    return engine.run_cleaning_pipeline(file_path, ops, output_path, dry_run)


@mcp.tool(annotations=EDITS)
def feature_engineering(
    file_path: str,
    features: list[str] = None,
    output_path: str = "",
    dry_run: bool = False,
    open_after: bool = True,
) -> dict:
    """Auto-create features: date parts, numeric bins, text length, one-hot."""
    return engine.feature_engineering(file_path, features, output_path, dry_run, open_after)


@mcp.tool(annotations=EDITS)
def enrich_with_geo(
    file_path: str,
    geo_file_path: str,
    join_column: str,
    geo_join_column: str,
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    """Merge dataset with geo data on location key. Saves enriched result."""
    return engine.enrich_with_geo(file_path, geo_file_path, join_column, geo_join_column, output_path, dry_run)


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


def main() -> None:
    parser = argparse.ArgumentParser(description="data_transform MCP Server")
    parser.add_argument(
        "--transport", choices=["stdio", "http"], default=os.environ.get("DA_TRANSFORM_TRANSPORT", "stdio")
    )
    args = parser.parse_args()

    if args.transport == "http":
        mcp.run(transport="streamable-http")
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
