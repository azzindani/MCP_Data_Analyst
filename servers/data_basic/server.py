"""Tier 1 MCP server — thin wrapper only. Zero domain logic."""

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
from shared.tool_annotations import EDITS, READS

try:
    from . import engine
except ImportError:
    import engine

_VERSION = "0.2.2"  # keep in sync with pyproject.toml [project].version

_oauth_bridge = build_oauth_bridge(
    "DA", state_dir=os.environ.get("DA_BASIC_OAUTH_STATE_DIR", "/tmp/data-basic-oauth-state")
)
_public_origin = os.environ.get("DA_PUBLIC_URL", "").rstrip("/")
_base_url = f"{_public_origin}/basic" if _public_origin else None
_HOST = os.environ.get("DATA_BASIC_HOST", "127.0.0.1")
_PORT = int(os.environ.get("DATA_BASIC_PORT", "8810"))
_token_verifier, _auth_settings = build_auth("DA", _base_url, _oauth_bridge)

mcp = FastMCP(
    "data_basic",
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
def load_dataset(
    file_path: str,
    encoding: str = "utf-8",
    separator: str = ",",
    max_rows: int = 0,
) -> dict:
    """Load CSV file. Returns schema, row count, dtypes, null counts."""
    return engine.load_dataset(file_path, encoding, separator, max_rows)


@mcp.tool(annotations=READS)
def load_geo_dataset(
    file_path: str,
    rename_column: str = "",
    keep_columns: list[str] = None,
) -> dict:
    """Load GeoJSON or shapefile. Returns geometry columns and CRS."""
    return engine.load_geo_dataset(file_path, rename_column, keep_columns)


@mcp.tool(annotations=READS)
def inspect_dataset(
    file_path: str,
    include_sample: bool = False,
) -> dict:
    """Inspect dataset schema, dtypes, null counts, row/col totals."""
    return engine.inspect_dataset(file_path, include_sample)


@mcp.tool(annotations=READS)
def read_column_stats(
    file_path: str,
    column: str,
) -> dict:
    """Stats for one column: mean median std min max nulls unique top."""
    return engine.read_column_stats(file_path, column)


@mcp.tool(annotations=READS)
def search_columns(
    file_path: str,
    has_nulls: bool = False,
    has_zeros: bool = False,
    dtype: str = "",
    name_contains: str = "",
    min_null_pct: float = 0.0,
) -> dict:
    """Find columns by criteria: has_nulls dtype has_zeros name_contains."""
    return engine.search_columns(file_path, has_nulls, has_zeros, dtype, name_contains, min_null_pct)


@mcp.tool(annotations=EDITS)
def apply_patch(
    file_path: str,
    ops: list[dict],
    dry_run: bool = False,
) -> dict:
    """Apply ordered ops to a CSV. ops: see Op Reference below."""
    return engine.apply_patch(file_path, ops, dry_run)


@mcp.tool(annotations=EDITS)
def restore_version(
    file_path: str,
    timestamp: str = "",
) -> dict:
    """Restore a snapshot. No timestamp overwrites with the newest."""
    return engine.restore_version(file_path, timestamp)


@mcp.tool(annotations=READS)
def read_receipt(
    file_path: str,
    last_n: int = 10,
) -> dict:
    """Read operation history log for a file. Returns receipt entries."""
    return engine.read_receipt(file_path, last_n)


@mcp.tool(annotations=READS)
def list_patch_ops(category: str = "") -> dict:
    """List apply_patch ops. Omit category to get all, grouped by category."""
    # This used to name five categories -- filtering numeric encoding temporal
    # structural -- and there are seven. The two it left out, `original` and
    # `grouped`, hold the thirteen most-used ops (drop_column, clean_text,
    # cast_column, fill_nulls, normalize, label_encode ...) and group_transform,
    # so a caller filtering by the advertised names could not reach them. All
    # seven will not fit in the 80-character docstring budget, so the fix is to
    # stop teaching a partial list: the ungrouped call returns every category as
    # a key, and an unknown category is refused with the real seven named.
    return engine.list_patch_ops(category)


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
    parser = argparse.ArgumentParser(description="data_basic MCP Server")
    parser.add_argument("--transport", choices=["stdio", "http"], default=os.environ.get("DA_BASIC_TRANSPORT", "stdio"))
    args = parser.parse_args()

    if args.transport == "http":
        mcp.run(transport="streamable-http")
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
