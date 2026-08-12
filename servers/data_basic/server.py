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

from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from shared.deploy_auth import build_oauth_bridge, build_token_verifier

try:
    from . import engine
except ImportError:
    import engine

_VERSION = "0.2.1"  # keep in sync with pyproject.toml [project].version

_oauth_bridge = build_oauth_bridge(
    "DA", state_dir=os.environ.get("DA_BASIC_OAUTH_STATE_DIR", "/tmp/data-basic-oauth-state")
)
_public_origin = os.environ.get("DA_PUBLIC_URL", "").rstrip("/")
_base_url = f"{_public_origin}/basic" if _public_origin else None
mcp = FastMCP("data_basic", auth=build_token_verifier("DA", _oauth_bridge, base_url=_base_url))
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


@mcp.tool()
def load_dataset(
    file_path: str,
    encoding: str = "utf-8",
    separator: str = ",",
    max_rows: int = 0,
) -> dict:
    """Load CSV file. Returns schema, row count, dtypes, null counts."""
    return engine.load_dataset(file_path, encoding, separator, max_rows)


@mcp.tool()
def load_geo_dataset(
    file_path: str,
    rename_column: str = "",
    keep_columns: list[str] = None,
) -> dict:
    """Load GeoJSON or shapefile. Returns geometry columns and CRS."""
    return engine.load_geo_dataset(file_path, rename_column, keep_columns)


@mcp.tool()
def inspect_dataset(
    file_path: str,
    include_sample: bool = False,
) -> dict:
    """Inspect dataset schema, dtypes, null counts, row/col totals."""
    return engine.inspect_dataset(file_path, include_sample)


@mcp.tool()
def read_column_stats(
    file_path: str,
    column: str,
) -> dict:
    """Stats for one column: mean median std min max nulls unique top."""
    return engine.read_column_stats(file_path, column)


@mcp.tool()
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


@mcp.tool()
def apply_patch(
    file_path: str,
    ops: list[dict],
    dry_run: bool = False,
) -> dict:
    """Apply ordered ops to a CSV. ops: see Op Reference below."""
    return engine.apply_patch(file_path, ops, dry_run)


@mcp.tool()
def restore_version(
    file_path: str,
    timestamp: str = "",
) -> dict:
    """Restore file to a snapshot. timestamp from backup filename."""
    return engine.restore_version(file_path, timestamp)


@mcp.tool()
def read_receipt(
    file_path: str,
    last_n: int = 10,
) -> dict:
    """Read operation history log for a file. Returns receipt entries."""
    return engine.read_receipt(file_path, last_n)


@mcp.tool()
def list_patch_ops(category: str = "") -> dict:
    """List apply_patch ops. category: filtering numeric encoding temporal structural."""
    return engine.list_patch_ops(category)


def main() -> None:
    parser = argparse.ArgumentParser(description="data_basic MCP Server")
    parser.add_argument("--transport", choices=["stdio", "http"], default=os.environ.get("DA_BASIC_TRANSPORT", "stdio"))
    parser.add_argument("--host", default=os.environ.get("DA_BASIC_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("DA_BASIC_PORT", "8810")))
    args = parser.parse_args()

    if args.transport == "http":
        mcp.run(transport="http", host=args.host, port=args.port)
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
