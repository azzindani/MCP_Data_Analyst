"""data_ingest MCP server — thin wrapper only. Zero domain logic."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(stream=sys.stderr, level=logging.WARNING)

_ROOT = str(Path(__file__).resolve().parents[2])
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from servers.data_ingest import engine
from shared.arg_errors import contract_errors
from shared.deploy_auth import build_auth, build_oauth_bridge
from shared.json_safe import sanitize_responses
from shared.token_estimate import measure_responses
from shared.tool_annotations import CREATES, EDITS, READS

_VERSION = "0.2.2"  # keep in sync with pyproject.toml [project].version

_oauth_bridge = build_oauth_bridge(
    "DA", state_dir=os.environ.get("DA_INGEST_OAUTH_STATE_DIR", "/tmp/data-ingest-oauth-state")
)
_public_origin = os.environ.get("DA_PUBLIC_URL", "").rstrip("/")
_base_url = f"{_public_origin}/ingest" if _public_origin else None
_HOST = os.environ.get("DATA_INGEST_HOST", "127.0.0.1")
_PORT = int(os.environ.get("DATA_INGEST_PORT", "8811"))
_token_verifier, _auth_settings = build_auth("DA", _base_url, _oauth_bridge)

mcp = FastMCP(
    "data_ingest",
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
def list_sheets(file_path: str) -> dict:
    """List sheets in xlsx/ods with row and col counts."""
    return engine.list_sheets(file_path)


@mcp.tool(annotations=EDITS)
def extract_sheet(
    file_path: str,
    sheet: str = "",
    output_path: str = "",
    header_row: int = 0,
    dry_run: bool = False,
    return_content: bool = False,
) -> dict:
    """Extract one sheet to CSV. sheet: name or index (default first)."""
    return engine.extract_sheet(file_path, sheet, output_path, header_row, dry_run, return_content)


@mcp.tool(annotations=CREATES)
def extract_all_sheets(
    file_path: str,
    output_dir: str = "",
    dry_run: bool = False,
) -> dict:
    """Extract all sheets to separate CSVs in output_dir."""
    return engine.extract_all_sheets(file_path, output_dir, dry_run)


@mcp.tool(annotations=READS)
def detect_tables(
    file_path: str,
    sheet: str = "",
    min_rows: int = 2,
    min_cols: int = 2,
) -> dict:
    """Detect separate tables in a sheet separated by blank rows/cols."""
    return engine.detect_tables(file_path, sheet, min_rows, min_cols)


@mcp.tool(annotations=EDITS)
def extract_table(
    file_path: str,
    table_index: int = 0,
    sheet: str = "",
    output_path: str = "",
    min_rows: int = 2,
    min_cols: int = 2,
    dry_run: bool = False,
    return_content: bool = False,
) -> dict:
    """Extract one detected table by index to CSV."""
    return engine.extract_table(file_path, table_index, sheet, output_path, min_rows, min_cols, dry_run, return_content)


@mcp.tool(annotations=EDITS)
def normalize_headers(
    file_path: str,
    lowercase: bool = True,
    replace_spaces: bool = True,
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    """CSV: strip whitespace, lowercase, dedup headers. output_path: write elsewhere."""
    return engine.normalize_headers(file_path, lowercase, replace_spaces, output_path, dry_run)


@mcp.tool(annotations=EDITS)
def trim_empty(file_path: str, output_path: str = "", dry_run: bool = False) -> dict:
    """CSV: drop empty leading/trailing rows and cols. output_path: write elsewhere."""
    return engine.trim_empty(file_path, output_path, dry_run)


@mcp.tool(annotations=EDITS)
def promote_header(
    file_path: str,
    row_index: int = 0,
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    """CSV: make row N the header; drop rows above. output_path: write elsewhere."""
    return engine.promote_header(file_path, row_index, output_path, dry_run)


@mcp.tool(annotations=EDITS)
def flatten_merged_cells(
    file_path: str,
    sheet: str = "",
    output_path: str = "",
    dry_run: bool = False,
    return_content: bool = False,
) -> dict:
    """Forward-fill merged cell regions in xlsx sheet to CSV."""
    return engine.flatten_merged_cells(file_path, sheet, output_path, dry_run, return_content)


@mcp.tool(annotations=EDITS)
def convert_file(
    file_path: str,
    output_format: str = "csv",
    output_path: str = "",
    sheet: str = "",
    dry_run: bool = False,
    return_content: bool = False,
) -> dict:
    """Convert xlsx/ods/csv/json/parquet to csv/json/parquet/excel."""
    return engine.convert_file(file_path, output_format, output_path, sheet, dry_run, return_content)


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
    parser = argparse.ArgumentParser(description="data_ingest MCP Server")
    parser.add_argument(
        "--transport", choices=["stdio", "http"], default=os.environ.get("DA_INGEST_TRANSPORT", "stdio")
    )
    args = parser.parse_args()

    if args.transport == "http":
        mcp.run(transport="streamable-http")
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
