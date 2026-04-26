"""data_ingest MCP server — thin wrapper only. Zero domain logic."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

logging.basicConfig(stream=sys.stderr, level=logging.WARNING)

_ROOT = str(Path(__file__).resolve().parents[2])
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from fastmcp import FastMCP

from servers.data_ingest import engine

mcp = FastMCP("data_ingest")


@mcp.tool()
def list_sheets(file_path: str) -> dict:
    """List sheets in xlsx/ods with row and col counts."""
    return engine.list_sheets(file_path)


@mcp.tool()
def extract_sheet(
    file_path: str,
    sheet: str = "",
    output_path: str = "",
    header_row: int = 0,
    dry_run: bool = False,
) -> dict:
    """Extract one sheet to CSV. sheet: name or index (default first)."""
    return engine.extract_sheet(file_path, sheet, output_path, header_row, dry_run)


@mcp.tool()
def extract_all_sheets(
    file_path: str,
    output_dir: str = "",
    dry_run: bool = False,
) -> dict:
    """Extract all sheets to separate CSVs in output_dir."""
    return engine.extract_all_sheets(file_path, output_dir, dry_run)


@mcp.tool()
def detect_tables(
    file_path: str,
    sheet: str = "",
    min_rows: int = 2,
    min_cols: int = 2,
) -> dict:
    """Detect separate tables in a sheet separated by blank rows/cols."""
    return engine.detect_tables(file_path, sheet, min_rows, min_cols)


@mcp.tool()
def extract_table(
    file_path: str,
    table_index: int = 0,
    sheet: str = "",
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    """Extract one detected table by index to CSV."""
    return engine.extract_table(file_path, table_index, sheet, output_path, dry_run)


@mcp.tool()
def normalize_headers(
    file_path: str,
    lowercase: bool = True,
    replace_spaces: bool = True,
    dry_run: bool = False,
) -> dict:
    """Strip whitespace, lowercase, dedup column headers in a CSV."""
    return engine.normalize_headers(file_path, lowercase, replace_spaces, dry_run)


@mcp.tool()
def trim_empty(file_path: str, dry_run: bool = False) -> dict:
    """Drop fully-empty leading/trailing rows and columns from CSV."""
    return engine.trim_empty(file_path, dry_run)


@mcp.tool()
def promote_header(
    file_path: str,
    row_index: int = 0,
    dry_run: bool = False,
) -> dict:
    """Make row N the header; drop rows above it."""
    return engine.promote_header(file_path, row_index, dry_run)


@mcp.tool()
def flatten_merged_cells(
    file_path: str,
    sheet: str = "",
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    """Forward-fill merged cell regions in xlsx sheet to CSV."""
    return engine.flatten_merged_cells(file_path, sheet, output_path, dry_run)


@mcp.tool()
def convert_file(
    file_path: str,
    output_format: str = "csv",
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    """Convert xlsx/ods/csv/json/parquet to csv/json/parquet/excel."""
    return engine.convert_file(file_path, output_format, output_path, dry_run)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
