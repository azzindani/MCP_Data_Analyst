"""Tier 1 MCP server — thin wrapper only. Zero domain logic."""

from __future__ import annotations

import sys
import logging

logging.basicConfig(stream=sys.stderr, level=logging.WARNING)

from fastmcp import FastMCP

try:
    from . import engine
except ImportError:
    import engine

mcp = FastMCP("data_basic")


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
    return engine.search_columns(
        file_path, has_nulls, has_zeros, dtype, name_contains, min_null_pct
    )


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


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
