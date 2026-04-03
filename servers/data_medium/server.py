"""Tier 2 MCP server — thin wrapper only. Zero domain logic."""

from __future__ import annotations

import sys
import logging
from pathlib import Path
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
) -> dict:
    """Scan numeric columns for outliers. method: iqr std both."""
    return engine.check_outliers(file_path, columns, method, th1, th3)


@mcp.tool()
def scan_nulls_zeros(
    file_path: str,
    include_zeros: bool = True,
    min_count: int = 1,
) -> dict:
    """Scan all columns for nulls and zeros. Returns counts and pcts."""
    return engine.scan_nulls_zeros(file_path, include_zeros, min_count)


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


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
