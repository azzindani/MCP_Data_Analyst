"""T2 data_transform MCP server — thin wrapper only. Zero domain logic."""

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

mcp = FastMCP("data_transform")


@mcp.tool()
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


@mcp.tool()
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


@mcp.tool()
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
    )


@mcp.tool()
def resample_timeseries(
    file_path: str,
    date_col: str,
    freq: str = "M",
    agg_func: str = "sum",
    value_cols: list[str] = None,
    group_by: str = None,
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    """Resample time series by freq: D W M Q Y H. agg: sum mean count min max."""
    return engine.resample_timeseries(file_path, date_col, freq, agg_func, value_cols, group_by, output_path, dry_run)


@mcp.tool()
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


@mcp.tool()
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


@mcp.tool()
def smart_impute(
    file_path: str,
    columns: list[str] = None,
    output_path: str = "",
    dry_run: bool = False,
    open_after: bool = True,
) -> dict:
    """Smart impute missing values using column-type-appropriate strategies."""
    return engine.smart_impute(file_path, columns, output_path, dry_run, open_after)


@mcp.tool()
def run_cleaning_pipeline(
    file_path: str,
    ops: list[dict],
    dry_run: bool = False,
) -> dict:
    """Run ordered cleaning ops in one call. Single snapshot taken."""
    return engine.run_cleaning_pipeline(file_path, ops, dry_run)


@mcp.tool()
def feature_engineering(
    file_path: str,
    features: list[str] = None,
    output_path: str = "",
    dry_run: bool = False,
    open_after: bool = True,
) -> dict:
    """Auto-create features: date parts, numeric bins, text length, one-hot."""
    return engine.feature_engineering(file_path, features, output_path, dry_run, open_after)


@mcp.tool()
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


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
