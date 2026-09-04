"""Tier 1 engine — all domain logic. Zero MCP imports.

Ring-2 layer in the 3-ring onion model.

Lateral ring-2 peers (I/O infrastructure, same layer):
  shared/file_utils.py       — path resolution, CSV reading, atomic writes
  shared/version_control.py  — snapshot / restore (CoW)
  shared/receipt.py          — operation receipt log
  shared/platform_utils.py   — environment-driven size limits

Ring-1 dependencies (pure utilities, inner layer):
  shared/progress.py         — ok/fail/info/warn/undo helpers (no I/O)
  shared/patch_validator.py  — op-array validation (no I/O)
  shared/column_utils.py     — column inference helpers (no I/O)

Ring-3 caller (outermost MCP boundary):
  server.py                  — thin FastMCP wrapper; one-line tool bodies only

Accepted trade-off (§8 Config):
  get_max_rows() / get_max_results() are called here (ring-2) rather than
  being injected from server.py (ring-3) to preserve the one-line server rule.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

# Shared utilities
_ROOT = str(Path(__file__).resolve().parents[2])
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

_HERE = str(Path(__file__).resolve().parent)
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from _patch_ops import OP_HANDLERS, _parse_expr

from shared.file_utils import atomic_write_text, count_data_rows, error_text, hint_for_error, resolve_path
from shared.patch_validator import VALID_OPS, validate_ops
from shared.platform_utils import get_max_results, get_max_rows
from shared.progress import fail, info, ok, undo, warn
from shared.receipt import append_receipt, read_receipt_log
from shared.receipt import read_receipt as _read_receipt_scoped
from shared.version_control import (
    discard_snapshot_if_unchanged,
    drop_snapshot_if_unwritten,
    list_versions,
    restore,
    snapshot,
)

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stderr, level=logging.WARNING)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _token_estimate(obj) -> int:
    return len(str(obj)) // 4


from shared.file_utils import read_csv as _read_csv  # noqa: E402
from shared.file_utils import read_csv_preserving_ids as _read_csv_for_write  # noqa: E402


def _dtype_label(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "int64"
    if pd.api.types.is_float_dtype(series):
        return "float64"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime64"
    return "object"


def _classify_columns(df: pd.DataFrame) -> tuple[list[str], list[str], list[str]]:
    numeric, categorical, datetime_cols = [], [], []
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            datetime_cols.append(col)
        elif pd.api.types.is_numeric_dtype(df[col]):
            numeric.append(col)
        else:
            categorical.append(col)
    return numeric, categorical, datetime_cols


# ---------------------------------------------------------------------------
# Ring-1 pure helpers — called after I/O; no I/O themselves
# ---------------------------------------------------------------------------


def _profile_df(df: pd.DataFrame) -> dict:
    """Pure: schema profile from a loaded DataFrame. No I/O."""
    return {
        "dtypes": {col: _dtype_label(df[col]) for col in df.columns},
        "null_counts": {col: int(df[col].isna().sum()) for col in df.columns},
        "unique_counts": {col: int(df[col].nunique()) for col in df.columns},
        "sample": df.head(2).fillna("").to_dict(orient="records"),
    }


def _inspect_df(df: pd.DataFrame) -> dict:
    """Pure: full inspection stats from a DataFrame. No I/O."""
    rows = len(df)
    cols = len(df.columns)
    dtypes = {col: _dtype_label(df[col]) for col in df.columns}
    null_counts = {col: int(df[col].isna().sum()) for col in df.columns}
    null_pct = {col: round(null_counts[col] / rows * 100, 2) if rows > 0 else 0.0 for col in df.columns}
    unique_counts = {col: int(df[col].nunique()) for col in df.columns}
    numeric_cols, categorical_cols, datetime_cols = _classify_columns(df)
    return {
        "rows": rows,
        "columns": cols,
        "column_names": list(df.columns),
        "dtypes": dtypes,
        "null_counts": null_counts,
        "null_pct": null_pct,
        "unique_counts": unique_counts,
        "numeric_columns": numeric_cols,
        "categorical_columns": categorical_cols,
        "datetime_columns": datetime_cols,
    }


def _top_values(series: pd.Series, limit: int = 10) -> dict:
    """The most frequent values and their counts, whatever the dtype."""
    counts = series.value_counts().head(limit).to_dict()
    return {str(k): int(v) for k, v in counts.items()}


def _stats_for_series(series: pd.Series, column: str) -> dict:
    """Pure: dtype-appropriate stats dict for a Series. No I/O."""
    dtype = _dtype_label(series)
    count = int(series.count())
    null_count = int(series.isna().sum())
    null_pct = round(null_count / len(series) * 100, 2) if len(series) > 0 else 0.0

    if pd.api.types.is_datetime64_any_dtype(series):
        return {
            "column": column,
            "dtype": dtype,
            "count": count,
            "null_count": null_count,
            "null_pct": null_pct,
            "min": str(series.min()),
            "max": str(series.max()),
        }

    if pd.api.types.is_numeric_dtype(series):
        clean = series.dropna()
        mean_val = float(clean.mean()) if len(clean) > 0 else None
        median_val = float(clean.median()) if len(clean) > 0 else None
        std_val = float(clean.std()) if len(clean) > 1 else None
        min_val = float(clean.min()) if len(clean) > 0 else None
        max_val = float(clean.max()) if len(clean) > 0 else None
        zero_count = int((series == 0).sum())
        q1 = float(clean.quantile(0.25)) if len(clean) > 0 else None
        q3 = float(clean.quantile(0.75)) if len(clean) > 0 else None
        iqr = (q3 - q1) if (q1 is not None and q3 is not None) else None
        lower_iqr = (q1 - 1.5 * iqr) if (iqr is not None and q1 is not None) else None
        upper_iqr = (q3 + 1.5 * iqr) if (iqr is not None and q3 is not None) else None
        outlier_iqr = int(((clean < lower_iqr) | (clean > upper_iqr)).sum()) if iqr is not None else 0
        if mean_val is not None and std_val is not None:
            lower_std = mean_val - 3 * std_val
            upper_std = mean_val + 3 * std_val
            outlier_std = int(((clean < lower_std) | (clean > upper_std)).sum())
        else:
            outlier_std = 0
        return {
            "column": column,
            "dtype": dtype,
            "count": count,
            "null_count": null_count,
            "null_pct": null_pct,
            "zero_count": zero_count,
            "mean": round(mean_val, 4) if mean_val is not None else None,
            "median": round(median_val, 4) if median_val is not None else None,
            "std": round(std_val, 4) if std_val is not None else None,
            "min": round(min_val, 4) if min_val is not None else None,
            "max": round(max_val, 4) if max_val is not None else None,
            "q1": round(q1, 4) if q1 is not None else None,
            "q3": round(q3, 4) if q3 is not None else None,
            "iqr": round(iqr, 4) if iqr is not None else None,
            "outlier_count_iqr": outlier_iqr,
            "outlier_count_std": outlier_std,
            # The docstring reads "mean median std min max nulls unique top"
            # and the numeric branch produced neither of the last two, so the
            # two things a caller asks for when a column is mostly one value
            # were the two it could not get. Found by asking the sibling
            # question of ml_basic's read_column_profile, which promised "top
            # values" and returned them for categorical columns alone.
            "unique_count": int(series.nunique()),
            "top_values": _top_values(series),
        }

    # Categorical path
    unique_count = int(series.nunique())
    return {
        "column": column,
        "dtype": dtype,
        "count": count,
        "null_count": null_count,
        "null_pct": null_pct,
        "unique_count": unique_count,
        "top_values": _top_values(series),
    }


# The three groups search_columns filters by, and the spellings that mean one
# of them unambiguously. Every alias on the left is a dtype label some tool in
# this fleet prints -- pandas' own float64/int64/object, and the friendlier
# int/float/str the schemas use -- so a caller reading a dtype out of one tool
# and passing it to this one is using a name it taught them.
DTYPE_FILTERS: frozenset[str] = frozenset({"numeric", "datetime", "object"})
DTYPE_FILTER_ALIASES: dict[str, str] = {
    "float": "numeric",
    "float64": "numeric",
    "int": "numeric",
    "int64": "numeric",
    "number": "numeric",
    "numerical": "numeric",
    "category": "object",
    "categorical": "object",
    "str": "object",
    "string": "object",
    "text": "object",
    "date": "datetime",
    "datetime64": "datetime",
    "timestamp": "datetime",
}


def _search_df(
    df: pd.DataFrame,
    has_nulls: bool,
    has_zeros: bool,
    dtype: str,
    name_contains: str,
    min_null_pct: float,
) -> tuple[list[str], dict, dict, dict]:
    """Pure: filter columns by criteria. Returns (candidates, null_counts, zero_counts, dtypes). No I/O."""
    rows = len(df)
    candidates = list(df.columns)

    if name_contains:
        candidates = [c for c in candidates if name_contains.lower() in c.lower()]

    if dtype:
        # The chain used to end here with no else, so anything outside these
        # three was dropped and every column came back as a match:
        #
        #     search_columns(f, dtype="float64")  -> all 16 columns, success
        #
        # and float64 is not a wild guess -- it is exactly what load_dataset,
        # inspect_dataset and this tool's own `dtypes` field report. The
        # vocabulary the tool emits was not the vocabulary it accepted. The
        # aliases below close that; DTYPE_FILTERS is what an unlisted value is
        # refused against, one level up in search_columns.
        dtype = DTYPE_FILTER_ALIASES.get(dtype.strip().lower(), dtype)
        if dtype == "numeric":
            candidates = [c for c in candidates if pd.api.types.is_numeric_dtype(df[c])]
        elif dtype == "datetime":
            candidates = [c for c in candidates if pd.api.types.is_datetime64_any_dtype(df[c])]
        else:  # object -- validated by the caller, so this is the third of three
            candidates = [
                c
                for c in candidates
                if not pd.api.types.is_numeric_dtype(df[c]) and not pd.api.types.is_datetime64_any_dtype(df[c])
            ]

    if has_nulls or min_null_pct > 0.0:
        null_c = {c: int(df[c].isna().sum()) for c in candidates}
        null_p = {c: null_c[c] / rows * 100 if rows > 0 else 0.0 for c in candidates}
        if has_nulls:
            candidates = [c for c in candidates if null_c[c] > 0]
        if min_null_pct > 0.0:
            candidates = [c for c in candidates if null_p[c] >= min_null_pct]

    if has_zeros:
        candidates = [c for c in candidates if pd.api.types.is_numeric_dtype(df[c]) and int((df[c] == 0).sum()) > 0]

    null_counts = {c: int(df[c].isna().sum()) for c in candidates}
    zero_counts = {c: int((df[c] == 0).sum()) if pd.api.types.is_numeric_dtype(df[c]) else 0 for c in candidates}
    dtypes_out = {c: _dtype_label(df[c]) for c in candidates}
    return candidates, null_counts, zero_counts, dtypes_out


# ---------------------------------------------------------------------------
# load_dataset
# ---------------------------------------------------------------------------


def load_dataset(
    file_path: str,
    encoding: str = "utf-8",
    separator: str = ",",
    max_rows: int = 0,
) -> dict:
    progress = []
    try:
        path = resolve_path(file_path)

        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check file_path is absolute.",
                "progress": [fail("File not found", str(path))],
                "token_estimate": 30,
            }

        if path.suffix.lower() != ".csv":
            return {
                "success": False,
                "error": f"Expected .csv, got {path.suffix}",
                "hint": "Use file_path pointing to a .csv file.",
                "progress": [fail("Wrong file type", path.suffix)],
                "token_estimate": 30,
            }

        if path.stat().st_size == 0:
            return {
                "success": False,
                "error": f"File is empty: {path.name}",
                "hint": "Verify the file has header + data rows.",
                "progress": [fail("Empty file", path.name)],
                "token_estimate": 30,
            }

        try:
            df = _read_csv(str(path), encoding=encoding, separator=separator, max_rows=max_rows)
        except UnicodeDecodeError:
            return {
                "success": False,
                "error": f"Cannot decode with {encoding}",
                "hint": "Try encoding='ISO-8859-1' or 'latin1'.",
                "progress": [fail("Encoding error", encoding)],
                "token_estimate": 30,
            }

        if df.empty and len(df.columns) == 0:
            return {
                "success": False,
                "error": f"File is empty: {path.name}",
                "hint": "Verify the file has header + data rows.",
                "progress": [fail("Empty file", path.name)],
                "token_estimate": 30,
            }

        # Every count below -- nulls, uniques, and the dtype pandas settles on
        # -- describes the rows that were read, and with max_rows set that is
        # the head of the file rather than the file. On the reference dataset
        # the first null in link_clicks is at row 2,011, so max_rows=1000
        # reports that column as having none, in the same flat shape a
        # whole-file answer would use. A zero meaning "not looked at" is
        # indistinguishable from "there are none".
        #
        # The sample is not widened -- reading it all is what the caller asked
        # not to do -- but the response says what it counted. auto_detect_schema
        # samples 1,000 rows by default and was corrected for this in an earlier
        # round; this is the sibling that was missed.
        total_rows = count_data_rows(path)
        counted_from_sample = total_rows > len(df)

        if max_rows > 0:
            progress.append(
                warn(
                    "Row sampling active",
                    f"read {len(df):,} of {total_rows:,} rows; counts describe the sample"
                    if counted_from_sample
                    else f"max_rows={max_rows} covers the whole file ({total_rows:,} rows)",
                )
            )

        max_r = get_max_rows()
        if len(df) > max_r and max_rows == 0:
            progress.append(
                warn(
                    "Large dataset",
                    f"Constrained mode: returning metadata only, {len(df)} rows total",
                )
            )

        # Ring-1 pure helper — no I/O
        profile = _profile_df(df)

        progress.append(ok(f"Loaded {path.name}", f"{len(df):,} rows × {len(df.columns)} cols"))

        result = {
            "success": True,
            "op": "load_dataset",
            "file": path.name,
            "file_path": str(path),
            "rows": len(df),
            "total_rows": total_rows,
            "counted_from_sample": counted_from_sample,
            "columns": len(df.columns),
            "dtypes": profile["dtypes"],
            "null_counts": profile["null_counts"],
            "unique_counts": profile["unique_counts"],
            "sample": profile["sample"],
            "encoding_used": encoding,
            "hint": (
                f"Counts come from the first {len(df):,} of {total_rows:,} rows, so a null or "
                "unique count of 0 may only mean this sample. Call inspect_dataset() for the "
                "whole column, or raise max_rows."
                if counted_from_sample
                else "Call search_columns() or inspect_dataset() to explore next."
            ),
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("load_dataset error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check that file_path is absolute and the file exists."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# load_geo_dataset
# ---------------------------------------------------------------------------


def load_geo_dataset(
    file_path: str,
    rename_column: str = "",
    keep_columns: list[str] = None,
) -> dict:
    progress = []
    try:
        try:
            import geopandas as gpd
        except ImportError:
            return {
                "success": False,
                "error": "geopandas not installed",
                "hint": "Install geopandas: uv add geopandas",
                "progress": [fail("Missing dependency", "geopandas")],
                "token_estimate": 20,
            }

        path = resolve_path(file_path)

        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check file_path is absolute.",
                "progress": [fail("File not found", path.name)],
                "token_estimate": 20,
            }

        valid_exts = {".geojson", ".shp", ".json"}
        if path.suffix.lower() not in valid_exts:
            return {
                "success": False,
                "error": f"Expected .geojson or .shp, got {path.suffix}",
                "hint": "Use a .geojson or .shp file.",
                "progress": [fail("Wrong file type", path.suffix)],
                "token_estimate": 20,
            }

        gdf = gpd.read_file(str(path))

        # rename_column names the NEW name; the old one is hardcoded as "name",
        # which the schema does not say and the docstring does not mention. On a
        # GeoJSON whose label column is called anything else the argument did
        # nothing at all, silently -- a sweep renaming "site" got a file
        # byte-identical to the no-argument call and a success either way.
        renamed_from = ""
        if rename_column:
            if "name" in gdf.columns:
                gdf = gdf.rename(columns={"name": rename_column})
                renamed_from = "name"
            else:
                progress.append(
                    warn(
                        "rename_column had nothing to rename",
                        f"it renames a column called 'name', and this file has none. Columns: {list(gdf.columns)}",
                    )
                )

        # Names that are not in the file were dropped from the keep list without
        # comment, so asking to keep a misspelled column quietly kept fewer
        # columns than requested.
        missing_keep: list[str] = []
        if keep_columns:
            geo_col = gdf.geometry.name
            missing_keep = [c for c in keep_columns if c not in gdf.columns]
            cols_to_keep = [c for c in keep_columns if c in gdf.columns]
            if geo_col not in cols_to_keep:
                cols_to_keep.append(geo_col)
            gdf = gdf[cols_to_keep]
            if missing_keep:
                progress.append(
                    warn(
                        f"{len(missing_keep)} requested column(s) are not in the file",
                        f"{', '.join(missing_keep)} — kept {', '.join(cols_to_keep)}",
                    )
                )

        sample_rows = gdf.head(2).copy()
        geo_col = gdf.geometry.name
        sample_rows[geo_col] = sample_rows[geo_col].apply(lambda g: g.wkt if g is not None else None)
        sample = sample_rows.fillna("").to_dict(orient="records")

        crs = str(gdf.crs) if gdf.crs else "unknown"
        geom_types = gdf.geometry.geom_type.dropna().unique().tolist()
        geometry_type = geom_types[0] if geom_types else "unknown"

        progress.append(ok(f"Loaded {path.name}", f"{len(gdf)} rows"))

        result = {
            "success": True,
            "op": "load_geo_dataset",
            "file": path.name,
            "rows": len(gdf),
            "columns": list(gdf.columns),
            "crs": crs,
            "geometry_type": geometry_type,
            # Empty when rename_column did nothing, so the caller can tell a
            # rename that happened from one that had no column to act on.
            "renamed_from": renamed_from,
            "columns_not_found": missing_keep,
            "sample": sample,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("load_geo_dataset error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Verify the file is a valid GeoJSON or shapefile."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# inspect_dataset
# ---------------------------------------------------------------------------


def inspect_dataset(
    file_path: str,
    include_sample: bool = False,
) -> dict:
    progress = []
    try:
        path = resolve_path(file_path)

        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check file_path is absolute and the file exists.",
                "progress": [fail("File not found", path.name)],
                "token_estimate": 20,
            }

        df = _read_csv(str(path))

        # Ring-1 pure helper — no I/O
        stats = _inspect_df(df)
        rows = stats["rows"]
        cols = stats["columns"]

        result: dict = {
            "success": True,
            "op": "inspect_dataset",
            "file": path.name,
            "file_path": str(path),
            **stats,
        }

        if include_sample:
            result["sample"] = df.head(2).fillna("").to_dict(orient="records")

        # Truncate column_names if response would exceed ~500 tokens
        estimate = _token_estimate(result)
        if estimate > 500:
            max_c = get_max_results()
            if len(result["column_names"]) > max_c:
                result["column_names"] = result["column_names"][:max_c]
                result["truncated"] = True
                result["total_columns"] = cols
                result["hint"] = (
                    f"Returned first {max_c} of {cols} columns. "
                    "Call read_column_stats(file_path, column=<name>) for a specific column."
                )
                progress.append(
                    warn(
                        "Response truncated",
                        f"Returned first {max_c} of {cols} column names",
                    )
                )

        progress.append(ok(f"Inspected {path.name}", f"{rows:,} rows × {cols} cols"))
        result["progress"] = progress
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("inspect_dataset error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check file_path is absolute and the file is a valid CSV."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# read_column_stats
# ---------------------------------------------------------------------------


def read_column_stats(
    file_path: str,
    column: str,
) -> dict:
    progress = []
    try:
        path = resolve_path(file_path)

        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check file_path is absolute and the file exists.",
                "progress": [fail("File not found", path.name)],
                "token_estimate": 20,
            }

        df = _read_csv(str(path))

        if column not in df.columns:
            available = ", ".join(df.columns.tolist())
            return {
                "success": False,
                "error": f"Column not found: {column}",
                "hint": f"Use inspect_dataset() first. Available: {available}",
                "progress": [fail("Column not found", column)],
                "token_estimate": 30,
            }

        series = df[column]
        progress.append(ok(f"Stats for {column}", _dtype_label(series)))

        # Ring-1 pure helper — no I/O
        stats = _stats_for_series(series, column)

        result = {
            "success": True,
            "op": "read_column_stats",
            "file_path": str(path),
            **stats,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("read_column_stats error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Use inspect_dataset() first to verify column names."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# search_columns
# ---------------------------------------------------------------------------


def search_columns(
    file_path: str,
    has_nulls: bool = False,
    has_zeros: bool = False,
    dtype: str = "",
    name_contains: str = "",
    min_null_pct: float = 0.0,
) -> dict:
    progress = []
    try:
        path = resolve_path(file_path)

        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check file_path is absolute and the file exists.",
                "progress": [fail("File not found", path.name)],
                "token_estimate": 20,
            }

        # A dtype this tool cannot filter by used to be ignored in silence, so
        # the answer was every column and the caller had no way to tell that
        # from a genuine match. Refused here, before the file is read.
        if dtype and DTYPE_FILTER_ALIASES.get(dtype.strip().lower(), dtype) not in DTYPE_FILTERS:
            return {
                "success": False,
                "error": f"Cannot filter by dtype '{dtype}'.",
                "hint": (
                    f"Use one of: {', '.join(sorted(DTYPE_FILTERS))}. "
                    f"Concrete pandas names are accepted too ({', '.join(sorted(DTYPE_FILTER_ALIASES))})."
                ),
                "progress": [fail("Unknown dtype filter", dtype)],
                "token_estimate": 30,
            }

        # Accepting int64 as an alias for the numeric group is deliberate, but
        # the widening was invisible: dtype="int64" answered with columns this
        # same response labels float64, and nothing said the filter had been
        # read as "numeric". A round-16 phase saw exactly that and reported the
        # filter as broken -- which is the proof the disclosure was missing.
        dtype_applied = DTYPE_FILTER_ALIASES.get(dtype.strip().lower(), dtype.strip().lower()) if dtype else ""
        widened = bool(dtype) and dtype_applied != dtype.strip().lower()
        if widened:
            progress.append(
                info(
                    f"Filtered by '{dtype_applied}', not '{dtype.strip()}' exactly",
                    f"this tool groups dtypes into {', '.join(sorted(DTYPE_FILTERS))}",
                )
            )

        df = _read_csv(str(path))

        # Ring-1 pure helper — no I/O
        candidates, null_counts, zero_counts, dtypes_out = _search_df(
            df, has_nulls, has_zeros, dtype, name_contains, min_null_pct
        )

        # Truncate
        max_r = get_max_results()
        total_matched = len(candidates)
        truncated = total_matched > max_r
        if truncated:
            candidates = candidates[:max_r]
            progress.append(warn("Results truncated", f"Showing first {max_r} matching columns"))

        progress.append(ok(f"Searched {path.name}", f"{len(candidates)} column(s) matched"))

        result: dict = {
            "success": True,
            "op": "search_columns",
            "file_path": str(path),
            "matched": len(candidates),
            "columns": candidates,
            "null_counts": {c: null_counts[c] for c in candidates},
            "zero_counts": {c: zero_counts[c] for c in candidates},
            "dtypes": {c: dtypes_out[c] for c in candidates},
            "dtype_filter": dtype_applied,
            "truncated": truncated,
            "progress": progress,
        }
        if truncated:
            result["total_matched"] = total_matched
            result["hint"] = (
                f"Returned first {max_r} of {total_matched} matches. "
                "Refine criteria or call read_column_stats(file_path, column=<name>)."
            )
        else:
            result["hint"] = "Call read_column_stats(file_path, column=<name>) to inspect each match."
        if widened:
            result["hint"] = (
                f"'{dtype.strip()}' was read as the '{dtype_applied}' group, so the matches include "
                f"every {dtype_applied} column whatever its exact dtype -- check the dtypes field to "
                f"see each one. {result['hint']}"
            )
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("search_columns error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check file_path is absolute and the file is a valid CSV."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# apply_patch — op dispatch table
# ---------------------------------------------------------------------------

# Defined in _patch_ops beside the handlers, and shared with
# run_cleaning_pipeline so the two tools cannot drift apart.
_OP_HANDLERS = OP_HANDLERS


# Ring-1 pure transform — no I/O, no exception catching. Raises on error.
def _apply_op(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    """Apply a single op to df. Pure; raises on error. No I/O."""
    op_name = op.get("op", "")
    handler = _OP_HANDLERS[op_name]
    return handler(df, op)


def apply_patch(
    file_path: str,
    ops: list[dict],
    dry_run: bool = False,
) -> dict:
    progress = []
    backup = None
    try:
        path = resolve_path(file_path)

        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check file_path is absolute and the file exists.",
                "progress": [fail("File not found", path.name)],
                "token_estimate": 20,
            }

        # Validate op schema before touching the file
        errors = validate_ops(ops)
        if errors:
            # The validator's messages are specific and already self-answering:
            #
            #   Op 0 (drop_column): unknown field(s) column -- did you mean
            #   columns? drop_column accepts: columns, op, params
            #
            # Reciting the 50-op vocabulary underneath that answers a question
            # the caller did not ask. Round 18 obeyed this hint literally: it
            # re-picked an op from the list -- the op it had already chosen
            # correctly -- and failed again on the same field name, then needed
            # list_patch_ops() to learn what the error had just told it.
            # Only an unknown OP wants the op list.
            hint = (
                f"Valid ops: {', '.join(sorted(VALID_OPS))}"
                if any("unknown op" in e for e in errors)
                else "The error above names each bad op, field and value, and suggests the correction. Apply it and call again."
            )
            return {
                "success": False,
                "error": "; ".join(errors),
                "hint": hint,
                "progress": [fail("Validation failed", str(errors))],
                "token_estimate": 30,
            }

        # This tool rewrites the caller's own file in place, so a column pandas
        # re-typed on the way in is written back re-typed -- and a zero-padded
        # identifier does not survive the round trip. Adding one computed column
        # to a five-column file turned employee_id 0007 into 7 and zip 01970
        # into 1970, under success: true, in columns no op named.
        df = _read_csv_for_write(str(path))

        if dry_run:
            # Ring-2 shell: accumulate errors from ring-1 _apply_op raises (H4).
            dry_df = df.copy()
            dry_results: list[dict] = []
            dry_errors: list[dict] = []
            for i, op in enumerate(ops):
                try:
                    dry_df, op_result = _apply_op(dry_df, op)
                    dry_results.append(op_result)
                except Exception as exc:
                    dry_errors.append({"op_index": i, "op": op.get("op", ""), "error": error_text(exc)})
            would_change = [{"op": op.get("op", ""), "params": op} for op in ops]
            result = {
                "success": len(dry_errors) == 0,
                "dry_run": True,
                "op": "apply_patch",
                "file_path": str(path),
                "would_change": would_change,
                "validated": len(dry_results),
                "validation_errors": dry_errors,
                "progress": [info("Dry run — no changes written", path.name)],
            }
            if dry_errors:
                result["hint"] = "Fix validation_errors before running without dry_run=True."
            result["token_estimate"] = _token_estimate(result)
            return result

        # Take snapshot before first write (ring-2 I/O)
        backup = snapshot(str(path))
        progress.append(info("Snapshot created", Path(backup).name))

        # Ring-2 shell: accumulate all op errors; ring-1 _apply_op raises on error.
        results: list[dict] = []
        op_errors: list[dict] = []
        for i, op in enumerate(ops):
            try:
                df, op_result = _apply_op(df, op)
                results.append(op_result)
                progress.append(ok(f"Applied {op_result.get('op', '?')}", str(op_result)))
            except Exception as exc:
                op_errors.append({"op_index": i, "op": op.get("op", ""), "error": error_text(exc)})
                progress.append(fail(f"Op {i} ({op.get('op', '?')}) failed", str(exc)))

        if op_errors:
            # Do NOT write the modified df — leave the original intact.
            return {
                "success": False,
                "error": f"{len(op_errors)} op(s) failed",
                "op_errors": op_errors,
                "applied": len(results),
                "failed": len(op_errors),
                "backup": drop_snapshot_if_unwritten(backup, path, progress),
                # The comment three lines above this branch reads "Do NOT write
                # the modified df -- leave the original intact", and it is
                # accurate: applied is 0 and the file never changes. Offering
                # restore_version here answered a failed op with a rollback of
                # unrelated work -- what round 18 spent a whole round removing
                # from the Office fleet, still live in this repo.
                "hint": (
                    "Nothing was written -- the original file is intact, so there is nothing to undo. "
                    "Each failing op is listed above with its reason. Fix them and call again."
                ),
                "file_path": str(path),
                "progress": progress,
                "token_estimate": _token_estimate(progress),
            }

        # All ops succeeded — write atomically (G6)
        atomic_write_text(path, df.to_csv(index=False))
        progress.append(ok(f"Saved {path.name}", f"{len(ops)} op(s) applied"))

        # A retried patch whose ops match nothing changed no bytes, and the
        # snapshot taken before it is then a full second copy of a file that
        # never moved. Compared after the fact, so it is exact: a backup equal
        # to the file now on disk cannot restore anything it does not hold.
        kept = discard_snapshot_if_unchanged(backup, path)
        if not kept and backup:
            progress.append(info("Snapshot discarded", "the file is unchanged"))
        backup = kept

        append_receipt(
            str(path),
            tool="apply_patch",
            args={"ops": ops},
            result=f"applied {len(ops)} ops",
            backup=backup,
        )

        # An op that ran and could not do its job says so in a `note`.
        # "applied" counts ops that executed, not ops that had an effect --
        # the same distinction run_cleaning_pipeline draws, and these two
        # tools share their op handlers, so they should draw it the same way.
        no_effect = [entry for entry in results if entry.get("note")]
        for entry in no_effect:
            progress.append(warn(f"{entry['op']} changed nothing", entry["note"]))

        hint = "Call read_column_stats() or inspect_dataset() to verify the changes."
        if no_effect:
            hint = (
                f"{len(no_effect)} of {len(ops)} op(s) ran without changing anything -- read `note` on each "
                "entry of ops_with_no_effect before treating this file as patched."
            )
        result = {
            "success": True,
            "op": "apply_patch",
            "file_path": str(path),
            "applied": len(ops),
            "ops_with_no_effect": no_effect,
            "results": results,
            "backup": backup,
            "changed_file": bool(backup),
            "hint": hint,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("apply_patch error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Use restore_version() to undo if a snapshot was taken."),
            "backup": drop_snapshot_if_unwritten(backup, path),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# restore_version
# ---------------------------------------------------------------------------


def restore_version(
    file_path: str,
    timestamp: str = "",
) -> dict:
    progress = []
    try:
        path = resolve_path(file_path)

        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check file_path is absolute and the file exists.",
                "progress": [fail("File not found", path.name)],
                "token_estimate": 20,
            }

        versions = list_versions(str(path))
        if not versions:
            return {
                "success": False,
                "error": f"No backups found for {path.name}",
                "hint": "Use apply_patch first to create a snapshot.",
                "available_versions": [],
                "progress": [fail("No backups", path.name)],
                "token_estimate": 20,
            }

        versions_dir = path.parent / ".mcp_versions"

        if timestamp:
            # Find backup matching timestamp
            matching = [v for v in versions if timestamp in v]
            if not matching:
                return {
                    "success": False,
                    "error": f"No backup matching timestamp: {timestamp}",
                    "hint": f"Available: {', '.join(versions)}",
                    "available_versions": versions,
                    "progress": [fail("Timestamp not found", timestamp)],
                    "token_estimate": 40,
                }
            backup_name = matching[0]
        else:
            # Most recent. This tool is annotated EDITS and there is no separate
            # "list the versions" tool, so omitting the timestamp -- the natural
            # way to ask what snapshots exist -- overwrites the file instead.
            # The counter-snapshot below means nothing is lost, but the caller
            # has to be told which snapshot was picked and that it was picked
            # for want of an argument.
            backup_name = versions[0]

        backup_path = str(versions_dir / backup_name)

        # Create a counter-snapshot before overwriting
        counter_backup = snapshot(str(path))
        progress.append(info("Counter-snapshot created", Path(counter_backup).name))

        restore(str(path), backup_path)
        progress.append(ok(f"Restored {path.name}", backup_name))
        if not timestamp:
            progress.append(
                warn(
                    f"No timestamp given — restored the newest of {len(versions)}",
                    "pass timestamp= from available_versions to pick another",
                )
            )

        # Restoring to a state the file already held changes nothing, and the
        # counter-snapshot of it is then a duplicate of the live file.
        kept_counter = discard_snapshot_if_unchanged(counter_backup, path)
        if not kept_counter and counter_backup:
            progress.append(info("Counter-snapshot discarded", "the file was already at this version"))
        counter_backup = kept_counter

        # A restore replaces the dataset's entire contents, which makes it the
        # single event most worth being able to look up later -- and it was the
        # one write on this server that recorded nothing. read_receipt showed
        # every apply_patch and no sign that any of them had been rolled back.
        append_receipt(
            str(path),
            tool="restore_version",
            args={"timestamp": timestamp},
            result=f"restored from {backup_name}",
            backup=counter_backup,
        )

        result = {
            "success": True,
            "op": "restore_version",
            "file": path.name,
            "file_path": str(path),
            "restored_from": backup_path,
            "available_versions": versions,
            "newest_by_default": not timestamp,
            "hint": (
                (
                    f"No timestamp was given, so the newest of {len(versions)} snapshot(s) was "
                    f"written over {path.name}: {backup_name}. Pass timestamp= from "
                    "available_versions to choose a different one. "
                )
                if not timestamp
                else ""
            )
            + "Call inspect_dataset() to confirm the restored state.",
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("restore_version error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check that the backup path is valid."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# read_receipt
# ---------------------------------------------------------------------------


def read_receipt(
    file_path: str,
    last_n: int = 10,
) -> dict:
    progress = []
    try:
        path = resolve_path(file_path)
        entries, scope = _read_receipt_scoped(str(path), last_n=last_n)
        total = len(read_receipt_log(str(path), last_n=0))

        if last_n == 0 and total > get_max_rows():
            progress.append(
                warn(
                    "Large receipt log",
                    f"Returning all {total} entries; constrained mode limit: {get_max_rows()}",
                )
            )

        progress.append(ok(f"Receipt for {path.name}", f"{len(entries)} entries returned"))

        result = {
            "success": True,
            "op": "read_receipt",
            "file": path.name,
            "file_path": str(path),
            "total_entries": total,
            "returned": len(entries),
            "entries": entries,
            # A caller that ran twenty operations and reads two entries has to
            # be able to learn from this response that eighteen of them were
            # never eligible. Without it the log reads as an audit trail that
            # lost most of its history.
            "scope": scope,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("read_receipt error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check file_path is absolute and the file exists."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# list_patch_ops — on-demand op catalog
# ---------------------------------------------------------------------------

_OP_CATALOG: dict[str, list[dict]] = {
    "original": [
        {"op": "drop_column", "params": "columns: list[str]"},
        {
            "op": "clean_text",
            "params": "scope: headers|values|both, "
            "operations: strip|lower|upper|title|collapse_spaces (default strip+title)",
        },
        {"op": "cast_column", "params": "column, dtype: int|float|str|datetime"},
        {"op": "replace_values", "params": "column, mapping: {old: new}"},
        {"op": "add_column", "params": "name, mode: math|threshold, expr|source+threshold"},
        {
            "op": "cap_outliers",
            "params": "column, method: iqr|std, threshold: number (IQR multiplier, default 1.5; sigma count for std, default 3), th1/th3: quantiles for iqr",
        },
        {
            "op": "fill_nulls",
            "params": "column, strategy: mean|median|mode|ffill|bfill|drop|value, fill_zeros: bool (optional)",
        },
        {"op": "drop_duplicates", "params": "subset: list[str] (default all cols), keep: first|last|False"},
        {"op": "normalize", "params": "column, method: minmax|zscore"},
        {"op": "label_encode", "params": "column, new_column"},
        {"op": "extract_regex", "params": "column, pattern, new_column"},
        {
            "op": "date_diff",
            "params": "date_col_a, date_col_b, new_column, unit: days|months|years (computes date_col_a minus date_col_b)",
        },
        {"op": "rank_column", "params": "column, new_column, method: average|min|max|first|dense"},
    ],
    "filtering": [
        {"op": "sort", "params": "by: list[str], ascending: list[bool]"},
        {"op": "filter_isin", "params": "column, values: list"},
        {"op": "filter_not_isin", "params": "column, values: list"},
        {"op": "filter_between", "params": "column, min, max"},
        {"op": "filter_date_range", "params": "column, start, end (ISO strings)"},
        {"op": "filter_regex", "params": "column, pattern"},
        {"op": "filter_quantile", "params": "column, min_q, max_q (0–1)"},
        {"op": "filter_top_n", "params": "column, n, keep: top|bottom"},
        {"op": "dedup_subset", "params": "columns: list[str], keep: first|last"},
    ],
    "numeric": [
        {"op": "log_transform", "params": "column, method: log1p|log2|log10|log, new_column"},
        {"op": "sqrt_transform", "params": "column, new_column, safe: bool"},
        {"op": "boxcox_transform", "params": "column, new_column (requires all values > 0)"},
        {"op": "yeojohnson_transform", "params": "column, new_column (works on negatives)"},
        {"op": "robust_scale", "params": "column, new_column (median/IQR scale)"},
        {"op": "winsorize", "params": "column, lower_q, upper_q (percentile bounds)"},
        {"op": "bin_column", "params": "column, bins: int|list, labels: list, new_column"},
        {"op": "qbin_column", "params": "column, q: int, labels: list, new_column"},
        {"op": "clip_values", "params": "column, min, max"},
        {"op": "round_values", "params": "column, decimals: int"},
        {"op": "abs_values", "params": "column, new_column"},
    ],
    "encoding": [
        {"op": "ordinal_encode", "params": "column, order: list[str], new_column"},
        {"op": "binary_encode", "params": "column, threshold|value, new_column"},
        {"op": "frequency_encode", "params": "column, new_column"},
    ],
    "temporal": [
        {"op": "lag", "params": "column, periods: int, new_column"},
        {"op": "lead", "params": "column, periods: int, new_column"},
        {"op": "diff", "params": "column, periods: int, new_column"},
        {"op": "pct_change", "params": "column, periods: int, new_column"},
        {"op": "rolling_agg", "params": "column, window: int, agg: mean|std|min|max|sum, new_column"},
        {"op": "ewm", "params": "column, span: int, new_column"},
        {"op": "cumulative", "params": "column, agg: sum|prod|max|min, new_column"},
    ],
    "grouped": [
        {
            "op": "group_transform",
            "params": (
                "group_by: list[str], column, new_column, agg: "
                "sum|mean|median|max|min|std|count|nunique|"
                "share|rank|cumsum|zscore|diff_from_mean|pct_of_max"
            ),
        },
    ],
    "structural": [
        {"op": "column_math", "params": "formula: 'col_a + col_b', target_column"},
        {
            "op": "conditional_assign",
            "params": (
                "new_column, default, conditions: list of "
                "{column, op: equals|not_equals|gt|gte|lt|lte|contains|isin, value, label} "
                "(label is assigned when the condition matches; first match wins)"
            ),
        },
        {"op": "split_column", "params": "column, delimiter, new_columns: list[str], drop_original"},
        {"op": "combine_columns", "params": "columns: list[str], delimiter, new_column, drop_originals"},
        {"op": "regex_replace", "params": "column, pattern, replacement"},
        {"op": "str_slice", "params": "column, start, end, new_column"},
        {
            "op": "concat_file",
            "params": "file_path, direction: rows|columns, fill_missing, add_source_column (add_source accepted)",
        },
        {"op": "melt", "params": "id_vars: list, value_vars: list, var_name, value_name"},
    ],
}

_VALID_CATEGORIES = frozenset(_OP_CATALOG.keys())


def list_patch_ops(category: str = "") -> dict:
    """Return the full apply_patch op catalog. Filter by category."""
    try:
        cat = category.strip().lower()
        if cat and cat not in _VALID_CATEGORIES:
            return {
                "success": False,
                "error": f"Unknown category: '{cat}'",
                "hint": f"Valid categories: {', '.join(sorted(_VALID_CATEGORIES))}",
                "progress": [fail("Unknown category", cat)],
                "token_estimate": 20,
            }
        if cat:
            ops = {cat: _OP_CATALOG[cat]}
        else:
            ops = _OP_CATALOG
        total = sum(len(v) for v in ops.values())
        result = {
            "success": True,
            "op": "list_patch_ops",
            "category": cat or "all",
            "total_ops": total,
            "ops": ops,
            "progress": [ok("Op catalog returned", f"{total} ops in {len(ops)} categories")],
        }
        result["token_estimate"] = _token_estimate(result)
        return result
    except Exception as exc:
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Call with no arguments to list all ops."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 10,
        }
