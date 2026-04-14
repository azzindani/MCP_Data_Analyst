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

from _patch_ops import (
    _op_add_column,
    _op_cap_outliers,
    _op_cast_column,
    _op_clean_text,
    _op_date_diff,
    _op_drop_column,
    _op_drop_duplicates,
    _op_extract_regex,
    _op_fill_nulls,
    _op_label_encode,
    _op_normalize,
    _op_rank_column,
    _op_replace_values,
    _parse_expr,
)

from shared.file_utils import atomic_write_text, resolve_path
from shared.patch_validator import VALID_OPS, validate_ops
from shared.platform_utils import get_max_results, get_max_rows
from shared.progress import fail, info, ok, undo, warn
from shared.receipt import append_receipt, read_receipt_log
from shared.version_control import list_versions, restore, snapshot

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stderr, level=logging.WARNING)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _token_estimate(obj) -> int:
    return len(str(obj)) // 4


from shared.file_utils import read_csv as _read_csv  # noqa: E402


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
        }

    # Categorical path
    top_values = series.value_counts().head(10).to_dict()
    top_values = {str(k): int(v) for k, v in top_values.items()}
    unique_count = int(series.nunique())
    return {
        "column": column,
        "dtype": dtype,
        "count": count,
        "null_count": null_count,
        "null_pct": null_pct,
        "unique_count": unique_count,
        "top_values": top_values,
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
        if dtype == "numeric":
            candidates = [c for c in candidates if pd.api.types.is_numeric_dtype(df[c])]
        elif dtype == "datetime":
            candidates = [c for c in candidates if pd.api.types.is_datetime64_any_dtype(df[c])]
        elif dtype == "object":
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

        if max_rows > 0:
            progress.append(
                warn(
                    "Row sampling active",
                    f"max_rows={max_rows}; constrained mode may apply",
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
            "columns": len(df.columns),
            "dtypes": profile["dtypes"],
            "null_counts": profile["null_counts"],
            "unique_counts": profile["unique_counts"],
            "sample": profile["sample"],
            "encoding_used": encoding,
            "hint": "Call search_columns() or inspect_dataset() to explore next.",
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("load_dataset error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check that file_path is absolute and the file exists.",
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

        if rename_column:
            if "name" in gdf.columns:
                gdf = gdf.rename(columns={"name": rename_column})

        if keep_columns:
            geo_col = gdf.geometry.name
            cols_to_keep = [c for c in keep_columns if c in gdf.columns]
            if geo_col not in cols_to_keep:
                cols_to_keep.append(geo_col)
            gdf = gdf[cols_to_keep]

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
            "sample": sample,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("load_geo_dataset error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Verify the file is a valid GeoJSON or shapefile.",
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
            "error": str(exc),
            "hint": "Check file_path is absolute and the file is a valid CSV.",
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
            "error": str(exc),
            "hint": "Use inspect_dataset() first to verify column names.",
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
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("search_columns error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path is absolute and the file is a valid CSV.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# apply_patch — op dispatch table
# ---------------------------------------------------------------------------

_OP_HANDLERS = {
    "drop_column": _op_drop_column,
    "clean_text": _op_clean_text,
    "cast_column": _op_cast_column,
    "replace_values": _op_replace_values,
    "add_column": _op_add_column,
    "cap_outliers": _op_cap_outliers,
    "fill_nulls": _op_fill_nulls,
    "drop_duplicates": _op_drop_duplicates,
    "normalize": _op_normalize,
    "label_encode": _op_label_encode,
    "extract_regex": _op_extract_regex,
    "date_diff": _op_date_diff,
    "rank_column": _op_rank_column,
}


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
            return {
                "success": False,
                "error": "; ".join(errors),
                "hint": f"Valid ops: {', '.join(sorted(VALID_OPS))}",
                "progress": [fail("Validation failed", str(errors))],
                "token_estimate": 30,
            }

        df = _read_csv(str(path))

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
                    dry_errors.append({"op_index": i, "op": op.get("op", ""), "error": str(exc)})
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
                op_errors.append({"op_index": i, "op": op.get("op", ""), "error": str(exc)})
                progress.append(fail(f"Op {i} ({op.get('op', '?')}) failed", str(exc)))

        if op_errors:
            # Do NOT write the modified df — leave the original intact.
            return {
                "success": False,
                "error": f"{len(op_errors)} op(s) failed",
                "op_errors": op_errors,
                "applied": len(results),
                "failed": len(op_errors),
                "backup": backup,
                "hint": ("Fix failing ops and retry. Call restore_version() if you want to reset to the snapshot."),
                "file_path": str(path),
                "progress": progress,
                "token_estimate": _token_estimate(progress),
            }

        # All ops succeeded — write atomically (G6)
        atomic_write_text(path, df.to_csv(index=False))
        progress.append(ok(f"Saved {path.name}", f"{len(ops)} op(s) applied"))

        append_receipt(
            str(path),
            tool="apply_patch",
            args={"ops": ops},
            result=f"applied {len(ops)} ops",
            backup=backup,
        )

        result = {
            "success": True,
            "op": "apply_patch",
            "file_path": str(path),
            "applied": len(ops),
            "results": results,
            "backup": backup,
            "hint": ("Call read_column_stats() or inspect_dataset() to verify the changes."),
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("apply_patch error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Use restore_version() to undo if a snapshot was taken.",
            "backup": backup,
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
            # Most recent
            backup_name = versions[0]

        backup_path = str(versions_dir / backup_name)

        # Create a counter-snapshot before overwriting
        counter_backup = snapshot(str(path))
        progress.append(info("Counter-snapshot created", Path(counter_backup).name))

        restore(str(path), backup_path)
        progress.append(ok(f"Restored {path.name}", backup_name))

        result = {
            "success": True,
            "op": "restore_version",
            "file": path.name,
            "file_path": str(path),
            "restored_from": backup_path,
            "available_versions": versions,
            "hint": "Call inspect_dataset() to confirm the restored state.",
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("restore_version error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check that the backup path is valid.",
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
        entries = read_receipt_log(str(path), last_n=last_n)
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
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("read_receipt error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path is absolute and the file exists.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }
