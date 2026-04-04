"""Tier 2 engine — profiling, cleaning pipelines, aggregations. Zero MCP imports."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

# Shared utilities
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from shared.file_utils import resolve_path
from shared.platform_utils import get_max_results, get_max_rows
from shared.progress import fail, info, ok, warn
from shared.receipt import append_receipt
from shared.version_control import snapshot

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stderr, level=logging.WARNING)


def _token_estimate(obj) -> int:
    return len(str(obj)) // 4


def _read_csv(
    file_path: str, encoding: str = "utf-8", separator: str = ",", max_rows: int = 0
) -> pd.DataFrame:
    kwargs: dict = {"encoding": encoding, "sep": separator, "low_memory": False}
    if max_rows > 0:
        kwargs["nrows"] = max_rows
    return pd.read_csv(file_path, **kwargs)


def _dtype_label(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "int64"
    if pd.api.types.is_float_dtype(series):
        return "float64"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime64"
    return "object"


# ---------------------------------------------------------------------------
# check_outliers
# ---------------------------------------------------------------------------


def check_outliers(
    file_path: str,
    columns: list[str] = None,
    method: str = "both",
    th1: float = 0.25,
    th3: float = 0.75,
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
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

        if columns is not None:
            missing = [c for c in columns if c not in df.columns]
            if missing:
                return {
                    "success": False,
                    "error": f"Columns not found: {missing}",
                    "hint": f"Available: {', '.join(df.columns)}",
                    "progress": [fail("Column not found", str(missing))],
                    "token_estimate": 30,
                }
            numeric_cols = [c for c in columns if c in numeric_cols]

        results = {}
        cols_with_outliers = 0
        for col in numeric_cols:
            clean = df[col].dropna()
            if len(clean) == 0:
                continue
            r = {}
            if method in ("iqr", "both"):
                q1 = float(clean.quantile(th1))
                q3 = float(clean.quantile(th3))
                iqr = q3 - q1
                lower = q1 - 1.5 * iqr
                upper = q3 + 1.5 * iqr
                count = int(((clean < lower) | (clean > upper)).sum())
                r["has_outliers_iqr"] = count > 0
                r["outlier_count_iqr"] = count
                r["lower_limit_iqr"] = round(lower, 4)
                r["upper_limit_iqr"] = round(upper, 4)
                if count > 0:
                    cols_with_outliers += 1

            if method in ("std", "both"):
                mean_v = float(clean.mean())
                std_v = float(clean.std()) if len(clean) > 1 else 0
                lower_s = mean_v - 3 * std_v
                upper_s = mean_v + 3 * std_v
                count_s = int(((clean < lower_s) | (clean > upper_s)).sum())
                r["has_outliers_std"] = count_s > 0
                r["outlier_count_std"] = count_s
                r["lower_limit_std"] = round(lower_s, 4)
                r["upper_limit_std"] = round(upper_s, 4)
                if count_s > 0 and method == "std":
                    cols_with_outliers += 1

            results[col] = r

        max_r = get_max_results()
        truncated = len(results) > max_r
        if truncated:
            keys = list(results.keys())[:max_r]
            results = {k: results[k] for k in keys}
            progress.append(warn("Results truncated", f"Showing first {max_r} columns"))

        progress.append(
            ok(
                f"Checked outliers in {path.name}",
                f"{len(results)} columns scanned, {cols_with_outliers} with outliers",
            )
        )

        result = {
            "success": True,
            "op": "check_outliers",
            "method": method,
            "scanned_columns": len(results),
            "columns_with_outliers": cols_with_outliers,
            "results": results,
            "truncated": truncated,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("check_outliers error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path is absolute and the file is a valid CSV.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# scan_nulls_zeros
# ---------------------------------------------------------------------------


def scan_nulls_zeros(
    file_path: str,
    include_zeros: bool = True,
    min_count: int = 1,
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
        total_rows = len(df)
        results = {}
        suggested = {}

        for col in df.columns:
            null_c = int(df[col].isna().sum())
            null_p = round(null_c / total_rows * 100, 2) if total_rows > 0 else 0.0

            zero_c = None
            zero_p = None
            if include_zeros and pd.api.types.is_numeric_dtype(df[col]):
                zero_c = int((df[col] == 0).sum())
                zero_p = round(zero_c / total_rows * 100, 2) if total_rows > 0 else 0.0

            # Object columns: detect null-like strings
            if df[col].dtype == "object":
                null_like = df[col].isin(["", "-", "N/A", "null", "None"]).sum()
                null_c += int(null_like)
                null_p = round(null_c / total_rows * 100, 2) if total_rows > 0 else 0.0

            flagged = null_c >= min_count or (
                zero_c is not None and zero_c >= min_count
            )
            if not flagged:
                continue

            entry: dict = {
                "null_count": null_c,
                "null_pct": null_p,
            }
            if zero_c is not None:
                entry["zero_count"] = zero_c
                entry["zero_pct"] = zero_p
            else:
                entry["zero_count"] = None
                entry["zero_pct"] = None

            results[col] = entry

            # Suggested actions
            if null_c > 0:
                if pd.api.types.is_numeric_dtype(df[col]):
                    suggested[col] = "apply_patch op=fill_nulls strategy=median"
                else:
                    suggested[col] = "apply_patch op=fill_nulls strategy=mode"
            if zero_c is not None and zero_c > 0:
                suggested[col] = (
                    "apply_patch op=fill_nulls fill_zeros=true strategy=mean"
                )

        clean_count = len(df.columns) - len(results)
        progress.append(
            ok(f"Scanned {path.name}", f"{clean_count} clean, {len(results)} flagged")
        )

        result = {
            "success": True,
            "op": "scan_nulls_zeros",
            "total_rows": total_rows,
            "clean_columns": clean_count,
            "flagged_columns": len(results),
            "results": results,
            "suggested_actions": suggested,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("scan_nulls_zeros error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path is absolute and the file is a valid CSV.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# enrich_with_geo
# ---------------------------------------------------------------------------


def enrich_with_geo(
    file_path: str,
    geo_file_path: str,
    join_column: str,
    geo_join_column: str,
    output_path: str = "",
    dry_run: bool = False,
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
        geo_path = resolve_path(geo_file_path)

        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check file_path is absolute.",
                "progress": [fail("File not found", path.name)],
                "token_estimate": 20,
            }
        if not geo_path.exists():
            return {
                "success": False,
                "error": f"Geo file not found: {geo_path.name}",
                "hint": "Check geo_file_path is absolute.",
                "progress": [fail("Geo file not found", geo_path.name)],
                "token_estimate": 20,
            }

        df = _read_csv(str(path))
        gdf = gpd.read_file(str(geo_path))

        if join_column not in df.columns:
            return {
                "success": False,
                "error": f"Column '{join_column}' not found in main dataset",
                "hint": f"Available: {', '.join(df.columns)}",
                "progress": [fail("Column not found", join_column)],
                "token_estimate": 30,
            }
        if geo_join_column not in gdf.columns:
            return {
                "success": False,
                "error": f"Column '{geo_join_column}' not found in geo dataset",
                "hint": f"Available: {', '.join(gdf.columns)}",
                "progress": [fail("Column not found", geo_join_column)],
                "token_estimate": 30,
            }

        # Mismatch detection
        main_vals = set(df[join_column].dropna().astype(str).unique())
        geo_vals = set(gdf[geo_join_column].dropna().astype(str).unique())
        unmatched_main = list(main_vals - geo_vals)[:20]
        unmatched_geo = list(geo_vals - main_vals)[:20]

        # Left join
        gdf_flat = gdf.copy()
        gdf_flat[geo_join_column] = gdf_flat[geo_join_column].astype(str)
        df[join_column] = df[join_column].astype(str)

        new_cols = [c for c in gdf_flat.columns if c != geo_join_column]
        merged = df.merge(
            gdf_flat, left_on=join_column, right_on=geo_join_column, how="left"
        )

        # Serialize geometry as WKT
        geo_col = gdf.geometry.name
        if geo_col in merged.columns:
            merged[geo_col] = merged[geo_col].apply(
                lambda g: g.wkt if g is not None else None
            )

        matched = int(merged[geo_col].notna().sum()) if geo_col in merged.columns else 0

        if dry_run:
            progress.append(info("Dry run — no changes written", path.name))
            result = {
                "success": True,
                "dry_run": True,
                "op": "enrich_with_geo",
                "rows_before": len(df),
                "rows_after": len(merged),
                "matched": matched,
                "unmatched_main": unmatched_main,
                "unmatched_geo": unmatched_geo,
                "new_columns": new_cols,
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        # Write
        backup = snapshot(str(path))
        out = output_path if output_path else str(path)
        merged.to_csv(out, index=False)

        append_receipt(
            str(path),
            tool="enrich_with_geo",
            args={"geo_file": geo_path.name},
            result=f"matched {matched} rows",
            backup=backup,
        )

        progress.append(
            ok(f"Enriched {path.name}", f"{matched} rows matched with {geo_path.name}")
        )

        result = {
            "success": True,
            "op": "enrich_with_geo",
            "rows_before": len(df),
            "rows_after": len(merged),
            "matched": matched,
            "unmatched_main": unmatched_main,
            "unmatched_geo": unmatched_geo,
            "new_columns": new_cols,
            "output_file": Path(out).name,
            "backup": backup,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("enrich_with_geo error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and parameters.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# auto_detect_schema — smart column type inference
# ---------------------------------------------------------------------------


def auto_detect_schema(
    file_path: str,
    max_rows: int = 1000,
) -> dict:
    """Auto-detect column types, dates, IDs, categories with cleaning suggestions."""
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

        df = _read_csv(str(path), max_rows=max_rows if max_rows > 0 else 0)
        suggestions = []
        column_info = []

        for c in df.columns:
            info = {"column": c, "detected_type": _dtype_label(df[c])}
            info["null_count"] = int(df[c].isna().sum())
            info["null_pct"] = (
                round(info["null_count"] / len(df) * 100, 2) if len(df) > 0 else 0
            )
            info["unique_count"] = int(df[c].nunique())
            info["unique_pct"] = (
                round(info["unique_count"] / len(df) * 100, 2) if len(df) > 0 else 0
            )

            # Auto-detect special types
            non_null = df[c].dropna()
            if len(non_null) > 0:
                sample_str = str(non_null.iloc[0]).strip()
                # Date detection
                if info["detected_type"] == "object":
                    try:
                        pd.to_datetime(non_null.head(100), errors="raise")
                        info["detected_type"] = "datetime"
                        info["is_date"] = True
                    except (ValueError, TypeError):
                        info["is_date"] = False
                else:
                    info["is_date"] = False

                # ID detection (high uniqueness, often string/numeric)
                if info["unique_pct"] > 90 and info["unique_count"] > 10:
                    info["is_id"] = True
                    suggestions.append(
                        f"Column '{c}' looks like an ID column ({info['unique_count']} unique values)"
                    )
                else:
                    info["is_id"] = False

                # Category detection (low uniqueness)
                if info["unique_pct"] < 5 and info["unique_count"] > 1:
                    info["is_category"] = True
                else:
                    info["is_category"] = False

                # Text detection (object with long strings)
                if info["detected_type"] == "object" and not info.get("is_date"):
                    avg_len = non_null.astype(str).str.len().mean()
                    info["avg_text_length"] = round(avg_len, 1)
                    if avg_len > 50:
                        info["is_free_text"] = True
                    else:
                        info["is_free_text"] = False

            # Cleaning suggestions
            if info["null_pct"] > 50:
                suggestions.append(
                    f"Column '{c}' has {info['null_pct']}% nulls — consider dropping"
                )
            elif info["null_pct"] > 0:
                if info["detected_type"] in ("int64", "float64"):
                    suggestions.append(
                        f"Column '{c}' has {info['null_count']} nulls — consider filling with median"
                    )
                elif info["detected_type"] == "object":
                    suggestions.append(
                        f"Column '{c}' has {info['null_count']} nulls — consider filling with mode"
                    )

            column_info.append(info)

        progress.append(ok(f"Schema detected", f"{len(column_info)} columns analyzed"))

        result = {
            "success": True,
            "op": "auto_detect_schema",
            "rows": len(df),
            "columns": len(column_info),
            "column_info": column_info,
            "suggestions": suggestions,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("auto_detect_schema error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path is absolute and the file is a valid CSV.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# smart_impute — auto-select imputation strategy
# ---------------------------------------------------------------------------


def smart_impute(
    file_path: str,
    columns: list[str] = None,
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    """Auto-impute missing values using column-type-appropriate strategies."""
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
        cols_to_impute = columns if columns else df.columns.tolist()
        imputation_log = []

        for c in cols_to_impute:
            if c not in df.columns:
                continue
            null_count = int(df[c].isna().sum())
            if null_count == 0:
                continue

            if pd.api.types.is_numeric_dtype(df[c]):
                strategy = "median"
                value = round(float(df[c].median()), 4)
                df[c] = df[c].fillna(value)
            elif pd.api.types.is_datetime64_any_dtype(df[c]):
                strategy = "forward_fill"
                df[c] = df[c].ffill().bfill()
                value = "ffill/bfill"
            else:
                strategy = "mode"
                mode_val = df[c].mode()
                value = str(mode_val.iloc[0]) if len(mode_val) > 0 else "unknown"
                df[c] = df[c].fillna(value)

            imputation_log.append(
                {
                    "column": c,
                    "strategy": strategy,
                    "filled": null_count,
                    "value_used": value,
                }
            )

        if not imputation_log:
            progress.append(info("No nulls found", "Nothing to impute"))
            result = {
                "success": True,
                "op": "smart_impute",
                "columns_imputed": 0,
                "total_filled": 0,
                "imputation_log": [],
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        if dry_run:
            progress.append(
                info(
                    "Dry run — no changes written",
                    f"Would impute {len(imputation_log)} columns",
                )
            )
            result = {
                "success": True,
                "dry_run": True,
                "op": "smart_impute",
                "columns_imputed": len(imputation_log),
                "total_filled": sum(r["filled"] for r in imputation_log),
                "imputation_log": imputation_log,
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        out = Path(output_path) if output_path else path
        df.to_csv(str(out), index=False)

        progress.append(
            ok(
                f"Smart imputation complete",
                f"{len(imputation_log)} columns, {sum(r['filled'] for r in imputation_log)} cells filled",
            )
        )

        result = {
            "success": True,
            "op": "smart_impute",
            "columns_imputed": len(imputation_log),
            "total_filled": sum(r["filled"] for r in imputation_log),
            "imputation_log": imputation_log,
            "output_file": out.name,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("smart_impute error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and column names.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# merge_datasets — auto-detect join keys
# ---------------------------------------------------------------------------


def merge_datasets(
    file_path: str,
    right_file_path: str,
    left_on: str = "",
    right_on: str = "",
    how: str = "left",
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    """Merge two datasets with auto-detect join keys and mismatch detection."""
    progress = []
    try:
        path = resolve_path(file_path)
        right_path = resolve_path(right_file_path)
        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check file_path is absolute and the file exists.",
                "progress": [fail("File not found", path.name)],
                "token_estimate": 20,
            }
        if not right_path.exists():
            return {
                "success": False,
                "error": f"Right file not found: {right_path.name}",
                "hint": "Check right_file_path is absolute and the file exists.",
                "progress": [fail("File not found", right_path.name)],
                "token_estimate": 20,
            }

        left_df = _read_csv(str(path))
        right_df = _read_csv(str(right_path))

        valid_hows = {"left", "right", "inner", "outer"}
        if how not in valid_hows:
            return {
                "success": False,
                "error": f"Invalid join type: {how}",
                "hint": f"Valid types: {', '.join(sorted(valid_hows))}",
                "progress": [fail("Invalid join type", how)],
                "token_estimate": 20,
            }

        # Auto-detect join keys
        common_cols = set(left_df.columns) & set(right_df.columns)
        if not left_on and not right_on:
            if len(common_cols) == 1:
                left_on = right_on = list(common_cols)[0]
                progress.append(info("Auto-detected join key", left_on))
            elif len(common_cols) > 1:
                return {
                    "success": False,
                    "error": f"Multiple common columns found: {common_cols}",
                    "hint": f"Specify left_on and right_on parameters",
                    "progress": [fail("Ambiguous join keys", str(common_cols))],
                    "token_estimate": 30,
                }
            else:
                return {
                    "success": False,
                    "error": "No common columns found for auto-join",
                    "hint": "Specify left_on and right_on parameters",
                    "progress": [fail("No common columns", "")],
                    "token_estimate": 20,
                }
        else:
            right_on = right_on or left_on

        if left_on not in left_df.columns:
            return {
                "success": False,
                "error": f"Left key not found: {left_on}",
                "hint": f"Available: {', '.join(left_df.columns)}",
                "progress": [fail("Column not found", left_on)],
                "token_estimate": 30,
            }
        if right_on not in right_df.columns:
            return {
                "success": False,
                "error": f"Right key not found: {right_on}",
                "hint": f"Available: {', '.join(right_df.columns)}",
                "progress": [fail("Column not found", right_on)],
                "token_estimate": 30,
            }

        # Mismatch detection
        left_keys = set(left_df[left_on].dropna().astype(str).unique())
        right_keys = set(right_df[right_on].dropna().astype(str).unique())
        left_only = list(left_keys - right_keys)[:20]
        right_only = list(right_keys - left_keys)[:20]

        merged = left_df.merge(right_df, left_on=left_on, right_on=right_on, how=how)

        if dry_run:
            progress.append(
                info(
                    "Dry run — no changes written",
                    f"Would produce {len(merged)} rows × {len(merged.columns)} columns",
                )
            )
            result = {
                "success": True,
                "dry_run": True,
                "op": "merge_datasets",
                "left_rows": len(left_df),
                "right_rows": len(right_df),
                "merged_rows": len(merged),
                "merged_columns": len(merged.columns),
                "join_key": left_on,
                "left_only_keys": left_only,
                "right_only_keys": right_only,
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        out = (
            Path(output_path)
            if output_path
            else path.parent / f"{path.stem}_merged{path.suffix}"
        )
        merged.to_csv(str(out), index=False)

        progress.append(
            ok(
                f"Datasets merged",
                f"{len(merged)} rows × {len(merged.columns)} columns",
            )
        )

        result = {
            "success": True,
            "op": "merge_datasets",
            "left_rows": len(left_df),
            "right_rows": len(right_df),
            "merged_rows": len(merged),
            "merged_columns": len(merged.columns),
            "join_key": left_on,
            "left_only_keys": left_only,
            "right_only_keys": right_only,
            "output_file": out.name,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("merge_datasets error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and parameters.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# auto_detect_schema — smart column type inference
# ---------------------------------------------------------------------------


def auto_detect_schema(
    file_path: str,
    max_rows: int = 1000,
) -> dict:
    """Auto-detect column types, dates, IDs, categories with cleaning suggestions."""
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

        df = _read_csv(str(path), max_rows=max_rows if max_rows > 0 else 0)
        suggestions = []
        column_info = []

        for c in df.columns:
            info = {"column": c, "detected_type": _dtype_label(df[c])}
            info["null_count"] = int(df[c].isna().sum())
            info["null_pct"] = (
                round(info["null_count"] / len(df) * 100, 2) if len(df) > 0 else 0
            )
            info["unique_count"] = int(df[c].nunique())
            info["unique_pct"] = (
                round(info["unique_count"] / len(df) * 100, 2) if len(df) > 0 else 0
            )

            # Auto-detect special types
            non_null = df[c].dropna()
            if len(non_null) > 0:
                sample_str = str(non_null.iloc[0]).strip()
                # Date detection
                if info["detected_type"] == "object":
                    try:
                        pd.to_datetime(non_null.head(100), errors="raise")
                        info["detected_type"] = "datetime"
                        info["is_date"] = True
                    except (ValueError, TypeError):
                        info["is_date"] = False
                else:
                    info["is_date"] = False

                # ID detection (high uniqueness, often string/numeric)
                if info["unique_pct"] > 90 and info["unique_count"] > 10:
                    info["is_id"] = True
                    suggestions.append(
                        f"Column '{c}' looks like an ID column ({info['unique_count']} unique values)"
                    )
                else:
                    info["is_id"] = False

                # Category detection (low uniqueness)
                if info["unique_pct"] < 5 and info["unique_count"] > 1:
                    info["is_category"] = True
                else:
                    info["is_category"] = False

                # Text detection (object with long strings)
                if info["detected_type"] == "object" and not info.get("is_date"):
                    avg_len = non_null.astype(str).str.len().mean()
                    info["avg_text_length"] = round(avg_len, 1)
                    if avg_len > 50:
                        info["is_free_text"] = True
                    else:
                        info["is_free_text"] = False

            # Cleaning suggestions
            if info["null_pct"] > 50:
                suggestions.append(
                    f"Column '{c}' has {info['null_pct']}% nulls — consider dropping"
                )
            elif info["null_pct"] > 0:
                if info["detected_type"] in ("int64", "float64"):
                    suggestions.append(
                        f"Column '{c}' has {info['null_count']} nulls — consider filling with median"
                    )
                elif info["detected_type"] == "object":
                    suggestions.append(
                        f"Column '{c}' has {info['null_count']} nulls — consider filling with mode"
                    )

            column_info.append(info)

        progress.append(ok(f"Schema detected", f"{len(column_info)} columns analyzed"))

        result = {
            "success": True,
            "op": "auto_detect_schema",
            "rows": len(df),
            "columns": len(column_info),
            "column_info": column_info,
            "suggestions": suggestions,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("auto_detect_schema error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path is absolute and the file is a valid CSV.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# smart_impute — auto-select imputation strategy
# ---------------------------------------------------------------------------


def smart_impute(
    file_path: str,
    columns: list[str] = None,
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    """Auto-impute missing values using column-type-appropriate strategies."""
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
        cols_to_impute = columns if columns else df.columns.tolist()
        imputation_log = []

        for c in cols_to_impute:
            if c not in df.columns:
                continue
            null_count = int(df[c].isna().sum())
            if null_count == 0:
                continue

            if pd.api.types.is_numeric_dtype(df[c]):
                strategy = "median"
                value = round(float(df[c].median()), 4)
                df[c] = df[c].fillna(value)
            elif pd.api.types.is_datetime64_any_dtype(df[c]):
                strategy = "forward_fill"
                df[c] = df[c].ffill().bfill()
                value = "ffill/bfill"
            else:
                strategy = "mode"
                mode_val = df[c].mode()
                value = str(mode_val.iloc[0]) if len(mode_val) > 0 else "unknown"
                df[c] = df[c].fillna(value)

            imputation_log.append(
                {
                    "column": c,
                    "strategy": strategy,
                    "filled": null_count,
                    "value_used": value,
                }
            )

        if not imputation_log:
            progress.append(info("No nulls found", "Nothing to impute"))
            result = {
                "success": True,
                "op": "smart_impute",
                "columns_imputed": 0,
                "total_filled": 0,
                "imputation_log": [],
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        if dry_run:
            progress.append(
                info(
                    "Dry run — no changes written",
                    f"Would impute {len(imputation_log)} columns",
                )
            )
            result = {
                "success": True,
                "dry_run": True,
                "op": "smart_impute",
                "columns_imputed": len(imputation_log),
                "total_filled": sum(r["filled"] for r in imputation_log),
                "imputation_log": imputation_log,
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        out = Path(output_path) if output_path else path
        df.to_csv(str(out), index=False)

        progress.append(
            ok(
                f"Smart imputation complete",
                f"{len(imputation_log)} columns, {sum(r['filled'] for r in imputation_log)} cells filled",
            )
        )

        result = {
            "success": True,
            "op": "smart_impute",
            "columns_imputed": len(imputation_log),
            "total_filled": sum(r["filled"] for r in imputation_log),
            "imputation_log": imputation_log,
            "output_file": out.name,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("smart_impute error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and column names.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# merge_datasets — auto-detect join keys
# ---------------------------------------------------------------------------


def merge_datasets(
    file_path: str,
    right_file_path: str,
    left_on: str = "",
    right_on: str = "",
    how: str = "left",
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    """Merge two datasets with auto-detect join keys and mismatch detection."""
    progress = []
    try:
        path = resolve_path(file_path)
        right_path = resolve_path(right_file_path)
        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check file_path is absolute and the file exists.",
                "progress": [fail("File not found", path.name)],
                "token_estimate": 20,
            }
        if not right_path.exists():
            return {
                "success": False,
                "error": f"Right file not found: {right_path.name}",
                "hint": "Check right_file_path is absolute and the file exists.",
                "progress": [fail("File not found", right_path.name)],
                "token_estimate": 20,
            }

        left_df = _read_csv(str(path))
        right_df = _read_csv(str(right_path))

        valid_hows = {"left", "right", "inner", "outer"}
        if how not in valid_hows:
            return {
                "success": False,
                "error": f"Invalid join type: {how}",
                "hint": f"Valid types: {', '.join(sorted(valid_hows))}",
                "progress": [fail("Invalid join type", how)],
                "token_estimate": 20,
            }

        # Auto-detect join keys
        common_cols = set(left_df.columns) & set(right_df.columns)
        if not left_on and not right_on:
            if len(common_cols) == 1:
                left_on = right_on = list(common_cols)[0]
                progress.append(info("Auto-detected join key", left_on))
            elif len(common_cols) > 1:
                return {
                    "success": False,
                    "error": f"Multiple common columns found: {common_cols}",
                    "hint": f"Specify left_on and right_on parameters",
                    "progress": [fail("Ambiguous join keys", str(common_cols))],
                    "token_estimate": 30,
                }
            else:
                return {
                    "success": False,
                    "error": "No common columns found for auto-join",
                    "hint": "Specify left_on and right_on parameters",
                    "progress": [fail("No common columns", "")],
                    "token_estimate": 20,
                }
        else:
            right_on = right_on or left_on

        if left_on not in left_df.columns:
            return {
                "success": False,
                "error": f"Left key not found: {left_on}",
                "hint": f"Available: {', '.join(left_df.columns)}",
                "progress": [fail("Column not found", left_on)],
                "token_estimate": 30,
            }
        if right_on not in right_df.columns:
            return {
                "success": False,
                "error": f"Right key not found: {right_on}",
                "hint": f"Available: {', '.join(right_df.columns)}",
                "progress": [fail("Column not found", right_on)],
                "token_estimate": 30,
            }

        # Mismatch detection
        left_keys = set(left_df[left_on].dropna().astype(str).unique())
        right_keys = set(right_df[right_on].dropna().astype(str).unique())
        left_only = list(left_keys - right_keys)[:20]
        right_only = list(right_keys - left_keys)[:20]

        merged = left_df.merge(right_df, left_on=left_on, right_on=right_on, how=how)

        if dry_run:
            progress.append(
                info(
                    "Dry run — no changes written",
                    f"Would produce {len(merged)} rows × {len(merged.columns)} columns",
                )
            )
            result = {
                "success": True,
                "dry_run": True,
                "op": "merge_datasets",
                "left_rows": len(left_df),
                "right_rows": len(right_df),
                "merged_rows": len(merged),
                "merged_columns": len(merged.columns),
                "join_key": left_on,
                "left_only_keys": left_only,
                "right_only_keys": right_only,
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        out = (
            Path(output_path)
            if output_path
            else path.parent / f"{path.stem}_merged{path.suffix}"
        )
        merged.to_csv(str(out), index=False)

        progress.append(
            ok(
                f"Datasets merged",
                f"{len(merged)} rows × {len(merged.columns)} columns",
            )
        )

        result = {
            "success": True,
            "op": "merge_datasets",
            "left_rows": len(left_df),
            "right_rows": len(right_df),
            "merged_rows": len(merged),
            "merged_columns": len(merged.columns),
            "join_key": left_on,
            "left_only_keys": left_only,
            "right_only_keys": right_only,
            "output_file": out.name,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("merge_datasets error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file paths and join keys.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# feature_engineering — auto-create features
# ---------------------------------------------------------------------------


def feature_engineering(
    file_path: str,
    features: list[str] = None,
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    """Auto-create features: date parts, numeric bins, text length, one-hot encoding."""
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
        created_features = []

        for c in df.columns:
            if features and c not in features:
                continue

            # Date features
            try:
                dt = pd.to_datetime(df[c], errors="raise")
                df[f"{c}_year"] = dt.dt.year
                df[f"{c}_month"] = dt.dt.month
                df[f"{c}_day"] = dt.dt.day
                df[f"{c}_dayofweek"] = dt.dt.dayofweek
                created_features.extend(
                    [f"{c}_year", f"{c}_month", f"{c}_day", f"{c}_dayofweek"]
                )
                continue
            except (ValueError, TypeError):
                pass

            # Numeric features
            if pd.api.types.is_numeric_dtype(df[c]):
                # Binning
                if df[c].nunique() > 10:
                    df[f"{c}_bin"] = pd.qcut(df[c], q=5, duplicates="drop").astype(str)
                    created_features.append(f"{c}_bin")
                # Log transform
                if (df[c] > 0).all():
                    df[f"{c}_log"] = np.log1p(df[c])
                    created_features.append(f"{c}_log")

            # Text features
            if df[c].dtype == "object":
                df[f"{c}_length"] = df[c].astype(str).str.len()
                df[f"{c}_word_count"] = df[c].astype(str).str.split().str.len()
                created_features.extend([f"{c}_length", f"{c}_word_count"])
                # One-hot encoding for low-cardinality
                if df[c].nunique() <= 10 and df[c].nunique() > 1:
                    dummies = pd.get_dummies(df[c], prefix=c, dtype=int)
                    for dc in dummies.columns:
                        df[dc] = dummies[dc]
                    created_features.extend(dummies.columns.tolist())

        if dry_run:
            progress.append(
                info(
                    "Dry run — no changes written",
                    f"Would create {len(created_features)} features",
                )
            )
            result = {
                "success": True,
                "dry_run": True,
                "op": "feature_engineering",
                "original_columns": len(df.columns) - len(created_features),
                "new_features": created_features,
                "total_features": len(created_features),
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        out = Path(output_path) if output_path else path
        df.to_csv(str(out), index=False)

        progress.append(
            ok(
                f"Feature engineering complete",
                f"{len(created_features)} features created",
            )
        )

        result = {
            "success": True,
            "op": "feature_engineering",
            "original_columns": len(df.columns) - len(created_features),
            "new_features": created_features,
            "total_features": len(created_features),
            "output_file": out.name,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("feature_engineering error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and column names.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# statistical_tests — auto-select test by data types
# ---------------------------------------------------------------------------


def statistical_tests(
    file_path: str,
    test_type: str = "",
    column_a: str = "",
    column_b: str = "",
    group_column: str = "",
) -> dict:
    """Auto-select and run statistical tests: t-test, ANOVA, chi-square, correlation."""
    progress = []
    try:
        try:
            from scipy import stats
        except ImportError:
            return {
                "success": False,
                "error": "scipy not installed",
                "hint": "Install: uv add scipy",
                "progress": [fail("Missing dependency", "scipy")],
                "token_estimate": 20,
            }

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

        # Auto-detect test type if not specified
        if not test_type:
            if column_a and column_b:
                if pd.api.types.is_numeric_dtype(
                    df[column_a]
                ) and pd.api.types.is_numeric_dtype(df[column_b]):
                    test_type = "correlation"
                elif (
                    pd.api.types.is_numeric_dtype(df[column_a])
                    and df[column_b].dtype == "object"
                ):
                    test_type = "anova"
                    group_column = column_b
                    column_b = ""
                elif df[column_a].dtype == "object" and df[column_b].dtype == "object":
                    test_type = "chi_square"
            elif column_a and group_column:
                test_type = "t_test" if df[group_column].nunique() == 2 else "anova"

        if not test_type:
            return {
                "success": False,
                "error": "Cannot auto-detect test type",
                "hint": "Specify test_type: t_test, anova, chi_square, correlation",
                "progress": [fail("Auto-detect failed", "")],
                "token_estimate": 20,
            }

        result = {"success": True, "op": "statistical_tests", "test_type": test_type}

        if test_type == "correlation" and column_a and column_b:
            valid = df[[column_a, column_b]].dropna()
            stat, p_value = stats.pearsonr(valid[column_a], valid[column_b])
            result["statistic"] = round(float(stat), 4)
            result["p_value"] = round(float(p_value), 6)
            result["significant"] = p_value < 0.05
            result["interpretation"] = (
                "significant" if p_value < 0.05 else "not significant"
            )
            result["columns"] = [column_a, column_b]

        elif test_type == "t_test" and column_a and group_column:
            groups = df.groupby(group_column)[column_a].apply(list)
            if len(groups) == 2:
                stat, p_value = stats.ttest_ind(
                    [x for x in groups.iloc[0] if pd.notna(x)],
                    [x for x in groups.iloc[1] if pd.notna(x)],
                )
                result["statistic"] = round(float(stat), 4)
                result["p_value"] = round(float(p_value), 6)
                result["significant"] = p_value < 0.05
                result["interpretation"] = (
                    "significant" if p_value < 0.05 else "not significant"
                )
                result["groups"] = list(groups.index)

        elif test_type == "anova" and column_a and group_column:
            groups = df.groupby(group_column)[column_a].apply(list)
            group_data = [[x for x in g if pd.notna(x)] for g in groups]
            stat, p_value = stats.f_oneway(*group_data)
            result["statistic"] = round(float(stat), 4)
            result["p_value"] = round(float(p_value), 6)
            result["significant"] = p_value < 0.05
            result["interpretation"] = (
                "significant" if p_value < 0.05 else "not significant"
            )
            result["groups"] = list(groups.index)

        elif test_type == "chi_square" and column_a and column_b:
            ct = pd.crosstab(df[column_a], df[column_b])
            stat, p_value, dof, expected = stats.chi2_contingency(ct)
            result["statistic"] = round(float(stat), 4)
            result["p_value"] = round(float(p_value), 6)
            result["degrees_of_freedom"] = int(dof)
            result["significant"] = p_value < 0.05
            result["interpretation"] = (
                "significant" if p_value < 0.05 else "not significant"
            )
            result["columns"] = [column_a, column_b]

        progress.append(
            ok(
                f"{test_type} test complete",
                f"p={result.get('p_value', 'N/A')}, {result.get('interpretation', '')}",
            )
        )
        result["progress"] = progress
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("statistical_tests error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path, column names, and test parameters.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# time_series_analysis — auto-detect date column
# ---------------------------------------------------------------------------


def time_series_analysis(
    file_path: str,
    date_column: str = "",
    value_columns: list[str] = None,
    period: str = "M",
) -> dict:
    """Auto-detect date column, compute trend, seasonality, rolling stats."""
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

        # Auto-detect date column
        if not date_column:
            for c in df.columns:
                try:
                    pd.to_datetime(df[c].head(100), errors="raise")
                    date_column = c
                    break
                except (ValueError, TypeError):
                    pass
            if not date_column:
                return {
                    "success": False,
                    "error": "No date column detected",
                    "hint": "Specify date_column parameter",
                    "progress": [fail("No date column", "")],
                    "token_estimate": 20,
                }
            progress.append(info("Auto-detected date column", date_column))

        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
        df = df.dropna(subset=[date_column])

        numeric_cols = (
            value_cols
            if (
                value_cols := (
                    value_columns
                    or [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
                )
            )
            else []
        )

        if not numeric_cols:
            return {
                "success": False,
                "error": "No numeric columns found",
                "hint": "Specify value_columns parameter",
                "progress": [fail("No numeric columns", "")],
                "token_estimate": 20,
            }

        df["period"] = df[date_column].dt.to_period(period).astype(str)
        aggregated = df.groupby("period", as_index=False)[numeric_cols].agg(
            ["sum", "mean", "count"]
        )
        aggregated.columns = ["_".join(col).strip() for col in aggregated.columns]

        # Rolling stats
        rolling = (
            aggregated[[c for c in aggregated.columns if c.endswith("_sum")]]
            .rolling(window=3, min_periods=1)
            .mean()
        )
        rolling.columns = [
            c.replace("_sum", "_rolling_mean_3") for c in rolling.columns
        ]
        result_df = pd.concat([aggregated, rolling], axis=1)

        result_rows = result_df.fillna(0).to_dict(orient="records")

        progress.append(
            ok(
                f"Time series analysis complete",
                f"{len(result_rows)} periods, {len(numeric_cols)} metrics",
            )
        )

        result = {
            "success": True,
            "op": "time_series_analysis",
            "date_column": date_column,
            "value_columns": numeric_cols,
            "period": period,
            "num_periods": len(result_rows),
            "result": result_rows,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("time_series_analysis error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and parameters.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# cohort_analysis — auto-detect cohort/date columns
# ---------------------------------------------------------------------------


def cohort_analysis(
    file_path: str,
    cohort_column: str = "",
    date_column: str = "",
    value_column: str = "",
) -> dict:
    """Cohort retention analysis with auto-detection of cohort identifiers."""
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

        # Auto-detect columns
        if not cohort_column:
            for c in df.columns:
                if df[c].nunique() > 10 and df[c].dtype == "object":
                    cohort_column = c
                    break
        if not date_column:
            for c in df.columns:
                try:
                    pd.to_datetime(df[c].head(100), errors="raise")
                    date_column = c
                    break
                except (ValueError, TypeError):
                    pass

        if not cohort_column or not date_column:
            return {
                "success": False,
                "error": "Cannot auto-detect cohort and date columns",
                "hint": "Specify cohort_column and date_column parameters",
                "progress": [fail("Auto-detect failed", "")],
                "token_estimate": 20,
            }

        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
        df = df.dropna(subset=[date_column])

        # Cohort assignment (first date per entity)
        df["cohort"] = (
            df.groupby(cohort_column)[date_column]
            .transform("min")
            .dt.to_period("M")
            .astype(str)
        )
        df["period"] = df[date_column].dt.to_period("M").astype(str)

        # Build cohort matrix
        cohort_data = (
            df.groupby(["cohort", cohort_column])
            .agg(count=(value_column if value_column else cohort_column, "count"))
            .reset_index()
        )
        cohort_pivot = (
            cohort_data.groupby(["cohort", "period"])["count"]
            .sum()
            .unstack(fill_value=0)
        )

        # Convert to retention percentages
        cohort_sizes = cohort_pivot.iloc[:, 0]
        retention = cohort_pivot.div(cohort_sizes, axis=0).round(4)

        result = {
            "success": True,
            "op": "cohort_analysis",
            "cohort_column": cohort_column,
            "date_column": date_column,
            "num_cohorts": len(retention),
            "cohort_matrix": retention.fillna(0).to_dict(),
            "progress": [ok(f"Cohort analysis complete", f"{len(retention)} cohorts")],
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("cohort_analysis error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and parameters.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# feature_engineering — auto-create features
# ---------------------------------------------------------------------------


def feature_engineering(
    file_path: str,
    features: list[str] = None,
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    """Auto-create features: date parts, numeric bins, text length, one-hot encoding."""
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
        created_features = []

        for c in df.columns:
            if features and c not in features:
                continue

            # Date features
            try:
                dt = pd.to_datetime(df[c], errors="raise")
                df[f"{c}_year"] = dt.dt.year
                df[f"{c}_month"] = dt.dt.month
                df[f"{c}_day"] = dt.dt.day
                df[f"{c}_dayofweek"] = dt.dt.dayofweek
                created_features.extend(
                    [f"{c}_year", f"{c}_month", f"{c}_day", f"{c}_dayofweek"]
                )
                continue
            except (ValueError, TypeError):
                pass

            # Numeric features
            if pd.api.types.is_numeric_dtype(df[c]):
                # Binning
                if df[c].nunique() > 10:
                    df[f"{c}_bin"] = pd.qcut(df[c], q=5, duplicates="drop").astype(str)
                    created_features.append(f"{c}_bin")
                # Log transform
                if (df[c] > 0).all():
                    df[f"{c}_log"] = np.log1p(df[c])
                    created_features.append(f"{c}_log")

            # Text features
            if df[c].dtype == "object":
                df[f"{c}_length"] = df[c].astype(str).str.len()
                df[f"{c}_word_count"] = df[c].astype(str).str.split().str.len()
                created_features.extend([f"{c}_length", f"{c}_word_count"])
                # One-hot encoding for low-cardinality
                if df[c].nunique() <= 10 and df[c].nunique() > 1:
                    dummies = pd.get_dummies(df[c], prefix=c, dtype=int)
                    for dc in dummies.columns:
                        df[dc] = dummies[dc]
                    created_features.extend(dummies.columns.tolist())

        if dry_run:
            progress.append(
                info(
                    "Dry run — no changes written",
                    f"Would create {len(created_features)} features",
                )
            )
            result = {
                "success": True,
                "dry_run": True,
                "op": "feature_engineering",
                "original_columns": len(df.columns) - len(created_features),
                "new_features": created_features,
                "total_features": len(created_features),
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        out = Path(output_path) if output_path else path
        df.to_csv(str(out), index=False)

        progress.append(
            ok(
                f"Feature engineering complete",
                f"{len(created_features)} features created",
            )
        )

        result = {
            "success": True,
            "op": "feature_engineering",
            "original_columns": len(df.columns) - len(created_features),
            "new_features": created_features,
            "total_features": len(created_features),
            "output_file": out.name,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("feature_engineering error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and column names.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# statistical_tests — auto-select test by data types
# ---------------------------------------------------------------------------


def statistical_tests(
    file_path: str,
    test_type: str = "",
    column_a: str = "",
    column_b: str = "",
    group_column: str = "",
) -> dict:
    """Auto-select and run statistical tests: t-test, ANOVA, chi-square, correlation."""
    progress = []
    try:
        try:
            from scipy import stats
        except ImportError:
            return {
                "success": False,
                "error": "scipy not installed",
                "hint": "Install: uv add scipy",
                "progress": [fail("Missing dependency", "scipy")],
                "token_estimate": 20,
            }

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

        # Auto-detect test type if not specified
        if not test_type:
            if column_a and column_b:
                if pd.api.types.is_numeric_dtype(
                    df[column_a]
                ) and pd.api.types.is_numeric_dtype(df[column_b]):
                    test_type = "correlation"
                elif (
                    pd.api.types.is_numeric_dtype(df[column_a])
                    and df[column_b].dtype == "object"
                ):
                    test_type = "anova"
                    group_column = column_b
                    column_b = ""
                elif df[column_a].dtype == "object" and df[column_b].dtype == "object":
                    test_type = "chi_square"
            elif column_a and group_column:
                test_type = "t_test" if df[group_column].nunique() == 2 else "anova"

        if not test_type:
            return {
                "success": False,
                "error": "Cannot auto-detect test type",
                "hint": "Specify test_type: t_test, anova, chi_square, correlation",
                "progress": [fail("Auto-detect failed", "")],
                "token_estimate": 20,
            }

        result = {"success": True, "op": "statistical_tests", "test_type": test_type}

        if test_type == "correlation" and column_a and column_b:
            valid = df[[column_a, column_b]].dropna()
            stat, p_value = stats.pearsonr(valid[column_a], valid[column_b])
            result["statistic"] = round(float(stat), 4)
            result["p_value"] = round(float(p_value), 6)
            result["significant"] = p_value < 0.05
            result["interpretation"] = (
                "significant" if p_value < 0.05 else "not significant"
            )
            result["columns"] = [column_a, column_b]

        elif test_type == "t_test" and column_a and group_column:
            groups = df.groupby(group_column)[column_a].apply(list)
            if len(groups) == 2:
                stat, p_value = stats.ttest_ind(
                    [x for x in groups.iloc[0] if pd.notna(x)],
                    [x for x in groups.iloc[1] if pd.notna(x)],
                )
                result["statistic"] = round(float(stat), 4)
                result["p_value"] = round(float(p_value), 6)
                result["significant"] = p_value < 0.05
                result["interpretation"] = (
                    "significant" if p_value < 0.05 else "not significant"
                )
                result["groups"] = list(groups.index)

        elif test_type == "anova" and column_a and group_column:
            groups = df.groupby(group_column)[column_a].apply(list)
            group_data = [[x for x in g if pd.notna(x)] for g in groups]
            stat, p_value = stats.f_oneway(*group_data)
            result["statistic"] = round(float(stat), 4)
            result["p_value"] = round(float(p_value), 6)
            result["significant"] = p_value < 0.05
            result["interpretation"] = (
                "significant" if p_value < 0.05 else "not significant"
            )
            result["groups"] = list(groups.index)

        elif test_type == "chi_square" and column_a and column_b:
            ct = pd.crosstab(df[column_a], df[column_b])
            stat, p_value, dof, expected = stats.chi2_contingency(ct)
            result["statistic"] = round(float(stat), 4)
            result["p_value"] = round(float(p_value), 6)
            result["degrees_of_freedom"] = int(dof)
            result["significant"] = p_value < 0.05
            result["interpretation"] = (
                "significant" if p_value < 0.05 else "not significant"
            )
            result["columns"] = [column_a, column_b]

        progress.append(
            ok(
                f"{test_type} test complete",
                f"p={result.get('p_value', 'N/A')}, {result.get('interpretation', '')}",
            )
        )
        result["progress"] = progress
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("statistical_tests error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path, column names, and test parameters.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# time_series_analysis — auto-detect date column
# ---------------------------------------------------------------------------


def time_series_analysis(
    file_path: str,
    date_column: str = "",
    value_columns: list[str] = None,
    period: str = "M",
) -> dict:
    """Auto-detect date column, compute trend, seasonality, rolling stats."""
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

        # Auto-detect date column
        if not date_column:
            for c in df.columns:
                try:
                    pd.to_datetime(df[c].head(100), errors="raise")
                    date_column = c
                    break
                except (ValueError, TypeError):
                    pass
            if not date_column:
                return {
                    "success": False,
                    "error": "No date column detected",
                    "hint": "Specify date_column parameter",
                    "progress": [fail("No date column", "")],
                    "token_estimate": 20,
                }
            progress.append(info("Auto-detected date column", date_column))

        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
        df = df.dropna(subset=[date_column])

        numeric_cols = (
            value_cols
            if (
                value_cols := (
                    value_columns
                    or [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c != date_column]
                )
            )
            else []
        )

        if not numeric_cols:
            return {
                "success": False,
                "error": "No numeric columns found",
                "hint": "Specify value_columns parameter",
                "progress": [fail("No numeric columns", "")],
                "token_estimate": 20,
            }

        df["period"] = df[date_column].dt.to_period(period).astype(str)
        aggregated = df.groupby("period", as_index=False)[numeric_cols].agg(
            ["sum", "mean", "count"]
        )
        aggregated.columns = ["_".join(col).strip() for col in aggregated.columns]

        # Rolling stats
        rolling = (
            aggregated[[c for c in aggregated.columns if c.endswith("_sum")]]
            .rolling(window=3, min_periods=1)
            .mean()
        )
        rolling.columns = [
            c.replace("_sum", "_rolling_mean_3") for c in rolling.columns
        ]
        result_df = pd.concat([aggregated, rolling], axis=1)

        result_rows = result_df.fillna(0).to_dict(orient="records")

        progress.append(
            ok(
                f"Time series analysis complete",
                f"{len(result_rows)} periods, {len(numeric_cols)} metrics",
            )
        )

        result = {
            "success": True,
            "op": "time_series_analysis",
            "date_column": date_column,
            "value_columns": numeric_cols,
            "period": period,
            "num_periods": len(result_rows),
            "result": result_rows,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("time_series_analysis error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and parameters.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# cohort_analysis — auto-detect cohort/date columns
# ---------------------------------------------------------------------------


def cohort_analysis(
    file_path: str,
    cohort_column: str = "",
    date_column: str = "",
    value_column: str = "",
) -> dict:
    """Cohort retention analysis with auto-detection of cohort identifiers."""
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

        # Auto-detect columns
        if not cohort_column:
            for c in df.columns:
                if df[c].nunique() > 10 and df[c].dtype == "object":
                    cohort_column = c
                    break
        if not date_column:
            for c in df.columns:
                try:
                    pd.to_datetime(df[c].head(100), errors="raise")
                    date_column = c
                    break
                except (ValueError, TypeError):
                    pass

        if not cohort_column or not date_column:
            return {
                "success": False,
                "error": "Cannot auto-detect cohort and date columns",
                "hint": "Specify cohort_column and date_column parameters",
                "progress": [fail("Auto-detect failed", "")],
                "token_estimate": 20,
            }

        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
        df = df.dropna(subset=[date_column])

        # Cohort assignment (first date per entity)
        df["cohort"] = (
            df.groupby(cohort_column)[date_column]
            .transform("min")
            .dt.to_period("M")
            .astype(str)
        )
        df["period"] = df[date_column].dt.to_period("M").astype(str)

        # Build cohort matrix
        cohort_data = (
            df.groupby(["cohort", "period"])
            .agg(count=(value_column if value_column else cohort_column, "count"))
            .reset_index()
        )
        cohort_pivot = (
            cohort_data.groupby(["cohort", "period"])["count"]
            .sum()
            .unstack(fill_value=0)
        )

        # Convert to retention percentages
        cohort_sizes = cohort_pivot.iloc[:, 0]
        retention = cohort_pivot.div(cohort_sizes, axis=0).round(4)

        result = {
            "success": True,
            "op": "cohort_analysis",
            "cohort_column": cohort_column,
            "date_column": date_column,
            "num_cohorts": len(retention),
            "cohort_matrix": retention.fillna(0).to_dict(),
            "progress": [ok(f"Cohort analysis complete", f"{len(retention)} cohorts")],
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("cohort_analysis error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and parameters.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# validate_dataset
# ---------------------------------------------------------------------------


def validate_dataset(
    file_path: str,
    expected_dtypes: dict = None,
    max_null_pct: float = 5.0,
    check_duplicates: bool = True,
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
        issues = []
        total_rows = len(df)
        null_summary = {}
        dtype_mismatches = {}

        # Null check
        for col in df.columns:
            nc = int(df[col].isna().sum())
            if nc > 0:
                pct = round(nc / total_rows * 100, 2) if total_rows > 0 else 0
                null_summary[col] = nc
                if pct > max_null_pct:
                    issues.append(
                        {
                            "severity": "error",
                            "column": col,
                            "issue": f"{nc} nulls ({pct}%)",
                        }
                    )
                else:
                    issues.append(
                        {
                            "severity": "warning",
                            "column": col,
                            "issue": f"{nc} nulls ({pct}%)",
                        }
                    )

        # Zero check for numeric
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                zc = int((df[col] == 0).sum())
                if zc > 0:
                    pct = round(zc / total_rows * 100, 2) if total_rows > 0 else 0
                    issues.append(
                        {
                            "severity": "warning",
                            "column": col,
                            "issue": f"{zc} zeros ({pct}%)",
                        }
                    )

        # Duplicate check
        dup_count = 0
        if check_duplicates:
            dup_count = int(df.duplicated().sum())
            if dup_count > 0:
                issues.append(
                    {
                        "severity": "info",
                        "column": None,
                        "issue": f"{dup_count} duplicate rows",
                    }
                )

        # Dtype check
        if expected_dtypes:
            for col, expected in expected_dtypes.items():
                if col in df.columns:
                    actual = _dtype_label(df[col])
                    if expected.lower() not in actual.lower():
                        dtype_mismatches[col] = {
                            "expected": expected,
                            "actual": actual,
                        }
                        issues.append(
                            {
                                "severity": "error",
                                "column": col,
                                "issue": f"Expected {expected}, got {actual}",
                            }
                        )

        # Score
        penalty = 0
        for iss in issues:
            if iss["severity"] == "error":
                penalty += 5
            elif iss["severity"] == "warning":
                penalty += 2
            else:
                penalty += 1
        score = max(0, 100 - penalty)

        passed = len(issues) == 0
        progress.append(
            ok(
                f"Validated {path.name}",
                f"Score: {score}/100, {'PASSED' if passed else 'ISSUES FOUND'}",
            )
        )

        result = {
            "success": True,
            "op": "validate_dataset",
            "passed": passed,
            "score": score,
            "issues": issues,
            "dtype_mismatches": dtype_mismatches,
            "duplicate_count": dup_count,
            "null_summary": null_summary,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("validate_dataset error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path is absolute and the file is a valid CSV.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# compute_aggregations
# ---------------------------------------------------------------------------


def compute_aggregations(
    file_path: str,
    group_by: list[str],
    agg_column: str,
    agg_func: str = "sum",
    sort_desc: bool = True,
    top_n: int = 0,
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

        missing = [c for c in group_by if c not in df.columns]
        if missing:
            return {
                "success": False,
                "error": f"Group-by columns not found: {missing}",
                "hint": f"Available: {', '.join(df.columns)}",
                "progress": [fail("Column not found", str(missing))],
                "token_estimate": 30,
            }
        if agg_column not in df.columns:
            return {
                "success": False,
                "error": f"Aggregation column not found: {agg_column}",
                "hint": f"Available: {', '.join(df.columns)}",
                "progress": [fail("Column not found", agg_column)],
                "token_estimate": 30,
            }

        valid_funcs = {"sum", "mean", "count", "min", "max"}
        if agg_func not in valid_funcs:
            return {
                "success": False,
                "error": f"Invalid agg_func: {agg_func}",
                "hint": f"Valid functions: {', '.join(sorted(valid_funcs))}",
                "progress": [fail("Invalid function", agg_func)],
                "token_estimate": 30,
            }

        grouped = df.groupby(group_by, as_index=False)[agg_column].agg(agg_func)
        if sort_desc:
            grouped = grouped.sort_values(by=agg_column, ascending=False)

        if top_n > 0:
            grouped = grouped.head(top_n)

        # Truncate
        max_r = get_max_rows()
        truncated = len(grouped) > max_r
        if truncated:
            grouped = grouped.head(max_r)
            progress.append(warn("Results truncated", f"Showing first {max_r} groups"))

        result_list = grouped.fillna("").to_dict(orient="records")

        progress.append(
            ok(f"Aggregated {path.name}", f"{len(result_list)} groups returned")
        )

        result = {
            "success": True,
            "op": "compute_aggregations",
            "group_by": group_by,
            "agg_column": agg_column,
            "agg_func": agg_func,
            "groups": len(result_list),
            "returned": len(result_list),
            "result": result_list,
            "truncated": truncated,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("compute_aggregations error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and column names are correct.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# run_cleaning_pipeline
# ---------------------------------------------------------------------------


def run_cleaning_pipeline(
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

        if not ops:
            return {
                "success": False,
                "error": "At least one op is required",
                "hint": "Provide a list of ops to apply.",
                "progress": [fail("No ops provided", "")],
                "token_estimate": 20,
            }

        # Import engine functions from Tier 1
        tier1_engine = Path(__file__).resolve().parents[1] / "data_basic" / "engine.py"
        import importlib.util

        spec = importlib.util.spec_from_file_location("tier1_engine", str(tier1_engine))
        t1 = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(t1)

        df = _read_csv(str(path))

        if dry_run:
            would_change = []
            for op in ops:
                op_name = op.get("op", "")
                would_change.append({"op": op_name, "params": op})
            result = {
                "success": True,
                "dry_run": True,
                "op": "run_cleaning_pipeline",
                "total_ops": len(ops),
                "would_change": would_change,
                "progress": [info("Dry run — no changes written", path.name)],
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        # Single snapshot
        backup = snapshot(str(path))
        progress.append(info("Snapshot created", Path(backup).name))

        summary = []
        for i, op in enumerate(ops):
            op_name = op.get("op", "")
            handler_map = {
                "drop_column": t1._op_drop_column,
                "clean_text": t1._op_clean_text,
                "cast_column": t1._op_cast_column,
                "replace_values": t1._op_replace_values,
                "add_column": t1._op_add_column,
                "cap_outliers": t1._op_cap_outliers,
                "fill_nulls": t1._op_fill_nulls,
                "drop_duplicates": t1._op_drop_duplicates,
            }
            handler = handler_map.get(op_name)
            if handler is None:
                progress.append(fail(f"Unknown op: {op_name}", ""))
                return {
                    "success": False,
                    "error": f"Unknown op: {op_name}",
                    "hint": f"Valid ops: {', '.join(sorted(handler_map.keys()))}",
                    "applied": i,
                    "backup": backup,
                    "progress": progress,
                    "token_estimate": _token_estimate(progress),
                }
            try:
                df, op_result = handler(df, op)
                summary.append(op_result)
                progress.append(ok(f"Applied {op_name}", str(op_result)))
            except Exception as exc:
                progress.append(fail(f"Op {i} ({op_name}) failed", str(exc)))
                # Restore from snapshot
                t1.restore(str(path), backup)
                return {
                    "success": False,
                    "error": f"Op {i} ({op_name}): {exc}",
                    "hint": "Restored from snapshot. Fix the op and retry.",
                    "applied": i,
                    "backup": backup,
                    "progress": progress,
                    "token_estimate": _token_estimate(progress),
                }

        df.to_csv(str(path), index=False)

        append_receipt(
            str(path),
            tool="run_cleaning_pipeline",
            args={"ops": ops},
            result=f"applied {len(ops)} ops",
            backup=backup,
        )

        progress.append(ok(f"Saved {path.name}", f"{len(ops)} ops applied"))

        result = {
            "success": True,
            "op": "run_cleaning_pipeline",
            "total_ops": len(ops),
            "applied": len(ops),
            "summary": summary,
            "backup": backup,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("run_cleaning_pipeline error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Use restore_version to undo if a snapshot was taken.",
            "backup": backup,
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# correlation_analysis
# ---------------------------------------------------------------------------


def correlation_analysis(
    file_path: str,
    method: str = "pearson",
    top_n: int = 10,
) -> dict:
    """Correlation matrix + top N strongest pairs for numeric columns."""
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
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

        if len(numeric_cols) < 2:
            return {
                "success": False,
                "error": "Need at least 2 numeric columns for correlation",
                "hint": f"Only found {len(numeric_cols)} numeric columns: {', '.join(numeric_cols)}",
                "progress": [fail("Insufficient numeric columns", str(numeric_cols))],
                "token_estimate": 20,
            }

        valid_methods = {"pearson", "spearman", "kendall"}
        if method not in valid_methods:
            return {
                "success": False,
                "error": f"Invalid method: {method}",
                "hint": f"Valid methods: {', '.join(sorted(valid_methods))}",
                "progress": [fail("Invalid method", method)],
                "token_estimate": 20,
            }

        corr = df[numeric_cols].corr(method=method)

        # Extract pairs (upper triangle)
        pairs = []
        for i in range(len(numeric_cols)):
            for j in range(i + 1, len(numeric_cols)):
                val = corr.iloc[i, j]
                if not pd.isna(val):
                    pairs.append(
                        {
                            "col_a": numeric_cols[i],
                            "col_b": numeric_cols[j],
                            "correlation": round(float(val), 4),
                        }
                    )

        pairs.sort(key=lambda x: abs(x["correlation"]), reverse=True)
        top_pairs = pairs[:top_n]

        # Build matrix as dict of dicts
        corr_matrix = {}
        for c in numeric_cols:
            corr_matrix[c] = {
                c2: round(float(corr.loc[c, c2]), 4) for c2 in numeric_cols
            }

        progress.append(
            ok(
                f"Correlation analysis complete",
                f"{len(numeric_cols)} columns, {len(pairs)} pairs, method={method}",
            )
        )

        result = {
            "success": True,
            "op": "correlation_analysis",
            "method": method,
            "numeric_columns": numeric_cols,
            "total_pairs": len(pairs),
            "top_correlations": top_pairs,
            "correlation_matrix": corr_matrix,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("correlation_analysis error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path is absolute and the file is a valid CSV.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# cross_tabulate
# ---------------------------------------------------------------------------


def cross_tabulate(
    file_path: str,
    row_column: str,
    col_column: str,
    values_column: str = "",
    agg_func: str = "count",
    normalize: str = "",
) -> dict:
    """Contingency table between two categorical columns."""
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

        if row_column not in df.columns:
            return {
                "success": False,
                "error": f"Row column not found: {row_column}",
                "hint": f"Available: {', '.join(df.columns)}",
                "progress": [fail("Column not found", row_column)],
                "token_estimate": 30,
            }
        if col_column not in df.columns:
            return {
                "success": False,
                "error": f"Column column not found: {col_column}",
                "hint": f"Available: {', '.join(df.columns)}",
                "progress": [fail("Column not found", col_column)],
                "token_estimate": 30,
            }

        if values_column:
            if values_column not in df.columns:
                return {
                    "success": False,
                    "error": f"Values column not found: {values_column}",
                    "hint": f"Available: {', '.join(df.columns)}",
                    "progress": [fail("Column not found", values_column)],
                    "token_estimate": 30,
                }
            table = pd.crosstab(
                df[row_column],
                df[col_column],
                values=df[values_column],
                aggfunc=agg_func,
            )
        else:
            table = pd.crosstab(df[row_column], df[col_column])

        if normalize:
            norm_options = {"all": True, "index": 0, "columns": 1}
            if normalize in norm_options:
                table = table / table.sum(axis=norm_options[normalize])
                table = table.round(4)

        # Convert to list of dicts for JSON serialization
        result_rows = []
        for idx, row in table.iterrows():
            rd = {row_column: idx}
            for c in table.columns:
                rd[str(c)] = round(float(row[c]), 4) if pd.notna(row[c]) else 0
            result_rows.append(rd)

        progress.append(
            ok(
                f"Cross-tabulation complete",
                f"{table.shape[0]} × {table.shape[1]} table",
            )
        )

        result = {
            "success": True,
            "op": "cross_tabulate",
            "row_column": row_column,
            "col_column": col_column,
            "values_column": values_column or None,
            "agg_func": agg_func,
            "normalize": normalize or None,
            "table_shape": [table.shape[0], table.shape[1]],
            "result": result_rows,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("cross_tabulate error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and column names.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# pivot_table
# ---------------------------------------------------------------------------


def pivot_table(
    file_path: str,
    index: list[str],
    columns: list[str] = None,
    values: list[str] = None,
    agg_func: str = "sum",
    fill_value: float = 0,
) -> dict:
    """Multi-dimensional pivot/aggregation table."""
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

        # Validate columns
        for c in index:
            if c not in df.columns:
                return {
                    "success": False,
                    "error": f"Index column not found: {c}",
                    "hint": f"Available: {', '.join(df.columns)}",
                    "progress": [fail("Column not found", c)],
                    "token_estimate": 30,
                }

        if columns:
            for c in columns:
                if c not in df.columns:
                    return {
                        "success": False,
                        "error": f"Column not found: {c}",
                        "hint": f"Available: {', '.join(df.columns)}",
                        "progress": [fail("Column not found", c)],
                        "token_estimate": 30,
                    }

        if values:
            for c in values:
                if c not in df.columns:
                    return {
                        "success": False,
                        "error": f"Values column not found: {c}",
                        "hint": f"Available: {', '.join(df.columns)}",
                        "progress": [fail("Column not found", c)],
                        "token_estimate": 30,
                    }

        table = pd.pivot_table(
            df,
            index=index,
            columns=columns,
            values=values,
            aggfunc=agg_func,
            fill_value=fill_value,
        )

        # Flatten multi-index columns
        if isinstance(table.columns, pd.MultiIndex):
            table.columns = [
                "_".join(str(c) for c in col).strip("_") for col in table.columns
            ]
        table = table.reset_index()

        result_rows = table.fillna(fill_value).to_dict(orient="records")

        progress.append(
            ok(
                f"Pivot table created",
                f"{len(result_rows)} rows, {len(table.columns)} columns",
            )
        )

        result = {
            "success": True,
            "op": "pivot_table",
            "index": index,
            "columns": columns,
            "values": values,
            "agg_func": agg_func,
            "rows": len(result_rows),
            "result_columns": list(table.columns),
            "result": result_rows,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("pivot_table error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and column names.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# value_counts
# ---------------------------------------------------------------------------


def value_counts(
    file_path: str,
    columns: list[str],
    top_n: int = 20,
    include_pct: bool = True,
) -> dict:
    """Frequency tables with percentages for categorical columns."""
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
        total_rows = len(df)

        missing_cols = [c for c in columns if c not in df.columns]
        if missing_cols:
            return {
                "success": False,
                "error": f"Columns not found: {missing_cols}",
                "hint": f"Available: {', '.join(df.columns)}",
                "progress": [fail("Column not found", str(missing_cols))],
                "token_estimate": 30,
            }

        results = {}
        for c in columns:
            vc = df[c].value_counts().head(top_n)
            entries = []
            for val, count in vc.items():
                entry = {"value": str(val), "count": int(count)}
                if include_pct:
                    entry["pct"] = round(count / total_rows * 100, 2)
                entries.append(entry)
            results[c] = {
                "total_unique": int(df[c].nunique()),
                "total_rows": total_rows,
                "top_values": entries,
            }

        progress.append(
            ok(f"Value counts computed", f"{len(columns)} columns, top {top_n} each")
        )

        result = {
            "success": True,
            "op": "value_counts",
            "columns": columns,
            "top_n": top_n,
            "results": results,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("value_counts error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and column names.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# filter_rows
# ---------------------------------------------------------------------------


def filter_rows(
    file_path: str,
    conditions: list[dict],
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    """Filter rows by conditions. Supports: equals, contains, gt, lt, gte, lte, not_null, is_null."""
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
        original_rows = len(df)

        mask = pd.Series(True, index=df.index)
        for cond in conditions:
            col = cond.get("column", "")
            op = cond.get("op", "")
            value = cond.get("value")

            if col not in df.columns:
                return {
                    "success": False,
                    "error": f"Column not found: {col}",
                    "hint": f"Available: {', '.join(df.columns)}",
                    "progress": [fail("Column not found", col)],
                    "token_estimate": 30,
                }

            if op == "equals":
                mask &= df[col] == value
            elif op == "not_equals":
                mask &= df[col] != value
            elif op == "contains":
                mask &= (
                    df[col].astype(str).str.contains(str(value), case=False, na=False)
                )
            elif op == "gt":
                mask &= pd.to_numeric(df[col], errors="coerce") > value
            elif op == "lt":
                mask &= pd.to_numeric(df[col], errors="coerce") < value
            elif op == "gte":
                mask &= pd.to_numeric(df[col], errors="coerce") >= value
            elif op == "lte":
                mask &= pd.to_numeric(df[col], errors="coerce") <= value
            elif op == "not_null":
                mask &= df[col].notna()
            elif op == "is_null":
                mask &= df[col].isna()
            else:
                return {
                    "success": False,
                    "error": f"Unknown filter op: {op}",
                    "hint": "Valid ops: equals, not_equals, contains, gt, lt, gte, lte, not_null, is_null",
                    "progress": [fail("Unknown op", op)],
                    "token_estimate": 30,
                }

        filtered_df = df[mask]
        remaining = len(filtered_df)

        if dry_run:
            progress.append(
                info(
                    "Dry run — no changes written",
                    f"Would keep {remaining} of {original_rows} rows",
                )
            )
            result = {
                "success": True,
                "dry_run": True,
                "op": "filter_rows",
                "original_rows": original_rows,
                "filtered_rows": remaining,
                "removed_rows": original_rows - remaining,
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        # Write filtered data
        out = Path(output_path) if output_path else path
        filtered_df.to_csv(str(out), index=False)

        progress.append(
            ok(f"Filtered {path.name}", f"{remaining} of {original_rows} rows kept")
        )

        result = {
            "success": True,
            "op": "filter_rows",
            "original_rows": original_rows,
            "filtered_rows": remaining,
            "removed_rows": original_rows - remaining,
            "output_file": out.name,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("filter_rows error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path, column names, and condition values.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# sample_data
# ---------------------------------------------------------------------------


def sample_data(
    file_path: str,
    method: str = "random",
    n: int = 100,
    random_state: int = 42,
    output_path: str = "",
) -> dict:
    """Sample rows from dataset. Methods: random, head, tail, stratified."""
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
        total_rows = len(df)

        valid_methods = {"random", "head", "tail"}
        if method not in valid_methods:
            return {
                "success": False,
                "error": f"Invalid method: {method}",
                "hint": f"Valid methods: {', '.join(sorted(valid_methods))}",
                "progress": [fail("Invalid method", method)],
                "token_estimate": 20,
            }

        actual_n = min(n, total_rows)

        if method == "random":
            sampled = df.sample(n=actual_n, random_state=random_state)
        elif method == "head":
            sampled = df.head(actual_n)
        elif method == "tail":
            sampled = df.tail(actual_n)

        if output_path:
            out = Path(output_path)
        else:
            out = path.parent / f"{path.stem}_sample_{method}_{actual_n}.csv"

        sampled.to_csv(str(out), index=False)

        progress.append(
            ok(f"Sampled {method}", f"{actual_n} of {total_rows} rows → {out.name}")
        )

        result = {
            "success": True,
            "op": "sample_data",
            "method": method,
            "original_rows": total_rows,
            "sampled_rows": actual_n,
            "output_file": out.name,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("sample_data error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and parameters.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# correlation_analysis
# ---------------------------------------------------------------------------


def correlation_analysis(
    file_path: str,
    method: str = "pearson",
    top_n: int = 10,
) -> dict:
    """Correlation matrix + top N strongest pairs for numeric columns."""
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
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

        if len(numeric_cols) < 2:
            return {
                "success": False,
                "error": "Need at least 2 numeric columns for correlation",
                "hint": f"Only found {len(numeric_cols)} numeric columns: {', '.join(numeric_cols)}",
                "progress": [fail("Insufficient numeric columns", str(numeric_cols))],
                "token_estimate": 20,
            }

        valid_methods = {"pearson", "spearman", "kendall"}
        if method not in valid_methods:
            return {
                "success": False,
                "error": f"Invalid method: {method}",
                "hint": f"Valid methods: {', '.join(sorted(valid_methods))}",
                "progress": [fail("Invalid method", method)],
                "token_estimate": 20,
            }

        corr = df[numeric_cols].corr(method=method)

        # Extract pairs (upper triangle)
        pairs = []
        for i in range(len(numeric_cols)):
            for j in range(i + 1, len(numeric_cols)):
                val = corr.iloc[i, j]
                if not pd.isna(val):
                    pairs.append(
                        {
                            "col_a": numeric_cols[i],
                            "col_b": numeric_cols[j],
                            "correlation": round(float(val), 4),
                        }
                    )

        pairs.sort(key=lambda x: abs(x["correlation"]), reverse=True)
        top_pairs = pairs[:top_n]

        # Build matrix as dict of dicts
        corr_matrix = {}
        for c in numeric_cols:
            corr_matrix[c] = {
                c2: round(float(corr.loc[c, c2]), 4) for c2 in numeric_cols
            }

        progress.append(
            ok(
                f"Correlation analysis complete",
                f"{len(numeric_cols)} columns, {len(pairs)} pairs, method={method}",
            )
        )

        result = {
            "success": True,
            "op": "correlation_analysis",
            "method": method,
            "numeric_columns": numeric_cols,
            "total_pairs": len(pairs),
            "top_correlations": top_pairs,
            "correlation_matrix": corr_matrix,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("correlation_analysis error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path is absolute and the file is a valid CSV.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# cross_tabulate
# ---------------------------------------------------------------------------


def cross_tabulate(
    file_path: str,
    row_column: str,
    col_column: str,
    values_column: str = "",
    agg_func: str = "count",
    normalize: str = "",
) -> dict:
    """Contingency table between two categorical columns."""
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

        if row_column not in df.columns:
            return {
                "success": False,
                "error": f"Row column not found: {row_column}",
                "hint": f"Available: {', '.join(df.columns)}",
                "progress": [fail("Column not found", row_column)],
                "token_estimate": 30,
            }
        if col_column not in df.columns:
            return {
                "success": False,
                "error": f"Column column not found: {col_column}",
                "hint": f"Available: {', '.join(df.columns)}",
                "progress": [fail("Column not found", col_column)],
                "token_estimate": 30,
            }

        if values_column:
            if values_column not in df.columns:
                return {
                    "success": False,
                    "error": f"Values column not found: {values_column}",
                    "hint": f"Available: {', '.join(df.columns)}",
                    "progress": [fail("Column not found", values_column)],
                    "token_estimate": 30,
                }
            table = pd.crosstab(
                df[row_column],
                df[col_column],
                values=df[values_column],
                aggfunc=agg_func,
            )
        else:
            table = pd.crosstab(df[row_column], df[col_column])

        if normalize:
            norm_options = {"all": True, "index": 0, "columns": 1}
            if normalize in norm_options:
                table = table / table.sum(axis=norm_options[normalize])
                table = table.round(4)

        # Convert to list of dicts for JSON serialization
        result_rows = []
        for idx, row in table.iterrows():
            rd = {row_column: idx}
            for c in table.columns:
                rd[str(c)] = round(float(row[c]), 4) if pd.notna(row[c]) else 0
            result_rows.append(rd)

        progress.append(
            ok(
                f"Cross-tabulation complete",
                f"{table.shape[0]} × {table.shape[1]} table",
            )
        )

        result = {
            "success": True,
            "op": "cross_tabulate",
            "row_column": row_column,
            "col_column": col_column,
            "values_column": values_column or None,
            "agg_func": agg_func,
            "normalize": normalize or None,
            "table_shape": [table.shape[0], table.shape[1]],
            "result": result_rows,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("cross_tabulate error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and column names.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# pivot_table
# ---------------------------------------------------------------------------


def pivot_table(
    file_path: str,
    index: list[str],
    columns: list[str] = None,
    values: list[str] = None,
    agg_func: str = "sum",
    fill_value: float = 0,
) -> dict:
    """Multi-dimensional pivot/aggregation table."""
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

        # Validate columns
        for c in index:
            if c not in df.columns:
                return {
                    "success": False,
                    "error": f"Index column not found: {c}",
                    "hint": f"Available: {', '.join(df.columns)}",
                    "progress": [fail("Column not found", c)],
                    "token_estimate": 30,
                }

        if columns:
            for c in columns:
                if c not in df.columns:
                    return {
                        "success": False,
                        "error": f"Column not found: {c}",
                        "hint": f"Available: {', '.join(df.columns)}",
                        "progress": [fail("Column not found", c)],
                        "token_estimate": 30,
                    }

        if values:
            for c in values:
                if c not in df.columns:
                    return {
                        "success": False,
                        "error": f"Values column not found: {c}",
                        "hint": f"Available: {', '.join(df.columns)}",
                        "progress": [fail("Column not found", c)],
                        "token_estimate": 30,
                    }

        table = pd.pivot_table(
            df,
            index=index,
            columns=columns,
            values=values,
            aggfunc=agg_func,
            fill_value=fill_value,
        )

        # Flatten multi-index columns
        if isinstance(table.columns, pd.MultiIndex):
            table.columns = [
                "_".join(str(c) for c in col).strip("_") for col in table.columns
            ]
        table = table.reset_index()

        result_rows = table.fillna(fill_value).to_dict(orient="records")

        progress.append(
            ok(
                f"Pivot table created",
                f"{len(result_rows)} rows, {len(table.columns)} columns",
            )
        )

        result = {
            "success": True,
            "op": "pivot_table",
            "index": index,
            "columns": columns,
            "values": values,
            "agg_func": agg_func,
            "rows": len(result_rows),
            "result_columns": list(table.columns),
            "result": result_rows,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("pivot_table error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and column names.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# value_counts
# ---------------------------------------------------------------------------


def value_counts(
    file_path: str,
    columns: list[str],
    top_n: int = 20,
    include_pct: bool = True,
) -> dict:
    """Frequency tables with percentages for categorical columns."""
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
        total_rows = len(df)

        missing_cols = [c for c in columns if c not in df.columns]
        if missing_cols:
            return {
                "success": False,
                "error": f"Columns not found: {missing_cols}",
                "hint": f"Available: {', '.join(df.columns)}",
                "progress": [fail("Column not found", str(missing_cols))],
                "token_estimate": 30,
            }

        results = {}
        for c in columns:
            vc = df[c].value_counts().head(top_n)
            entries = []
            for val, count in vc.items():
                entry = {"value": str(val), "count": int(count)}
                if include_pct:
                    entry["pct"] = round(count / total_rows * 100, 2)
                entries.append(entry)
            results[c] = {
                "total_unique": int(df[c].nunique()),
                "total_rows": total_rows,
                "top_values": entries,
            }

        progress.append(
            ok(f"Value counts computed", f"{len(columns)} columns, top {top_n} each")
        )

        result = {
            "success": True,
            "op": "value_counts",
            "columns": columns,
            "top_n": top_n,
            "results": results,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("value_counts error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and column names.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# filter_rows
# ---------------------------------------------------------------------------


def filter_rows(
    file_path: str,
    conditions: list[dict],
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    """Filter rows by conditions. Supports: equals, contains, gt, lt, gte, lte, not_null, is_null."""
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
        original_rows = len(df)

        mask = pd.Series(True, index=df.index)
        for cond in conditions:
            col = cond.get("column", "")
            op = cond.get("op", "")
            value = cond.get("value")

            if col not in df.columns:
                return {
                    "success": False,
                    "error": f"Column not found: {col}",
                    "hint": f"Available: {', '.join(df.columns)}",
                    "progress": [fail("Column not found", col)],
                    "token_estimate": 30,
                }

            if op == "equals":
                mask &= df[col] == value
            elif op == "not_equals":
                mask &= df[col] != value
            elif op == "contains":
                mask &= (
                    df[col].astype(str).str.contains(str(value), case=False, na=False)
                )
            elif op == "gt":
                mask &= pd.to_numeric(df[col], errors="coerce") > value
            elif op == "lt":
                mask &= pd.to_numeric(df[col], errors="coerce") < value
            elif op == "gte":
                mask &= pd.to_numeric(df[col], errors="coerce") >= value
            elif op == "lte":
                mask &= pd.to_numeric(df[col], errors="coerce") <= value
            elif op == "not_null":
                mask &= df[col].notna()
            elif op == "is_null":
                mask &= df[col].isna()
            else:
                return {
                    "success": False,
                    "error": f"Unknown filter op: {op}",
                    "hint": "Valid ops: equals, not_equals, contains, gt, lt, gte, lte, not_null, is_null",
                    "progress": [fail("Unknown op", op)],
                    "token_estimate": 30,
                }

        filtered_df = df[mask]
        remaining = len(filtered_df)

        if dry_run:
            progress.append(
                info(
                    "Dry run — no changes written",
                    f"Would keep {remaining} of {original_rows} rows",
                )
            )
            result = {
                "success": True,
                "dry_run": True,
                "op": "filter_rows",
                "original_rows": original_rows,
                "filtered_rows": remaining,
                "removed_rows": original_rows - remaining,
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        # Write filtered data
        out = Path(output_path) if output_path else path
        filtered_df.to_csv(str(out), index=False)

        progress.append(
            ok(f"Filtered {path.name}", f"{remaining} of {original_rows} rows kept")
        )

        result = {
            "success": True,
            "op": "filter_rows",
            "original_rows": original_rows,
            "filtered_rows": remaining,
            "removed_rows": original_rows - remaining,
            "output_file": out.name,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("filter_rows error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path, column names, and condition values.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# sample_data
# ---------------------------------------------------------------------------


def sample_data(
    file_path: str,
    method: str = "random",
    n: int = 100,
    random_state: int = 42,
    output_path: str = "",
) -> dict:
    """Sample rows from dataset. Methods: random, head, tail, stratified."""
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
        total_rows = len(df)

        valid_methods = {"random", "head", "tail"}
        if method not in valid_methods:
            return {
                "success": False,
                "error": f"Invalid method: {method}",
                "hint": f"Valid methods: {', '.join(sorted(valid_methods))}",
                "progress": [fail("Invalid method", method)],
                "token_estimate": 20,
            }

        actual_n = min(n, total_rows)

        if method == "random":
            sampled = df.sample(n=actual_n, random_state=random_state)
        elif method == "head":
            sampled = df.head(actual_n)
        elif method == "tail":
            sampled = df.tail(actual_n)

        if output_path:
            out = Path(output_path)
        else:
            out = path.parent / f"{path.stem}_sample_{method}_{actual_n}.csv"

        sampled.to_csv(str(out), index=False)

        progress.append(
            ok(f"Sampled {method}", f"{actual_n} of {total_rows} rows → {out.name}")
        )

        result = {
            "success": True,
            "op": "sample_data",
            "method": method,
            "original_rows": total_rows,
            "sampled_rows": actual_n,
            "output_file": out.name,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("sample_data error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and parameters.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }
