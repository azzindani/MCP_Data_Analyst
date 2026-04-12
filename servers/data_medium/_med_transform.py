"""Transformation tools for data_medium. No MCP imports."""

from __future__ import annotations

import importlib.util
import logging
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_HERE = str(Path(__file__).resolve().parent)
for _p in (str(_ROOT), _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import pandas as pd
from _med_helpers import (
    _is_string_col,
    _open_file,
    _read_csv,
    _token_estimate,
)

from shared.file_utils import resolve_path
from shared.platform_utils import get_max_rows
from shared.progress import fail, info, ok, warn
from shared.receipt import append_receipt
from shared.version_control import snapshot

logger = logging.getLogger(__name__)


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

        main_vals = set(df[join_column].dropna().astype(str).unique())
        geo_vals = set(gdf[geo_join_column].dropna().astype(str).unique())
        unmatched_main = list(main_vals - geo_vals)[:20]
        unmatched_geo = list(geo_vals - main_vals)[:20]

        gdf_flat = gdf.copy()
        gdf_flat[geo_join_column] = gdf_flat[geo_join_column].astype(str)
        df[join_column] = df[join_column].astype(str)

        new_cols = [c for c in gdf_flat.columns if c != geo_join_column]
        merged = df.merge(gdf_flat, left_on=join_column, right_on=geo_join_column, how="left")

        geo_col = gdf.geometry.name
        if geo_col in merged.columns:
            merged[geo_col] = merged[geo_col].apply(lambda g: g.wkt if g is not None else None)

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

        backup = snapshot(str(path))
        out = str(Path(output_path).resolve()) if output_path else str(path)
        merged.to_csv(out, index=False)

        append_receipt(
            str(path),
            tool="enrich_with_geo",
            args={"geo_file": geo_path.name},
            result=f"matched {matched} rows",
            backup=backup,
        )

        progress.append(ok(f"Enriched {path.name}", f"{matched} rows matched with {geo_path.name}"))

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
            "hint": "Check file paths are absolute and join columns exist.",
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

        max_r = get_max_rows()
        truncated = len(grouped) > max_r
        if truncated:
            grouped = grouped.head(max_r)
            progress.append(warn("Results truncated", f"Showing first {max_r} groups"))

        result_list = grouped.fillna("").to_dict(orient="records")

        progress.append(ok(f"Aggregated {path.name}", f"{len(result_list)} groups returned"))

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

        tier1_engine = Path(__file__).resolve().parents[1] / "data_basic" / "engine.py"

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
# smart_impute
# ---------------------------------------------------------------------------


def smart_impute(
    file_path: str,
    columns: list[str] = None,
    output_path: str = "",
    dry_run: bool = False,
    open_after: bool = True,
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

        df = _read_csv(str(path))
        target_cols = columns if columns else list(df.columns)
        missing_cols = [c for c in target_cols if c not in df.columns]
        if missing_cols:
            return {
                "success": False,
                "error": f"Columns not found: {missing_cols}",
                "hint": f"Available: {', '.join(df.columns)}",
                "progress": [fail("Column not found", str(missing_cols))],
                "token_estimate": 20,
            }

        imputation_plan = []
        for col in target_cols:
            null_count = int(df[col].isna().sum())
            if null_count == 0:
                continue
            s = df[col]
            if pd.api.types.is_numeric_dtype(s):
                strategy = "median"
                fill_val = s.median()
            elif pd.api.types.is_datetime64_any_dtype(s):
                strategy = "ffill"
                fill_val = None
            else:
                strategy = "mode"
                mode_vals = s.mode()
                fill_val = mode_vals.iloc[0] if len(mode_vals) > 0 else None

            imputation_plan.append(
                {
                    "column": col,
                    "strategy": strategy,
                    "null_count": null_count,
                    "fill_value": str(fill_val) if fill_val is not None else None,
                }
            )

        if dry_run:
            progress.append(info("Dry run — no changes written", path.name))
            result = {
                "success": True,
                "dry_run": True,
                "op": "smart_impute",
                "would_change": imputation_plan,
                "columns_to_impute": len(imputation_plan),
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        backup = snapshot(str(path))
        for plan in imputation_plan:
            col = plan["column"]
            strategy = plan["strategy"]
            if strategy == "median":
                df[col] = df[col].fillna(df[col].median())
            elif strategy == "mode":
                mode_vals = df[col].mode()
                if len(mode_vals) > 0:
                    df[col] = df[col].fillna(mode_vals.iloc[0])
            elif strategy == "ffill":
                df[col] = df[col].ffill()

        out = Path(output_path).resolve() if output_path else path
        df.to_csv(str(out), index=False)

        if open_after:
            _open_file(out)

        append_receipt(
            str(path),
            tool="smart_impute",
            args={"columns": target_cols},
            result=f"imputed {len(imputation_plan)} columns",
            backup=backup,
        )
        progress.append(ok(f"Imputed {path.name}", f"{len(imputation_plan)} columns filled"))

        result = {
            "success": True,
            "op": "smart_impute",
            "imputed": imputation_plan,
            "columns_imputed": len(imputation_plan),
            "output_file": out.name,
            "backup": backup,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("smart_impute error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Use restore_version to undo if a snapshot was taken.",
            "backup": backup,
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# merge_datasets
# ---------------------------------------------------------------------------


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
    progress = []
    backup = None
    try:
        path = resolve_path(file_path)
        right_path = resolve_path(right_file_path)

        for p in [path, right_path]:
            if not p.exists():
                return {
                    "success": False,
                    "error": f"File not found: {p.name}",
                    "hint": "Check file_path is absolute and the file exists.",
                    "progress": [fail("File not found", p.name)],
                    "token_estimate": 20,
                }

        valid_hows = {"left", "right", "inner", "outer"}
        if how not in valid_hows:
            return {
                "success": False,
                "error": f"Invalid join type: {how}",
                "hint": f"Valid: {', '.join(sorted(valid_hows))}",
                "progress": [fail("Invalid join type", how)],
                "token_estimate": 20,
            }

        left_df = _read_csv(str(path))
        right_df = _read_csv(str(right_path))

        if not left_on or not right_on:
            common = [c for c in left_df.columns if c in right_df.columns]
            if not common:
                return {
                    "success": False,
                    "error": "No common columns found for auto-detect join.",
                    "hint": "Specify left_on and right_on explicitly.",
                    "progress": [fail("No common columns", "")],
                    "token_estimate": 20,
                }
            left_on = right_on = common[0]
            progress.append(info("Auto-detected join key", left_on))

        if left_on not in left_df.columns:
            return {
                "success": False,
                "error": f"left_on column '{left_on}' not in left dataset",
                "hint": f"Available: {', '.join(left_df.columns)}",
                "progress": [fail("Column not found", left_on)],
                "token_estimate": 20,
            }
        if right_on not in right_df.columns:
            return {
                "success": False,
                "error": f"right_on column '{right_on}' not in right dataset",
                "hint": f"Available: {', '.join(right_df.columns)}",
                "progress": [fail("Column not found", right_on)],
                "token_estimate": 20,
            }

        left_vals = set(left_df[left_on].dropna().astype(str))
        right_vals = set(right_df[right_on].dropna().astype(str))
        unmatched_left = list(left_vals - right_vals)[:20]
        unmatched_right = list(right_vals - left_vals)[:20]

        merged = left_df.merge(
            right_df,
            left_on=left_on,
            right_on=right_on,
            how=how,
            suffixes=("", "_right"),
        )
        if left_on != right_on and right_on in merged.columns:
            merged = merged.drop(columns=[right_on])

        rows_matched = int(merged[left_on].notna().sum())

        if dry_run:
            progress.append(info("Dry run — no changes written", path.name))
            result = {
                "success": True,
                "dry_run": True,
                "op": "merge_datasets",
                "left_rows": len(left_df),
                "right_rows": len(right_df),
                "result_rows": len(merged),
                "matched": rows_matched,
                "unmatched_left": unmatched_left,
                "unmatched_right": unmatched_right,
                "how": how,
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        backup = snapshot(str(path))
        out = Path(output_path).resolve() if output_path else path
        merged.to_csv(str(out), index=False)

        if open_after:
            _open_file(out)

        append_receipt(
            str(path),
            tool="merge_datasets",
            args={"right": right_path.name, "how": how, "on": left_on},
            result=f"merged {len(merged)} rows",
            backup=backup,
        )
        progress.append(
            ok(
                f"Merged {path.name} + {right_path.name}",
                f"{len(merged)} rows ({how} join)",
            )
        )

        result = {
            "success": True,
            "op": "merge_datasets",
            "left_rows": len(left_df),
            "right_rows": len(right_df),
            "result_rows": len(merged),
            "matched": rows_matched,
            "unmatched_left": unmatched_left,
            "unmatched_right": unmatched_right,
            "how": how,
            "output_file": out.name,
            "backup": backup,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("merge_datasets error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Use restore_version to undo if a snapshot was taken.",
            "backup": backup,
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# feature_engineering
# ---------------------------------------------------------------------------


def feature_engineering(
    file_path: str,
    features: list[str] = None,
    output_path: str = "",
    dry_run: bool = False,
    open_after: bool = True,
) -> dict:
    """features: list of 'date_parts','bins','text_length','one_hot' or None=all."""
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

        df = _read_csv(str(path))
        valid_features = {"date_parts", "bins", "text_length", "one_hot"}
        requested = set(features) if features else valid_features
        invalid = requested - valid_features
        if invalid:
            return {
                "success": False,
                "error": f"Invalid feature types: {invalid}",
                "hint": f"Valid: {', '.join(sorted(valid_features))}",
                "progress": [fail("Invalid feature type", str(invalid))],
                "token_estimate": 20,
            }

        new_columns = []

        if "date_parts" in requested:
            date_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
            for col in date_cols:
                for part in ("year", "month", "day", "dayofweek"):
                    new_col = f"{col}_{part}"
                    df[new_col] = getattr(df[col].dt, part)
                    new_columns.append(new_col)

        if "text_length" in requested:
            text_cols = [c for c in df.columns if _is_string_col(df[c])]
            for col in text_cols:
                new_col = f"{col}_len"
                df[new_col] = df[col].astype(str).str.len()
                new_columns.append(new_col)

        if "bins" in requested:
            num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c not in new_columns]
            for col in num_cols[:5]:
                try:
                    new_col = f"{col}_bin"
                    df[new_col] = pd.qcut(df[col], q=4, labels=["Q1", "Q2", "Q3", "Q4"], duplicates="drop")
                    new_columns.append(new_col)
                except Exception:
                    pass

        if "one_hot" in requested:
            cat_cols = [
                c for c in df.columns if _is_string_col(df[c]) and df[c].nunique() <= 10 and c not in new_columns
            ]
            for col in cat_cols[:5]:
                dummies = pd.get_dummies(df[col], prefix=col, drop_first=False).astype(int)
                df = pd.concat([df, dummies], axis=1)
                new_columns.extend(dummies.columns.tolist())

        if dry_run:
            progress.append(info("Dry run — no changes written", path.name))
            result = {
                "success": True,
                "dry_run": True,
                "op": "feature_engineering",
                "would_add": new_columns,
                "features_requested": list(requested),
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        backup = snapshot(str(path))
        out = Path(output_path).resolve() if output_path else path
        df.to_csv(str(out), index=False)

        if open_after:
            _open_file(out)

        append_receipt(
            str(path),
            tool="feature_engineering",
            args={"features": list(requested)},
            result=f"added {len(new_columns)} columns",
            backup=backup,
        )
        progress.append(ok(f"Features added to {path.name}", f"{len(new_columns)} new columns"))

        result = {
            "success": True,
            "op": "feature_engineering",
            "features_applied": list(requested),
            "new_columns": new_columns,
            "columns_added": len(new_columns),
            "output_file": out.name,
            "backup": backup,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("feature_engineering error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Use restore_version to undo if a snapshot was taken.",
            "backup": backup,
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }
