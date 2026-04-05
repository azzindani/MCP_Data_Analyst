"""Tier 2 engine — profiling, cleaning pipelines, aggregations. Zero MCP imports."""

from __future__ import annotations

import importlib.util
import logging
import subprocess
import sys
import webbrowser
from pathlib import Path

import pandas as pd

try:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots
    _PLOTLY_AVAILABLE = True
except ImportError:
    _PLOTLY_AVAILABLE = False

# Shared utilities
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shared.file_utils import resolve_path
from shared.platform_utils import get_max_results, get_max_rows
from shared.html_theme import css_vars, device_mode_js, plotly_template, save_chart as _html_save_chart
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
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
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

        result: dict = {
            "success": True,
            "op": "check_outliers",
            "method": method,
            "scanned_columns": len(results),
            "columns_with_outliers": cols_with_outliers,
            "results": results,
            "truncated": truncated,
            "progress": progress,
        }

        if _PLOTLY_AVAILABLE and results:
            scanned = list(results.keys())
            fig = make_subplots(rows=1, cols=len(scanned), subplot_titles=scanned)
            for i, col in enumerate(scanned):
                fig.add_trace(
                    go.Box(y=df[col].dropna(), name=col, showlegend=False),
                    row=1, col=i + 1,
                )
            fig.update_layout(
                title=f"Outlier Distribution — {path.name}",
                template=plotly_template(theme),
                height=450,
            )
            abs_p, fname = _save_chart(fig, output_path, "outliers", path, open_after, theme)
            result["output_path"] = abs_p
            result["output_name"] = fname
            progress.append(ok("Chart saved", fname))
        elif not _PLOTLY_AVAILABLE:
            progress.append(warn("plotly not installed", "pip install plotly to enable HTML export"))

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
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
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

        result: dict = {
            "success": True,
            "op": "scan_nulls_zeros",
            "total_rows": total_rows,
            "clean_columns": clean_count,
            "flagged_columns": len(results),
            "results": results,
            "suggested_actions": suggested,
            "progress": progress,
        }

        if _PLOTLY_AVAILABLE and results:
            cols = list(results.keys())
            null_vals = [results[c]["null_count"] for c in cols]
            zero_vals = [results[c]["zero_count"] or 0 for c in cols]
            fig = go.Figure()
            fig.add_trace(go.Bar(x=null_vals, y=cols, orientation="h", name="Nulls", marker_color="#EF553B"))
            if include_zeros:
                fig.add_trace(go.Bar(x=zero_vals, y=cols, orientation="h", name="Zeros", marker_color="#636EFA"))
            fig.update_layout(
                title=f"Null & Zero Counts — {path.name}",
                barmode="group",
                xaxis_title="Count",
                template=plotly_template(theme),
                height=max(300, len(cols) * 30 + 100),
            )
            abs_p, fname = _save_chart(fig, output_path, "nulls_zeros", path, open_after, theme)
            result["output_path"] = abs_p
            result["output_name"] = fname
            progress.append(ok("Chart saved", fname))
        elif not _PLOTLY_AVAILABLE:
            progress.append(warn("plotly not installed", "pip install plotly to enable HTML export"))

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
            "hint": "Check file paths are absolute and join columns exist.",
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
# _open_file helper
# ---------------------------------------------------------------------------


def _open_file(path: Path) -> None:
    """Open file in default system app. Silently ignored on failure."""
    try:
        webbrowser.open(f"file://{path.resolve()}")
    except Exception:
        try:
            if sys.platform == "win32":
                subprocess.Popen(["start", str(path.resolve())], shell=True)
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(path.resolve())])
            else:
                subprocess.Popen(["xdg-open", str(path.resolve())])
        except Exception:
            pass


def _save_chart(fig, output_path: str, stem_suffix: str, input_path: Path, open_after: bool, theme: str = "dark") -> tuple[str, str]:
    """Save plotly figure to themed responsive HTML. Returns (abs_path_str, filename)."""
    return _html_save_chart(fig, output_path, stem_suffix, input_path, theme, open_after, _open_file)


# ---------------------------------------------------------------------------
# correlation_analysis
# ---------------------------------------------------------------------------


def correlation_analysis(
    file_path: str,
    method: str = "pearson",
    top_n: int = 10,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
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

        valid_methods = {"pearson", "kendall", "spearman"}
        if method not in valid_methods:
            return {
                "success": False,
                "error": f"Invalid method: {method}",
                "hint": f"Valid methods: {', '.join(sorted(valid_methods))}",
                "progress": [fail("Invalid method", method)],
                "token_estimate": 20,
            }

        df = _read_csv(str(path))
        num_df = df.select_dtypes(include="number")
        if num_df.shape[1] < 2:
            return {
                "success": False,
                "error": "At least 2 numeric columns required.",
                "hint": "Use inspect_dataset() to check column dtypes.",
                "progress": [fail("Not enough numeric columns", path.name)],
                "token_estimate": 20,
            }

        corr = num_df.corr(method=method)
        pairs = []
        cols = corr.columns.tolist()
        for i in range(len(cols)):
            for j in range(i + 1, len(cols)):
                val = corr.iloc[i, j]
                if pd.notna(val):
                    pairs.append({"col_a": cols[i], "col_b": cols[j], "correlation": round(float(val), 4)})
        pairs.sort(key=lambda x: abs(x["correlation"]), reverse=True)
        top_pairs = pairs[: max(1, top_n)]

        matrix = {
            col: {c: round(float(v), 4) if pd.notna(v) else None for c, v in row.items()}
            for col, row in corr.to_dict().items()
        }

        progress.append(ok(f"Correlation for {path.name}", f"method={method}, {len(cols)} columns"))

        result: dict = {
            "success": True,
            "op": "correlation_analysis",
            "method": method,
            "columns": cols,
            "top_pairs": top_pairs,
            "matrix": matrix,
            "progress": progress,
        }

        if _PLOTLY_AVAILABLE:
            z = [[matrix[r][c] if matrix[r][c] is not None else 0.0 for c in cols] for r in cols]
            fig = go.Figure(go.Heatmap(
                z=z, x=cols, y=cols,
                colorscale="RdBu", zmid=0,
                text=[[f"{v:.2f}" if v is not None else "" for v in row] for row in z],
                texttemplate="%{text}",
            ))
            fig.update_layout(
                title=f"Correlation Heatmap — {path.name} ({method})",
                template=plotly_template(theme),
            )
            abs_p, fname = _save_chart(fig, output_path, "correlation", path, open_after, theme)
            result["output_path"] = abs_p
            result["output_name"] = fname
            progress.append(ok("Chart saved", fname))
        else:
            progress.append(warn("plotly not installed", "pip install plotly to enable HTML export"))

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
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
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
        for col in [row_column, col_column]:
            if col not in df.columns:
                return {
                    "success": False,
                    "error": f"Column '{col}' not found",
                    "hint": f"Available: {', '.join(df.columns)}",
                    "progress": [fail("Column not found", col)],
                    "token_estimate": 20,
                }
        if values_column and values_column not in df.columns:
            return {
                "success": False,
                "error": f"Values column '{values_column}' not found",
                "hint": f"Available: {', '.join(df.columns)}",
                "progress": [fail("Column not found", values_column)],
                "token_estimate": 20,
            }

        norm = normalize if normalize in ("index", "columns", "all") else False
        if values_column:
            ct = pd.crosstab(
                df[row_column], df[col_column],
                values=df[values_column], aggfunc=agg_func, normalize=norm
            )
        else:
            ct = pd.crosstab(df[row_column], df[col_column], normalize=norm)

        table = {
            str(row_idx): {str(c): (round(float(v), 4) if pd.notna(v) else None) for c, v in row.items()}
            for row_idx, row in ct.to_dict(orient="index").items()
        }

        max_r = get_max_rows()
        rows_returned = min(len(table), max_r)
        truncated = len(table) > max_r
        if truncated:
            keys = list(table.keys())[:max_r]
            table = {k: table[k] for k in keys}
            progress.append(warn("Results truncated", f"Showing first {max_r} rows"))

        progress.append(ok(f"Cross-tabulated {path.name}", f"{row_column} × {col_column}"))

        result: dict = {
            "success": True,
            "op": "cross_tabulate",
            "row_column": row_column,
            "col_column": col_column,
            "agg_func": agg_func if values_column else "count",
            "normalize": normalize or False,
            "rows": len(ct),
            "cols": len(ct.columns),
            "returned": rows_returned,
            "table": table,
            "truncated": truncated,
            "progress": progress,
        }

        if _PLOTLY_AVAILABLE:
            row_keys = list(ct.index.astype(str))
            col_keys = list(ct.columns.astype(str))
            z = ct.values.tolist()
            fig = go.Figure(go.Heatmap(
                z=z, x=col_keys, y=row_keys,
                colorscale="Blues",
                text=[[f"{v:.2f}" if isinstance(v, float) else str(v) for v in row] for row in z],
                texttemplate="%{text}",
            ))
            fig.update_layout(
                title=f"Cross-Tabulation: {row_column} × {col_column}",
                xaxis_title=col_column,
                yaxis_title=row_column,
                template=plotly_template(theme),
            )
            abs_p, fname = _save_chart(fig, output_path, "crosstab", path, open_after, theme)
            result["output_path"] = abs_p
            result["output_name"] = fname
            progress.append(ok("Chart saved", fname))
        else:
            progress.append(warn("plotly not installed", "pip install plotly to enable HTML export"))

        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("cross_tabulate error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and column names are correct.",
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
        all_cols = set(df.columns)
        missing = [c for c in (index or []) if c not in all_cols]
        if missing:
            return {
                "success": False,
                "error": f"Index columns not found: {missing}",
                "hint": f"Available: {', '.join(df.columns)}",
                "progress": [fail("Column not found", str(missing))],
                "token_estimate": 20,
            }

        pt = pd.pivot_table(
            df,
            index=index,
            columns=columns if columns else None,
            values=values if values else None,
            aggfunc=agg_func,
            fill_value=fill_value,
        )

        # Flatten multi-level columns
        if isinstance(pt.columns, pd.MultiIndex):
            pt.columns = ["_".join(str(c) for c in col).strip("_") for col in pt.columns]

        pt = pt.reset_index()
        max_r = get_max_rows()
        truncated = len(pt) > max_r
        records = pt.head(max_r).fillna("").to_dict(orient="records")
        if truncated:
            progress.append(warn("Results truncated", f"Showing first {max_r} rows"))

        progress.append(ok(f"Pivot table for {path.name}", f"{len(records)} rows"))

        result = {
            "success": True,
            "op": "pivot_table",
            "index": index,
            "columns": columns,
            "values": values,
            "agg_func": agg_func,
            "rows": len(pt),
            "returned": len(records),
            "result": records,
            "truncated": truncated,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("pivot_table error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and column names. values must be numeric for most agg_funcs.",
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
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
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
        missing = [c for c in columns if c not in df.columns]
        if missing:
            return {
                "success": False,
                "error": f"Columns not found: {missing}",
                "hint": f"Available: {', '.join(df.columns)}",
                "progress": [fail("Column not found", str(missing))],
                "token_estimate": 20,
            }

        results = {}
        for col in columns:
            vc = df[col].value_counts(dropna=False).head(top_n)
            total = len(df)
            if include_pct:
                results[col] = [
                    {"value": str(v), "count": int(c), "pct": round(c / total * 100, 2)}
                    for v, c in vc.items()
                ]
            else:
                results[col] = [{"value": str(v), "count": int(c)} for v, c in vc.items()]

        progress.append(ok(f"Value counts for {path.name}", f"{len(columns)} columns"))

        result: dict = {
            "success": True,
            "op": "value_counts",
            "columns": columns,
            "top_n": top_n,
            "results": results,
            "progress": progress,
        }

        if _PLOTLY_AVAILABLE:
            n_cols = len(columns)
            fig = make_subplots(rows=1, cols=n_cols, subplot_titles=columns)
            for i, col in enumerate(columns):
                entries = results[col]
                vals = [e["value"] for e in entries]
                counts = [e["count"] for e in entries]
                fig.add_trace(go.Bar(x=counts, y=vals, orientation="h", name=col, showlegend=False), row=1, col=i + 1)
            fig.update_layout(title=f"Value Counts — {path.name}", template=plotly_template(theme), height=400)
            abs_p, fname = _save_chart(fig, output_path, "value_counts", path, open_after, theme)
            result["output_path"] = abs_p
            result["output_name"] = fname
            progress.append(ok("Chart saved", fname))
        else:
            progress.append(warn("plotly not installed", "pip install plotly to enable HTML export"))

        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("value_counts error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and column names are correct.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# filter_rows
# ---------------------------------------------------------------------------


def _apply_condition(df: pd.DataFrame, cond: dict) -> pd.Series:
    """Return boolean mask for a single condition dict."""
    col = cond.get("column", "")
    op = cond.get("op", "")
    val = cond.get("value")
    s = df[col]
    if op == "equals":
        return s == val
    if op == "not_equals":
        return s != val
    if op == "contains":
        return s.astype(str).str.contains(str(val), case=False, na=False)
    if op == "gt":
        return pd.to_numeric(s, errors="coerce") > float(val)
    if op == "gte":
        return pd.to_numeric(s, errors="coerce") >= float(val)
    if op == "lt":
        return pd.to_numeric(s, errors="coerce") < float(val)
    if op == "lte":
        return pd.to_numeric(s, errors="coerce") <= float(val)
    if op == "is_null":
        return s.isna()
    if op == "not_null":
        return s.notna()
    raise ValueError(f"Unknown op '{op}'. Valid: equals not_equals contains gt gte lt lte is_null not_null")


def filter_rows(
    file_path: str,
    conditions: list[dict],
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

        if not conditions:
            return {
                "success": False,
                "error": "At least one condition is required.",
                "hint": "Provide conditions list with column, op, value keys.",
                "progress": [fail("No conditions", "")],
                "token_estimate": 20,
            }

        df = _read_csv(str(path))

        # Validate columns in conditions
        for cond in conditions:
            col = cond.get("column", "")
            if col not in df.columns:
                return {
                    "success": False,
                    "error": f"Column '{col}' not found in conditions",
                    "hint": f"Available: {', '.join(df.columns)}",
                    "progress": [fail("Column not found", col)],
                    "token_estimate": 20,
                }

        mask = pd.Series([True] * len(df), index=df.index)
        for cond in conditions:
            mask &= _apply_condition(df, cond)

        filtered = df[mask]
        rows_before = len(df)
        rows_after = len(filtered)

        if dry_run:
            progress.append(info("Dry run — no changes written", path.name))
            result = {
                "success": True,
                "dry_run": True,
                "op": "filter_rows",
                "rows_before": rows_before,
                "rows_after": rows_after,
                "rows_removed": rows_before - rows_after,
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        backup = snapshot(str(path))
        out = Path(output_path) if output_path else path
        filtered.to_csv(str(out), index=False)

        if open_after:
            _open_file(out)

        append_receipt(
            str(path),
            tool="filter_rows",
            args={"conditions": conditions},
            result=f"kept {rows_after}/{rows_before} rows",
            backup=backup,
        )
        progress.append(ok(f"Filtered {path.name}", f"{rows_after}/{rows_before} rows kept"))

        result = {
            "success": True,
            "op": "filter_rows",
            "rows_before": rows_before,
            "rows_after": rows_after,
            "rows_removed": rows_before - rows_after,
            "output_file": out.name,
            "backup": backup,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("filter_rows error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check conditions use valid ops: equals contains gt lt gte lte is_null not_null.",
            "backup": backup,
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
    open_after: bool = True,
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

        valid_methods = {"random", "head", "tail"}
        if method not in valid_methods:
            return {
                "success": False,
                "error": f"Invalid method: {method}",
                "hint": f"Valid methods: {', '.join(sorted(valid_methods))}",
                "progress": [fail("Invalid method", method)],
                "token_estimate": 20,
            }

        df = _read_csv(str(path))
        n = min(n, len(df))

        if method == "random":
            sample = df.sample(n=n, random_state=random_state)
        elif method == "head":
            sample = df.head(n)
        else:
            sample = df.tail(n)

        max_r = get_max_rows()
        truncated = len(sample) > max_r
        records = sample.head(max_r).fillna("").to_dict(orient="records")

        if output_path:
            out = Path(output_path)
            sample.to_csv(str(out), index=False)
            if open_after:
                _open_file(out)
            progress.append(ok(f"Sample saved to {out.name}", f"{n} rows"))
        else:
            progress.append(ok(f"Sampled {path.name}", f"{n} rows ({method})"))

        result = {
            "success": True,
            "op": "sample_data",
            "method": method,
            "total_rows": len(df),
            "sampled": n,
            "returned": len(records),
            "truncated": truncated,
            "sample": records,
            "progress": progress,
        }
        if output_path:
            result["output_file"] = Path(output_path).name
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("sample_data error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path is absolute and the file is a valid CSV.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# auto_detect_schema
# ---------------------------------------------------------------------------


def auto_detect_schema(
    file_path: str,
    max_rows: int = 1000,
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

        df = _read_csv(str(path), max_rows=max_rows)
        suggestions = []
        column_info = {}

        for col in df.columns:
            s = df[col]
            info_entry: dict = {"current_dtype": str(s.dtype), "inferred_type": None, "suggestion": None}

            if s.dtype == object:
                # Try datetime
                try:
                    parsed = pd.to_datetime(s.dropna().head(50), errors="raise")
                    if len(parsed) > 0:
                        info_entry["inferred_type"] = "datetime"
                        info_entry["suggestion"] = f"cast_column col={col} dtype=datetime"
                        suggestions.append(info_entry["suggestion"])
                except Exception:
                    pass

                if info_entry["inferred_type"] is None:
                    # Try numeric
                    numeric_try = pd.to_numeric(s.dropna().head(50), errors="coerce")
                    if numeric_try.notna().mean() > 0.9:
                        info_entry["inferred_type"] = "numeric"
                        info_entry["suggestion"] = f"cast_column col={col} dtype=float"
                        suggestions.append(info_entry["suggestion"])

                if info_entry["inferred_type"] is None:
                    unique_ratio = s.nunique() / max(len(s.dropna()), 1)
                    # Detect ID columns
                    if s.nunique() == len(s.dropna()) and s.nunique() > 10:
                        info_entry["inferred_type"] = "id"
                        info_entry["suggestion"] = f"drop_column col={col} (likely ID, low analytical value)"
                    elif unique_ratio < 0.05:
                        info_entry["inferred_type"] = "category"
                    else:
                        info_entry["inferred_type"] = "text"

            elif pd.api.types.is_integer_dtype(s):
                unique_ratio = s.nunique() / max(len(s.dropna()), 1)
                if unique_ratio < 0.05 and s.nunique() <= 20:
                    info_entry["inferred_type"] = "category_encoded"
                    info_entry["suggestion"] = f"consider label meanings for {col}"
                else:
                    info_entry["inferred_type"] = "int"
            elif pd.api.types.is_float_dtype(s):
                info_entry["inferred_type"] = "float"
            elif pd.api.types.is_datetime64_any_dtype(s):
                info_entry["inferred_type"] = "datetime"

            column_info[col] = info_entry

        progress.append(ok(f"Schema detected for {path.name}", f"{len(column_info)} columns"))

        result = {
            "success": True,
            "op": "auto_detect_schema",
            "file": path.name,
            "rows_sampled": min(max_rows, len(df)),
            "columns": column_info,
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

            imputation_plan.append({
                "column": col,
                "strategy": strategy,
                "null_count": null_count,
                "fill_value": str(fill_val) if fill_val is not None else None,
            })

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

        out = Path(output_path) if output_path else path
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

        # Auto-detect join keys if not provided
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

        merged = left_df.merge(right_df, left_on=left_on, right_on=right_on, how=how, suffixes=("", "_right"))
        # Remove duplicate join key column if names differ
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
        out = Path(output_path) if output_path else path
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
        progress.append(ok(f"Merged {path.name} + {right_path.name}", f"{len(merged)} rows ({how} join)"))

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
            text_cols = [c for c in df.columns if df[c].dtype == object]
            for col in text_cols:
                new_col = f"{col}_len"
                df[new_col] = df[col].astype(str).str.len()
                new_columns.append(new_col)

        if "bins" in requested:
            num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c not in new_columns]
            for col in num_cols[:5]:  # cap at 5 to avoid explosion
                try:
                    new_col = f"{col}_bin"
                    df[new_col] = pd.qcut(df[col], q=4, labels=["Q1", "Q2", "Q3", "Q4"], duplicates="drop")
                    new_columns.append(new_col)
                except Exception:
                    pass

        if "one_hot" in requested:
            cat_cols = [
                c for c in df.columns
                if df[c].dtype == object and df[c].nunique() <= 10 and c not in new_columns
            ]
            for col in cat_cols[:5]:  # cap at 5
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
        out = Path(output_path) if output_path else path
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


# ---------------------------------------------------------------------------
# statistical_tests
# ---------------------------------------------------------------------------


def statistical_tests(
    file_path: str,
    test_type: str = "",
    column_a: str = "",
    column_b: str = "",
    group_column: str = "",
) -> dict:
    progress = []
    try:
        from scipy import stats as scipy_stats
    except ImportError:
        return {
            "success": False,
            "error": "scipy not installed",
            "hint": "Install scipy: uv add scipy",
            "progress": [fail("Missing dependency", "scipy")],
            "token_estimate": 20,
        }

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
        for col in [column_a, column_b, group_column]:
            if col and col not in df.columns:
                return {
                    "success": False,
                    "error": f"Column '{col}' not found",
                    "hint": f"Available: {', '.join(df.columns)}",
                    "progress": [fail("Column not found", col)],
                    "token_estimate": 20,
                }

        # Auto-select test if not specified
        if not test_type:
            a_num = column_a and pd.api.types.is_numeric_dtype(df[column_a])
            b_num = column_b and pd.api.types.is_numeric_dtype(df[column_b])
            g_cat = group_column and not pd.api.types.is_numeric_dtype(df[group_column])

            if a_num and b_num:
                test_type = "correlation"
            elif a_num and g_cat:
                n_groups = df[group_column].nunique()
                test_type = "anova" if n_groups > 2 else "ttest"
            elif column_a and column_b and not a_num and not b_num:
                test_type = "chi_square"
            else:
                return {
                    "success": False,
                    "error": "Cannot auto-select test. Specify test_type.",
                    "hint": "Valid: ttest anova chi_square correlation",
                    "progress": [fail("Auto-select failed", "")],
                    "token_estimate": 20,
                }

        test_result = {}

        if test_type == "ttest":
            groups = df[group_column].dropna().unique() if group_column else []
            if len(groups) == 2:
                g1 = df[df[group_column] == groups[0]][column_a].dropna()
                g2 = df[df[group_column] == groups[1]][column_a].dropna()
            elif column_a and column_b:
                g1 = df[column_a].dropna()
                g2 = df[column_b].dropna()
            else:
                return {
                    "success": False,
                    "error": "t-test requires two numeric columns or one numeric + binary group column.",
                    "hint": "Set column_a + column_b, or column_a + group_column (2 groups).",
                    "progress": [fail("Invalid t-test params", "")],
                    "token_estimate": 20,
                }
            stat, pval = scipy_stats.ttest_ind(g1, g2)
            test_result = {
                "test": "Independent t-test",
                "statistic": round(float(stat), 4),
                "p_value": round(float(pval), 6),
                "significant": float(pval) < 0.05,
                "interpretation": "Means differ significantly (p<0.05)" if float(pval) < 0.05 else "No significant difference (p≥0.05)",
            }

        elif test_type == "anova":
            if not group_column or not column_a:
                return {
                    "success": False,
                    "error": "ANOVA requires column_a (numeric) and group_column (categorical).",
                    "hint": "Set column_a to numeric column and group_column to category column.",
                    "progress": [fail("Invalid ANOVA params", "")],
                    "token_estimate": 20,
                }
            groups_data = [grp[column_a].dropna().values for _, grp in df.groupby(group_column)]
            stat, pval = scipy_stats.f_oneway(*groups_data)
            test_result = {
                "test": "One-Way ANOVA",
                "statistic": round(float(stat), 4),
                "p_value": round(float(pval), 6),
                "groups": int(df[group_column].nunique()),
                "significant": float(pval) < 0.05,
                "interpretation": "Group means differ significantly (p<0.05)" if float(pval) < 0.05 else "No significant difference between groups (p≥0.05)",
            }

        elif test_type == "chi_square":
            if not column_a or not column_b:
                return {
                    "success": False,
                    "error": "Chi-square requires column_a and column_b (both categorical).",
                    "hint": "Set column_a and column_b to categorical columns.",
                    "progress": [fail("Invalid chi-square params", "")],
                    "token_estimate": 20,
                }
            ct = pd.crosstab(df[column_a], df[column_b])
            stat, pval, dof, expected = scipy_stats.chi2_contingency(ct)
            test_result = {
                "test": "Chi-Square Test of Independence",
                "statistic": round(float(stat), 4),
                "p_value": round(float(pval), 6),
                "degrees_of_freedom": int(dof),
                "significant": float(pval) < 0.05,
                "interpretation": "Significant association (p<0.05)" if float(pval) < 0.05 else "No significant association (p≥0.05)",
            }

        elif test_type == "correlation":
            if not column_a or not column_b:
                return {
                    "success": False,
                    "error": "Correlation test requires column_a and column_b (both numeric).",
                    "hint": "Set column_a and column_b to numeric columns.",
                    "progress": [fail("Invalid correlation params", "")],
                    "token_estimate": 20,
                }
            a = pd.to_numeric(df[column_a], errors="coerce").dropna()
            b = pd.to_numeric(df[column_b], errors="coerce").dropna()
            min_len = min(len(a), len(b))
            stat, pval = scipy_stats.pearsonr(a.iloc[:min_len], b.iloc[:min_len])
            test_result = {
                "test": "Pearson Correlation",
                "statistic": round(float(stat), 4),
                "p_value": round(float(pval), 6),
                "significant": float(pval) < 0.05,
                "interpretation": f"Correlation r={round(float(stat), 3)}, {'significant' if float(pval) < 0.05 else 'not significant'} (p={'<' if float(pval) < 0.05 else '≥'}0.05)",
            }

        else:
            return {
                "success": False,
                "error": f"Unknown test_type: {test_type}",
                "hint": "Valid: ttest anova chi_square correlation",
                "progress": [fail("Invalid test type", test_type)],
                "token_estimate": 20,
            }

        progress.append(ok(f"Statistical test on {path.name}", test_result.get("test", test_type)))

        result = {
            "success": True,
            "op": "statistical_tests",
            "test_type": test_type,
            **test_result,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("statistical_tests error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check column names and ensure numeric/categorical types are correct.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# time_series_analysis
# ---------------------------------------------------------------------------


def time_series_analysis(
    file_path: str,
    date_column: str = "",
    value_columns: list[str] = None,
    period: str = "M",
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
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

        valid_periods = {"Y", "Q", "M", "W", "D"}
        if period not in valid_periods:
            return {
                "success": False,
                "error": f"Invalid period: {period}",
                "hint": f"Valid: {', '.join(sorted(valid_periods))}",
                "progress": [fail("Invalid period", period)],
                "token_estimate": 20,
            }

        # Translate deprecated pandas period aliases
        _period_map = {"M": "ME", "Q": "QE", "Y": "YE"}
        resample_period = _period_map.get(period, period)

        df = _read_csv(str(path))

        # Auto-detect date column
        if not date_column:
            date_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
            if not date_cols:
                # Try to parse object columns as dates
                for col in df.columns:
                    if df[col].dtype == object:
                        try:
                            pd.to_datetime(df[col].dropna().head(10), errors="raise")
                            date_column = col
                            break
                        except Exception:
                            pass
            else:
                date_column = date_cols[0]

        if not date_column or date_column not in df.columns:
            return {
                "success": False,
                "error": "No date column found or specified.",
                "hint": "Set date_column to a datetime column, or cast it first with apply_patch.",
                "progress": [fail("No date column", "")],
                "token_estimate": 20,
            }

        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
        df = df.dropna(subset=[date_column])

        # Auto-detect value columns
        if not value_columns:
            value_columns = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])][:5]

        missing_vals = [c for c in value_columns if c not in df.columns]
        if missing_vals:
            return {
                "success": False,
                "error": f"Value columns not found: {missing_vals}",
                "hint": f"Available: {', '.join(df.columns)}",
                "progress": [fail("Column not found", str(missing_vals))],
                "token_estimate": 20,
            }

        df = df.set_index(date_column).sort_index()
        resampled = df[value_columns].resample(resample_period).sum()

        # Rolling stats (7 and 30 periods)
        rolling_7 = resampled.rolling(window=7, min_periods=1).mean()
        rolling_30 = resampled.rolling(window=30, min_periods=1).mean()

        max_r = get_max_rows()
        truncated = len(resampled) > max_r
        resampled_trunc = resampled.tail(max_r)

        trend_data = {}
        try:
            from scipy.stats import linregress as _linregress
            for col in value_columns:
                ts = resampled[col].dropna()
                if len(ts) >= 2:
                    slope, _, r_val, _, _ = _linregress(range(len(ts)), ts.values)
                    trend_data[col] = {
                        "slope": round(float(slope), 4),
                        "r_squared": round(float(r_val ** 2), 4),
                        "direction": "up" if slope > 0 else "down" if slope < 0 else "flat",
                    }
        except ImportError:
            pass

        records = resampled_trunc.reset_index().fillna("").to_dict(orient="records")
        for rec in records:
            for k, v in list(rec.items()):
                if hasattr(v, "isoformat"):
                    rec[k] = v.isoformat()

        if truncated:
            progress.append(warn("Results truncated", f"Showing last {max_r} periods"))

        progress.append(ok(f"Time series analysis for {path.name}", f"{len(resampled)} periods ({period})"))

        result: dict = {
            "success": True,
            "op": "time_series_analysis",
            "date_column": date_column,
            "value_columns": value_columns,
            "period": period,
            "total_periods": len(resampled),
            "date_range": {
                "start": str(df.index.min()),
                "end": str(df.index.max()),
            },
            "trend": trend_data,
            "data": records,
            "truncated": truncated,
            "progress": progress,
        }

        if _PLOTLY_AVAILABLE:
            fig = go.Figure()
            x_vals = [str(i) for i in resampled.index]
            for col in value_columns:
                fig.add_trace(go.Scatter(x=x_vals, y=resampled[col].tolist(), name=col, mode="lines+markers"))
            fig.update_layout(
                title=f"Time Series — {path.name} (period={period})",
                xaxis_title=date_column,
                template=plotly_template(theme),
            )
            abs_p, fname = _save_chart(fig, output_path, "time_series", path, open_after, theme)
            result["output_path"] = abs_p
            result["output_name"] = fname
            progress.append(ok("Chart saved", fname))
        else:
            progress.append(warn("plotly not installed", "pip install plotly to enable HTML export"))

        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("time_series_analysis error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check date_column is a datetime column and value_columns are numeric.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# cohort_analysis
# ---------------------------------------------------------------------------


def cohort_analysis(
    file_path: str,
    cohort_column: str = "",
    date_column: str = "",
    value_column: str = "",
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
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

        # Auto-detect date column
        if not date_column:
            date_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
            if not date_cols:
                for col in df.columns:
                    if df[col].dtype == object:
                        try:
                            pd.to_datetime(df[col].dropna().head(10), errors="raise")
                            date_column = col
                            break
                        except Exception:
                            pass
            else:
                date_column = date_cols[0]

        if not date_column or date_column not in df.columns:
            return {
                "success": False,
                "error": "No date column found or specified.",
                "hint": "Set date_column to a datetime column.",
                "progress": [fail("No date column", "")],
                "token_estimate": 20,
            }

        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
        df = df.dropna(subset=[date_column])

        # Auto-detect cohort column if not specified
        if not cohort_column:
            cat_cols = [c for c in df.columns if df[c].dtype == object and df[c].nunique() < 50]
            if cat_cols:
                cohort_column = cat_cols[0]
                progress.append(info("Auto-detected cohort column", cohort_column))

        if not cohort_column or cohort_column not in df.columns:
            # Fall back to year-month cohort from date column
            df["_cohort"] = df[date_column].dt.to_period("M").astype(str)
            cohort_column = "_cohort"
            progress.append(info("Using date-based cohort", "year-month"))

        # Auto-detect value column
        if not value_column:
            num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
            if num_cols:
                value_column = num_cols[0]

        # Build period column
        df["_period"] = df[date_column].dt.to_period("M").astype(str)

        if value_column and value_column in df.columns:
            pivot = df.pivot_table(
                index=cohort_column,
                columns="_period",
                values=value_column,
                aggfunc="sum",
                fill_value=0,
            )
        else:
            pivot = df.pivot_table(
                index=cohort_column,
                columns="_period",
                values=date_column,
                aggfunc="count",
                fill_value=0,
            )

        max_r = get_max_rows()
        truncated = len(pivot) > max_r
        pivot_trunc = pivot.head(max_r)

        matrix = {
            str(idx): {str(col): int(v) if hasattr(v, "item") else v for col, v in row.items()}
            for idx, row in pivot_trunc.to_dict(orient="index").items()
        }

        if truncated:
            progress.append(warn("Results truncated", f"Showing first {max_r} cohorts"))

        progress.append(ok(f"Cohort analysis for {path.name}", f"{len(pivot)} cohorts × {len(pivot.columns)} periods"))

        result: dict = {
            "success": True,
            "op": "cohort_analysis",
            "cohort_column": cohort_column,
            "date_column": date_column,
            "value_column": value_column or "count",
            "cohorts": len(pivot),
            "periods": len(pivot.columns),
            "matrix": matrix,
            "truncated": truncated,
            "progress": progress,
        }

        if _PLOTLY_AVAILABLE:
            row_keys = list(pivot_trunc.index.astype(str))
            col_keys = list(pivot_trunc.columns.astype(str))
            z = pivot_trunc.values.tolist()
            fig = go.Figure(go.Heatmap(
                z=z, x=col_keys, y=row_keys,
                colorscale="Blues",
                text=[[str(v) for v in row] for row in z],
                texttemplate="%{text}",
            ))
            fig.update_layout(
                title=f"Cohort Analysis — {path.name}",
                xaxis_title="Period",
                yaxis_title=cohort_column,
                template=plotly_template(theme),
            )
            abs_p, fname = _save_chart(fig, output_path, "cohort", path, open_after, theme)
            result["output_path"] = abs_p
            result["output_name"] = fname
            progress.append(ok("Chart saved", fname))
        else:
            progress.append(warn("plotly not installed", "pip install plotly to enable HTML export"))

        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("cohort_analysis error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check date_column is a datetime column and cohort_column exists.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }
