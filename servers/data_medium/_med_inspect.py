"""Inspection and detection tools for data_medium. No MCP imports."""

from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_HERE = str(Path(__file__).resolve().parent)
for _p in (str(_ROOT), _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import pandas as pd

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    from shared.html_theme import plotly_template

    _PLOTLY_AVAILABLE = True
except ImportError:
    _PLOTLY_AVAILABLE = False

from _med_helpers import (
    _dtype_label,
    _is_string_col,
    _open_file,
    _read_csv,
    _save_chart,
    _token_estimate,
    is_numeric_col,
)

from shared.file_utils import resolve_path
from shared.platform_utils import get_max_results, get_max_rows
from shared.progress import fail, info, ok, warn
from shared.receipt import append_receipt
from shared.version_control import snapshot

logger = logging.getLogger(__name__)


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
        numeric_cols = [c for c in df.columns if is_numeric_col(df[c])]

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
            r: dict = {}
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
            "file_path": str(path),
            "method": method,
            "scanned_columns": len(results),
            "columns_with_outliers": cols_with_outliers,
            "results": results,
            "truncated": truncated,
            "hint": "Call apply_patch() with op=cap_outliers or run_cleaning_pipeline() to act on findings.",
            "progress": progress,
        }

        if _PLOTLY_AVAILABLE and results:
            scanned = list(results.keys())
            fig = make_subplots(rows=1, cols=len(scanned), subplot_titles=scanned)
            for i, col in enumerate(scanned):
                fig.add_trace(
                    go.Box(y=df[col].dropna(), name=col, showlegend=False),
                    row=1,
                    col=i + 1,
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

            if df[col].dtype == "object":
                null_like = df[col].isin(["", "-", "N/A", "null", "None"]).sum()
                null_c += int(null_like)
                null_p = round(null_c / total_rows * 100, 2) if total_rows > 0 else 0.0

            flagged = null_c >= min_count or (zero_c is not None and zero_c >= min_count)
            if not flagged:
                continue

            entry: dict = {"null_count": null_c, "null_pct": null_p}
            if zero_c is not None:
                entry["zero_count"] = zero_c
                entry["zero_pct"] = zero_p
            else:
                entry["zero_count"] = None
                entry["zero_pct"] = None

            results[col] = entry

            if null_c > 0:
                if pd.api.types.is_numeric_dtype(df[col]):
                    suggested[col] = "apply_patch op=fill_nulls strategy=median"
                else:
                    suggested[col] = "apply_patch op=fill_nulls strategy=mode"
            if zero_c is not None and zero_c > 0:
                suggested[col] = "apply_patch op=fill_nulls fill_zeros=true strategy=mean"

        clean_count = len(df.columns) - len(results)
        progress.append(ok(f"Scanned {path.name}", f"{clean_count} clean, {len(results)} flagged"))

        result: dict = {
            "success": True,
            "op": "scan_nulls_zeros",
            "file_path": str(path),
            "total_rows": total_rows,
            "clean_columns": clean_count,
            "flagged_columns": len(results),
            "results": results,
            "suggested_actions": suggested,
            "hint": "Call apply_patch() or run_cleaning_pipeline() to act on findings.",
            "progress": progress,
        }

        if _PLOTLY_AVAILABLE and results:
            # Sort by null count descending so highest is at top of the h-bar chart
            sorted_cols = sorted(results.keys(), key=lambda c: results[c]["null_count"])
            cols = sorted_cols
            null_vals = [results[c]["null_count"] for c in cols]
            zero_vals = [results[c]["zero_count"] or 0 for c in cols]
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=null_vals,
                    y=cols,
                    orientation="h",
                    name="Nulls",
                    marker_color="#EF553B",
                )
            )
            if include_zeros:
                fig.add_trace(
                    go.Bar(
                        x=zero_vals,
                        y=cols,
                        orientation="h",
                        name="Zeros",
                        marker_color="#636EFA",
                    )
                )
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

        if expected_dtypes:
            for col, expected in expected_dtypes.items():
                if col in df.columns:
                    actual = _dtype_label(df[col])
                    if expected.lower() not in actual.lower():
                        dtype_mismatches[col] = {"expected": expected, "actual": actual}
                        issues.append(
                            {
                                "severity": "error",
                                "column": col,
                                "issue": f"Expected {expected}, got {actual}",
                            }
                        )

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
            "file_path": str(path),
            "passed": passed,
            "score": score,
            "issues": issues,
            "dtype_mismatches": dtype_mismatches,
            "duplicate_count": dup_count,
            "null_summary": null_summary,
            "hint": "Call apply_patch() or run_cleaning_pipeline() to act on findings.",
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
            info_entry: dict = {
                "current_dtype": str(s.dtype),
                "inferred_type": None,
                "suggestion": None,
            }

            if _is_string_col(s):
                try:
                    parsed = pd.to_datetime(s.dropna().head(50), errors="raise")
                    if len(parsed) > 0:
                        info_entry["inferred_type"] = "datetime"
                        info_entry["suggestion"] = f"cast_column col={col} dtype=datetime"
                        suggestions.append(info_entry["suggestion"])
                except Exception:
                    pass

                if info_entry["inferred_type"] is None:
                    numeric_try = pd.to_numeric(s.dropna().head(50), errors="coerce")
                    if numeric_try.notna().mean() > 0.9:
                        info_entry["inferred_type"] = "numeric"
                        info_entry["suggestion"] = f"cast_column col={col} dtype=float"
                        suggestions.append(info_entry["suggestion"])

                if info_entry["inferred_type"] is None:
                    unique_ratio = s.nunique() / max(len(s.dropna()), 1)
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
            "file_path": str(path),
            "file": path.name,
            "rows_sampled": min(max_rows, len(df)),
            "columns": column_info,
            "suggestions": suggestions,
            "hint": "Call apply_patch() or run_cleaning_pipeline() to act on findings.",
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
# _apply_condition (helper for filter_rows)
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


# ---------------------------------------------------------------------------
# filter_rows
# ---------------------------------------------------------------------------


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
                "file_path": str(path),
                "rows_before": rows_before,
                "rows_after": rows_after,
                "rows_removed": rows_before - rows_after,
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        backup = snapshot(str(path))
        out = resolve_path(output_path) if output_path else path
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
            "file_path": str(path),
            "rows_before": rows_before,
            "rows_after": rows_after,
            "rows_removed": rows_before - rows_after,
            "output_file": out.name,
            "backup": backup,
            "hint": "Call inspect_dataset() or read_column_stats() to verify the changes.",
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
            out = resolve_path(output_path)
            sample.to_csv(str(out), index=False)
            if open_after:
                _open_file(out)
            progress.append(ok(f"Sample saved to {out.name}", f"{n} rows"))
        else:
            progress.append(ok(f"Sampled {path.name}", f"{n} rows ({method})"))

        result = {
            "success": True,
            "op": "sample_data",
            "file_path": str(path),
            "method": method,
            "total_rows": len(df),
            "sampled": n,
            "returned": len(records),
            "truncated": truncated,
            "sample": records,
            "hint": "Call apply_patch() or run_cleaning_pipeline() to act on findings.",
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
# analyze_text_column (new)
# ---------------------------------------------------------------------------


def analyze_text_column(
    file_path: str,
    column: str,
    top_n: int = 20,
) -> dict:
    """Analyze text column: length stats, word freq, pattern detection."""
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
            return {
                "success": False,
                "error": f"Column '{column}' not found",
                "hint": f"Available: {', '.join(df.columns)}",
                "progress": [fail("Column not found", column)],
                "token_estimate": 20,
            }

        if not _is_string_col(df[column]):
            return {
                "success": False,
                "error": f"Column '{column}' is not a string/text column",
                "hint": "Use a column with dtype object or string.",
                "progress": [fail("Not a text column", column)],
                "token_estimate": 20,
            }

        s = df[column]
        null_count = int(s.isna().sum())
        non_null = s.dropna()
        blank_count = int((non_null.astype(str).str.strip() == "").sum())
        unique_count = int(s.nunique())

        # Character length stats
        lengths = non_null.astype(str).str.len()
        char_stats = {
            "min": int(lengths.min()) if len(lengths) > 0 else 0,
            "max": int(lengths.max()) if len(lengths) > 0 else 0,
            "mean": round(float(lengths.mean()), 2) if len(lengths) > 0 else 0.0,
            "median": round(float(lengths.median()), 2) if len(lengths) > 0 else 0.0,
        }

        # Word frequency
        punct_re = re.compile(r"[^\w\s]")
        all_words: list[str] = []
        for txt in non_null.astype(str):
            cleaned = punct_re.sub("", txt.lower())
            all_words.extend(cleaned.split())

        from collections import Counter

        word_counts = Counter(all_words)
        word_freq = {w: c for w, c in word_counts.most_common(top_n)}

        # Pattern detection
        email_re = re.compile(r"\S+@\S+\.\S+")
        url_re = re.compile(r"https?://")
        phone_re = re.compile(r"\d{3}[-.\s]\d{3,4}[-.\s]\d{4}")
        number_re = re.compile(r"^\d+\.?\d*$")

        patterns = {
            "emails": int(non_null.astype(str).apply(lambda x: bool(email_re.search(x))).sum()),
            "urls": int(non_null.astype(str).apply(lambda x: bool(url_re.search(x))).sum()),
            "phone_numbers": int(non_null.astype(str).apply(lambda x: bool(phone_re.search(x))).sum()),
            "pure_numbers": int(non_null.astype(str).apply(lambda x: bool(number_re.match(x))).sum()),
        }

        sample = non_null.head(3).tolist()

        progress.append(ok(f"Analyzed text column '{column}'", f"{len(non_null)} non-null values"))

        result = {
            "success": True,
            "op": "analyze_text_column",
            "file_path": str(path),
            "column": column,
            "total_count": len(s),
            "null_count": null_count,
            "blank_count": blank_count,
            "unique_count": unique_count,
            "char_stats": char_stats,
            "word_freq": word_freq,
            "patterns": patterns,
            "sample": sample,
            "hint": "Call apply_patch() or run_cleaning_pipeline() to act on findings.",
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("analyze_text_column error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path is absolute, column exists, and is a text column.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }
