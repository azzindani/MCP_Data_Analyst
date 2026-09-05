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

try:
    from scipy import stats as _scipy_stats

    _SCIPY_OK = True
except ImportError:
    _scipy_stats = None  # type: ignore
    _SCIPY_OK = False

from _med_helpers import (
    _dtype_label,
    _is_string_col,
    _open_file,
    _read_csv,
    _save_chart,
    _token_estimate,
    is_numeric_col,
)

from shared.column_utils import (
    DATE_MATCH_THRESHOLD,
    column_pair_mask,
    condition_column,
    filter_operand_error,
    missing_column_error,
    parse_dates,
    type_sample,
)
from shared.counts import counted
from shared.file_utils import count_data_rows as _count_data_rows
from shared.file_utils import error_text, hint_for_error, resolve_path
from shared.insights import from_outliers, write_insights
from shared.platform_utils import get_max_results, get_max_rows
from shared.progress import fail, info, ok, warn
from shared.receipt import append_receipt
from shared.small_sample import MIN_N_IQR, MIN_N_SHAPIRO, finite, min_n_for_zscore, rounded, shapiro_p
from shared.value_alias import render_valid
from shared.value_alias import resolve as resolve_op
from shared.version_control import drop_snapshot_if_unwritten, snapshot

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
    theme: str = "device",
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
        undetermined_cols: set[str] = set()
        min_n_std = min_n_for_zscore(3.0)
        # "Flags anomalous rows" is the last clause of this tool's docstring and
        # nothing row-level was ever produced: counts per column, limits per
        # column, and a box plot. A caller asked for the flagged rows by giving
        # output_path=outliers.csv and got an HTML chart. The masks exist inside
        # this loop already -- keeping them is what makes the sentence true.
        flags: dict[str, pd.Series] = {}
        for col in numeric_cols:
            clean = df[col].dropna()
            if len(clean) == 0:
                continue
            r: dict = {"n": int(len(clean))}
            if method in ("iqr", "both"):
                # Below four values the 1.5*IQR fence always lands outside the
                # sample, whatever the values are -- see MIN_N_IQR. "0 outliers"
                # there states a property of the row count, dressed as a finding
                # about the data.
                if len(clean) < MIN_N_IQR:
                    r["has_outliers_iqr"] = None
                    r["outlier_count_iqr"] = None
                    r["iqr_status"] = (
                        f"undetermined at n={len(clean)}: the 1.5*IQR fence cannot fall inside a sample "
                        f"smaller than {MIN_N_IQR}, so no value could have been flagged"
                    )
                    undetermined_cols.add(col)
                else:
                    q1 = float(clean.quantile(th1))
                    q3 = float(clean.quantile(th3))
                    iqr = q3 - q1
                    lower = q1 - 1.5 * iqr
                    upper = q3 + 1.5 * iqr
                    hit = (clean < lower) | (clean > upper)
                    count = int(hit.sum())
                    if count:
                        flags.setdefault(col, pd.Series(False, index=df.index)).loc[clean.index] |= hit
                    r["has_outliers_iqr"] = count > 0
                    r["outlier_count_iqr"] = count
                    r["lower_limit_iqr"] = round(lower, 4)
                    r["upper_limit_iqr"] = round(upper, 4)
                    if iqr == 0:
                        # Enough rows but no spread: zero is a real answer, and
                        # the bounds still sit on the data rather than around it.
                        r["iqr_status"] = "zero spread: q1 == q3, so the fence has no width"
                    if count > 0:
                        cols_with_outliers += 1

            if method in ("std", "both"):
                # The largest z-score any of n points can reach is (n-1)/sqrt(n),
                # which first exceeds 3 at n=11. Below that a 3-sigma scan is
                # guaranteed to find nothing.
                if len(clean) < min_n_std:
                    r["has_outliers_std"] = None
                    r["outlier_count_std"] = None
                    r["std_status"] = (
                        f"undetermined at n={len(clean)}: the largest z-score attainable by any of n points "
                        f"is (n-1)/sqrt(n), which first exceeds 3 at n={min_n_std}"
                    )
                    undetermined_cols.add(col)
                else:
                    mean_v = float(clean.mean())
                    std_v = float(clean.std())
                    lower_s = mean_v - 3 * std_v
                    upper_s = mean_v + 3 * std_v
                    hit_s = (clean < lower_s) | (clean > upper_s)
                    count_s = int(hit_s.sum())
                    if count_s:
                        flags.setdefault(col, pd.Series(False, index=df.index)).loc[clean.index] |= hit_s
                    r["has_outliers_std"] = count_s > 0
                    r["outlier_count_std"] = count_s
                    r["lower_limit_std"] = round(lower_s, 4)
                    r["upper_limit_std"] = round(upper_s, 4)
                    if std_v == 0:
                        r["std_status"] = "zero spread: every value is identical, so the bounds have no width"
                    if count_s > 0 and method == "std":
                        cols_with_outliers += 1

            results[col] = r

        max_r = get_max_results()
        # Taken before the cut. `scanned_columns` below used to be len(results)
        # after it, so a 40-column scan reported "scanned_columns": 20 -- a
        # count of what survived the slice, under a name that claims to say how
        # much work was done.
        total_scanned = len(results)
        if total_scanned > max_r:
            keys = list(results.keys())[:max_r]
            results = {k: results[k] for k in keys}
            progress.append(warn("Results truncated", f"Showing first {max_r} of {total_scanned} columns"))

        undetermined_shown = sorted(undetermined_cols & set(results))
        if undetermined_shown:
            progress.append(
                warn(
                    "Sample too small to detect outliers",
                    f"{len(undetermined_shown)} column(s) undetermined: {', '.join(undetermined_shown)}",
                )
            )
        progress.append(
            ok(
                f"Checked outliers in {path.name}",
                f"{len(results)} columns scanned, {cols_with_outliers} with outliers",
            )
        )

        hint = "Call apply_patch() with op=cap_outliers or run_cleaning_pipeline() to act on findings."
        if undetermined_shown and not cols_with_outliers:
            hint = (
                "No column had enough rows for an outlier verdict, so there is nothing to act on. "
                "columns_undetermined lists them; each carries the n it had."
            )
        result: dict = {
            "success": True,
            "op": "check_outliers",
            "file_path": str(path),
            "method": method,
            "scanned_columns": total_scanned,
            "columns_with_outliers": cols_with_outliers,
            "columns_undetermined": undetermined_shown,
            "results": results,
            **counted(len(results), total_scanned),
            "hint": hint,
            "progress": progress,
        }

        # The rows themselves, bounded, each saying which columns flagged it.
        # A count tells a caller how many there are; this tells them which.
        flagged_rows: list[dict] = []
        flagged_total = 0
        if flags:
            per_row: dict = {}
            for col, mask in flags.items():
                for idx in df.index[mask]:
                    per_row.setdefault(int(idx), []).append(col)
            flagged_total = len(per_row)
            for idx in sorted(per_row)[:max_r]:
                flagged_rows.append({"row": idx, "columns": sorted(per_row[idx])})
        result["flagged_rows"] = flagged_rows
        result["flagged_rows_total"] = flagged_total
        result["flagged_rows_truncated"] = flagged_total > len(flagged_rows)
        if flagged_total:
            shown = (
                f"{len(flagged_rows)} of {flagged_total}" if flagged_total > len(flagged_rows) else str(flagged_total)
            )
            progress.append(ok(f"Flagged {flagged_total} anomalous row(s)", f"listing {shown}"))
        if flagged_total > len(flagged_rows):
            # detect_anomalies() goes first because it is the only tool that
            # puts the whole flag list in a file, and getting the rows out is
            # what a caller passing output_path=outliers.csv is after. This
            # tool's output_path is its chart, so that caller got an HTML box
            # plot and a hint naming two tools that both make them rebuild the
            # outlier condition by hand. Round 16 followed that path to the end
            # and wrote: "the 3373 flagged rows exist ONLY in the reply text".
            result["hint"] = (
                f"{flagged_total} row(s) were flagged and the first {len(flagged_rows)} are listed. "
                "detect_anomalies() writes every flagged row to a CSV -- this tool's output_path is "
                "its chart, not the rows. Or use filter_rows() on the columns named here to page "
                "through the rest, or apply_patch() with op=cap_outliers to act on all of them."
            )

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
            abs_p, fname = _save_chart(fig, output_path, "outliers", path, open_after, theme, progress)
            result["output_path"] = abs_p
            result["output_name"] = fname
            progress.append(ok("Chart saved", fname))

            # A box plot per column says where the fences are. It does not say
            # which column is worth acting on, which is the question a caller
            # asked by running this at all.
            summary_rows = [
                {
                    "column": col,
                    "outlier_count": r.get("outlier_count_iqr") or 0,
                    "lower_limit": r.get("iqr_lower"),
                    "upper_limit": r.get("iqr_upper"),
                }
                for col, r in results.items()
            ]
            found = from_outliers(summary_rows, len(df))
            result["insights"] = found
            result["insights_path"] = write_insights(
                abs_p, found, op="check_outliers", source=path.name, source_path=str(path), extra={"method": method}
            )
        elif not _PLOTLY_AVAILABLE:
            progress.append(warn("plotly not installed", "pip install plotly to enable HTML export"))

        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("check_outliers error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check file_path is absolute and the file is a valid CSV."),
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
    theme: str = "device",
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
            abs_p, fname = _save_chart(fig, output_path, "nulls_zeros", path, open_after, theme, progress)
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
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check file_path is absolute and the file is a valid CSV."),
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
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check file_path is absolute and the file is a valid CSV."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# auto_detect_schema
# ---------------------------------------------------------------------------


# One threshold for every string-column type guess, and one sampler, both from
# shared/column_utils so the four date-detection sites in this repo cannot come
# apart again. Aliased rather than re-exported so the local reads stay short.
_TYPE_MATCH_THRESHOLD = DATE_MATCH_THRESHOLD
_type_sample = type_sample


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
        # Everything below is inferred from the sample, and `current_dtype` in
        # particular is a fact about the sample presented as a fact about the
        # column. On the reference dataset the first null in link_clicks is at
        # row 2,011, so a 1,000-row sample sees a gapless integer column and
        # reports int64 -- while the file is float64 with 546 nulls, which is
        # what inspect_dataset and validate_dataset say about the same column
        # on the same server. The low cardinality of those first rows also made
        # it "category_encoded". Neither reading is wrong about what was read;
        # the response just never said how little that was.
        total_rows = _count_data_rows(path)
        sampled = total_rows > len(df)
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
                # One rule for both types, and it is the repo's own.
                #
                # This used to be `pd.to_datetime(head(50), errors="raise")`:
                # all-or-nothing, on the first fifty values, with no dayfirst
                # handling -- while the numeric branch below already accepted a
                # 90% match on the same sample. So three DD-MM-YYYY columns of
                # one file came back datetime with a cast suggestion and a
                # fourth, identically formatted, came back text with none. One
                # unparseable value among the first fifty was the whole
                # difference, and nothing in the response said so.
                #
                # `parse_dates` is what every other date-aware tool here uses:
                # format="mixed", errors="coerce", and an orientation chosen
                # from the data rather than assumed.
                candidates = _type_sample(s)
                if len(candidates):
                    parsed, date_meta = parse_dates(candidates)
                    date_match = float(parsed.notna().mean())
                    if date_match >= _TYPE_MATCH_THRESHOLD:
                        info_entry["inferred_type"] = "datetime"
                        info_entry["match_rate"] = round(date_match, 3)
                        info_entry["dayfirst"] = date_meta["dayfirst"]
                        info_entry["suggestion"] = f"cast_column col={col} dtype=datetime"
                        suggestions.append(info_entry["suggestion"])
                        # The repo's rule is that an ambiguous orientation is
                        # surfaced, never swallowed.
                        if date_meta.get("ambiguous"):
                            info_entry["dayfirst_ambiguous"] = date_meta["reason"]

                if info_entry["inferred_type"] is None and len(candidates):
                    numeric_try = pd.to_numeric(candidates, errors="coerce")
                    numeric_match = float(numeric_try.notna().mean())
                    if numeric_match >= _TYPE_MATCH_THRESHOLD:
                        info_entry["inferred_type"] = "numeric"
                        info_entry["match_rate"] = round(numeric_match, 3)
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
            "rows_sampled": len(df),
            "total_rows": total_rows,
            "inferred_from_sample": sampled,
            "columns": column_info,
            "suggestions": suggestions,
            "hint": (
                f"Types come from the first {len(df):,} of {total_rows:,} rows; a dtype or null "
                "count for the whole column comes from inspect_dataset(). Raise max_rows to widen "
                "the sample, or call apply_patch() / run_cleaning_pipeline() to act on findings."
                if sampled
                else "Call apply_patch() or run_cleaning_pipeline() to act on findings."
            ),
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("auto_detect_schema error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check file_path is absolute and the file is a valid CSV."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# _apply_condition (helper for filter_rows)
# ---------------------------------------------------------------------------


def _apply_condition(df: pd.DataFrame, cond: dict) -> pd.Series:
    """Return boolean mask for a single condition dict."""
    col = condition_column(cond)
    op = cond.get("op", "") or cond.get("operator", "")
    val = cond.get("value")
    s = df[col]
    # One table, shared with data_transform. This server used to spell them
    # `startswith`/`endswith` and that one `starts_with`/`ends_with`, so a
    # caller taught by either was refused by the other, and `not_contains`
    # reached only one of them. Resolving here accepts both spellings, and
    # `==` besides.
    op = resolve_op(op, field="filter op")
    # --- column against column ---
    pair = column_pair_mask(df, cond, col, op)
    if pair is not None:
        return pair
    # --- original ops ---
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
    # --- new ops ---
    if op == "isin":
        # accept "values" key (list) or "value" key (single or list)
        values = cond.get("values", val if isinstance(val, list) else [val])
        return s.isin(values)
    if op == "not_isin":
        values = cond.get("values", val if isinstance(val, list) else [val])
        return ~s.isin(values)
    if op == "between":
        # The fallback to `val` covers {"min": ..., "max": ...} being written as
        # a single value, but the obvious way to give a range one key is a pair
        # -- {"op": "between", "value": [0, 10000]} -- and float() on that list
        # raised "float() argument must be a string or a real number, not
        # 'list'", which names nothing the caller wrote.
        if isinstance(val, list | tuple) and len(val) == 2:
            min_v, max_v = val
        else:
            min_v = cond.get("min", val)
            max_v = cond.get("max", val)
        try:
            low, high = float(min_v), float(max_v)
        except TypeError, ValueError:
            raise ValueError(
                f"Filter op 'between' needs numeric bounds. Got keys: {sorted(cond)}. "
                "Write it as {'column': ..., 'op': 'between', 'min': 0, 'max': 100} "
                "or pass value as a two-item list."
            ) from None
        return pd.to_numeric(s, errors="coerce").between(low, high)
    if op == "date_range":
        start = cond.get("start")
        end = cond.get("end")
        parsed = pd.to_datetime(s, errors="coerce")
        mask = pd.Series([True] * len(df), index=df.index)
        if start:
            mask &= parsed >= pd.Timestamp(start)
        if end:
            mask &= parsed <= pd.Timestamp(end)
        return mask
    if op == "regex":
        import re

        # accept "pattern" key or fall back to "value"
        pattern = str(cond.get("pattern", val))
        try:
            re.compile(pattern)
        except re.error as exc:
            raise ValueError(f"Invalid regex: {exc}")
        return s.astype(str).str.contains(pattern, regex=True, na=False)
    if op == "quantile_between":
        min_q = cond.get("min_q", 0.0)
        max_q = cond.get("max_q", 1.0)
        numeric_s = pd.to_numeric(s, errors="coerce")
        q_low = float(numeric_s.quantile(min_q))
        q_high = float(numeric_s.quantile(max_q))
        return numeric_s.between(q_low, q_high)
    if op == "starts_with":
        return s.astype(str).str.startswith(str(val), na=False)
    if op == "ends_with":
        return s.astype(str).str.endswith(str(val), na=False)
    if op == "not_contains":
        # Present in data_transform since it shipped, never here. Same table
        # now names it, so this server has to be able to answer it.
        return ~s.astype(str).str.contains(str(val), case=False, na=False)
    # Unreachable for a resolvable op -- `resolve_op` above raises first, with
    # the valid list and a did_you_mean. Kept so a newly added canonical op
    # that nobody wired up here fails loudly instead of silently matching all.
    raise ValueError(f"Filter op {op!r} is known but not implemented here. Valid: {render_valid()}")


# ---------------------------------------------------------------------------
# filter_rows
# ---------------------------------------------------------------------------


def filter_rows(
    file_path: str,
    conditions: list[dict],
    output_path: str = "",
    dry_run: bool = False,
    open_after: bool = True,
    sort_by: list[str] = None,
    sort_ascending: list[bool] = None,
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
                "hint": "Provide conditions list with column, op (or operator), value keys.",
                "progress": [fail("No conditions", "")],
                "token_estimate": 20,
            }

        df = _read_csv(str(path))

        for i, cond in enumerate(conditions):
            col = condition_column(cond)
            if not col:
                error, hint = missing_column_error(cond)
                return {
                    "success": False,
                    "error": error,
                    "hint": hint,
                    "progress": [fail("Condition names no column", ", ".join(str(k) for k in cond))],
                    "token_estimate": 20,
                }
            # A condition with no operand used to compare every value against
            # None: nothing matched, and the tool wrote an empty file under
            # `success: true, rows_kept: 0`. Refuse it here, where the column
            # check already happens and nothing has been written yet.
            operand_error = filter_operand_error(cond, cond.get("op", "") or cond.get("operator", ""), i)
            if operand_error:
                return {
                    "success": False,
                    "error": operand_error,
                    "hint": "Fix the condition above and retry. Nothing was written.",
                    "progress": [fail("Incomplete condition", f"condition {i}")],
                    "token_estimate": 20,
                }
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

        filtered = df[mask].reset_index(drop=True)

        # Apply sorting after filtering
        if sort_by:
            missing_sort = [c for c in sort_by if c not in filtered.columns]
            if missing_sort:
                return {
                    "success": False,
                    "error": f"sort_by columns not found: {missing_sort}",
                    "hint": f"Available: {', '.join(filtered.columns)}",
                    "progress": [fail("Sort column not found", str(missing_sort))],
                    "token_estimate": 20,
                }
            asc = sort_ascending if sort_ascending else [True] * len(sort_by)
            filtered = filtered.sort_values(by=sort_by, ascending=asc).reset_index(drop=True)

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

        if output_path:
            out = resolve_path(output_path)
        else:
            out = path.parent / f"{path.stem}_filtered{path.suffix}"
        backup = snapshot(str(path)) if out == path else None
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
            "output_path": str(out),
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
            "error": error_text(exc),
            "hint": f"Valid ops: {render_valid()}. Error: {exc}",
            "backup": drop_snapshot_if_unwritten(backup, path),
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
    top_n: int = 0,
) -> dict:
    progress = []
    # Five sibling tools call "how many rows" top_n; this is the only n.
    if top_n:
        n = top_n
        progress.append(info("Argument alias", "Read n from an accepted alternative spelling"))
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

        # `max_r` was fetched here and then never used: a hardcoded 20 did the
        # cutting and computed the flag. So constrained mode, which exists to
        # make responses smaller, shrank this response by nothing at all.
        max_r = get_max_rows()
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
            # `total` here is the size of the sample that was drawn, not the
            # file: the sample is on disk when output_path was given, and this
            # count says how much of *it* is in the response.
            **counted(len(records), len(sample)),
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
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check file_path is absolute and the file is a valid CSV."),
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

        sample = [str(v)[:80] for v in non_null.head(2).tolist()]

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
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check file_path is absolute, column exists, and is a text column."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# extended_stats
# ---------------------------------------------------------------------------


def extended_stats(
    file_path: str,
    columns: list[str] = None,
    percentiles: list[float] = None,
    compute_ci: bool = True,
    ci_level: float = 0.95,
) -> dict:
    progress = []
    _scipy_ok = _SCIPY_OK
    scipy_stats = _scipy_stats

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
        # The default list is whole percentages, so the code divided by 100 and
        # keyed on int(p). A caller passing pandas' convention -- [0.25, 0.5,
        # 0.75] -- therefore got int() of each, which is 0 for all three: one
        # key "p0" holding the 0.75th percentile, while percentiles_computed
        # echoed the request back as if all three had been honoured.
        #
        # Both conventions are reasonable and they are distinguishable: a list
        # whose values are all <= 1 can only be fractions, because p1 through
        # p100 as percentages are never all <= 1 unless the caller asked for
        # the 1st percentile alone, where the two readings agree to within the
        # 1st percentile itself. Keys are formatted so 2.5 and 25 stay
        # different names.
        pcts = list(percentiles) if percentiles else [5, 10, 25, 50, 75, 90, 95, 99]
        bad = [p for p in pcts if not 0 <= float(p) <= 100]
        if bad:
            return {
                "success": False,
                "error": f"Percentiles out of range: {bad}",
                "hint": "Give percentiles as 0-100 (25, 50, 75) or as fractions (0.25, 0.5, 0.75).",
                "progress": [fail("Invalid percentiles", str(bad))],
                "token_estimate": 20,
            }
        as_fractions = bool(pcts) and all(float(p) <= 1 for p in pcts)
        if as_fractions:
            pcts = [float(p) * 100 for p in pcts]
        pcts = sorted({float(p) for p in pcts})
        target_cols = columns or [c for c in df.columns if is_numeric_col(df[c])]

        missing = [c for c in target_cols if c not in df.columns]
        if missing:
            return {
                "success": False,
                "error": f"Columns not found: {missing}",
                "hint": f"Available: {', '.join(df.columns)}",
                "progress": [fail("Columns not found", str(missing))],
                "token_estimate": 20,
            }

        stats_out: dict = {}
        for col in target_cols:
            series = df[col].dropna()
            if not is_numeric_col(df[col]) or len(series) == 0:
                continue

            n = len(series)
            mean_val = float(series.mean())
            std_val = float(series.std())
            median_val = float(series.median())

            # Skewness & kurtosis. Both are NaN below n=3 (skew) and n=4
            # (kurtosis), and NaN fails every comparison in an if/elif chain, so
            # the chain fell through to its `else` and called a single row
            # "approximately symmetric" with "approximately normal tails" -- two
            # confident shape descriptions sitting beside the honest `null`s
            # that the same row produced for std and variance. The `finite`
            # check goes first, where a fall-through cannot reach past it.
            skew = float(series.skew())
            kurt = float(series.kurtosis())

            if finite(skew) is None:
                skew_label = None
            elif skew > 1:
                skew_label = "strongly right-skewed"
            elif skew > 0.5:
                skew_label = "moderately right-skewed"
            elif skew < -1:
                skew_label = "strongly left-skewed"
            elif skew < -0.5:
                skew_label = "moderately left-skewed"
            else:
                skew_label = "approximately symmetric"

            if finite(kurt) is None:
                kurt_label = None
            elif kurt > 3:
                kurt_label = "leptokurtic (heavy tails)"
            elif kurt < -1:
                kurt_label = "platykurtic (light tails)"
            else:
                kurt_label = "approximately normal tails"

            # Percentiles
            pct_vals = {f"p{p:g}": round(float(series.quantile(p / 100)), 4) for p in pcts}

            # Coefficient of variation. `rounded`, not `round`, because it is
            # std over mean and std is NaN below two values -- so a column with
            # a non-zero mean produced NaN here and carried it all the way into
            # the response as the bare JSON token. The zero-mean guard beside it
            # hid how narrow the escape was: `spends` has mean 0 and came back
            # null, while `impressions` had mean 2 and came back NaN.
            cv = rounded(std_val / mean_val) if mean_val != 0 else None

            # CI for the mean (t-distribution)
            ci = None
            if compute_ci and _scipy_ok and n >= 2:
                sem = scipy_stats.sem(series)
                t_crit = scipy_stats.t.ppf((1 + ci_level) / 2, df=n - 1)
                ci = {
                    "level": ci_level,
                    "lower": round(mean_val - t_crit * sem, 4),
                    "upper": round(mean_val + t_crit * sem, 4),
                }

            # MAD (median absolute deviation)
            mad = float((series - median_val).abs().median())

            # Distribution shape hint. Shapiro-Wilk needs three values; below
            # that scipy raises rather than returning NaN, and `nan > 0.05` is
            # False either way -- so a single row used to be reported as
            # "non-normal (Shapiro p<0.05)", a test result for a test that
            # cannot be run on one number.
            if not _scipy_ok:
                shape_hint = "install scipy for distribution test"
            else:
                p_norm = shapiro_p(series.to_numpy(), scipy_stats)
                if p_norm is None:
                    shape_hint = (
                        f"undetermined: Shapiro-Wilk needs at least {MIN_N_SHAPIRO} values, this column has {n}"
                    )
                elif p_norm > 0.05:
                    shape_hint = f"likely normal (Shapiro p>{p_norm:.2f})"
                else:
                    shape_hint = "non-normal (Shapiro p<0.05)"

            stats_out[col] = {
                "n": n,
                "null_count": int(df[col].isna().sum()),
                "mean": rounded(mean_val),
                "median": rounded(median_val),
                # std/variance/skew/kurtosis are undefined at the small n each
                # needs. `round(nan, 4)` is still NaN, which json.dumps writes as
                # the bare token NaN -- not valid JSON, and read as a number by
                # some clients. None says "not computed" in a way JSON can carry.
                "std": rounded(std_val),
                "variance": rounded(float(series.var())),
                "mad": rounded(mad),
                "min": round(float(series.min()), 4),
                "max": round(float(series.max()), 4),
                "range": round(float(series.max() - series.min()), 4),
                "iqr": round(float(series.quantile(0.75) - series.quantile(0.25)), 4),
                "cv": cv,
                "skewness": rounded(skew),
                "skewness_label": skew_label,
                "kurtosis": rounded(kurt),
                "kurtosis_label": kurt_label,
                "percentiles": pct_vals,
                "confidence_interval": ci,
                "distribution_hint": shape_hint,
            }

        if not stats_out:
            progress.append(warn("No numeric columns found", "Pass numeric column names via 'columns' param"))
        else:
            progress.append(ok(f"Extended stats for {path.name}", f"{len(stats_out)} numeric columns analysed"))

        result = {
            "success": True,
            "op": "extended_stats",
            "file_path": str(path),
            "columns_analysed": list(stats_out.keys()),
            "stats": stats_out,
            "percentiles_computed": pcts,
            "ci_level": ci_level,
            "hint": "Use apply_patch() with log_transform or bin_column to act on distribution findings."
            if stats_out
            else "Call inspect_dataset() to find numeric column names, then pass them via columns param.",
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("extended_stats error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check file_path is absolute and columns are numeric."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }
