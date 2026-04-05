"""Reporting and aggregation tools for data_medium. No MCP imports."""
from __future__ import annotations

import logging
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
    _read_csv,
    _save_chart,
    _token_estimate,
)
from shared.file_utils import resolve_path
from shared.platform_utils import get_max_rows
from shared.progress import fail, ok, warn

logger = logging.getLogger(__name__)


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
                df[row_column],
                df[col_column],
                values=df[values_column],
                aggfunc=agg_func,
                normalize=norm,
            )
        else:
            ct = pd.crosstab(df[row_column], df[col_column], normalize=norm)

        table = {
            str(row_idx): {
                str(c): (round(float(v), 4) if pd.notna(v) else None)
                for c, v in row.items()
            }
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
            fig = go.Figure(
                go.Heatmap(
                    z=z,
                    x=col_keys,
                    y=row_keys,
                    colorscale="Blues",
                    text=[
                        [f"{v:.2f}" if isinstance(v, float) else str(v) for v in row]
                        for row in z
                    ],
                    texttemplate="%{text}",
                )
            )
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
                fig.add_trace(
                    go.Bar(x=counts, y=vals, orientation="h", name=col, showlegend=False),
                    row=1,
                    col=i + 1,
                )
            fig.update_layout(
                title=f"Value Counts — {path.name}",
                template=plotly_template(theme),
                height=400,
            )
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
# compare_datasets (new)
# ---------------------------------------------------------------------------


def compare_datasets(
    file_path_a: str,
    file_path_b: str,
    key_columns: list[str] = None,
    output_path: str = "",
) -> dict:
    """Compare two CSVs: schema diff, row counts, value changes."""
    progress = []
    try:
        path_a = resolve_path(file_path_a)
        path_b = resolve_path(file_path_b)

        for p in [path_a, path_b]:
            if not p.exists():
                return {
                    "success": False,
                    "error": f"File not found: {p.name}",
                    "hint": "Check file paths are absolute and files exist.",
                    "progress": [fail("File not found", p.name)],
                    "token_estimate": 20,
                }

        df_a = _read_csv(str(path_a))
        df_b = _read_csv(str(path_b))

        cols_a = set(df_a.columns)
        cols_b = set(df_b.columns)
        cols_only_a = sorted(cols_a - cols_b)
        cols_only_b = sorted(cols_b - cols_a)
        cols_in_both = sorted(cols_a & cols_b)

        dtype_changes = {}
        for col in cols_in_both:
            label_a = _dtype_label(df_a[col])
            label_b = _dtype_label(df_b[col])
            if label_a != label_b:
                dtype_changes[col] = {"a": label_a, "b": label_b}

        rows_a = len(df_a)
        rows_b = len(df_b)
        row_diff = rows_b - rows_a

        null_diff = {}
        mean_diff = {}
        for col in cols_in_both:
            null_a = int(df_a[col].isna().sum())
            null_b = int(df_b[col].isna().sum())
            if null_a != null_b:
                null_diff[col] = {
                    "null_count_a": null_a,
                    "null_count_b": null_b,
                    "change": null_b - null_a,
                }
            if pd.api.types.is_numeric_dtype(df_a[col]) and pd.api.types.is_numeric_dtype(df_b[col]):
                mean_a = float(df_a[col].mean()) if rows_a > 0 else 0.0
                mean_b = float(df_b[col].mean()) if rows_b > 0 else 0.0
                if abs(mean_a - mean_b) > 1e-9:
                    pct_chg = round((mean_b - mean_a) / mean_a * 100, 2) if mean_a != 0 else None
                    mean_diff[col] = {
                        "mean_a": round(mean_a, 4),
                        "mean_b": round(mean_b, 4),
                        "pct_change": pct_chg,
                    }

        dup_a = int(df_a.duplicated().sum())
        dup_b = int(df_b.duplicated().sum())

        progress.append(
            ok(
                f"Compared {path_a.name} vs {path_b.name}",
                f"{len(cols_only_a)} cols only in A, {len(cols_only_b)} only in B",
            )
        )

        result = {
            "success": True,
            "op": "compare_datasets",
            "file_a": path_a.name,
            "file_b": path_b.name,
            "rows_a": rows_a,
            "rows_b": rows_b,
            "row_diff": row_diff,
            "columns_only_in_a": cols_only_a,
            "columns_only_in_b": cols_only_b,
            "columns_in_both": cols_in_both,
            "dtype_changes": dtype_changes,
            "null_diff": null_diff,
            "mean_diff": mean_diff,
            "duplicates_a": dup_a,
            "duplicates_b": dup_b,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("compare_datasets error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check both file paths are absolute and point to valid CSV files.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }
