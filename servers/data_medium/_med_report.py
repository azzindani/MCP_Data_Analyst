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

import numpy as np
import pandas as pd

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    from shared.html_theme import calc_chart_height, plotly_template

    _PLOTLY_AVAILABLE = True
except ImportError:
    _PLOTLY_AVAILABLE = False

from _med_helpers import (
    _dtype_label,
    _read_csv,
    _save_chart,
    _token_estimate,
)

from shared.arg_alias import missing as missing_arg
from shared.arg_alias import pick
from shared.choice import AGG_ALIASES, AGG_FUNCS, UnknownChoice, normalize_mode
from shared.choice import refusal as choice_refusal
from shared.choice import resolve as resolve_choice
from shared.counts import counted
from shared.file_utils import error_text, hint_for_error, resolve_path
from shared.html_layout import discriminated_suffix
from shared.insights import from_crosstab, write_insights
from shared.platform_utils import get_max_rows
from shared.progress import fail, info, ok, warn

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

        # This line used to read `normalize if normalize in (...) else False`,
        # which turned a typo into "do not normalise" and then echoed the
        # caller's word back in the response -- a crosstab that was never
        # normalised reporting that it was. Refuse instead, and name the set.
        try:
            norm = normalize_mode(normalize)
        except UnknownChoice as exc:
            return choice_refusal("cross_tabulate", exc)

        if values_column:
            # Reaches pd.crosstab(aggfunc=...) directly, so an unvalidated typo
            # came back as pandas' own complaint. The no-values branch below
            # cannot use agg_func at all and says so instead.
            try:
                agg_func = resolve_choice(agg_func, AGG_FUNCS, field="agg_func", aliases=AGG_ALIASES)
            except UnknownChoice as exc:
                return choice_refusal("cross_tabulate", exc)
            ct = pd.crosstab(
                df[row_column],
                df[col_column],
                values=df[values_column],
                aggfunc=agg_func,
                normalize=norm,
            )
        else:
            # `aggfunc` has no meaning without `values`: pandas counts, and
            # `agg_func` is dropped whole. Saying so is the difference between
            # a caller who asked for a mean and got counts, and one who knows.
            if agg_func and agg_func != "count":
                progress.append(
                    warn(
                        f"agg_func={agg_func!r} was not applied",
                        "cross_tabulate can only aggregate with values_column=; without it the "
                        "table counts rows. Pass values_column to aggregate that column instead.",
                    )
                )
            agg_func = "count"
            ct = pd.crosstab(df[row_column], df[col_column], normalize=norm)

        table = {
            str(row_idx): {str(c): (round(float(v), 4) if pd.notna(v) else None) for c, v in row.items()}
            for row_idx, row in ct.to_dict(orient="index").items()
        }

        # Kept whole for the insights below: the findings are about the data,
        # not about the page of it that fits in a response.
        full_table = table
        max_r = get_max_rows()
        total_table_rows = len(table)
        if total_table_rows > max_r:
            keys = list(table.keys())[:max_r]
            table = {k: table[k] for k in keys}
            progress.append(warn("Results truncated", f"Showing first {max_r} of {total_table_rows} rows"))

        progress.append(ok(f"Cross-tabulated {path.name}", f"{row_column} × {col_column}"))

        result: dict = {
            "success": True,
            "op": "cross_tabulate",
            "file_path": str(path),
            "row_column": row_column,
            "col_column": col_column,
            "agg_func": agg_func if values_column else "count",
            # The value USED, not the value sent. These differ whenever an
            # alias resolved ("rows" -> "index"), and used to differ silently
            # whenever the value was not understood at all.
            "normalize": norm,
            "rows": len(ct),
            "cols": len(ct.columns),
            **counted(len(table), total_table_rows),
            "table": table,
            "hint": "Use a more targeted call with specific row_column or col_column filters.",
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
                    text=[[f"{v:.2f}" if isinstance(v, float) else str(v) for v in row] for row in z],
                    texttemplate="%{text}",
                )
            )
            fig.update_layout(
                title=f"Cross-Tabulation: {row_column} × {col_column}",
                xaxis_title=col_column,
                yaxis_title=row_column,
                template=plotly_template(theme),
                height=calc_chart_height(len(row_keys), mode="heatmap"),
            )
            # grade x purpose and purpose x grade are different tables of the
            # same file, and they used to be the same filename.
            stem = discriminated_suffix("crosstab", row_column, col_column, values_column, agg_func, norm)
            abs_p, fname = _save_chart(fig, output_path, stem, path, open_after, theme, progress)
            result["output_path"] = abs_p
            result["output_name"] = fname
            progress.append(ok("Chart saved", fname))

            # A cross-tab is a table of counts, and the question a reader has is
            # "which combination is unusual". Answering it means comparing each
            # cell against the product of its margins -- arithmetic the table
            # contains and does not do. Computed from the full `table`, not the
            # page of it above, so a truncated response still gets every finding.
            found = from_crosstab(full_table, row_column, col_column)
            result["insights"] = found
            result["insights_path"] = write_insights(
                abs_p,
                found,
                op="cross_tabulate",
                source=path.name,
                source_path=str(path),
                extra={"row_column": row_column, "col_column": col_column},
            )
        else:
            progress.append(warn("plotly not installed", "pip install plotly to enable HTML export"))

        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("cross_tabulate error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check file_path and column names are correct."),
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
        # Unvalidated, this reached pandas and came back as "'typo' is not a
        # valid function for 'DataFrameGroupBy' object" under the generic
        # handler's hint -- "Check file_path and column names" -- which names
        # the two arguments that were not the problem. Same table as
        # compute_aggregations, so the siblings answer with the same list.
        try:
            agg_func = resolve_choice(agg_func, AGG_FUNCS, field="agg_func", aliases=AGG_ALIASES)
        except UnknownChoice as exc:
            return choice_refusal("pivot_table", exc)

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

        # Without `values`, pandas pivots every remaining column -- and
        # aggfunc="sum" over a text column concatenates it. Pivoting the real
        # 16,834-row file by one index returned two rows whose cells held
        # "ConversionsConversionsConversions..." 19,063 characters long: 1.32 MB,
        # 330,195 tokens, under success: true, from a server built for a
        # 12,000-token context. A pivot's values are its measures, so default to
        # the columns that can actually be aggregated.
        chosen_values = list(values) if values else None
        if not chosen_values:
            spoken_for = set(index or []) | set(columns or [])
            numeric = [c for c in df.select_dtypes("number").columns if c not in spoken_for]
            if not numeric:
                return {
                    "success": False,
                    "error": "No numeric column to aggregate",
                    "hint": (f"Pass values=['col'] naming what to aggregate. Columns here: {', '.join(df.columns)}"),
                    "progress": [fail("Nothing to aggregate", "no numeric columns")],
                    "token_estimate": 20,
                }
            chosen_values = numeric
            skipped = [c for c in df.columns if c not in numeric and c not in spoken_for]
            if skipped:
                progress.append(
                    warn(
                        f"Aggregating the {len(numeric)} numeric column(s) only",
                        f"not text: {', '.join(skipped[:6])}" + (" ..." if len(skipped) > 6 else ""),
                    )
                )

        pt = pd.pivot_table(
            df,
            index=index,
            columns=columns if columns else None,
            values=chosen_values,
            aggfunc=agg_func,
            fill_value=fill_value,
        )

        if isinstance(pt.columns, pd.MultiIndex):
            pt.columns = ["_".join(str(c) for c in col).strip("_") for col in pt.columns]

        pt = pt.reset_index()
        # One cap, used for both the slice and the flag. These were two numbers
        # -- head(10) against `len(pt) > get_max_rows()` -- so a 20-row pivot
        # returned 10 rows and reported truncated: False, and a 257-row one
        # warned "Showing first 100 rows" having returned 10.
        max_r = get_max_rows()
        records = pt.head(max_r).fillna("").to_dict(orient="records")
        if len(records) < len(pt):
            progress.append(warn("Results truncated", f"Showing first {len(records)} of {len(pt)} rows"))

        progress.append(ok(f"Pivot table for {path.name}", f"{len(records)} rows"))

        result = {
            "success": True,
            "op": "pivot_table",
            "file_path": str(path),
            "index": index,
            "columns": columns,
            "values": chosen_values,
            "agg_func": agg_func,
            "rows": len(pt),
            **counted(len(records), len(pt)),
            "result": records,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("pivot_table error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check file_path and column names. values must be numeric for most agg_funcs."),
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
                    {"value": str(v), "count": int(c), "pct": round(c / total * 100, 2)} for v, c in vc.items()
                ]
            else:
                results[col] = [{"value": str(v), "count": int(c)} for v, c in vc.items()]

        progress.append(ok(f"Value counts for {path.name}", f"{len(columns)} columns"))

        result: dict = {
            "success": True,
            "op": "value_counts",
            "file_path": str(path),
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
                # value_counts() is descending; reverse so highest is at top of h-bar
                vals = [e["value"] for e in reversed(entries)]
                counts = [e["count"] for e in reversed(entries)]
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
            stem = discriminated_suffix("value_counts", *columns)
            abs_p, fname = _save_chart(fig, output_path, stem, path, open_after, theme, progress)
            result["output_path"] = abs_p
            result["output_name"] = fname
            progress.append(ok("Chart saved", fname))
        else:
            progress.append(warn("plotly not installed", "pip install plotly to enable HTML export"))

        # Trim response: keep top 5 per column (HTML chart has full data)
        result["results"] = {col: entries[:5] for col, entries in result["results"].items()}
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("value_counts error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check file_path and column names are correct."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# compare_datasets (new)
# ---------------------------------------------------------------------------


def _keyed_diff(df_a, df_b, key_columns: list[str], limit: int) -> dict:
    """Row-level differences between two frames joined on a key.

    `key_columns` was declared on the tool, forwarded through the wrapper, and
    read nowhere -- so "value changes" in the docstring meant column means, and
    a caller who edited one cell saw nothing. Round 11 changed three cells in a
    16,834-row copy and the response reported only the two numeric means that
    shifted; the edit to a text column was invisible.
    """
    missing_a = [c for c in key_columns if c not in df_a.columns]
    missing_b = [c for c in key_columns if c not in df_b.columns]
    if missing_a or missing_b:
        return {"error": f"Key columns missing — A: {missing_a}, B: {missing_b}"}

    left = df_a.set_index(key_columns, drop=False)
    right = df_b.set_index(key_columns, drop=False)
    if left.index.has_duplicates or right.index.has_duplicates:
        return {
            "error": "Key columns are not unique; a keyed comparison needs one row per key.",
            "duplicate_keys_a": int(left.index.duplicated().sum()),
            "duplicate_keys_b": int(right.index.duplicated().sum()),
        }

    keys_a, keys_b = set(left.index), set(right.index)
    added = sorted(keys_b - keys_a, key=str)
    removed = sorted(keys_a - keys_b, key=str)
    common = keys_a & keys_b

    shared_cols = [c for c in df_a.columns if c in df_b.columns and c not in key_columns]
    changed: list[dict] = []
    changed_by_column: dict[str, int] = {}
    if common:
        common_idx = [k for k in left.index if k in common]
        la, rb = left.loc[common_idx, shared_cols], right.loc[common_idx, shared_cols]
        # NaN != NaN, so compare the null masks separately or every null row
        # reads as a change.
        differs = (la != rb) & ~(la.isna() & rb.isna())
        for col in shared_cols:
            n = int(differs[col].sum())
            if n:
                changed_by_column[col] = n
        rows_with_change = differs.any(axis=1)
        for key in la.index[rows_with_change][:limit]:
            row = {"key": key if not isinstance(key, tuple) else list(key), "changes": {}}
            for col in shared_cols:
                if bool(differs.loc[key, col]):
                    row["changes"][col] = {"a": _cell(la.loc[key, col]), "b": _cell(rb.loc[key, col])}
            changed.append(row)
        changed_rows = int(rows_with_change.sum())
    else:
        changed_rows = 0

    return {
        "key_columns": key_columns,
        "rows_matched": len(common),
        "rows_added": len(added),
        "rows_removed": len(removed),
        "rows_changed": changed_rows,
        "changed_by_column": changed_by_column,
        "changed_sample": changed,
        "changed_sample_truncated": changed_rows > len(changed),
        "added_keys": [k if not isinstance(k, tuple) else list(k) for k in added[:limit]],
        "removed_keys": [k if not isinstance(k, tuple) else list(k) for k in removed[:limit]],
    }


def _cell(value):
    """A cell value JSON can carry.

    numpy scalars are not int/float/str/bool, so an isinstance check alone
    turns every number into its repr -- 30 comes back as "30" and a reader
    cannot tell a numeric change from a text one.
    """
    if pd.isna(value):
        return None
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        return float(value)
    if isinstance(value, str):
        return value
    return str(value)


def compare_datasets(
    file_path_a: str = "",
    file_path_b: str = "",
    key_columns: list[str] = None,
    file_path: str = "",
    right_file_path: str = "",
) -> dict:
    """Compare two CSVs: schema diff, row counts, value changes."""
    progress = []
    # 60 tools in this repo call the dataset file_path; this is the one that
    # does not, and merge_datasets -- the other two-file tool -- spells the
    # second one right_file_path.
    file_path_a, a_note = pick("compare_datasets", "file_path_a", file_path_a, file_path)
    if not file_path_a:
        return missing_arg("compare_datasets", "file_path_a", "file_path")
    file_path_b, b_note = pick("compare_datasets", "file_path_b", file_path_b, right_file_path)
    if not file_path_b:
        return missing_arg("compare_datasets", "file_path_b", "right_file_path")
    for note in (a_note, b_note):
        if note:
            progress.append(info("Argument alias", note))
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

        keyed = _keyed_diff(df_a, df_b, list(key_columns), get_max_rows()) if key_columns else None
        if keyed and keyed.get("error"):
            return {
                "success": False,
                "op": "compare_datasets",
                "error": keyed["error"],
                "hint": "Drop key_columns for a column-level comparison, or pick columns that identify a row.",
                "progress": progress + [fail("Keyed comparison not possible", keyed["error"])],
                "token_estimate": 40,
            }

        progress.append(
            ok(
                f"Compared {path_a.name} vs {path_b.name}",
                f"{len(cols_only_a)} cols only in A, {len(cols_only_b)} only in B",
            )
        )

        result = {
            "success": True,
            "op": "compare_datasets",
            "file_path_a": str(path_a),
            "file_path_b": str(path_b),
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
        if keyed:
            result["keyed_diff"] = keyed
            progress.append(
                ok(
                    "Keyed comparison",
                    f"{keyed['rows_changed']} changed, {keyed['rows_added']} added, {keyed['rows_removed']} removed",
                )
            )
        else:
            result["hint"] = "Column-level only. Pass key_columns=['id'] to see which rows changed and how."
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("compare_datasets error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check both file paths are absolute and point to valid CSV files."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }
