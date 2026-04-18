"""T2 data_transform engine — all transformation logic. Zero MCP imports."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_MED = str(Path(__file__).resolve().parents[1] / "data_medium")
for _p in (str(_ROOT), _MED):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import pandas as pd

# Re-export existing data_medium transforms
from _med_transform import (  # type: ignore[import]
    concat_datasets,
    enrich_with_geo,
    feature_engineering,
    merge_datasets,
    resample_timeseries,
    run_cleaning_pipeline,
    smart_impute,
)

from shared.file_utils import atomic_write_text, resolve_path
from shared.file_utils import read_csv as _shared_read_csv
from shared.platform_utils import get_max_rows
from shared.progress import fail, info, ok, warn
from shared.receipt import append_receipt
from shared.version_control import snapshot

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stderr, level=logging.WARNING)


def _token_estimate(obj: object) -> int:
    return len(str(obj)) // 4


def _read_csv(path: str) -> pd.DataFrame:
    return _shared_read_csv(path)


# ---------------------------------------------------------------------------
# filter_dataset — upgraded from filter_rows
# ---------------------------------------------------------------------------

_FILTER_OPS = frozenset(
    {
        "equals",
        "not_equals",
        "contains",
        "not_contains",
        "gt",
        "lt",
        "gte",
        "lte",
        "not_null",
        "is_null",
        "isin",
        "not_isin",
        "between",
        "regex",
        "date_range",
        "quantile_between",
        "starts_with",
        "ends_with",
    }
)


def _apply_condition(df: pd.DataFrame, cond: dict) -> pd.Series:
    col = cond.get("column", "")
    op = cond.get("op", "")
    if col not in df.columns:
        raise ValueError(f"Column '{col}' not found. Available: {list(df.columns)}")
    if op not in _FILTER_OPS:
        raise ValueError(f"Unknown filter op '{op}'. Valid: {', '.join(sorted(_FILTER_OPS))}")
    s = df[col]
    if op == "equals":
        return s == cond["value"]
    elif op == "not_equals":
        return s != cond["value"]
    elif op == "contains":
        return s.astype(str).str.contains(str(cond["value"]), na=False)
    elif op == "not_contains":
        return ~s.astype(str).str.contains(str(cond["value"]), na=False)
    elif op == "starts_with":
        return s.astype(str).str.startswith(str(cond["value"]), na=False)
    elif op == "ends_with":
        return s.astype(str).str.endswith(str(cond["value"]), na=False)
    elif op == "gt":
        return pd.to_numeric(s, errors="coerce") > float(cond["value"])
    elif op == "lt":
        return pd.to_numeric(s, errors="coerce") < float(cond["value"])
    elif op == "gte":
        return pd.to_numeric(s, errors="coerce") >= float(cond["value"])
    elif op == "lte":
        return pd.to_numeric(s, errors="coerce") <= float(cond["value"])
    elif op == "not_null":
        return s.notna()
    elif op == "is_null":
        return s.isna()
    elif op == "isin":
        return s.isin(cond["values"])
    elif op == "not_isin":
        return ~s.isin(cond["values"])
    elif op == "between":
        num = pd.to_numeric(s, errors="coerce")
        return (num >= float(cond["min"])) & (num <= float(cond["max"]))
    elif op == "regex":
        return s.astype(str).str.match(cond["pattern"], na=False)
    elif op == "date_range":
        dates = pd.to_datetime(s, errors="coerce")
        start = pd.to_datetime(cond.get("start")) if cond.get("start") else None
        end = pd.to_datetime(cond.get("end")) if cond.get("end") else None
        mask = pd.Series(True, index=df.index)
        if start is not None:
            mask &= dates >= start
        if end is not None:
            mask &= dates <= end
        return mask
    elif op == "quantile_between":
        num = pd.to_numeric(s, errors="coerce")
        lo = float(num.quantile(float(cond.get("min_q", 0.0))))
        hi = float(num.quantile(float(cond.get("max_q", 1.0))))
        return (num >= lo) & (num <= hi)
    return pd.Series(True, index=df.index)


def filter_dataset(
    file_path: str,
    conditions: list[dict],
    sort_by: list[str] = None,
    sort_ascending: list[bool] = None,
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    """Filter rows by conditions + optional sort. Saves result file."""
    progress = []
    backup = None
    try:
        path = resolve_path(file_path)
        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check file_path is absolute.",
                "progress": [fail("File not found", path.name)],
                "token_estimate": 20,
            }
        df = _read_csv(str(path))
        before = len(df)

        # Apply all conditions (AND logic)
        if conditions:
            mask = pd.Series(True, index=df.index)
            for cond in conditions:
                mask &= _apply_condition(df, cond)
            df = df[mask]

        after = len(df)
        removed = before - after

        # Sort if requested
        if sort_by:
            ascending = sort_ascending if sort_ascending else [True] * len(sort_by)
            df = df.sort_values(by=sort_by, ascending=ascending)
            progress.append(info("Sorted", f"by {sort_by}"))

        progress.append(ok("Filtered", f"{before} → {after} rows (removed {removed})"))

        if dry_run:
            result = {
                "success": True,
                "dry_run": True,
                "op": "filter_dataset",
                "before_rows": before,
                "after_rows": after,
                "removed": removed,
                "conditions": conditions,
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        out_path = resolve_path(output_path) if output_path else path
        backup = snapshot(str(path)) if out_path == path else None
        atomic_write_text(str(out_path), df.to_csv(index=False))
        append_receipt(
            str(path),
            tool="filter_dataset",
            args={"conditions": conditions, "sort_by": sort_by},
            result=f"removed {removed} rows",
            backup=backup or "",
        )
        result = {
            "success": True,
            "op": "filter_dataset",
            "file": path.name,
            "output_path": str(out_path),
            "before_rows": before,
            "after_rows": after,
            "removed": removed,
            "columns": len(df.columns),
            "backup": backup or "",
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result
    except Exception as exc:
        logger.exception("filter_dataset error")
        return {
            "success": False,
            "error": str(exc),
            "hint": f"Valid filter ops: {', '.join(sorted(_FILTER_OPS))}",
            "backup": backup or "",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# reshape_dataset — pivot / melt / split_column / combine_columns / transpose
# ---------------------------------------------------------------------------


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
    progress = []
    backup = None
    valid_modes = {"pivot", "melt", "split_column", "combine_columns", "transpose"}
    try:
        if mode not in valid_modes:
            return {
                "success": False,
                "error": f"Unknown mode '{mode}'",
                "hint": f"Valid modes: {', '.join(sorted(valid_modes))}",
                "progress": [fail("Unknown mode", mode)],
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
        df = _read_csv(str(path))
        before_shape = list(df.shape)

        if mode == "pivot":
            if not index:
                return {
                    "success": False,
                    "error": "pivot requires 'index' parameter.",
                    "hint": "Provide index: list of columns to use as row identifiers.",
                    "progress": [fail("Missing param", "index")],
                    "token_estimate": 20,
                }
            df = df.pivot_table(
                index=index,
                columns=columns or None,
                values=values or None,
                aggfunc=agg_func,
                fill_value=0,
            )
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ["_".join(str(c) for c in col).strip("_") for col in df.columns]
            df = df.reset_index()
            progress.append(ok("Pivoted", f"{before_shape} → {list(df.shape)}"))

        elif mode == "melt":
            df = df.melt(
                id_vars=id_vars,
                value_vars=value_vars,
                var_name=var_name,
                value_name=value_name,
            )
            progress.append(ok("Melted (wide→long)", f"{before_shape} → {list(df.shape)}"))

        elif mode == "split_column":
            if not split_column or split_column not in df.columns:
                return {
                    "success": False,
                    "error": f"split_column '{split_column}' not in dataset.",
                    "hint": f"Available columns: {list(df.columns)}",
                    "progress": [fail("Column not found", split_column)],
                    "token_estimate": 20,
                }
            parts = df[split_column].astype(str).str.split(delimiter, expand=True)
            if new_columns:
                parts.columns = new_columns[: len(parts.columns)]
            else:
                parts.columns = [f"{split_column}_{i}" for i in range(len(parts.columns))]
            if drop_original:
                df = df.drop(columns=[split_column])
            df = pd.concat([df, parts], axis=1)
            progress.append(ok(f"Split '{split_column}'", f"into {len(parts.columns)} columns"))

        elif mode == "combine_columns":
            cols = combine_columns or []
            missing = [c for c in cols if c not in df.columns]
            if missing:
                return {
                    "success": False,
                    "error": f"Columns not found: {missing}",
                    "hint": f"Available: {list(df.columns)}",
                    "progress": [fail("Columns not found", str(missing))],
                    "token_estimate": 20,
                }
            df[new_column] = df[cols].astype(str).apply(lambda row: combine_delimiter.join(row), axis=1)
            if drop_originals:
                df = df.drop(columns=cols)
            progress.append(ok(f"Combined into '{new_column}'", f"from {cols}"))

        elif mode == "transpose":
            df = df.set_index(df.columns[0]).transpose().reset_index()
            df.columns.name = None
            progress.append(ok("Transposed", f"{before_shape} → {list(df.shape)}"))

        if dry_run:
            result = {
                "success": True,
                "dry_run": True,
                "op": "reshape_dataset",
                "mode": mode,
                "before_shape": before_shape,
                "after_shape": list(df.shape),
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        out_path = resolve_path(output_path) if output_path else path
        backup = snapshot(str(path)) if out_path == path else None
        atomic_write_text(str(out_path), df.to_csv(index=False))
        append_receipt(
            str(path),
            tool="reshape_dataset",
            args={"mode": mode},
            result=f"{before_shape} → {list(df.shape)}",
            backup=backup or "",
        )
        result = {
            "success": True,
            "op": "reshape_dataset",
            "mode": mode,
            "file": path.name,
            "output_path": str(out_path),
            "before_shape": before_shape,
            "after_shape": list(df.shape),
            "backup": backup or "",
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result
    except Exception as exc:
        logger.exception("reshape_dataset error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check mode and required parameters for each mode.",
            "backup": backup or "",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# aggregate_dataset — unified groupby / crosstab / value_counts / describe / window
# ---------------------------------------------------------------------------


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
    progress = []
    backup = None
    valid_modes = {"groupby", "crosstab", "value_counts", "describe", "window"}
    try:
        if mode not in valid_modes:
            return {
                "success": False,
                "error": f"Unknown mode '{mode}'",
                "hint": f"Valid modes: {', '.join(sorted(valid_modes))}",
                "progress": [fail("Unknown mode", mode)],
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
        df = _read_csv(str(path))
        result_data: dict = {}

        if mode == "groupby":
            if not group_by:
                return {
                    "success": False,
                    "error": "groupby mode requires 'group_by' list.",
                    "hint": "Provide group_by: list of column names.",
                    "progress": [fail("Missing param", "group_by")],
                    "token_estimate": 20,
                }
            missing = [c for c in group_by if c not in df.columns]
            if missing:
                return {
                    "success": False,
                    "error": f"Group-by columns not found: {missing}",
                    "hint": f"Available columns: {list(df.columns)}",
                    "progress": [fail("Columns not found", str(missing))],
                    "token_estimate": 20,
                }
            if agg:
                # agg = {"col": "sum,mean" or "sum"}
                agg_dict = {}
                for col, funcs in agg.items():
                    if col not in df.columns:
                        return {
                            "success": False,
                            "error": f"Agg column not found: '{col}'",
                            "hint": f"Available: {list(df.columns)}",
                            "progress": [fail("Column not found", col)],
                            "token_estimate": 20,
                        }
                    func_list = [f.strip() for f in str(funcs).split(",")]
                    agg_dict[col] = func_list if len(func_list) > 1 else func_list[0]
                grouped = df.groupby(group_by).agg(agg_dict)
                if isinstance(grouped.columns, pd.MultiIndex):
                    grouped.columns = ["_".join(c).strip("_") for c in grouped.columns]
            else:
                # Default: sum all numeric
                numeric_cols = [c for c in df.columns if c not in group_by and pd.api.types.is_numeric_dtype(df[c])]
                if not numeric_cols:
                    return {
                        "success": False,
                        "error": "No numeric columns to aggregate.",
                        "hint": "Provide 'agg' dict or ensure numeric columns exist.",
                        "progress": [fail("No numeric columns", "")],
                        "token_estimate": 20,
                    }
                grouped = df.groupby(group_by)[numeric_cols].sum()
            grouped = grouped.reset_index()
            if sort_desc and len(grouped.columns) > len(group_by):
                sort_col = [c for c in grouped.columns if c not in group_by][0]
                grouped = grouped.sort_values(sort_col, ascending=False)
            if top_n:
                grouped = grouped.head(top_n)
            _response_cap = 20
            truncated = len(grouped) > _response_cap
            result_data = {
                "rows": len(grouped),
                "data": grouped.head(_response_cap).fillna("").to_dict(orient="records"),
                "truncated": truncated,
            }
            progress.append(ok("Grouped by", f"{group_by} → {len(grouped)} groups"))

        elif mode == "crosstab":
            if not row_col or not col_col:
                return {
                    "success": False,
                    "error": "crosstab requires 'row_col' and 'col_col'.",
                    "hint": "Provide row_col and col_col column names.",
                    "progress": [fail("Missing params", "row_col / col_col")],
                    "token_estimate": 20,
                }
            for c in (row_col, col_col):
                if c not in df.columns:
                    return {
                        "success": False,
                        "error": f"Column not found: '{c}'",
                        "hint": f"Available: {list(df.columns)}",
                        "progress": [fail("Column not found", c)],
                        "token_estimate": 20,
                    }
            ct = pd.crosstab(
                df[row_col],
                df[col_col],
                values=df[values_col] if values_col and values_col in df.columns else None,
                aggfunc="sum" if values_col else None,
                normalize=normalize or False,
            )
            result_data = {
                "rows": ct.shape[0],
                "cols": ct.shape[1],
                "data": ct.to_dict(),
            }
            progress.append(ok("Cross-tabulated", f"{row_col} × {col_col}"))

        elif mode == "value_counts":
            cols = columns or df.select_dtypes(include=["object", "category"]).columns.tolist()
            vc_results = {}
            for col in cols:
                if col not in df.columns:
                    continue
                vc = df[col].value_counts(dropna=False).head(top_n or 20)
                entry: dict = {"counts": vc.to_dict()}
                if include_pct:
                    entry["pct"] = (vc / len(df) * 100).round(2).to_dict()
                vc_results[col] = entry
            result_data = {"columns": list(vc_results.keys()), "value_counts": vc_results}
            progress.append(ok("Value counts", f"{len(vc_results)} columns"))

        elif mode == "describe":
            num_desc = df.describe(include="number").round(4).to_dict()
            cat_desc = df.describe(include="object").to_dict() if not df.select_dtypes(include="object").empty else {}
            result_data = {"numeric": num_desc, "categorical": cat_desc}
            progress.append(ok("Describe", f"{len(df.columns)} columns"))

        elif mode == "window":
            if not order_by or order_by not in df.columns:
                return {
                    "success": False,
                    "error": "window mode requires valid 'order_by' column.",
                    "hint": f"Available: {list(df.columns)}",
                    "progress": [fail("Missing param", "order_by")],
                    "token_estimate": 20,
                }
            numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c != order_by]
            target_cols = columns or numeric_cols
            df = df.sort_values(order_by)
            for col in target_cols:
                if col not in df.columns:
                    continue
                new_col = f"{col}_window_{window_agg}{window}"
                if window_agg == "mean":
                    df[new_col] = df[col].rolling(window).mean()
                elif window_agg == "sum":
                    df[new_col] = df[col].rolling(window).sum()
                elif window_agg == "std":
                    df[new_col] = df[col].rolling(window).std()
                elif window_agg == "min":
                    df[new_col] = df[col].rolling(window).min()
                elif window_agg == "max":
                    df[new_col] = df[col].rolling(window).max()
            result_data = {
                "order_by": order_by,
                "window": window,
                "window_agg": window_agg,
                "new_columns": [f"{c}_window_{window_agg}{window}" for c in target_cols if c in df.columns],
            }
            progress.append(ok("Window functions applied", f"window={window} agg={window_agg}"))

        if dry_run:
            result = {
                "success": True,
                "dry_run": True,
                "op": "aggregate_dataset",
                "mode": mode,
                "result_preview": result_data,
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        out_path = resolve_path(output_path) if output_path else None
        if mode == "window" and out_path:
            backup = snapshot(str(path)) if out_path == path else None
            atomic_write_text(str(out_path), df.to_csv(index=False))

        result = {
            "success": True,
            "op": "aggregate_dataset",
            "mode": mode,
            "file": path.name,
            "backup": backup or "",
            "data": result_data,
            "progress": progress,
        }
        if out_path:
            result["output_path"] = str(out_path)
        result["token_estimate"] = _token_estimate(result)
        return result
    except Exception as exc:
        logger.exception("aggregate_dataset error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check mode and required parameters. Use inspect_dataset() to verify column names.",
            "backup": backup or "",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


__all__ = [
    "filter_dataset",
    "reshape_dataset",
    "aggregate_dataset",
    "resample_timeseries",
    "merge_datasets",
    "concat_datasets",
    "smart_impute",
    "run_cleaning_pipeline",
    "feature_engineering",
    "enrich_with_geo",
]
