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

from shared.column_utils import (
    column_pair_mask,
    condition_column,
    filter_operand_error,
    missing_column_error,
)
from shared.counts import counted
from shared.file_utils import atomic_write_text, error_text, hint_for_error, resolve_path
from shared.file_utils import read_csv as _shared_read_csv
from shared.lineage import note_lineage
from shared.platform_utils import get_max_rows
from shared.progress import fail, info, ok, warn
from shared.receipt import append_receipt
from shared.value_alias import CANONICAL as CANONICAL_OPS
from shared.value_alias import render_valid
from shared.value_alias import resolve as resolve_op
from shared.version_control import drop_snapshot_if_unwritten, snapshot

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stderr, level=logging.WARNING)


def _token_estimate(obj: object) -> int:
    return len(str(obj)) // 4


def _read_csv(path: str) -> pd.DataFrame:
    return _shared_read_csv(path)


# ---------------------------------------------------------------------------
# filter_dataset — upgraded from filter_rows
# ---------------------------------------------------------------------------

# The vocabulary lives in shared/value_alias.py and is spelled there once.
# It used to be a frozenset here and an if-chain in data_medium, and the two
# had drifted -- this server said `starts_with`, that one said `startswith`,
# and `not_contains` existed only here. Keep this an alias of the shared table,
# never a second copy of it.
_FILTER_OPS = frozenset(CANONICAL_OPS)


def _needs(cond: dict, op: str, key: str):
    """Read a key an op depends on, or say which key is missing.

    Each op reads a differently named key -- "value" for most, "values" for
    isin, "min"/"max" for between, "pattern" for regex -- and nothing documents
    that: the docstring names the ops, not their keys. Reading them straight off
    the dict raised a bare KeyError, so a caller who sent the documented op
    `between` with a `value` list got back the whole error `'min'`, under a hint
    listing the ops, which were already right.

    filter_rows in data-medium, doing the same job, already falls back to
    "value" for every one of these; its comments say so. This brings the two
    into line and names the key when there is nothing to fall back to.
    """
    if key in cond:
        return cond[key]
    if "value" in cond:
        return cond["value"]
    raise ValueError(
        f"Filter op '{op}' needs '{key}'. Got keys: {sorted(cond)}. "
        f"Write it as {{'column': ..., 'op': '{op}', '{key}': ...}}."
    )


def _as_list(value) -> list:
    """isin wants a list; a caller who means one value often writes it bare."""
    return list(value) if isinstance(value, list | tuple | set) else [value]


def _bounds(cond: dict, op: str) -> tuple[float, float]:
    """min/max for a range op, however the caller expressed them."""
    if "min" in cond and "max" in cond:
        return float(cond["min"]), float(cond["max"])
    value = cond.get("value")
    if isinstance(value, list | tuple) and len(value) == 2:
        return float(value[0]), float(value[1])
    raise ValueError(
        f"Filter op '{op}' needs numeric bounds. Got keys: {sorted(cond)}. "
        f"Write it as {{'column': ..., 'op': '{op}', 'min': 0, 'max': 100}} "
        "or pass value as a two-item list."
    )


def _apply_condition(df: pd.DataFrame, cond: dict) -> pd.Series:
    col = condition_column(cond)
    op = cond.get("op", "") or cond.get("operator", "")
    if not col:
        error, hint = missing_column_error(cond)
        raise ValueError(f"{error} {hint}")
    if col not in df.columns:
        raise ValueError(f"Column '{col}' not found. Available: {list(df.columns)}")
    # Resolve before anything switches on it: `==` and `startswith` are the two
    # spellings callers actually send, and neither used to reach the if-chain.
    op = resolve_op(op, field="filter op")
    pair = column_pair_mask(df, cond, col, op)
    if pair is not None:
        return pair
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
        return s.isin(_as_list(_needs(cond, op, "values")))
    elif op == "not_isin":
        return ~s.isin(_as_list(_needs(cond, op, "values")))
    elif op == "between":
        low, high = _bounds(cond, op)
        num = pd.to_numeric(s, errors="coerce")
        return (num >= low) & (num <= high)
    elif op == "regex":
        return s.astype(str).str.match(str(_needs(cond, op, "pattern")), na=False)
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

        # Every condition is checked before any is applied: a half-written one
        # used to reach the comparison and come back as the single word 'value'.
        for i, cond in enumerate(conditions or []):
            operand_error = filter_operand_error(cond, cond.get("op", "") or cond.get("operator", ""), i)
            if operand_error:
                return {
                    "success": False,
                    "error": operand_error,
                    "hint": "Fix the condition above and retry. Nothing was written.",
                    "progress": [fail("Incomplete condition", f"condition {i}")],
                    "token_estimate": 20,
                }

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

        if output_path:
            out_path = resolve_path(output_path)
        else:
            out_path = path.parent / f"{path.stem}_filtered{path.suffix}"
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
        # Only when a NEW file was made. Filtering in place is a mutation, and
        # the receipt above already records it; writing a lineage there would
        # claim the file was derived from itself.
        if out_path != path:
            note_lineage(
                result,
                out_path,
                op="filter_dataset",
                source=path,
                rows_before=before,
                rows_after=after,
                columns_before=len(df.columns),
                columns_after=len(df.columns),
                params={"conditions": conditions, "sort_by": sort_by},
            )
        result["token_estimate"] = _token_estimate(result)
        return result
    except Exception as exc:
        logger.exception("filter_dataset error")
        return {
            "success": False,
            "error": error_text(exc),
            # Was a constant list of the valid ops, returned for every failure
            # this try block can raise -- so a bad COLUMN was answered with the
            # op vocabulary, and a bad op was answered with a list the error had
            # already printed. hint_for_error reads the message first.
            "hint": hint_for_error(exc, f"Valid filter ops: {render_valid()}"),
            "backup": drop_snapshot_if_unwritten(backup, path),
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
    # One tool, two flags for "drop what I consumed", one letter apart:
    # mode=split_column reads drop_original, mode=combine_columns reads
    # drop_originals. Sending the wrong one is a valid argument, so pydantic
    # accepts it and the mode ignores it -- success: true and the source
    # columns still in the output. Honour either.
    drop_original = drop_original or drop_originals
    drop_originals = drop_original
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
            # Not supplying it and naming a column that is not there are
            # different mistakes. Reporting both as "split_column '' not in
            # dataset" tells a caller who omitted the argument to go looking for
            # a column named "" -- the other modes here say "pivot requires
            # 'index' parameter" and "crosstab requires 'row_col' and 'col_col'".
            if not split_column:
                return {
                    "success": False,
                    "error": "split_column mode requires a 'split_column' parameter.",
                    "hint": f"Name the column to split, e.g. split_column='{list(df.columns)[0]}'.",
                    "progress": [fail("Missing param", "split_column")],
                    "token_estimate": 20,
                }
            if split_column not in df.columns:
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
            # With no columns to combine this reported success and wrote a new
            # column full of NaN into the output file: df[[]].apply(axis=1) has
            # nothing to join, and nothing downstream noticed. A caller who
            # forgot the argument was told the reshape worked and handed back a
            # dataset with a junk column in it -- the worst way to fail.
            if not cols:
                return {
                    "success": False,
                    "error": "combine_columns mode requires a 'combine_columns' list.",
                    "hint": f"Name the columns to join, e.g. combine_columns={list(df.columns)[:2]}.",
                    "progress": [fail("Missing param", "combine_columns")],
                    "token_estimate": 20,
                }
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

        if output_path:
            out_path = resolve_path(output_path)
        else:
            out_path = path.parent / f"{path.stem}_reshaped{path.suffix}"
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
        if out_path != path:
            note_lineage(
                result,
                out_path,
                op="reshape_dataset",
                source=path,
                rows_before=before_shape[0],
                rows_after=int(df.shape[0]),
                columns_before=before_shape[1],
                columns_after=int(df.shape[1]),
                params={"mode": mode},
            )
        result["token_estimate"] = _token_estimate(result)
        return result
    except Exception as exc:
        logger.exception("reshape_dataset error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check mode and required parameters for each mode."),
            "backup": drop_snapshot_if_unwritten(backup, path),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# aggregate_dataset — unified groupby / crosstab / value_counts / describe / window
# ---------------------------------------------------------------------------


# What each aggregate_dataset mode actually reads, derived from its branch.
# The three universal arguments -- file_path, mode, output_path, dry_run -- and
# the row_column/col_column/values_column aliases are handled separately.
_AGGREGATE_MODE_ARGS: dict[str, frozenset[str]] = {
    "groupby": frozenset({"group_by", "agg", "sort_desc", "top_n"}),
    "crosstab": frozenset({"row_col", "col_col", "values_col", "normalize"}),
    "value_counts": frozenset({"columns", "top_n", "include_pct"}),
    "describe": frozenset(),
    "window": frozenset({"order_by", "columns", "group_by", "window", "window_agg"}),
}

# Compared against so that only an argument the caller actually changed is
# reported -- passing top_n=0 to a mode that ignores top_n asked for nothing.
_AGGREGATE_ARG_DEFAULTS: dict[str, object] = {
    "group_by": None,
    "agg": None,
    "sort_desc": True,
    "top_n": 0,
    "row_col": "",
    "col_col": "",
    "values_col": "",
    "normalize": "",
    "columns": None,
    "include_pct": True,
    "order_by": "",
    "window": 3,
    "window_agg": "mean",
}


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
    row_column: str = "",
    col_column: str = "",
    values_column: str = "",
) -> dict:
    """Aggregate data. mode: groupby crosstab value_counts describe window."""
    progress = []
    backup = None
    # mode="crosstab" does what cross_tabulate does, and cross_tabulate spells
    # these three row_column / col_column / values_column. A caller moving
    # between the two tools has no way to learn that from the schema.
    for short, long_, label in (
        (row_col, row_column, "row_col"),
        (col_col, col_column, "col_col"),
        (values_col, values_column, "values_col"),
    ):
        if not short and long_:
            progress.append(info("Argument alias", f"Read {label} from an accepted alternative spelling"))
    row_col = row_col or row_column
    col_col = col_col or col_column
    values_col = values_col or values_column
    # Only set for modes with a natural single result table (groupby, crosstab,
    # window) — value_counts/describe produce nested/heterogeneous structures
    # with no single flat CSV representation, so output_path is a no-op there.
    output_df: pd.DataFrame | None = None
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

        # An argument can be perfectly valid for this tool and mean nothing to
        # the mode it was sent with, and the schema cannot see the difference --
        # it describes the tool, while the vocabulary is per mode. So
        # strict_args passes it, the branch never reads it, and the caller is
        # told the run succeeded:
        #
        #     aggregate_dataset(mode="value_counts", row_col="device")
        #     -> success, and a frequency table of every object column
        #
        # which is not what was asked for and is indistinguishable from what
        # was. The same shape already cost a wrong number once in window mode,
        # where group_by was accepted and dropped -- see the note further down.
        mode_args = _AGGREGATE_MODE_ARGS[mode]
        given = {
            "group_by": group_by,
            "agg": agg,
            "sort_desc": sort_desc,
            "top_n": top_n,
            "row_col": row_col,
            "col_col": col_col,
            "values_col": values_col,
            "normalize": normalize,
            "columns": columns,
            "include_pct": include_pct,
            "order_by": order_by,
            "window": window,
            "window_agg": window_agg,
        }
        ignored = sorted(k for k, v in given.items() if v != _AGGREGATE_ARG_DEFAULTS[k] and k not in mode_args)
        if ignored:
            reads = ", ".join(sorted(mode_args)) or "no mode-specific arguments"
            return {
                "success": False,
                "error": f"mode='{mode}' does not read {', '.join(ignored)}.",
                "hint": f"mode='{mode}' reads: {reads}. Drop the others, or pick the mode that reads them.",
                "progress": [fail("Argument not read by this mode", ", ".join(ignored))],
                "token_estimate": 30,
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
            # This was a hardcoded `_response_cap = 20`, the third copy of that
            # mistake in this repo: it ignored get_max_rows(), so constrained
            # mode shrank nothing here and the preview was 20 rows whatever the
            # deployment asked for.
            max_r = get_max_rows()
            total_groups = len(grouped)
            result_data = {
                "rows": total_groups,
                "data": grouped.head(max_r).fillna("").to_dict(orient="records"),
                **counted(min(max_r, total_groups), total_groups),
            }
            output_df = grouped
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
            output_df = ct.reset_index()
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

            # group_by is in this tool's schema and was accepted here, then
            # silently dropped: the rolling ran over the whole frame. On a file
            # holding two platforms interleaved by date, the 3-day rolling mean
            # for Google came back 167.33 -- two Google rows averaged with a
            # Meta row -- where the answer is 200.0. Wrong number, success: true.
            missing_group = [c for c in (group_by or []) if c not in df.columns]
            if missing_group:
                return {
                    "success": False,
                    "error": f"group_by columns not found: {missing_group}",
                    "hint": f"Available: {list(df.columns)}",
                    "progress": [fail("Column not found", str(missing_group))],
                    "token_estimate": 20,
                }
            if window_agg not in {"mean", "sum", "std", "min", "max"}:
                return {
                    "success": False,
                    "error": f"Unknown window_agg '{window_agg}'",
                    "hint": "Use window_agg: mean, sum, std, min or max.",
                    "progress": [fail("Bad window_agg", window_agg)],
                    "token_estimate": 20,
                }
            for col in target_cols:
                if col not in df.columns:
                    continue
                new_col = f"{col}_window_{window_agg}{window}"
                if group_by:
                    df[new_col] = df.groupby(group_by, sort=False)[col].transform(
                        lambda s: getattr(s.rolling(window), window_agg)()
                    )
                else:
                    df[new_col] = getattr(df[col].rolling(window), window_agg)()
            if group_by:
                progress.append(ok("Rolled within each group", ", ".join(group_by)))
            result_data = {
                "order_by": order_by,
                "group_by": list(group_by or []),
                "window": window,
                "window_agg": window_agg,
                "new_columns": [f"{c}_window_{window_agg}{window}" for c in target_cols if c in df.columns],
            }
            output_df = df
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

        # output_path used to be echoed into the response for every mode even
        # though the file was only ever written for mode == "window" — a
        # false-success report (found live via the opencode harness real-tool
        # retest sweep: aggregate_dataset in groupby mode claimed
        # output_path but never created the file). Only claim output_path
        # when a table was actually written for this mode.
        out_path = resolve_path(output_path) if output_path else None
        if out_path and output_df is not None:
            backup = snapshot(str(path)) if out_path == path else None
            atomic_write_text(str(out_path), output_df.to_csv(index=False))

        result = {
            "success": True,
            "op": "aggregate_dataset",
            "mode": mode,
            "file": path.name,
            "backup": backup or "",
            "data": result_data,
            "progress": progress,
        }
        if out_path and output_df is not None:
            result["output_path"] = str(out_path)
            if out_path != path:
                note_lineage(
                    result,
                    out_path,
                    op="aggregate_dataset",
                    source=path,
                    rows_before=len(df),
                    rows_after=len(output_df),
                    columns_before=len(df.columns),
                    columns_after=len(output_df.columns),
                    params={"mode": mode},
                )
        elif out_path:
            result["progress"].append(warn(f"output_path ignored for mode '{mode}' — no flat table to write", mode))
        result["token_estimate"] = _token_estimate(result)
        return result
    except Exception as exc:
        logger.exception("aggregate_dataset error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(
                exc, "Check mode and required parameters. Use inspect_dataset() to verify column names."
            ),
            "backup": drop_snapshot_if_unwritten(backup, path),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


def list_derive_ops(op: str = "") -> dict:
    """Return the derive-spec grammar, whole or for one op.

    `feature_engineering(derive=[...])` is typed `list[dict]` with
    `additionalProperties: true`, so nothing in the schema says what goes in the
    dict. The only way to learn it was to fail repeatedly -- five calls to write
    one ratio, each error naming the single next missing key. `list_patch_ops`
    already solved this for the other nested grammar in the repo.
    """
    from shared.derive_ops import _OP_HELP

    name = op.strip().lower()
    if name and name not in _OP_HELP:
        return {
            "success": False,
            "error": f"Unknown derive op: '{name}'",
            "hint": f"Valid ops: {', '.join(sorted(_OP_HELP))}",
            "progress": [fail("Unknown derive op", name)],
            "token_estimate": 20,
        }
    wanted = {name: _OP_HELP[name]} if name else dict(_OP_HELP)
    result = {
        "success": True,
        "op": "list_derive_ops",
        "requested": name or "all",
        "total_ops": len(wanted),
        "ops": [{"op": k, "spec": v} for k, v in sorted(wanted.items())],
        "example": {
            "name": "ctr",
            "op": "arith",
            "column": "clicks",
            "how": "div",
            "other": "impressions",
        },
        "note": (
            "Specs apply in order, so a later derivation can read an earlier one's column. "
            "Pass the list as feature_engineering(derive=[...])."
        ),
        "progress": [ok("Derive grammar returned", f"{len(wanted)} op(s)")],
    }
    result["token_estimate"] = _token_estimate(result)
    return result


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
    "list_derive_ops",
    "enrich_with_geo",
]
