"""Transformation tools for data_medium. No MCP imports."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_HERE = str(Path(__file__).resolve().parent)
_DATA_BASIC = str(Path(__file__).resolve().parents[1] / "data_basic")
for _p in (str(_ROOT), _HERE, _DATA_BASIC):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import pandas as pd
from _med_helpers import (
    _is_string_col,
    _open_file,
    _read_csv,
    _token_estimate,
    is_numeric_col,
)
from _patch_ops import OP_HANDLERS  # type: ignore[import-not-found]

from shared.arg_alias import missing, pick, pick_list
from shared.choice import AGG_ALIASES, AGG_FUNCS, NUMERIC_AGG_FUNCS, UnknownChoice
from shared.choice import refusal as choice_refusal
from shared.choice import resolve as resolve_choice
from shared.column_utils import date_note, looks_like_dates, parse_dates
from shared.counts import counted
from shared.derive_ops import DeriveError, apply_derivations
from shared.file_utils import error_text, hint_for_error, resolve_path
from shared.patch_validator import unwrap_params, validate_ops
from shared.platform_utils import get_max_rows
from shared.progress import fail, info, ok, warn
from shared.receipt import append_receipt
from shared.small_sample import is_missing
from shared.version_control import drop_snapshot_if_unwritten, snapshot
from shared.version_control import restore as _restore

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

        # gpd.read_file() hands off to GDAL/fiona/pyogrio, which can fatally
        # abort the whole process (not raise a catchable Python exception)
        # on a file it can't recognize as a vector format — no amount of
        # try/except below protects against that. Same file-extension guard
        # data_basic's load_geo_dataset already uses, applied here before
        # ever reaching gpd.read_file(). Found live via the opencode harness
        # real-tool retest sweep: a plain CSV passed as geo_file_path
        # crashed and restarted the whole shared container with no
        # traceback logged anywhere, confirming it bypassed Python entirely.
        valid_geo_exts = {".geojson", ".shp", ".json"}
        if geo_path.suffix.lower() not in valid_geo_exts:
            return {
                "success": False,
                "error": f"Expected .geojson or .shp for geo_file_path, got {geo_path.suffix}",
                "hint": "Use a .geojson or .shp file for geo_file_path.",
                "progress": [fail("Wrong geo file type", geo_path.suffix)],
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
        # Zero matches means the join key never lined up — the output is the input
        # with a column of nulls bolted on. A real run wrote a 2 MB file and
        # reported plain success after matching nothing at all.
        no_match_hint = ""
        if not matched:
            no_match_hint = (
                f"0 of {len(df):,} rows matched — the join key values do not overlap, so no geography "
                f"was added. Unmatched examples from the dataset: {unmatched_main[:3]}; "
                f"from the geo file: {unmatched_geo[:3]}. Check the two key columns hold the same "
                "kind of identifier before re-running."
            )
            progress.append(warn("No rows matched", f"{geo_path.name} added nothing"))

        if dry_run:
            progress.append(info("Dry run — no changes written", path.name))
            result = {
                "success": True,
                "dry_run": True,
                "op": "enrich_with_geo",
                "file_path": str(path),
                "rows_before": len(df),
                "rows_after": len(merged),
                "matched": matched,
                "unmatched_main": unmatched_main,
                "unmatched_geo": unmatched_geo,
                "new_columns": new_cols,
                "progress": progress,
            }
            if no_match_hint:
                result["warning"] = no_match_hint
            result["token_estimate"] = _token_estimate(result)
            return result

        out = str(resolve_path(output_path)) if output_path else str(path)
        backup = snapshot(str(path)) if out == str(path) else None
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
            "file_path": str(path),
            "rows_before": len(df),
            "rows_after": len(merged),
            "matched": matched,
            "unmatched_main": unmatched_main,
            "unmatched_geo": unmatched_geo,
            "new_columns": new_cols,
            "output_file": Path(out).name,
            "backup": backup,
            "hint": "Call inspect_dataset() or read_column_stats() to verify the changes.",
            "progress": progress,
        }
        if no_match_hint:
            result["warning"] = no_match_hint
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("enrich_with_geo error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check file paths are absolute and join columns exist."),
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

        # One table, shared with pivot_table, so the two siblings accept the
        # same words. "average" and "avg" resolve to "mean" rather than being
        # refused for a vocabulary difference that carries no meaning.
        try:
            agg_func = resolve_choice(agg_func, AGG_FUNCS, field="agg_func", aliases=AGG_ALIASES)
        except UnknownChoice as exc:
            return choice_refusal("compute_aggregations", exc)

        # Coerce agg_column to numeric for numeric functions; skip for count
        if agg_func in NUMERIC_AGG_FUNCS:
            df[agg_column] = pd.to_numeric(df[agg_column], errors="coerce")
            non_numeric = int(df[agg_column].isna().sum())
            if non_numeric:
                progress.append(warn(f"Coerced '{agg_column}' to numeric", f"{non_numeric} non-numeric values → NaN"))

        grouped = df.groupby(group_by, as_index=False)[agg_column].agg(agg_func)
        if sort_desc:
            grouped = grouped.sort_values(by=agg_column, ascending=False)

        # How many groups the data actually has, fixed before any capping. This
        # used to be reported as the length of the returned list, so a caller
        # who got 20 rows was told there were 20 groups.
        total_groups = len(grouped)
        if top_n > 0:
            grouped = grouped.head(top_n)

        # What the caller asked to see, which is what `truncated` is measured
        # against. A `top_n=5` on 25 groups returns 5 of 5 requested, not 5 of
        # 25 cut: the caller chose that, and calling it truncation would send
        # them back for rows they deliberately declined. `groups` below still
        # reports all 25, so nothing is hidden -- the two numbers answer two
        # different questions.
        eligible = len(grouped)

        # A second, hardcoded `_response_cap = 20` sat below get_max_rows() and
        # did the real cutting. On the SFO cargo file grouped by year it
        # returned 20 of 25 years -- dropping 2011-2014 and 2023, the five
        # lowest -- while reporting "groups": 20 and "truncated": false. The
        # 2013 trough quoted in the report that shipped was simply not in the
        # tool's answer, and nothing in the response said a row was missing.
        # get_max_rows() is the repo's limit helper and was already here.
        max_r = get_max_rows()
        truncated = eligible > max_r
        if truncated:
            grouped = grouped.head(max_r)

        result_list = grouped.fillna("").to_dict(orient="records")
        if truncated:
            progress.append(
                warn(
                    "Results truncated",
                    f"Showing {len(result_list)} of {total_groups} groups"
                    + (" (sorted high to low)" if sort_desc else "")
                    + ". Raise MCP_CONSTRAINED_MODE=0 or narrow with top_n/filter_rows.",
                )
            )
        progress.append(ok(f"Aggregated {path.name}", f"{len(result_list)} of {total_groups} groups returned"))

        result = {
            "success": True,
            "op": "compute_aggregations",
            "file_path": str(path),
            "group_by": group_by,
            "agg_column": agg_column,
            "agg_func": agg_func,
            "groups": total_groups,
            **counted(len(result_list), eligible),
            "result": result_list,
            "hint": "Call apply_patch() or run_cleaning_pipeline() to act on findings.",
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("compute_aggregations error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check file_path and column names are correct."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# run_cleaning_pipeline helpers
# ---------------------------------------------------------------------------

# one_hot's two limits, named rather than inline so the response can quote them.
# Ten levels keeps a single column from adding hundreds; five columns keeps one
# call from doubling the frame's width. Both are reported when they bite.
_ONE_HOT_MAX_LEVELS = 10
_ONE_HOT_MAX_COLUMNS = 5

# Distinctive params that uniquely identify an op when "op" is omitted/malformed.
_OP_SIGNATURES: list[tuple[frozenset[str], str]] = [
    (frozenset({"dtype"}), "cast_column"),
    (frozenset({"strategy"}), "fill_nulls"),
    (frozenset({"mapping"}), "replace_values"),
    (frozenset({"expression"}), "add_column"),
    (frozenset({"method", "lower", "upper"}), "cap_outliers"),
    (frozenset({"subset"}), "drop_duplicates"),
]


def _coerce_op(raw: dict) -> dict:
    """Normalise a malformed op dict so 'op' is always a string.

    Handles two common LLM mistakes:
    1. {"op": {"column": "x", "dtype": "float"}, "patch": true}
       → params nested inside "op" key; infer op name from params.
    2. {"column": "x", "dtype": "float"}  (op key missing entirely)
       → same inference from top-level params.
    """
    op_val = raw.get("op", "")
    if isinstance(op_val, str) and op_val:
        return raw  # already correct

    # Collect candidate params: nested dict or top-level keys minus "op"/"patch"
    if isinstance(op_val, dict):
        params = {**op_val}
        # merge any other top-level keys (except "op" and "patch")
        for k, v in raw.items():
            if k not in ("op", "patch"):
                params.setdefault(k, v)
    else:
        params = {k: v for k, v in raw.items() if k not in ("op", "patch")}

    # Try to infer op name from distinctive params
    param_keys = frozenset(params.keys())
    inferred = ""
    for sig_keys, op_name in _OP_SIGNATURES:
        if sig_keys & param_keys:  # any overlap with signature
            inferred = op_name
            break

    return {"op": inferred, **params}


# ---------------------------------------------------------------------------
# run_cleaning_pipeline
# ---------------------------------------------------------------------------


def run_cleaning_pipeline(
    file_path: str,
    ops: list[dict],
    output_path: str = "",
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

        # Every op apply_patch runs, which is every op list_patch_ops
        # advertises. This used to be a hand-written table of eight.
        handler_map = OP_HANDLERS

        # Normalise ops: fix common LLM mistakes (missing op key, params nested
        # inside it). unwrap_params covers the other direction -- {"op": "name",
        # "params": {...}} -- which is the shape list_patch_ops prints, and
        # which apply_patch accepts. The two tools take the same op dicts, so
        # they must accept the same malformed ones.
        ops = [unwrap_params(_coerce_op(o)) for o in ops]

        # Validate all ops before touching the file or creating a snapshot.
        # validate_ops below is what actually names a bad op, against the same
        # vocabulary; reaching this branch means the handler table and the
        # validator have drifted apart, so say that rather than blaming the
        # caller for a name the catalog told them to use.
        unknown_ops = [op.get("op", "") for op in ops if op.get("op", "") not in handler_map]
        if unknown_ops:
            return {
                "success": False,
                "error": f"No handler registered for op(s): {unknown_ops}",
                "hint": "Call list_patch_ops() for the ops this server can run.",
                "applied": 0,
                "progress": [fail("No handler for op(s)", str(unknown_ops))],
                "token_estimate": 20,
            }

        # The op name was checked; its arguments were not, so a missing required
        # key reached the handler as a bare KeyError and was reported verbatim --
        # error "Op 1 (fill_nulls): 'strategy'" says neither what is wrong nor
        # how to fix it, and by then op 0 had been applied and rolled back.
        # validate_ops names the missing key, before anything is written.
        arg_errors = validate_ops(ops)
        if arg_errors:
            return {
                "success": False,
                "error": "; ".join(arg_errors),
                "hint": "Fix the op arguments named above and retry. Nothing was modified.",
                "applied": 0,
                "progress": [fail("Invalid op arguments", str(arg_errors))],
                "token_estimate": 20,
            }

        df = _read_csv(str(path))

        if dry_run:
            would_change = []
            for op in ops:
                op_name = op.get("op", "")
                would_change.append({"op": op_name, "params": op})
            # A dry run must say which file it would have written, or the
            # caller cannot tell an in-place clean from a redirected one.
            would_write = resolve_path(output_path) if output_path else path
            result = {
                "success": True,
                "dry_run": True,
                "op": "run_cleaning_pipeline",
                "file_path": str(path),
                "output_path": str(would_write),
                "total_ops": len(ops),
                "would_change": would_change,
                "progress": [info("Dry run — no changes written", would_write.name)],
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        # Unlike its nine sibling transform tools, this one defaults to cleaning
        # in place -- that is deliberate and unchanged, since a pipeline that
        # snapshots first is meant to advance the file it is given. What was
        # missing is the choice: every sibling accepts output_path, so a caller
        # naturally passes it here too and used to get a hard "unexpected
        # keyword" error. Only snapshot when the source is what gets rewritten.
        out = resolve_path(output_path) if output_path else path
        backup = snapshot(str(path)) if out == path else None
        if backup:
            progress.append(info("Snapshot created", Path(backup).name))

        summary = []
        for i, op in enumerate(ops):
            op_name = op.get("op", "")
            handler = handler_map[op_name]
            try:
                df, op_result = handler(df, op)
                summary.append(op_result)
                progress.append(ok(f"Applied {op_name}", str(op_result)))
            except Exception as exc:
                progress.append(fail(f"Op {i} ({op_name}) failed", str(exc)))
                # Nothing to roll back when the source was never the target:
                # ops run against an in-memory frame, so the file on disk is
                # still untouched at this point.
                if backup:
                    _restore(str(path), backup)
                return {
                    "success": False,
                    "error": f"Op {i} ({op_name}): {exc}",
                    "hint": (
                        "Restored from snapshot. Fix the op and retry."
                        if backup
                        else f"{path.name} was not modified. Fix the op and retry."
                    ),
                    "applied": i,
                    "backup": drop_snapshot_if_unwritten(backup, path, progress),
                    "progress": progress,
                    "token_estimate": _token_estimate(progress),
                }

        df.to_csv(str(out), index=False)

        append_receipt(
            str(path),
            tool="run_cleaning_pipeline",
            args={"ops": ops},
            result=f"applied {len(ops)} ops",
            backup=backup,
        )

        progress.append(ok(f"Saved {out.name}", f"{len(ops)} ops applied"))

        # An op that ran without changing anything says so in a `note`.
        # "applied: N" counts ops that executed, not ops that had an effect, so
        # without this the two are indistinguishable from the top of the
        # response: a fill_nulls over an all-null column came back under
        # "applied: 1" with a hint inviting the caller to verify the changes.
        no_effect = [entry for entry in summary if entry.get("note")]
        for entry in no_effect:
            progress.append(warn(f"{entry['op']} changed nothing", entry["note"]))

        hint = "Call inspect_dataset() or read_column_stats() to verify the changes."
        if no_effect:
            hint = (
                f"{len(no_effect)} of {len(ops)} op(s) ran without changing anything -- read `note` on each "
                "entry of ops_with_no_effect before treating this file as cleaned."
            )
        result = {
            "success": True,
            "op": "run_cleaning_pipeline",
            "file_path": str(path),
            "output_file": out.name,
            "output_path": str(out),
            "total_ops": len(ops),
            "applied": len(ops),
            "ops_with_no_effect": no_effect,
            "summary": summary,
            "backup": backup,
            "hint": hint,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("run_cleaning_pipeline error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Use restore_version to undo if a snapshot was taken."),
            "backup": drop_snapshot_if_unwritten(backup, path),
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
        skipped_plan = []
        for col in target_cols:
            null_count = int(df[col].isna().sum())
            if null_count == 0:
                continue
            s = df[col]
            # Declared scalar: every branch below assigns one, and it keeps
            # pd.isna() from being read as the Series-returning overload.
            fill_val: object = None
            if pd.api.types.is_numeric_dtype(s):
                strategy = "median"
                fill_val = s.median()
            elif pd.api.types.is_datetime64_any_dtype(s):
                strategy = "ffill"
            else:
                strategy = "mode"
                mode_vals = s.mode()
                fill_val = mode_vals.iloc[0] if len(mode_vals) > 0 else None

            entry = {
                "column": col,
                "strategy": strategy,
                "null_count": null_count,
                # The median of no numbers is NaN, and `str(nan)` is "nan" --
                # a fill value that reads like one and fills nothing.
                "fill_value": None if is_missing(fill_val) else str(fill_val),
            }
            # A column whose every value is null has nothing to impute *from*.
            # The median of no numbers is NaN, and fillna(NaN) changes nothing,
            # so this used to be reported as an imputed column that had in fact
            # been left exactly as it was found -- the one case where the
            # caller most needs to be told the tool could not help.
            if not bool(s.notna().any()):
                entry["skipped"] = True
                entry["reason"] = (
                    f"all {null_count} value(s) in '{col}' are null, so there is no {strategy} to fill from"
                )
                skipped_plan.append(entry)
                continue
            imputation_plan.append(entry)

        if dry_run:
            progress.append(info("Dry run — no changes written", path.name))
            result = {
                "success": True,
                "dry_run": True,
                "op": "smart_impute",
                "file_path": str(path),
                "would_change": imputation_plan,
                "columns_to_impute": len(imputation_plan),
                "skipped": skipped_plan,
                "columns_skipped": len(skipped_plan),
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

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

        if output_path:
            out = resolve_path(output_path)
        else:
            out = path.parent / f"{path.stem}_imputed{path.suffix}"
        backup = snapshot(str(path)) if out == path else None
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
        if skipped_plan:
            progress.append(
                warn(
                    "Columns left unchanged",
                    f"{len(skipped_plan)} all-null column(s): {', '.join(e['column'] for e in skipped_plan)}",
                )
            )
        progress.append(ok(f"Imputed {path.name}", f"{len(imputation_plan)} columns filled"))

        hint = "Call inspect_dataset() or read_column_stats() to verify the changes."
        if skipped_plan and not imputation_plan:
            hint = (
                "Nothing was filled: every column with nulls is entirely null. "
                "Drop those columns, or supply values from another source."
            )
        result = {
            "success": True,
            "op": "smart_impute",
            "file_path": str(path),
            "imputed": imputation_plan,
            "columns_imputed": len(imputation_plan),
            "skipped": skipped_plan,
            "columns_skipped": len(skipped_plan),
            "output_file": out.name,
            "output_path": str(out),
            "backup": backup,
            "hint": hint,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("smart_impute error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Use restore_version to undo if a snapshot was taken."),
            "backup": drop_snapshot_if_unwritten(backup, path),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# merge_datasets
# ---------------------------------------------------------------------------

# A join on a non-unique key multiplies rows instead of aligning them. Below the
# hard _MAX_MERGE_ROWS ceiling this still succeeds silently, and the caller has
# no way to tell a legitimate one-to-many expansion from a mistaken key: a real
# run joined 16,834 rows to 7,357 on a date column and wrote 756,755 rows — a
# 165 MB file — reported as a plain success.
_FANOUT_RATIO = 2.0


def _fanout_warning(left_rows: int, right_rows: int, result_rows: int, left_on: str, right_on: str) -> str:
    """Describe a join that multiplied rows, or '' when the size is unremarkable."""
    largest = max(left_rows, right_rows)
    if largest == 0 or result_rows <= largest * _FANOUT_RATIO:
        return ""
    key = left_on if left_on == right_on else f"{left_on}/{right_on}"
    return (
        f"{result_rows:,} rows out of {left_rows:,} × {right_rows:,} — "
        f"{result_rows / largest:.1f}× the larger input, because '{key}' repeats on both sides. "
        "Deduplicate the key or join on a more selective column if you expected row alignment."
    )


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

        # A join key that isn't actually unique on either side fans out
        # combinatorially (e.g. two ~7K-row tables sharing a low-cardinality
        # key can produce tens of millions of result rows) — pandas.merge()
        # will happily materialize that in memory with no limit, which OOM-
        # kills the whole shared container (every other concurrent request
        # dies with it), not just this call. Estimate the matched-row count
        # from value_counts — cheap — before running the real merge, which
        # is not. Found live via the opencode harness real-tool retest
        # sweep: a badly-keyed merge crashed and repeatedly restarted the
        # container (RestartCount climbed to 4, confirmed via `dmesg`
        # oom-kill entries for the server's python process).
        _MAX_MERGE_ROWS = 2_000_000
        left_counts = left_df[left_on].astype(str).value_counts()
        right_counts = right_df[right_on].astype(str).value_counts()
        common = set(left_counts.index) & set(right_counts.index)
        estimated_rows = sum(int(left_counts[k]) * int(right_counts[k]) for k in common)
        estimated_rows += len(left_df) + len(right_df)  # conservative allowance for unmatched rows
        if estimated_rows > _MAX_MERGE_ROWS:
            return {
                "success": False,
                "error": f"Join would produce an estimated {estimated_rows:,} rows (limit {_MAX_MERGE_ROWS:,}).",
                "hint": (
                    f"'{left_on}'/'{right_on}' isn't unique enough on one or both sides for this join — "
                    "check for a more selective key, or deduplicate first with run_cleaning_pipeline."
                ),
                "progress": [fail("Join too large", f"~{estimated_rows:,} estimated rows")],
                "token_estimate": 40,
            }

        merged = left_df.merge(
            right_df,
            left_on=left_on,
            right_on=right_on,
            how=how,
            suffixes=("", "_right"),
            indicator="_merge_side",
        )
        if left_on != right_on and right_on in merged.columns:
            merged = merged.drop(columns=[right_on])

        # This was `merged[left_on].notna().sum()` -- the count of rows whose
        # JOIN KEY is present. On a left join the key comes from the left frame
        # and is therefore never null, so `matched` was identical to
        # `result_rows` on every call: three result rows of which two found a
        # partner were reported as "matched: 3". The one number a caller checks
        # to find out whether the join worked was the one number that could not
        # say. pandas' own indicator answers it exactly.
        rows_matched = int((merged["_merge_side"] == "both").sum())
        merged = merged.drop(columns=["_merge_side"])
        fanout = _fanout_warning(len(left_df), len(right_df), len(merged), left_on, right_on)
        if fanout:
            progress.append(warn("Join fanned out", fanout))

        if dry_run:
            progress.append(info("Dry run — no changes written", path.name))
            result = {
                "success": True,
                "dry_run": True,
                "op": "merge_datasets",
                "file_path": str(path),
                "left_rows": len(left_df),
                "right_rows": len(right_df),
                "result_rows": len(merged),
                "matched": rows_matched,
                "unmatched_left": unmatched_left,
                "unmatched_right": unmatched_right,
                "how": how,
                "progress": progress,
            }
            if fanout:
                result["warning"] = fanout
            result["token_estimate"] = _token_estimate(result)
            return result

        if output_path:
            out = resolve_path(output_path)
        else:
            out = path.parent / f"{path.stem}_merged{path.suffix}"
        backup = snapshot(str(path)) if out == path else None
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
            "file_path": str(path),
            "left_rows": len(left_df),
            "right_rows": len(right_df),
            "result_rows": len(merged),
            "matched": rows_matched,
            "unmatched_left": unmatched_left,
            "unmatched_right": unmatched_right,
            "how": how,
            "output_file": out.name,
            "output_path": str(out),
            "backup": backup,
            "hint": "Call inspect_dataset() or read_column_stats() to verify the changes.",
            "progress": progress,
        }
        if fanout:
            result["warning"] = fanout
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("merge_datasets error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Use restore_version to undo if a snapshot was taken."),
            "backup": drop_snapshot_if_unwritten(backup, path),
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
    derive: list[dict] = None,
) -> dict:
    """features: 'date_parts','bins','text_length','one_hot'. derive: named specs."""
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
        if features:
            requested = set(features)
        elif derive:
            # A caller naming exact derivations is not also asking for one-hot
            # encoding of every text column; "all four families" is only the
            # default when nothing else was asked for.
            requested = set()
        else:
            requested = valid_features
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
        one_hot_encoded: list[str] = []
        one_hot_skipped: dict[str, str] = {}

        if derive:
            try:
                derived, derive_progress = apply_derivations(df, derive)
            except DeriveError as exc:
                return {
                    "success": False,
                    "error": str(exc),
                    "hint": (
                        "A derive spec is {'name': new_column, 'op': parse_date|date_part|arith|"
                        "compare|text, 'column': source, ...}. Call inspect_dataset() for column names."
                    ),
                    "progress": [fail("Invalid derivation", str(exc))],
                    "token_estimate": 20,
                }
            new_columns.extend(derived)
            progress.extend(derive_progress)

        if "date_parts" in requested:
            date_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
            parsed_cols = {c: df[c] for c in date_cols}
            # A plain CSV read never produces datetime64 columns on its own —
            # string date columns (e.g. "2019-10-16") were silently skipped
            # here, producing zero date-part columns with no error. Sniff
            # object columns the same way auto_detect_schema does (parse a
            # sample, keep it if it parses clean) before giving up on them.
            for col in df.columns:
                if col in parsed_cols or not _is_string_col(df[col]):
                    continue
                # Same rule as every other date guess in this repo.
                is_dates, _match, _meta = looks_like_dates(df[col])
                if not is_dates:
                    continue
                parsed_cols[col] = pd.to_datetime(df[col], errors="coerce")
                date_cols.append(col)
            for col in date_cols:
                for part in ("year", "month", "day", "dayofweek"):
                    new_col = f"{col}_{part}"
                    df[new_col] = getattr(parsed_cols[col].dt, part)
                    new_columns.append(new_col)

        if "text_length" in requested:
            text_cols = [c for c in df.columns if _is_string_col(df[c])]
            for col in text_cols:
                new_col = f"{col}_len"
                df[new_col] = df[col].astype(str).str.len()
                new_columns.append(new_col)

        if "bins" in requested:
            num_cols = [c for c in df.columns if is_numeric_col(df[c]) and c not in new_columns]
            for col in num_cols[:5]:
                try:
                    new_col = f"{col}_bin"
                    df[new_col] = pd.qcut(df[col], q=4, labels=["Q1", "Q2", "Q3", "Q4"], duplicates="drop")
                    new_columns.append(new_col)
                except Exception:
                    pass

        # Two limits sat here and neither was ever reported: text columns with
        # more than ten distinct values were skipped, and of whatever survived
        # only the first five were encoded. On the reference dataset that meant
        # 5 of 12 text columns encoded, a response reading "8 new columns", and
        # nothing at all about the seven it declined -- a caller who asked for
        # one-hot encoding got a partly encoded frame and no way to know.
        #
        # Both limits stay: one-hot on 16,834 distinct creative names is not
        # what anyone means. What changes is that the tool says so.
        if "one_hot" in requested:
            text_cols = [c for c in df.columns if _is_string_col(df[c]) and c not in new_columns]
            too_many = {c: int(df[c].nunique()) for c in text_cols if df[c].nunique() > _ONE_HOT_MAX_LEVELS}
            cat_cols = [c for c in text_cols if c not in too_many]
            over_cap = cat_cols[_ONE_HOT_MAX_COLUMNS:]
            for col in cat_cols[:_ONE_HOT_MAX_COLUMNS]:
                dummies = pd.get_dummies(df[col], prefix=col, drop_first=False).astype(int)
                df = pd.concat([df, dummies], axis=1)
                new_columns.extend(dummies.columns.tolist())
                one_hot_encoded.append(col)
            for col, levels in too_many.items():
                one_hot_skipped[col] = f"{levels} distinct values, above the {_ONE_HOT_MAX_LEVELS} one-hot allows"
            for col in over_cap:
                one_hot_skipped[col] = f"only the first {_ONE_HOT_MAX_COLUMNS} eligible columns are encoded per call"
            if one_hot_skipped:
                progress.append(
                    warn(
                        f"{len(one_hot_skipped)} column(s) not one-hot encoded",
                        "; ".join(f"{c}: {why}" for c, why in one_hot_skipped.items()),
                    )
                )

        if dry_run:
            progress.append(info("Dry run — no changes written", path.name))
            result = {
                "success": True,
                "dry_run": True,
                "op": "feature_engineering",
                "file_path": str(path),
                "would_add": new_columns,
                "features_requested": list(requested),
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        if output_path:
            out = resolve_path(output_path)
        else:
            out = path.parent / f"{path.stem}_features{path.suffix}"
        backup = snapshot(str(path)) if out == path else None
        df.to_csv(str(out), index=False)

        if open_after:
            _open_file(out)

        append_receipt(
            str(path),
            tool="feature_engineering",
            args={"features": list(requested), "derive": [d.get("name") for d in (derive or [])]},
            result=f"added {len(new_columns)} columns",
            backup=backup,
        )
        progress.append(ok(f"Features added to {path.name}", f"{len(new_columns)} new columns"))

        result = {
            "success": True,
            "op": "feature_engineering",
            "file_path": str(path),
            "features_applied": list(requested),
            "new_columns": new_columns,
            "columns_added": len(new_columns),
            # Which text columns one_hot actually reached, and why it left the
            # rest alone. Empty unless one_hot was requested.
            "one_hot_encoded": one_hot_encoded,
            "one_hot_skipped": one_hot_skipped,
            "output_file": out.name,
            "output_path": str(out),
            "backup": backup,
            "hint": "Call inspect_dataset() or read_column_stats() to verify the changes.",
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("feature_engineering error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Use restore_version to undo if a snapshot was taken."),
            "backup": drop_snapshot_if_unwritten(backup, path),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# resample_timeseries
# ---------------------------------------------------------------------------

_FREQ_MAP = {"M": "ME", "Q": "QE", "Y": "YE", "W": "W", "D": "D", "H": "h"}
_VALID_FREQS = frozenset(_FREQ_MAP.keys())
_VALID_AGGS = frozenset({"sum", "mean", "count", "min", "max", "median", "std", "first", "last"})


def resample_timeseries(
    file_path: str,
    date_col: str = "",
    freq: str = "M",
    agg_func: str = "sum",
    value_cols: list[str] = None,
    group_by: str = None,
    output_path: str = "",
    dry_run: bool = False,
    date_column: str = "",
    value_columns: list[str] = None,
    dayfirst: str = "auto",
) -> dict:
    progress = []
    # Four sibling tools spell these date_column and value_columns.
    date_col, note = pick("resample_timeseries", "date_col", date_col, date_column)
    if not date_col:
        return missing("resample_timeseries", "date_col", "date_column")
    if note:
        progress.append(info("Argument alias", note))
    if not value_cols and value_columns:
        value_cols = list(value_columns)
        progress.append(info("Argument alias", "Read value_cols from an accepted alternative spelling"))
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

        if freq not in _VALID_FREQS:
            return {
                "success": False,
                "error": f"Invalid freq: {freq}",
                "hint": f"Valid: {', '.join(sorted(_VALID_FREQS))}",
                "progress": [fail("Invalid freq", freq)],
                "token_estimate": 20,
            }

        agg_funcs = [a.strip() for a in agg_func.split(",")]
        invalid_aggs = [a for a in agg_funcs if a not in _VALID_AGGS]
        if invalid_aggs:
            return {
                "success": False,
                "error": f"Invalid agg_func: {invalid_aggs}",
                "hint": f"Valid: {', '.join(sorted(_VALID_AGGS))}",
                "progress": [fail("Invalid agg_func", str(invalid_aggs))],
                "token_estimate": 20,
            }

        df = _read_csv(str(path))

        if date_col not in df.columns:
            return {
                "success": False,
                "error": f"date_col '{date_col}' not found",
                "hint": f"Available columns: {', '.join(df.columns)}",
                "progress": [fail("Column not found", date_col)],
                "token_estimate": 20,
            }

        df[date_col], _fmt = parse_dates(df[date_col], dayfirst)
        progress.append(date_note(_fmt, date_col))
        null_dates = int(df[date_col].isna().sum())
        if null_dates > 0:
            progress.append(warn(f"Dropped {null_dates} rows", "unparseable dates"))
            df = df.dropna(subset=[date_col])

        if not value_cols:
            value_cols = [c for c in df.columns if is_numeric_col(df[c]) and c != date_col]

        missing_vc = [c for c in value_cols if c not in df.columns]
        if missing_vc:
            return {
                "success": False,
                "error": f"value_cols not found: {missing_vc}",
                "hint": f"Available: {', '.join(df.columns)}",
                "progress": [fail("Columns not found", str(missing_vc))],
                "token_estimate": 20,
            }

        pd_freq = _FREQ_MAP[freq]

        def _agg_group(sub: pd.DataFrame) -> pd.DataFrame:
            sub = sub.set_index(date_col).sort_index()
            parts = []
            for col in value_cols:
                rs = sub[[col]].resample(pd_freq)
                for af in agg_funcs:
                    agged = getattr(rs, af)()
                    col_label = f"{col}_{af}" if len(agg_funcs) > 1 else col
                    agged.columns = [col_label]
                    parts.append(agged)
            return pd.concat(parts, axis=1) if parts else sub[value_cols].resample(pd_freq).sum()

        if dry_run:
            out_cols = [f"{c}_{af}" for c in value_cols for af in agg_funcs] if len(agg_funcs) > 1 else value_cols
            result = {
                "success": True,
                "dry_run": True,
                "op": "resample_timeseries",
                "date_col": date_col,
                "freq": freq,
                "agg_func": agg_func,
                "value_cols": value_cols,
                "group_by": group_by,
                "would_produce_columns": out_cols,
                "progress": [info("Dry run — no changes written", path.name)],
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        if group_by:
            if group_by not in df.columns:
                return {
                    "success": False,
                    "error": f"group_by column '{group_by}' not found",
                    "hint": f"Available: {', '.join(df.columns)}",
                    "progress": [fail("Column not found", group_by)],
                    "token_estimate": 20,
                }
            parts = []
            for grp_val, sub in df.groupby(group_by):
                resampled = _agg_group(sub.drop(columns=[group_by]))
                resampled[group_by] = grp_val
                parts.append(resampled.reset_index())
            result_df = pd.concat(parts, ignore_index=True)
        else:
            result_df = _agg_group(df).reset_index()

        total_periods = len(result_df)
        progress.append(ok(f"Resampled {path.name}", f"{total_periods} periods (freq={freq}, agg={agg_func})"))

        out_path = (
            str(resolve_path(output_path)) if output_path else str(path.parent / f"{path.stem}_resampled_{freq}.csv")
        )
        result_df.to_csv(out_path, index=False)

        # The defect `compute_aggregations` was fixed for, in this same file,
        # left standing here: a second hardcoded cap did the real cutting while
        # `truncated` was computed against the other one. Resampled yearly, the
        # SFO cargo file returned 20 of 25 periods under `truncated: false` --
        # true of the FILE, which is under get_max_rows(), and not of the
        # `data` the caller was actually reading.
        #
        # `counted` derives the flag from the two numbers it ships beside, so
        # the pair cannot disagree again. `total_periods` stays for callers
        # already reading it.
        max_r = get_max_rows()
        sample = result_df.head(max_r).fillna("").to_dict(orient="records")
        for rec in sample:
            for k, v in list(rec.items()):
                if hasattr(v, "isoformat"):
                    rec[k] = v.isoformat()
        if len(sample) < total_periods:
            progress.append(
                warn(
                    "Preview truncated",
                    f"Showing {len(sample)} of {total_periods} periods. The full result is in {Path(out_path).name}.",
                )
            )

        result = {
            "success": True,
            "op": "resample_timeseries",
            "file_path": str(path),
            "date_col": date_col,
            "freq": freq,
            "agg_func": agg_func,
            "value_cols": value_cols,
            "group_by": group_by,
            "total_periods": total_periods,
            **counted(len(sample), total_periods),
            "data": sample,
            "output_path": out_path,
            "output_name": Path(out_path).name,
            "hint": "Use filter_date_range patch op or pass value_cols to narrow the result.",
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("resample_timeseries error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Ensure date_col is parseable as datetime and value_cols are numeric."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# concat_datasets
# ---------------------------------------------------------------------------


def concat_datasets(
    file_paths: list[str],
    direction: str = "rows",
    fill_missing: str = "null",
    add_source_column: bool = True,
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    progress = []
    try:
        if not file_paths or len(file_paths) < 2:
            return {
                "success": False,
                "error": "At least 2 file_paths required.",
                "hint": "Pass a list of 2+ absolute CSV file paths.",
                "progress": [fail("Too few files", str(file_paths))],
                "token_estimate": 20,
            }

        if direction not in ("rows", "columns"):
            return {
                "success": False,
                "error": f"Invalid direction: {direction}",
                "hint": "Valid: rows, columns",
                "progress": [fail("Invalid direction", direction)],
                "token_estimate": 20,
            }

        paths = []
        for fp in file_paths:
            p = resolve_path(fp)
            if not p.exists():
                return {
                    "success": False,
                    "error": f"File not found: {p.name}",
                    "hint": f"Check path: {fp}",
                    "progress": [fail("File not found", p.name)],
                    "token_estimate": 20,
                }
            paths.append(p)

        frames = [_read_csv(str(p)) for p in paths]

        if dry_run:
            schemas = [{"file": p.name, "rows": len(fr), "columns": list(fr.columns)} for p, fr in zip(paths, frames)]
            result = {
                "success": True,
                "dry_run": True,
                "op": "concat_datasets",
                "direction": direction,
                "files": [p.name for p in paths],
                "schemas": schemas,
                "progress": [info("Dry run — no changes written", "")],
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        if direction == "rows":
            if add_source_column:
                for p, fr in zip(paths, frames):
                    fr["__source"] = p.name
            if fill_missing == "drop":
                common = list(set.intersection(*(set(fr.columns) for fr in frames)))
                frames = [fr[common] for fr in frames]
            result_df = pd.concat(frames, ignore_index=True)
            detail = f"{len(result_df)} rows from {len(paths)} files"
        else:
            row_counts = [len(fr) for fr in frames]
            if len(set(row_counts)) > 1:
                return {
                    "success": False,
                    "error": f"Column concat requires equal row counts. Got: {row_counts}",
                    "hint": "Use direction='rows' or ensure all files have the same number of rows.",
                    "progress": [fail("Row count mismatch", str(row_counts))],
                    "token_estimate": 20,
                }
            result_df = pd.concat([fr.reset_index(drop=True) for fr in frames], axis=1)
            detail = f"{len(result_df.columns)} total columns from {len(paths)} files"

        progress.append(ok(f"Concatenated {len(paths)} files", detail))

        first_path = paths[0]
        out_path = (
            str(resolve_path(output_path)) if output_path else str(first_path.parent / f"{first_path.stem}_concat.csv")
        )
        result_df.to_csv(out_path, index=False)

        result = {
            "success": True,
            "op": "concat_datasets",
            "direction": direction,
            "files": [p.name for p in paths],
            "rows": len(result_df),
            "columns": len(result_df.columns),
            "output_path": out_path,
            "output_name": Path(out_path).name,
            "hint": "Use inspect_dataset() on the output to verify the result.",
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("concat_datasets error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check all file_paths are absolute and point to valid CSVs."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }
