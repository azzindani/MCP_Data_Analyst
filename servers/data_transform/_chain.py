"""run_chain: named steps over several tables, and values computed on the way, in one call.

A job like "clean the orders, join the customers, total by region, save it"
used to take one call per step, each re-reading the file the last one wrote,
or a script. A chain says the whole job as data: every step has an `id`,
reads an earlier step by that id (`from`, or the step above it), and a
`scalar` step computes one value that later formulas use as `$id`.

The whole chain is checked before any file is read -- every id, every
reference, every op's fields -- so a typo in step 7 costs nothing. Nothing is
written until every step has run, so a chain that fails half way leaves
every file as it was.
"""

from __future__ import annotations

import copy
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]
_MED = str(Path(__file__).resolve().parents[1] / "data_medium")
_DATA_BASIC = str(Path(__file__).resolve().parents[1] / "data_basic")
for _p in (str(_ROOT), _MED, _DATA_BASIC):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import numpy as np
import pandas as pd
from _med_transform import _MAX_MERGE_ROWS, _row_bytes  # type: ignore[import]
from _patch_ops import OP_HANDLERS, note_non_finite  # type: ignore[import-not-found]

from shared.counts import counted
from shared.exchange import attach_public_url
from shared.expr import FormulaError, aggregate, evaluate, substitute, variables_in
from shared.file_utils import (
    PathOutsideRootError,
    atomic_write_text,
    error_text,
    read_csv_preserving_ids,
    resolve_path,
)
from shared.lineage import note_lineage
from shared.patch_validator import VALID_OPS, _did_you_mean, validate_ops
from shared.platform_utils import get_max_columns, get_max_merge_bytes, get_max_rows
from shared.progress import fail, info, ok, warn
from shared.receipt import append_receipt
from shared.version_control import snapshot

logger = logging.getLogger(__name__)

MAX_STEPS = 50
ACTIONS = ("load", "ops", "scalar", "join", "group_by", "write")
_FIELDS = {
    "load": {"id", "load", "on_error"},
    "ops": {"id", "from", "ops", "fallback", "on_error"},
    "scalar": {"id", "from", "scalar", "on_error"},
    "join": {"id", "join", "on", "how", "on_error"},
    "group_by": {"id", "from", "group_by", "agg", "on_error"},
    "write": {"id", "from", "write", "on_error"},
}
HOW = ("left", "inner", "outer", "right")
ON_ERROR = ("stop", "skip")
# Ops only a chain has: a row filter and a derived column, both written in the
# formula language. Every other op is one of apply_patch's.
NATIVE_OPS = {"filter": ("where",), "derive": ("name", "expr")}
_ID = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_WHOLE_VARIABLE = re.compile(r"^\$([A-Za-z_]\w*)$")
_ANY_VARIABLE = re.compile(r"\$([A-Za-z_]\w*)")
_SAMPLE_ROWS = 3
_FANOUT = 2.0

STEP_SHAPE = (
    "A step is {'id': name, one of load | ops | scalar | join | group_by | write, "
    "'from': an earlier id (default: the table above it)}."
)


class StepError(Exception):
    """A step that cannot do its job, said as the step's own problem."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


def _token_estimate(obj: object) -> int:
    return len(str(obj)) // 4


# ---------------------------------------------------------------------------
# The plan: the whole chain checked before any file is read
# ---------------------------------------------------------------------------


def _names(value: Any) -> list[str] | None:
    """A column name or a list of them, as a list; None if it is neither."""
    if isinstance(value, str) and value.strip():
        return [value]
    if isinstance(value, list) and value and all(isinstance(v, str) and v.strip() for v in value):
        return list(value)
    return None


def _placeholder(value: Any, scalars: set[str]) -> Any:
    """`value` with each $name a scalar step defines replaced by a stand-in number, for validation."""
    if isinstance(value, dict):
        return {k: _placeholder(v, scalars) for k, v in value.items()}
    if isinstance(value, list):
        return [_placeholder(v, scalars) for v in value]
    if isinstance(value, str):
        whole = _WHOLE_VARIABLE.match(value)
        if whole and whole.group(1) in scalars:
            return 0.0
    return value


class _Planner:
    def __init__(self) -> None:
        self.errors: list[str] = []
        self.missing_file: tuple[str, str] | None = None
        self.kinds: dict[str, str] = {}
        self.scalars: set[str] = set()
        self.later: set[str] = set()
        self.targets: dict[Path, str] = {}

    def table(self, where: str, ref: Any) -> str | None:
        """`ref` if it names an earlier step that holds a table; else record why not."""
        if not isinstance(ref, str) or not ref:
            self.errors.append(f"{where}: a step id must be a name, got {ref!r}")
            return None
        kind = self.kinds.get(ref)
        if kind is None:
            if ref in self.later:
                self.errors.append(f"{where} reads {ref!r}, which comes after it; a step reads only steps above it")
            else:
                guess = _did_you_mean(ref, list(self.kinds))
                lead = f"did you mean {guess!r}? " if guess else ""
                self.errors.append(
                    f"{where} reads {ref!r}, which is not a step -- {lead}steps above it: "
                    f"{', '.join(self.kinds) or 'none'}"
                )
            return None
        if kind == "scalar":
            self.errors.append(f"{where} reads {ref!r}, a scalar step: use its value inside a formula as ${ref}")
            return None
        return ref if kind != "?" else None

    def variables(self, where: str, formula: Any, field: str) -> None:
        if not isinstance(formula, str) or not formula.strip():
            self.errors.append(f"{where}: {field} must be a formula, e.g. 'amount * (1 - discount)'")
            return
        for name in variables_in(formula):
            if name not in self.scalars:
                known = ", ".join(f"${s}" for s in sorted(self.scalars)) or "none yet"
                self.errors.append(
                    f"{where}: {field} uses ${name}, which no scalar step above it computes (variables: {known})"
                )

    def ops(self, where: str, ops: Any, field: str) -> None:
        if not isinstance(ops, list) or not ops:
            self.errors.append(
                f"{where}: {field} must be a non-empty list of ops, e.g. [{{'op': 'filter', 'where': 'amount > 0'}}]"
            )
            return
        for j, op in enumerate(ops):
            at = f"{where} {field}[{j}]"
            if not isinstance(op, dict):
                self.errors.append(f"{at}: an op is an object with an 'op' field, got {type(op).__name__}")
                continue
            name = op.get("op")
            if name in NATIVE_OPS:
                needed = NATIVE_OPS[name]
                unknown = sorted(set(op) - {"op", *needed})
                if unknown:
                    guess = _did_you_mean(unknown[0], list(needed))
                    lead = f"did you mean {guess}? " if guess else ""
                    self.errors.append(
                        f"{at} ({name}): unknown field(s) {', '.join(unknown)} -- {lead}{name} takes: {', '.join(needed)}"
                    )
                    continue
                if name == "derive" and not (isinstance(op.get("name"), str) and op["name"].strip()):
                    self.errors.append(f"{at} (derive): name must be the new column's name")
                formula_field = "where" if name == "filter" else "expr"
                self.variables(f"{at} ({name})", op.get(formula_field), formula_field)
                continue
            if name not in VALID_OPS:
                known = sorted(VALID_OPS | set(NATIVE_OPS))
                guess = _did_you_mean(str(name), known) if isinstance(name, str) else ""
                lead = f"did you mean {guess!r}? " if guess else ""
                self.errors.append(f"{at}: unknown op {name!r} -- {lead}ops: {', '.join(known)}")
                continue
            for problem in validate_ops([_placeholder(op, self.scalars)]):
                self.errors.append(f"{at}{problem.removeprefix('Op 0')}")

    def path(self, where: str, raw: Any, field: str) -> Path | None:
        if not isinstance(raw, str) or not raw.strip():
            self.errors.append(f"{where}: {field} must be a file path, e.g. 'sales.csv'")
            return None
        try:
            return resolve_path(raw.strip())
        except PathOutsideRootError as exc:
            self.errors.append(f"{where}: {exc}")
        except Exception as exc:
            self.errors.append(f"{where}: {field} {raw!r} cannot be used: {error_text(exc)}")
        return None


def plan(steps: Any, until: str = "") -> tuple[list[dict[str, Any]], list[str], tuple[str, str] | None]:
    """(planned steps, problems, (step id, file name) of a missing load) -- nothing is read."""
    p = _Planner()
    if not isinstance(steps, list) or not steps:
        return [], [f"steps is empty. {STEP_SHAPE} Example: [{{'id': 'sales', 'load': 'sales.csv'}}]"], None
    if len(steps) > MAX_STEPS:
        return (
            [],
            [f"{len(steps)} steps; a chain holds at most {MAX_STEPS}. Split it and write the middle table."],
            None,
        )
    p.later = {s["id"] for s in steps if isinstance(s, dict) and isinstance(s.get("id"), str)}
    planned: list[dict[str, Any]] = []
    last_table = ""
    for i, raw in enumerate(steps):
        if not isinstance(raw, dict):
            p.errors.append(f"steps[{i}] must be an object. {STEP_SHAPE}")
            continue
        sid = raw.get("id", f"step{i + 1}")
        if not isinstance(sid, str) or not _ID.match(sid):
            p.errors.append(
                f"steps[{i}]: id {sid!r} must be letters, digits and _, not starting with a digit, "
                "so later steps can read it by name and as $id"
            )
            continue
        where = f"step {sid!r}"
        if sid in p.kinds:
            p.errors.append(f"{where}: the id is used twice; each id names one step")
            continue
        present = [a for a in ACTIONS if a in raw]
        if len(present) != 1:
            p.kinds[sid] = "?"
            if present:
                p.errors.append(
                    f"{where} has {' and '.join(present)}: one action per step, so split it in {len(present)}"
                )
            else:
                extra = sorted(set(raw) - {"id", "from", "on_error"})
                guess = next((g for g in (_did_you_mean(k, list(ACTIONS)) for k in extra) if g), "")
                lead = f" -- did you mean {guess}?" if guess else ""
                p.errors.append(f"{where} does nothing: give it one of {', '.join(ACTIONS)}{lead}")
            continue
        kind = present[0]
        unknown = sorted(set(raw) - _FIELDS[kind])
        if unknown:
            guess = _did_you_mean(unknown[0], sorted(_FIELDS[kind]))
            lead = f"did you mean {guess}? " if guess else ""
            p.errors.append(
                f"{where} ({kind}): unknown field(s) {', '.join(unknown)} -- {lead}"
                f"a {kind} step takes: {', '.join(sorted(_FIELDS[kind]))}"
            )
        on_error = raw.get("on_error", "stop")
        if on_error not in ON_ERROR:
            p.errors.append(f"{where}: on_error {on_error!r} -- use 'stop' (the default) or 'skip'")
        step: dict[str, Any] = {"id": sid, "kind": kind, "on_error": on_error, "raw": raw, "reads": []}
        where = f"{where} ({kind})"
        if kind == "load":
            path = p.path(where, raw["load"], "load")
            if path is not None and not path.is_file():
                if p.missing_file is None:
                    p.missing_file = (sid, path.name)
                p.errors.append(f"{where}: {path.name!r} does not exist")
            step["path"] = path
        elif kind == "join":
            pair = raw["join"]
            if not (isinstance(pair, list) and len(pair) == 2):
                p.errors.append(f"{where}: join takes the ids of two steps above it, e.g. ['orders', 'customers']")
            else:
                reads = [p.table(where, ref) for ref in pair]
                step["reads"] = [r for r in reads if r]
            on = _names(raw.get("on"))
            if on is None:
                p.errors.append(f"{where}: on must name the key column(s) both tables share, e.g. 'customer_id'")
            step["on"] = on or []
            how = raw.get("how", "left")
            if how not in HOW:
                p.errors.append(f"{where}: how {how!r} -- use one of {', '.join(HOW)}")
            step["how"] = how
        else:
            ref = raw.get("from", last_table)
            if not ref:
                p.errors.append(f"{where} has no table to read: no step above it made one. Load a file first.")
            else:
                src = p.table(where, ref)
                step["reads"] = [src] if src else []
            if kind == "ops":
                p.ops(where, raw["ops"], "ops")
                if "fallback" in raw:
                    p.ops(where, raw["fallback"], "fallback")
            elif kind == "scalar":
                p.variables(where, raw["scalar"], "scalar")
            elif kind == "group_by":
                by = _names(raw["group_by"])
                if by is None:
                    p.errors.append(f"{where}: group_by must name one column or a list of them")
                step["by"] = by or []
                agg = raw.get("agg")
                if not (isinstance(agg, dict) and agg):
                    p.errors.append(
                        f"{where}: agg must map each new column to an aggregate, "
                        "e.g. {'revenue': 'sum(net)', 'orders': 'count()'}"
                    )
                else:
                    for name, formula in agg.items():
                        if name in (by or []):
                            p.errors.append(f"{where}: agg {name!r} is also a group_by column; give it another name")
                        p.variables(where, formula, f"agg {name!r}")
            elif kind == "write":
                path = p.path(where, raw["write"], "write")
                if path is not None:
                    if path.suffix.lower() != ".csv":
                        p.errors.append(
                            f"{where}: a chain writes .csv; for {path.suffix or 'no extension'} write the csv, "
                            "then convert it with export_data"
                        )
                    elif path.is_dir():
                        p.errors.append(f"{where}: {path.name!r} is a folder; name a .csv file")
                    elif path in p.targets:
                        p.errors.append(f"{where}: {path.name!r} is written by step {p.targets[path]!r} too")
                    else:
                        p.targets[path] = sid
                step["path"] = path
        p.kinds[sid] = kind
        if kind == "scalar":
            p.scalars.add(sid)
        else:
            last_table = sid
        planned.append(step)
    if until and until not in p.kinds:
        guess = _did_you_mean(until, list(p.kinds))
        lead = f"did you mean {guess!r}? " if guess else ""
        p.errors.append(f"until {until!r} is not a step -- {lead}steps: {', '.join(p.kinds)}")
    return planned, p.errors, p.missing_file


# ---------------------------------------------------------------------------
# Running the steps
# ---------------------------------------------------------------------------


def _plain(value: Any) -> Any:
    """A numpy or pandas scalar as the JSON value it stands for."""
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and not np.isfinite(value):
        return None
    return value


def _records(df: pd.DataFrame, rows: int) -> list[dict[str, Any]]:
    view = df.iloc[:rows, : get_max_columns()]
    return json.loads(view.to_json(orient="records", date_format="iso", default_handler=str))


def _bind(value: Any, values: dict[str, Any]) -> Any:
    """An op with each $name an earlier scalar computed written as its value."""
    if isinstance(value, dict):
        return {k: _bind(v, values) for k, v in value.items()}
    if isinstance(value, list):
        return [_bind(v, values) for v in value]
    if not isinstance(value, str):
        return value
    whole = _WHOLE_VARIABLE.match(value)
    if whole and whole.group(1) in values:
        return _plain(values[whole.group(1)])

    def inline(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in values:
            return match.group(0)
        plain = _plain(values[name])
        return repr(float(plain)) if isinstance(plain, (int, float)) and not isinstance(plain, bool) else str(plain)

    return _ANY_VARIABLE.sub(inline, value)


def _is_condition(mask: pd.Series) -> bool:
    if pd.api.types.is_bool_dtype(mask):
        return True
    present = mask.dropna()
    return bool(len(present) == 0 or present.map(type).isin([bool, np.bool_]).all())


def _filter(df: pd.DataFrame, where: str) -> tuple[pd.DataFrame, str]:
    mask = evaluate(where, df)
    if not _is_condition(mask):
        raise FormulaError(
            f"where {where!r} gives {mask.dtype} values; it must be true or false per row, e.g. 'amount > 0'."
        )
    kept = df[mask.fillna(False).astype(bool).to_numpy()].reset_index(drop=True)
    return kept, (f"no row matched {where!r}" if len(kept) == 0 and len(df) else "")


def _derive(df: pd.DataFrame, name: str, expr: str) -> tuple[pd.DataFrame, str]:
    replaced = name in df.columns
    df[name] = evaluate(expr, df)
    result = note_non_finite(df, {"op": "derive", "name": name, "null_count": int(df[name].isna().sum())})
    notes = [f"replaced the existing column {name!r}"] if replaced else []
    if result.get("warning"):
        notes.append(result["warning"])
    return df, "; ".join(notes)


def _apply_ops(
    src: pd.DataFrame, ops: list[dict], values: dict[str, Any], field: str
) -> tuple[pd.DataFrame, list[str]]:
    df = src.copy()
    notes: list[str] = []
    for j, raw in enumerate(ops):
        op = _bind(copy.deepcopy(raw), values)
        name = op.get("op", "")
        at = f"{field}[{j}] ({name})"
        try:
            if name == "filter":
                df, note = _filter(df, substitute(raw["where"], values))
            elif name == "derive":
                df, note = _derive(df, op["name"], substitute(raw["expr"], values))
            else:
                problems = validate_ops([op])
                if problems:
                    raise StepError(f"{field}[{j}]{problems[0].removeprefix('Op 0')}")
                df, result = OP_HANDLERS[name](df, op)
                result = note_non_finite(df, result)
                note = "; ".join(str(result[k]) for k in ("note", "warning") if result.get(k))
        except StepError:
            raise
        except Exception as exc:
            raise StepError(f"{at}: {error_text(exc)}") from exc
        if note:
            notes.append(f"{at}: {note}")
    return df, notes


def _shape(step: dict[str, Any], df: pd.DataFrame, before: pd.DataFrame | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"id": step["id"], "kind": step["kind"]}
    if step["reads"]:
        out["from"] = step["reads"] if step["kind"] == "join" else step["reads"][0]
    out["rows"] = len(df)
    out["columns"] = len(df.columns)
    if before is not None:
        cap = get_max_columns()
        added = [str(c) for c in df.columns if c not in before.columns]
        dropped = [str(c) for c in before.columns if c not in df.columns]
        if added:
            out["added"] = added[:cap]
        if dropped:
            out["dropped"] = dropped[:cap]
        if len(df) != len(before):
            out["rows_before"] = len(before)
    return out


def _join_rows(left: pd.DataFrame, right: pd.DataFrame, on: list[str], how: str) -> int:
    """How many rows the join builds, counted from the keys alone."""
    lk = left.groupby(on, dropna=False).size()
    rk = right.groupby(on, dropna=False).size()
    both = lk.index.intersection(rk.index)
    matched = int((lk.loc[both] * rk.loc[both]).sum())
    left_only = int(lk.sum() - lk.loc[both].sum())
    right_only = int(rk.sum() - rk.loc[both].sum())
    return matched + (left_only if how in ("left", "outer") else 0) + (right_only if how in ("right", "outer") else 0)


def _run_step(step: dict[str, Any], tables: dict[str, pd.DataFrame], values: dict[str, Any]) -> dict[str, Any]:
    kind, raw, sid = step["kind"], step["raw"], step["id"]
    if kind == "load":
        df = read_csv_preserving_ids(str(step["path"]))
        tables[sid] = df
        return {**_shape(step, df), "file": step["path"].name}
    src = tables[step["reads"][0]]
    if kind == "ops":
        try:
            df, notes = _apply_ops(src, raw["ops"], values, "ops")
        except StepError as first:
            if "fallback" not in raw:
                raise
            try:
                df, notes = _apply_ops(src, raw["fallback"], values, "fallback")
            except StepError as second:
                raise StepError(f"{first.message}; the fallback failed too: {second.message}") from second
            notes.insert(0, f"ops failed ({first.message}), so the fallback ran")
        tables[sid] = df
        out = _shape(step, df, src)
        if notes:
            out["notes"] = notes
        return out
    if kind == "scalar":
        value = aggregate(substitute(raw["scalar"], values), src)
        values[sid] = value
        out = {"id": sid, "kind": kind, "from": step["reads"][0], "value": _plain(value)}
        if _plain(value) is None:
            out["note"] = f"no usable values, so a formula that reads ${sid} will be refused"
        return out
    if kind == "group_by":
        by = step["by"]
        missing = [c for c in by if c not in src.columns]
        if missing:
            raise StepError(f"group_by {missing} is not a column of {step['reads'][0]!r}")
        index = pd.Series(0, index=src.index).groupby([src[c] for c in by], dropna=False, sort=True).size().index
        out_df = pd.DataFrame(index=index)
        for name, formula in raw["agg"].items():
            try:
                value = aggregate(substitute(formula, values), src, by)
            except FormulaError as exc:
                raise StepError(f"agg {name!r}: {exc}") from exc
            if isinstance(value, pd.Series):
                out_df[name] = value.to_numpy() if value.index.equals(index) else value.reindex(index).to_numpy()
            else:
                out_df[name] = value
        out_df.index.names = by
        df = out_df.reset_index()
        tables[sid] = df
        return {**_shape(step, df), "groups": len(df)}
    if kind == "join":
        a, b = step["reads"]
        left, right = tables[a], tables[b]
        on, how = step["on"], step["how"]
        for ref, side in ((a, left), (b, right)):
            missing = [c for c in on if c not in side.columns]
            if missing:
                shown = ", ".join(str(c) for c in list(side.columns)[: get_max_columns()])
                raise StepError(f"{ref!r} has no column {', '.join(missing)} to join on. Its columns: {shown}")
        rows = _join_rows(left, right, on, how)
        size = rows * (_row_bytes(left) + _row_bytes(right))
        cap = get_max_merge_bytes()
        if rows > _MAX_MERGE_ROWS or size > cap:
            raise StepError(
                f"joining {a!r} and {b!r} on {', '.join(on)} would build {rows:,} rows (~{size / 2**20:,.0f} MB), "
                f"over the limit of {_MAX_MERGE_ROWS:,} rows and {cap // 2**20:,} MB -- the key repeats on both "
                "sides. Group one side first, or join on a more selective key."
            )
        try:
            df = left.merge(right, on=on, how=how, suffixes=(f"_{a}", f"_{b}"))
        except ValueError as exc:
            raise StepError(f"{exc}. Cast the key to one type on both sides first, with a cast_column op.") from exc
        tables[sid] = df
        out = _shape(step, df)
        out["on"], out["how"] = on, how
        if len(df) > _FANOUT * max(len(left), len(right), 1):
            out["note"] = (
                f"{len(df):,} rows from {len(left):,} x {len(right):,}: {', '.join(on)} repeats on both sides. "
                "Deduplicate the key if you expected one row per match."
            )
        elif how == "left":
            unmatched = len(left) - int(left.set_index(on).index.isin(right.set_index(on).index).sum())
            if unmatched:
                out["note"] = (
                    f"{unmatched:,} of {len(left):,} {a!r} rows found no match in {b!r}; their new columns are empty"
                )
        return out
    # write: the table goes to disk once every step has run
    df = src
    tables[sid] = df
    return {**_shape(step, df), "file": step["path"].name}


def _sources(step_id: str, by_id: dict[str, dict[str, Any]]) -> list[Path]:
    """The files a step's table was built from."""
    seen: list[Path] = []
    todo = [step_id]
    visited: set[str] = set()
    while todo:
        sid = todo.pop()
        if sid in visited or sid not in by_id:
            continue
        visited.add(sid)
        step = by_id[sid]
        if step["kind"] == "load" and step.get("path") is not None and step["path"] not in seen:
            seen.append(step["path"])
        todo.extend(step["reads"])
    return seen


def _write(step: dict[str, Any], df: pd.DataFrame, by_id: dict[str, dict[str, Any]], original: list) -> dict[str, Any]:
    target: Path = step["path"]
    target.parent.mkdir(parents=True, exist_ok=True)
    backup = snapshot(str(target)) if target.exists() else ""
    atomic_write_text(target, df.to_csv(index=False))
    entry: dict[str, Any] = {"step": step["id"], "path": str(target), "rows": len(df), "columns": len(df.columns)}
    if backup:
        entry["backup"] = backup
    sources = _sources(step["id"], by_id)
    note_lineage(
        entry,
        target,
        op="run_chain",
        source=sources[0] if len(sources) == 1 else None,
        rows_after=len(df),
        columns_after=len(df.columns),
        params={"step": step["id"]},
        note="" if len(sources) == 1 else "built from " + ", ".join(p.name for p in sources),
    )
    attach_public_url(entry, target)
    append_receipt(
        str(target),
        tool="run_chain",
        args={"step": step["id"], "steps": original},
        result=f"wrote {len(df)} rows x {len(df.columns)} columns",
        backup=backup,
    )
    return entry


def _refusal(error: str, hint: str, **extra: Any) -> dict[str, Any]:
    result = {"success": False, "op": "run_chain", "error": error, "hint": hint, **extra}
    result.setdefault("progress", [fail("Chain refused", error[:200])])
    result["token_estimate"] = _token_estimate(result)
    return result


def run_chain(steps: list[dict], dry_run: bool = False, until: str = "") -> dict:
    """Run named steps in one call: load, ops, scalar, join, group_by, write."""
    try:
        original = copy.deepcopy(steps)
        planned, problems, missing = plan(copy.deepcopy(steps), until or "")
        if problems:
            if missing is not None and len(problems) == 1:
                sid, name = missing
                return _refusal(
                    f"File not found: {name}",
                    f"Step {sid!r} loads {name!r}, and nothing was read or written.",
                    failed_step=sid,
                )
            shown = problems[:6]
            more = f" (+{len(problems) - 6} more)" if len(problems) > 6 else ""
            return _refusal(
                "; ".join(shown) + more,
                "Nothing was read or written. Each problem names its step; fix them and call again. " + STEP_SHAPE,
                problems=problems,
            )
        return _execute(planned, original, until or "", dry_run)
    except Exception as exc:
        logger.exception("run_chain error")
        return _refusal(
            error_text(exc),
            "Nothing was written. Call again with dry_run=true to see each step's rows and columns.",
        )


def _execute(planned: list[dict[str, Any]], original: list, until: str, dry_run: bool) -> dict:
    by_id = {s["id"]: s for s in planned}
    last_read = {ref: k for k, s in enumerate(planned) for ref in s["reads"]}
    tables: dict[str, pd.DataFrame] = {}
    values: dict[str, Any] = {}
    skipped: dict[str, str] = {}
    pending: list[tuple[dict[str, Any], pd.DataFrame]] = []
    summaries: list[dict[str, Any]] = []
    progress: list[dict] = []
    latest = ""
    for k, step in enumerate(planned):
        sid, kind = step["id"], step["kind"]
        try:
            gone = [ref for ref in step["reads"] if ref in skipped]
            if gone:
                raise StepError(f"it reads {gone[0]!r}, which was skipped: {skipped[gone[0]]}")
            summary = _run_step(step, tables, values)
        except Exception as exc:
            message = exc.message if isinstance(exc, StepError) else error_text(exc)
            if step["on_error"] == "skip":
                skipped[sid] = message
                summaries.append({"id": sid, "kind": kind, "skipped": True, "error": message})
                progress.append(warn(f"Skipped {kind} {sid}", message[:200]))
                if until and sid == until:
                    break
                continue
            extra: dict[str, Any] = {"failed_step": sid, "steps": summaries}
            source = tables.get(step["reads"][0]) if step["reads"] else None
            if source is not None:
                extra["columns_available"] = [str(c) for c in list(source.columns)[: get_max_columns()]]
            return _refusal(
                f"step {sid!r} ({kind}) failed: {message}",
                f"Nothing was written. Fix step {sid!r} and call again"
                + (" -- columns_available lists what its input holds" if source is not None else "")
                + ". dry_run=true shows every step's rows and columns.",
                progress=[*progress, fail(f"{kind} {sid} failed", message[:200])],
                **extra,
            )
        if dry_run and kind not in ("scalar", "write"):
            summary["sample"] = _records(tables[sid], _SAMPLE_ROWS)
        summaries.append(summary)
        if kind == "write":
            pending.append((step, tables[sid]))
        if kind != "scalar":
            latest = sid
            progress.append(ok(f"{kind} {sid}", f"{summary['rows']} rows x {summary['columns']} columns"))
        else:
            progress.append(ok(f"scalar {sid}", str(summary["value"])))
        for ref in step["reads"]:
            if last_read.get(ref) == k and ref != latest:
                tables.pop(ref, None)
        if until and sid == until:
            break

    result: dict[str, Any] = {"success": True, "op": "run_chain", "steps": summaries}
    if dry_run:
        result["dry_run"] = True
    if values:
        result["variables"] = {name: _plain(v) for name, v in values.items()}
    if skipped:
        result["skipped"] = [{"id": sid, "error": msg} for sid, msg in skipped.items()]
    hint: list[str] = []
    if latest in tables:
        df = tables[latest]
        shown = min(get_max_rows(), len(df))
        result["result"] = {
            "step": latest,
            "columns": [str(c) for c in list(df.columns)[: get_max_columns()]],
            "data": _records(df, shown),
            **counted(shown, len(df)),
        }
        if shown < len(df):
            hint.append(f"The result shows {shown} of {len(df):,} rows; a write step saves the whole table.")
    if dry_run:
        result["would_write"] = [{"step": s["id"], "path": str(s["path"]), "rows": len(df)} for s, df in pending]
        progress.append(info("Dry run -- nothing written", f"{len(pending)} write step(s) planned"))
        hint.insert(0, "Nothing was written. Call again without dry_run to run it.")
    elif pending:
        written: list[dict[str, Any]] = []
        for step, df in pending:
            try:
                written.append(_write(step, df, by_id, original))
            except Exception as exc:
                return _refusal(
                    f"writing step {step['id']!r} to {step['path'].name} failed: {error_text(exc)}",
                    "Every step ran; the files listed in written were saved before this one failed.",
                    written=written,
                    steps=summaries,
                    progress=[*progress, fail(f"write {step['id']} failed", error_text(exc))],
                )
            progress.append(ok(f"Saved {step['path'].name}", f"{len(df)} rows"))
        result["written"] = written
        hint.insert(0, "Read a written file back with inspect_dataset; a backup in written undoes an overwrite.")
    if skipped:
        hint.append(f"{len(skipped)} step(s) failed and were skipped (on_error=skip): see skipped.")
    result["hint"] = " ".join(hint) or "Every step ran. Add a write step to save a table."
    result["progress"] = progress
    result["token_estimate"] = _token_estimate(result)
    return result
