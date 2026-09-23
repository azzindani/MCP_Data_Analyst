"""A chain as the pandas script that does the same thing.

A chain is the receipt of what ran, but only this server can run it. The
export hands it on -- to a person, or to a model working outside MCP -- as
plain pandas: the same steps in the same order, the same formulas with the
same meaning, $names as Python variables, and a saved chain it calls as a
Python function of its params.

"The same" is checked, not hoped for: the tests run each exported script and
compare the files it writes with the files the chain wrote. The script
carries small copies of the rules the chain applies -- a formula's arithmetic
is on floats, `and` is element-wise, an empty filter match is false -- so
that it cannot quietly mean something else. An op with no such translation
is refused by name rather than approximated.
"""

from __future__ import annotations

import ast
import keyword
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from shared.expr import AGGREGATES, _rewrite, _segments
from shared.file_utils import padded_id_columns, read_csv
from shared.patch_validator import validate_ops


class ExportError(ValueError):
    """A step the export cannot write as pandas without changing what it means."""


_VARIABLE = re.compile(r"\$([A-Za-z_]\w*)")
_WHOLE = re.compile(r"^\$([A-Za-z_]\w*)$")
_MARK = "__chainvar_{}__"
_MARKED = re.compile(r"^__chainvar_(\w+)__$")

HELPERS = """
np.seterr(all="ignore")


def _read_csv(path, ids=()):
    kw = {"low_memory": False, "dtype": {c: str for c in ids} or None}

    def attempt(extra):
        for enc in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
            try:
                return pd.read_csv(path, encoding=enc, **kw, **extra)
            except UnicodeDecodeError:
                continue
        return pd.read_csv(path, encoding="latin-1", **kw, **extra)

    try:
        df = attempt({})
    except Exception as exc:
        if "tokeniz" not in str(exc).lower() and "field" not in str(exc).lower():
            raise
        df = attempt({"on_bad_lines": "skip"})
    df.columns = df.columns.str.strip()
    return df


def _num(v):
    if isinstance(v, pd.Series):
        return v.astype(float)
    return np.float64(v)


def _truth(v):
    if isinstance(v, pd.Series):
        return v if pd.api.types.is_bool_dtype(v) else v.astype(bool)
    return bool(v)


def _not(v):
    v = _truth(v)
    return ~v if isinstance(v, pd.Series) else not v


def _where(cond, yes, no, index):
    if not any(isinstance(v, pd.Series) for v in (cond, yes, no)):
        return yes if cond else no
    return pd.Series(np.where(_truth(cond), yes, no), index=index)


def _coalesce(*values):
    out = values[0]
    for value in values[1:]:
        if isinstance(out, pd.Series):
            out = out.where(out.notna(), value)
        elif out is None or (isinstance(out, float) and np.isnan(out)):
            out = value
    return out


def _isnull(v):
    return v.isna() if isinstance(v, pd.Series) else pd.isna(v)


def _notnull(v):
    return ~_isnull(v) if isinstance(v, pd.Series) else not _isnull(v)


def _series(v, df):
    if isinstance(v, pd.Series):
        return v
    if isinstance(v, np.ndarray):
        return pd.Series(v, index=df.index)
    return pd.Series([v] * len(df), index=df.index)


def _first(s):
    s = s.dropna()
    return s.iloc[0] if len(s) else np.nan


def _last(s):
    s = s.dropna()
    return s.iloc[-1] if len(s) else np.nan


def _column(v, index):
    if isinstance(v, pd.Series):
        return v.to_numpy() if v.index.equals(index) else v.reindex(index).to_numpy()
    return v
"""

_ARITH = {ast.Add: "+", ast.Sub: "-", ast.Mult: "*", ast.Div: "/", ast.FloorDiv: "//", ast.Mod: "%", ast.Pow: "**"}
_CMP = {ast.Eq: "==", ast.NotEq: "!=", ast.Lt: "<", ast.LtE: "<=", ast.Gt: ">", ast.GtE: ">="}
_NUMPY = {"abs": "abs", "sqrt": "sqrt", "log": "log", "log10": "log10", "exp": "exp", "floor": "floor", "ceil": "ceil"}
_METHOD = {"count_distinct": "nunique", "percentile": "quantile"}
_NUMERIC_AGG = frozenset({"sum", "mean", "median", "std", "percentile"})


# Names the script itself uses. A param or scalar called `t` or `df` would
# overwrite the table dict or the working frame, so such an id gets a suffix.
_RESERVED = frozenset({"np", "pd", "t", "df", "out", "result", "exc", "float", "int"})


def py_name(name: str) -> str:
    """A step id as a Python name that cannot shadow a keyword or the script's own names."""
    if keyword.iskeyword(name) or name in _RESERVED or name.startswith("_"):
        return f"{name}_v"
    return name


def _value(value: Any) -> str:
    """A literal from an op or an argument, with a $name written as its variable."""
    if isinstance(value, str):
        whole = _WHOLE.match(value)
        if whole:
            return py_name(whole.group(1))
        if _VARIABLE.search(value):
            raise ExportError(f"{value!r} holds a $name inside other text; the export writes a $name only on its own")
        return repr(value)
    if isinstance(value, list):
        return "[" + ", ".join(_value(v) for v in value) + "]"
    if isinstance(value, dict):
        return "{" + ", ".join(f"{k!r}: {_value(v)}" for k, v in value.items()) + "}"
    return repr(value)


def _path(raw: str) -> str:
    """A load/write path as code: a literal, or an f-string when it reads params."""
    if not _VARIABLE.search(raw):
        return repr(raw)
    text = raw.replace("{", "{{").replace("}", "}}")
    text = _VARIABLE.sub(lambda m: "{" + py_name(m.group(1)) + "}", text)
    return "f" + repr(text)


def _tree(formula: str, columns: list[str]) -> tuple[ast.expr, dict[str, str]]:
    marked = "".join(
        _VARIABLE.sub(lambda m: _MARK.format(m.group(1)), raw) if kind == "code" else raw
        for kind, raw in _segments(formula)
    )
    text, names = _rewrite(marked, columns)
    return ast.parse(text.strip(), mode="eval").body, names


class _Row:
    """A row formula as pandas over the frame named `frame`."""

    def __init__(self, names: dict[str, str], frame: str) -> None:
        self.names = names
        self.frame = frame

    def code(self, node: ast.AST) -> str:
        match node:
            case ast.Constant(value=value):
                if isinstance(value, (bool, str)) or value is None:
                    return repr(value)
                return repr(float(value))
            case ast.Name(id=ident):
                marked = _MARKED.match(ident)
                if marked:
                    return py_name(marked.group(1))
                return f"{self.frame}[{self.names.get(ident, ident)!r}]"
            case ast.BinOp(left=left, op=op, right=right) if type(op) in _ARITH:
                return f"(_num({self.code(left)}) {_ARITH[type(op)]} _num({self.code(right)}))"
            case ast.UnaryOp(op=ast.USub(), operand=operand):
                return f"(-_num({self.code(operand)}))"
            case ast.UnaryOp(op=ast.UAdd(), operand=operand):
                return f"_num({self.code(operand)})"
            case ast.UnaryOp(op=ast.Not(), operand=operand):
                return f"_not({self.code(operand)})"
            case ast.Compare(left=left, ops=ops, comparators=comparators):
                parts, lhs = [], self.code(left)
                for op, right in zip(ops, comparators, strict=True):
                    rhs = self.code(right)
                    parts.append(f"({lhs} {_CMP[type(op)]} {rhs})")
                    lhs = rhs
                return parts[0] if len(parts) == 1 else "(" + " & ".join(f"_truth({p})" for p in parts) + ")"
            case ast.BoolOp(op=op, values=values):
                joint = " & " if isinstance(op, ast.And) else " | "
                return "(" + joint.join(f"_truth({self.code(v)})" for v in values) + ")"
            case ast.IfExp(test=test, body=body, orelse=orelse):
                return f"_where({self.code(test)}, {self.code(body)}, {self.code(orelse)}, {self.frame}.index)"
            case ast.Call(func=ast.Name(id=name), args=args):
                return self.call(name, args)
        raise ExportError(f"a formula uses {type(node).__name__}, which the export cannot write")

    def call(self, name: str, args: list[ast.expr]) -> str:
        parts = [self.code(a) for a in args]
        if name in _NUMPY:
            return f"np.{_NUMPY[name]}(_num({parts[0]}))"
        if name == "round":
            digits = f"int({parts[1]})" if len(parts) > 1 else "0"
            return f"np.round(_num({parts[0]}), {digits})"
        if name == "clip":
            return f"np.clip({', '.join(f'_num({p})' for p in parts)})"
        if name == "if_else":
            return f"_where({parts[0]}, {parts[1]}, {parts[2]}, {self.frame}.index)"
        if name in ("coalesce", "isnull", "notnull"):
            return f"_{name}({', '.join(parts)})"
        raise ExportError(f"a formula calls {name}(), which the export cannot write")


class _Agg(_Row):
    """An aggregate formula: one value over the frame, or one per group of `keys`."""

    def __init__(self, names: dict[str, str], frame: str, keys: str | None) -> None:
        super().__init__(names, frame)
        self.keys = keys

    def code(self, node: ast.AST) -> str:
        match node:
            case ast.Call(func=ast.Name(id=name), args=args) if name in AGGREGATES:
                return self.reduce(name, args)
            case ast.Call(func=ast.Name(id="round"), args=args):
                parts = [self.code(a) for a in args]
                return f"np.round({parts[0]}, {'int(' + parts[1] + ')' if len(parts) > 1 else '0'})"
            case ast.Call(func=ast.Name(id="abs"), args=[arg]):
                return f"np.abs({self.code(arg)})"
            case ast.BinOp(left=left, op=op, right=right) if type(op) in _ARITH:
                return f"({self.code(left)} {_ARITH[type(op)]} {self.code(right)})"
            case ast.UnaryOp(op=ast.USub(), operand=operand):
                return f"(-{self.code(operand)})"
            case ast.UnaryOp(op=ast.UAdd(), operand=operand):
                return self.code(operand)
            case ast.Constant(value=value) if isinstance(value, (int, float)) and not isinstance(value, bool):
                return repr(float(value))
        raise ExportError(f"an aggregate uses {type(node).__name__}, which the export cannot write")

    def reduce(self, name: str, args: list[ast.expr]) -> str:
        grouped = f".groupby({self.keys}, dropna=False, sort=True)" if self.keys else ""
        if not args:
            return f"{self.frame}{grouped}.size()" if self.keys else f"{self.frame}.shape[0]"
        values = f"_series({_Row(self.names, self.frame).code(args[0])}, {self.frame})"
        if name == "count_if":
            return f"{values}.fillna(False).astype(bool).astype(int){grouped}.sum()"
        if name in _NUMERIC_AGG:
            values = f"_num({values})"
        if name in ("first", "last") and not self.keys:
            return f"_{name}({values})"
        extra = ""
        if name == "percentile":
            q = args[1]
            if not isinstance(q, ast.Constant):
                raise ExportError("percentile's second argument must be a number")
            extra = repr(float(q.value) / 100)  # type: ignore[arg-type]
        return f"{values}{grouped}.{_METHOD.get(name, name)}({extra})"


def _normalized(op: dict) -> dict:
    """The op as apply_patch reads it: aliases resolved and nested params lifted."""
    copy = dict(op)
    validate_ops([copy])
    return copy


def _patch(op: dict) -> list[str]:
    """One apply_patch op as pandas over `df`, for the ops whose meaning is plain pandas."""
    o = _normalized(op)
    name = o["op"]
    col = _value(o.get("column", ""))
    if name == "drop_column":
        return [f"df = df.drop(columns={_value(o['columns'])})"]
    if name == "sort":
        return [
            f"df = df.sort_values(by={_value(o['by'])}, ascending={_value(o.get('ascending', True))}).reset_index(drop=True)"
        ]
    if name in ("drop_duplicates", "dedup_subset"):
        subset = o.get("subset") if name == "drop_duplicates" else o.get("columns")
        keep = o.get("keep", "first")
        keep = False if keep in ("False", "false", "None", "none", False) else keep
        tail = ".reset_index(drop=True)" if name == "dedup_subset" else ""
        return [f"df = df.drop_duplicates(subset={_value(subset)}, keep={_value(keep)}){tail}"]
    if name in ("filter_isin", "filter_not_isin"):
        negate = "~" if name == "filter_not_isin" else ""
        return [f"df = df[{negate}df[{col}].isin({_value(o['values'])})].reset_index(drop=True)"]
    if name == "filter_between":
        inclusive = _value(o.get("inclusive", "both"))
        return [
            f"df = df[df[{col}].between({_value(o['min'])}, {_value(o['max'])}, inclusive={inclusive})]"
            ".reset_index(drop=True)"
        ]
    if name == "filter_top_n":
        how = "nlargest" if o.get("keep", "top") == "top" else "nsmallest"
        return [f"df = df.{how}(np.int64({_value(o['n'])}), {col}).reset_index(drop=True)"]
    if name == "clip_values":
        return [f"df[{col}] = df[{col}].clip(lower={_value(o.get('min'))}, upper={_value(o.get('max'))})"]
    if name == "round_values":
        return [f"df[{col}] = df[{col}].round({_value(o.get('decimals', 2))})"]
    if name == "abs_values":
        return [f"df[{_value(o.get('new_column', o['column']))}] = df[{col}].abs()"]
    if name == "fill_nulls":
        return _fill_nulls(o, col)
    raise ExportError(f"op {name!r} has no pandas translation yet; the export covers {', '.join(EXPORTED_OPS)}")


def _fill_nulls(o: dict, col: str) -> list[str]:
    lines = []
    if o.get("fill_zeros", False):
        lines.append(f"if pd.api.types.is_numeric_dtype(df[{col}]):")
        lines.append(f"    df[{col}] = df[{col}].replace(0, pd.NA)")
    strategy = o["strategy"]
    if strategy in ("mean", "median"):
        lines.append(f"if not df[{col}].dropna().empty:")
        lines.append(f"    df[{col}] = df[{col}].fillna(np.float64(df[{col}].{strategy}()))")
    elif strategy == "mode":
        lines.append(f"if not df[{col}].mode().empty:")
        lines.append(f"    df[{col}] = df[{col}].fillna(df[{col}].mode().iloc[0])")
    elif strategy in ("ffill", "bfill"):
        lines.append(f"df[{col}] = df[{col}].{strategy}()")
    elif strategy == "drop":
        lines.append(f"df = df.dropna(subset=[{col}])")
    elif strategy == "value":
        lines.append(f"df[{col}] = df[{col}].fillna({_value(o['value'])})")
    else:
        raise ExportError(f"fill_nulls strategy {strategy!r} has no pandas translation")
    return lines


EXPORTED_OPS = (
    "filter",
    "derive",
    "drop_column",
    "sort",
    "drop_duplicates",
    "dedup_subset",
    "filter_isin",
    "filter_not_isin",
    "filter_between",
    "filter_top_n",
    "clip_values",
    "round_values",
    "abs_values",
    "fill_nulls",
)


class _Writer:
    def __init__(self) -> None:
        self.functions: dict[Path, tuple[str, list[str]]] = {}

    def ops(self, ops: list[dict], columns: list[list[str]] | None, where: str) -> list[str]:
        lines: list[str] = []
        cols: list[str] | None = None
        for j, op in enumerate(ops):
            # An op after the one that failed never ran; the columns it would
            # have seen are the last ones recorded.
            cols = columns[j] if columns and j < len(columns) else cols
            name = op.get("op")
            try:
                if name in ("filter", "derive"):
                    if cols is None:
                        raise ExportError("it never ran, so its formulas were never read against its columns")
                    field = "where" if name == "filter" else "expr"
                    node, names = _tree(op[field], cols)
                    code = _Row(names, "df").code(node)
                    if name == "filter":
                        lines.append(
                            f"df = df[_series({code}, df).fillna(False).astype(bool).to_numpy()].reset_index(drop=True)"
                        )
                    else:
                        lines.append(f"df[{op['name']!r}] = _series({code}, df)")
                else:
                    lines.extend(_patch(op))
            except ExportError as exc:
                raise ExportError(f"{where} op {j} ({name}): {exc}") from None
        return lines

    def steps(self, planned: list[dict[str, Any]], until: str, params: dict[str, str]) -> tuple[list[str], str]:
        """The code for a list of steps, and the id of the last table."""
        lines: list[str] = []
        latest = ""
        for step in planned:
            body = self.step(step, params)
            if step["on_error"] == "skip":
                lines += [
                    "_mark = len(_writes)",
                    "try:",
                    *("    " + b for b in body),
                    f"except Exception as exc:  # {step['id']}: on_error skip",
                    "    del _writes[_mark:]",
                    f'    print(f"skipped {step["id"]}: {{exc}}")',
                ]
            else:
                lines += body
            if step["kind"] not in ("scalar", "param"):
                latest = step["id"]
            if until and step["id"] == until:
                break
        return lines, latest

    def step(self, step: dict[str, Any], params: dict[str, str]) -> list[str]:
        kind, raw, sid = step["kind"], step["raw"], step["id"]
        where = f"step {sid!r} ({kind})"
        t = f"t[{sid!r}]"
        head = [f"# {sid}: {kind}"]
        if kind == "param":
            return head if sid in params else [*head, f"{py_name(sid)} = {raw['param']!r}"]
        if kind == "load":
            ids = ""
            if step.get("path") is not None:
                padded = padded_id_columns(str(step["path"]), read_csv(str(step["path"])))
                ids = f", ids={padded!r}" if padded else ""
            read = f"_read_csv({_path(raw['load'])}{ids})"
            if sid in params:  # a load a caller may hand a table to
                return [*head, f"{t} = {py_name(sid)} if {py_name(sid)} is not None else {read}"]
            return [*head, f"{t} = {read}"]
        if kind == "call":
            return [*head, f"{t} = {self.function(step)}"]
        if kind == "join":
            a, b = step["reads"]
            return [
                *head,
                f"{t} = t[{a!r}].merge(t[{b!r}], on={step['on']!r}, how={step['how']!r}, suffixes=({'_' + a!r}, {'_' + b!r}))",
            ]
        src = f"t[{step['reads'][0]!r}]"
        if kind == "write":
            return [*head, f"{t} = {src}", f"_writes.append(({t}, {_path(raw['write'])}))"]
        if kind == "ops":
            recorded = step.get("_cols", {})
            main = self.ops(raw["ops"], recorded.get("ops"), where)
            code = [f"df = {src}.copy()", *main]
            if "fallback" in raw:
                if "fallback" in recorded:
                    fallback = self.ops(raw["fallback"], recorded["fallback"], where + " fallback")
                    code = ["try:", *("    " + c for c in code), "except Exception:", f"    df = {src}.copy()"]
                    code += ["    " + c for c in fallback]
                # a fallback that never ran was never needed: the ops above are what ran
            return [*head, *code, f"{t} = df"]
        columns = step.get("_cols_in")
        if columns is None:
            raise ExportError(f"{where} never ran, so its formulas were never read against its columns")
        if kind == "scalar":
            node, names = _tree(raw["scalar"], columns)
            return [*head, f"df = {src}", f"{py_name(sid)} = {_Agg(names, 'df', None).code(node)}"]
        # group_by
        by = step["by"]
        lines = [
            *head,
            f"df = {src}",
            f"_keys = [df[c] for c in {by!r}]",
            "_index = pd.Series(0, index=df.index).groupby(_keys, dropna=False, sort=True).size().index",
            "out = pd.DataFrame(index=_index)",
        ]
        for name, formula in raw["agg"].items():
            node, names = _tree(formula, columns)
            lines.append(f"out[{name!r}] = _column({_Agg(names, 'df', '_keys').code(node)}, _index)")
        return [*lines, f"out.index.names = {by!r}", f"{t} = out.reset_index()"]

    def function(self, step: dict[str, Any]) -> str:
        """A call as a call of the Python function its chain file becomes."""
        path: Path = step["chain"]
        if path not in self.functions:
            stem = re.sub(r"\W+", "_", path.name.removesuffix(".json").removesuffix(".chain")).strip("_") or "chain"
            name = f"{stem}_chain" if not stem[0].isdigit() else f"chain_{stem}"
            taken = {n for n, _ in self.functions.values()}
            while name in taken:
                name += "_"
            sub = step["sub"]
            args = [(s["id"], repr(s["raw"]["param"])) for s in sub if s["kind"] == "param"]
            args += [(s["id"], "None") for s in sub if s["kind"] == "load"]
            params = {sid: sid for sid, _ in args}
            self.functions[path] = (name, [])  # placed first, so a chain that calls itself cannot loop here
            body, latest = self.steps(sub, "", params)
            signature = ", ".join(f"{py_name(a)}={default}" for a, default in args)
            code = [
                f"def {name}({signature}):",
                f'    """{path.name}, as a function: its params are keyword arguments; '
                'a load can be handed a table."""',
                "    t = {}",
                *("    " + line for line in body),
                f"    return t[{latest!r}]",
            ]
            self.functions[path] = (name, code)
        name = self.functions[path][0]
        given = [f"{py_name(k)}={_value(v)}" for k, v in (step["raw"].get("args") or {}).items()]
        given += [f"{py_name(k)}=t[{v!r}]" for k, v in step["feeds"].items()]
        return f"{name}({', '.join(given)})"


def export_script(planned: list[dict[str, Any]], until: str = "") -> str:
    """The pandas script for a chain that has run. Raises ExportError naming the step it cannot write."""
    writer = _Writer()
    body, latest = writer.steps(planned, until, {})
    functions = [line for _, code in writer.functions.values() for line in [*code, "", ""]]
    stamp = datetime.now(UTC).strftime("%Y-%m-%d")
    lines = [
        f'"""Exported from run_chain on {stamp}: the same steps, as pandas.',
        "",
        "Run it from the data folder -- relative paths are read from and written to",
        "the current directory. Each step's table is t[<id>]; $names are variables.",
        "Nothing is written until every step has run, as in the chain.",
        '"""',
        "",
        "import numpy as np",
        "import pandas as pd",
        HELPERS.rstrip(),
        "",
        "",
        *functions,
        "t = {}",
        "_writes = []",
        "",
        *body,
        "",
        "for _table, _path in _writes:",
        "    _table.to_csv(_path, index=False)",
    ]
    if latest:
        lines.append(f"result = t[{latest!r}]")
    return "\n".join(lines) + "\n"
