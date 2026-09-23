"""Column formulas for column_math and add_column: parsed and allow-listed, never eval().

The parser this replaces split the text on + - * / and folded the pieces left
to right, so ``a + b * 2`` returned ``(a + b) * 2`` under ``success: true`` --
22 where the caller meant 21 -- and it had no parentheses or unary minus with
which to write the intended version. A formula is now read by Python's own
grammar (``ast``), so precedence, parentheses and unary minus mean what they
mean everywhere else, and it is evaluated by walking that tree and accepting
only the node types below. Nothing reaches ``eval``.

Column names that are not identifiers (``Units Sold``) keep working bare, as
they always did, provided they hold no operator characters and do not look like
a number. Any column name can be written in backticks: ``\\`clicks-2\\` * 3``,
``\\`2020\\` - \\`2019\\```. The old parser read a column named ``2020`` where the
formula said 2020; a number that is also a column name is now refused until the
caller says which one they mean.
"""

from __future__ import annotations

import ast
import keyword
import re
from collections.abc import Callable
from typing import Any

import numpy as np
import pandas as pd

# Long enough for any formula a person writes; short enough that the parser's
# own nesting limit is the worst a caller can reach.
MAX_LENGTH = 1000

# A bare column name holding one of these would be read as an operator, so such
# a name has to be written in backticks. Every other name is matched as-is.
_OPERATOR_CHARS = frozenset("+-*/%()<>=!,`'\"&|~^@[]{}:;")

# Words the formula grammar itself uses. A column with one of these names has
# to be backticked; every other keyword-named column is matched bare.
_GRAMMAR_WORDS = frozenset({"and", "or", "not", "if", "else", "True", "False", "None"})

_PLACEHOLDER = "__formula_col_{}__"


class FormulaError(ValueError):
    """A formula that cannot be read or evaluated, said precisely enough to fix."""


def _num(value: Any, formula: str) -> Any:
    """A number or a float Series, or a refusal naming what was not numeric."""
    if isinstance(value, pd.Series):
        if pd.api.types.is_numeric_dtype(value) or pd.api.types.is_bool_dtype(value):
            return value.astype(float)
        try:
            return value.astype(float)
        except TypeError, ValueError:
            sample = next((v for v in value.dropna().tolist() if not _is_number(v)), None)
            label = f"Column {value.name!r}" if value.name is not None else "A value in the formula"
            raise FormulaError(
                f"{label} is not numeric (it holds text like {sample!r}), so it cannot be used in "
                f"arithmetic in {formula!r}. Cast it first with cast_column, or compare it with == instead."
            ) from None
    if isinstance(value, bool):
        return np.float64(value)
    if isinstance(value, (int, float, np.number)):
        return np.float64(value)
    raise FormulaError(f"Text {value!r} cannot be used in arithmetic in {formula!r}.")


def _is_number(value: Any) -> bool:
    try:
        float(value)
    except TypeError, ValueError:
        return False
    return True


def _truth(value: Any) -> Any:
    if isinstance(value, pd.Series):
        return value if pd.api.types.is_bool_dtype(value) else value.astype(bool)
    return bool(value)


def _where(cond: Any, yes: Any, no: Any, index: pd.Index) -> Any:
    if not any(isinstance(v, pd.Series) for v in (cond, yes, no)):
        return yes if cond else no
    return pd.Series(np.where(_truth(cond), yes, no), index=index)


def _coalesce(*values: Any) -> Any:
    out = values[0]
    for value in values[1:]:
        if isinstance(out, pd.Series):
            out = out.where(out.notna(), value)
        elif out is None or (isinstance(out, float) and np.isnan(out)):
            out = value
    return out


def _isnull(value: Any) -> Any:
    return value.isna() if isinstance(value, pd.Series) else pd.isna(value)


def _round(value: Any, digits: Any = 0) -> Any:
    return np.round(value, int(digits))


def _needs_index(*_args: Any) -> Any:  # pragma: no cover - if_else is dispatched before this is reached
    raise AssertionError("if_else is evaluated with the frame's index, in _Evaluator.call")


# name -> (numeric arguments?, allowed argument counts, implementation)
_FUNCTIONS: dict[str, tuple[bool, tuple[int, ...], Callable[..., Any]]] = {
    "abs": (True, (1,), np.abs),
    "round": (True, (1, 2), _round),
    "sqrt": (True, (1,), np.sqrt),
    "log": (True, (1,), np.log),
    "log10": (True, (1,), np.log10),
    "exp": (True, (1,), np.exp),
    "floor": (True, (1,), np.floor),
    "ceil": (True, (1,), np.ceil),
    "clip": (True, (3,), np.clip),
    "coalesce": (False, tuple(range(2, 17)), _coalesce),
    "if_else": (False, (3,), _needs_index),
    "isnull": (False, (1,), _isnull),
    "notnull": (False, (1,), lambda v: ~_isnull(v) if isinstance(v, pd.Series) else not _isnull(v)),
}

FUNCTION_NAMES: tuple[str, ...] = tuple(_FUNCTIONS)

_ARITHMETIC: dict[type, Callable[[Any, Any], Any]] = {
    ast.Add: lambda a, b: a + b,
    ast.Sub: lambda a, b: a - b,
    ast.Mult: lambda a, b: a * b,
    ast.Div: lambda a, b: a / b,
    ast.FloorDiv: lambda a, b: a // b,
    ast.Mod: lambda a, b: a % b,
    ast.Pow: lambda a, b: a**b,
}

_COMPARE: dict[type, Callable[[Any, Any], Any]] = {
    ast.Eq: lambda a, b: a == b,
    ast.NotEq: lambda a, b: a != b,
    ast.Lt: lambda a, b: a < b,
    ast.LtE: lambda a, b: a <= b,
    ast.Gt: lambda a, b: a > b,
    ast.GtE: lambda a, b: a >= b,
}

# What a refused node is called in the message, for the ones a caller is most
# likely to have written by accident.
_NODE_NAMES = {
    "Tuple": "a comma outside a function call",
    "Attribute": "attribute access (x.y)",
    "Subscript": "indexing (x[...])",
    "List": "a list ([...])",
    "Dict": "a dict ({...})",
    "Lambda": "lambda",
    "BitAnd": "& (write 'and')",
    "BitOr": "| (write 'or')",
    "BitXor": "^ (write ** for a power)",
    "MatMult": "@",
}

ALLOWED = (
    "+ - * / // % **, parentheses, comparisons (== != < <= > >=), and/or/not, "
    "'x if cond else y', numbers, 'quoted text', functions: " + ", ".join(FUNCTION_NAMES)
)


def _segments(formula: str) -> list[tuple[str, str]]:
    """Split into ('code' | 'text' | 'tick', raw) pieces, so names are never matched inside quotes."""
    out: list[tuple[str, str]] = []
    buf: list[str] = []
    i = 0
    while i < len(formula):
        ch = formula[i]
        if ch in "'\"`":
            if buf:
                out.append(("code", "".join(buf)))
                buf = []
            j = i + 1
            while j < len(formula) and formula[j] != ch:
                j += 2 if formula[j] == "\\" and ch != "`" else 1
            if j >= len(formula):
                what = "backtick" if ch == "`" else "quote"
                raise FormulaError(f"Formula {formula!r} has an unclosed {what} starting at position {i}.")
            kind = "tick" if ch == "`" else "text"
            out.append((kind, formula[i : j + 1]))
            i = j + 1
            continue
        buf.append(ch)
        i += 1
    if buf:
        out.append(("code", "".join(buf)))
    return out


def _bare_names(columns: list[str]) -> list[str]:
    """Columns that need matching as text: not identifiers, but free of operator characters."""
    names = []
    for col in columns:
        # A number-like name ("2020", "1") matched bare would turn the 1 in 1.5
        # into a column. Numbers stay numbers; such a column is backticked.
        if not col.strip() or _OPERATOR_CHARS & set(col) or _is_number(col):
            continue
        if not col.isidentifier() or (keyword.iskeyword(col) and col not in _GRAMMAR_WORDS):
            names.append(col)
    return sorted(names, key=len, reverse=True)


def _rewrite(formula: str, columns: list[str]) -> tuple[str, dict[str, str]]:
    """Replace backticked and bare non-identifier column names with placeholders."""
    names: dict[str, str] = {}

    def placeholder(col: str) -> str:
        for key, value in names.items():
            if value == col:
                return key
        key = _PLACEHOLDER.format(len(names))
        names[key] = col
        return key

    bare = _bare_names(columns)
    numeric_named = {col for col in columns if col.strip() and _is_number(col)}
    parts: list[str] = []
    for kind, raw in _segments(formula):
        if kind == "text":
            parts.append(raw)
        elif kind == "tick":
            parts.append(placeholder(raw[1:-1]))
        else:
            # The old parser read a column named "2020" where the formula said
            # 2020. Reading it as a number now would silently turn a year-over-
            # year difference into 1.0, so a number that is also a column name
            # is refused until the caller says which one they mean.
            for token in re.findall(r"(?<![\w.])\d+(?:\.\d+)?(?![\w.])", raw):
                if token in numeric_named:
                    raise FormulaError(
                        f"{token} in formula {formula!r} is both a number and a column name. "
                        f"Write `{token}` (backticks) for the column, or {float(token)!r} for the number."
                    )
            for col in bare:
                pattern = r"(?<!\w)" + re.escape(col) + r"(?!\w)"
                raw = re.sub(pattern, lambda _m, c=col: placeholder(c), raw)
            parts.append(raw)
    return "".join(parts), names


class _Evaluator:
    def __init__(self, df: pd.DataFrame, names: dict[str, str], formula: str) -> None:
        self.df = df
        self.names = names
        self.formula = formula

    def column(self, ident: str) -> pd.Series:
        col = self.names.get(ident, ident)
        if col in self.df.columns:
            return self.df[col]
        available = ", ".join(str(c) for c in self.df.columns)
        raise FormulaError(
            f"Unknown column {col!r} in formula {self.formula!r}. Available: {available}. "
            "Write a name that holds operator characters in backticks, e.g. `clicks-2`."
        )

    def refuse(self, node: ast.AST) -> FormulaError:
        kind = type(node).__name__
        return FormulaError(
            f"Formula {self.formula!r} uses {_NODE_NAMES.get(kind, kind)}, which formulas do not allow. "
            f"Allowed: {ALLOWED}."
        )

    def visit(self, node: ast.AST) -> Any:
        match node:
            case ast.Constant(value=value):
                if isinstance(value, bool) or isinstance(value, str) or value is None:
                    return value
                if isinstance(value, (int, float)):
                    return np.float64(value)
                raise self.refuse(node)
            case ast.Name(id=ident):
                return self.column(ident)
            case ast.BinOp(left=left, op=op, right=right):
                fn = _ARITHMETIC.get(type(op))
                if fn is None:
                    raise self.refuse(op)
                return fn(_num(self.visit(left), self.formula), _num(self.visit(right), self.formula))
            case ast.UnaryOp(op=ast.USub(), operand=operand):
                return -_num(self.visit(operand), self.formula)
            case ast.UnaryOp(op=ast.UAdd(), operand=operand):
                return _num(self.visit(operand), self.formula)
            case ast.UnaryOp(op=ast.Not(), operand=operand):
                value = _truth(self.visit(operand))
                return ~value if isinstance(value, pd.Series) else not value
            case ast.Compare(left=left, ops=ops, comparators=comparators):
                return self.compare(left, ops, comparators)
            case ast.BoolOp(op=op, values=values):
                result = _truth(self.visit(values[0]))
                for value in values[1:]:
                    other = _truth(self.visit(value))
                    result = (result & other) if isinstance(op, ast.And) else (result | other)
                return result
            case ast.IfExp(test=test, body=body, orelse=orelse):
                return _where(self.visit(test), self.visit(body), self.visit(orelse), self.df.index)
            case ast.Call():
                return self.call(node)
            case _:
                raise self.refuse(node)

    def compare(self, left: ast.expr, ops: list[ast.cmpop], comparators: list[ast.expr]) -> Any:
        result: Any = None
        lhs = self.visit(left)
        for op, right in zip(ops, comparators, strict=True):
            fn = _COMPARE.get(type(op))
            if fn is None:
                raise self.refuse(op)
            rhs = self.visit(right)
            try:
                step = fn(lhs, rhs)
            except TypeError as exc:
                raise FormulaError(
                    f"Formula {self.formula!r} compares values that cannot be ordered against each other "
                    f"(text against a number?): {exc}"
                ) from None
            result = step if result is None else (_truth(result) & _truth(step))
            lhs = rhs
        return result

    def call(self, node: ast.Call) -> Any:
        if not isinstance(node.func, ast.Name) or node.func.id not in _FUNCTIONS:
            name = node.func.id if isinstance(node.func, ast.Name) else type(node.func).__name__
            raise FormulaError(
                f"Formula {self.formula!r} calls {name!r}, which is not a formula function. "
                f"Functions: {', '.join(FUNCTION_NAMES)}."
            )
        name = node.func.id
        if node.keywords or any(isinstance(a, ast.Starred) for a in node.args):
            raise FormulaError(f"{name}() takes positional arguments only, in {self.formula!r}.")
        numeric, arities, fn = _FUNCTIONS[name]
        if len(node.args) not in arities:
            want = " or ".join(str(n) for n in arities) if len(arities) < 4 else f"{arities[0]} or more"
            raise FormulaError(f"{name}() takes {want} argument(s), got {len(node.args)}, in {self.formula!r}.")
        args = [self.visit(a) for a in node.args]
        if name == "if_else":
            return _where(args[0], args[1], args[2], self.df.index)
        if numeric:
            args = [_num(a, self.formula) for a in args]
        return fn(*args)


def _parse(formula: str, df: pd.DataFrame, allowed: str = ALLOWED) -> tuple[ast.Expression, dict[str, str]]:
    if not isinstance(formula, str) or not formula.strip():
        raise FormulaError("Formula is empty. Example: '(clicks + 1) / impressions * 100'.")
    if len(formula) > MAX_LENGTH:
        raise FormulaError(f"Formula is {len(formula)} characters; the limit is {MAX_LENGTH}.")
    text, names = _rewrite(formula, [str(c) for c in df.columns])
    try:
        tree = ast.parse(text.strip(), mode="eval")
    except SyntaxError as exc:
        raise FormulaError(
            f"Formula {formula!r} could not be read: {exc.msg}. Allowed: {allowed}. "
            "Write a column name that holds operator characters in backticks."
        ) from None
    except RecursionError, MemoryError:
        raise FormulaError(f"Formula {formula!r} is nested too deeply to read.") from None
    return tree, names


def evaluate(formula: str, df: pd.DataFrame) -> pd.Series:
    """Evaluate ``formula`` over ``df`` and return one value per row. Raises FormulaError."""
    tree, names = _parse(formula, df)
    with np.errstate(all="ignore"):
        result = _Evaluator(df, names, formula).visit(tree.body)
    if isinstance(result, pd.Series):
        return result
    if isinstance(result, np.ndarray):
        return pd.Series(result, index=df.index)
    return pd.Series([result] * len(df), index=df.index)


# ---------------------------------------------------------------------------
# Variables: $name in a formula is a value an earlier step computed
# ---------------------------------------------------------------------------

_VARIABLE = re.compile(r"\$([A-Za-z_]\w*)")


def _literal(name: str, value: Any) -> str:
    if isinstance(value, pd.Timestamp):
        return repr(value.isoformat())
    if isinstance(value, str):
        return repr(value)
    if isinstance(value, (bool, np.bool_)):
        return str(bool(value))
    if isinstance(value, (int, float, np.number)):
        if not np.isfinite(float(value)):
            raise FormulaError(f"${name} is {value} -- the step that computed it found no usable values.")
        return repr(float(value))
    raise FormulaError(f"${name} holds a {type(value).__name__}, which a formula cannot use.")


def substitute(formula: str, values: dict[str, Any]) -> str:
    """``formula`` with every ``$name`` written as the value it holds.

    Only code is rewritten: a ``$`` inside 'quoted text' or a `backticked`
    column name stays as written. A ``$name`` nobody computed is refused
    here, naming the ones that exist.
    """
    parts: list[str] = []
    for kind, raw in _segments(formula):
        if kind != "code":
            parts.append(raw)
            continue

        def value_of(match: re.Match[str]) -> str:
            name = match.group(1)
            if name not in values:
                known = ", ".join(f"${k}" for k in values) or "none yet"
                raise FormulaError(f"${name} in {formula!r} is not a variable. Variables: {known}.")
            return _literal(name, values[name])

        parts.append(_VARIABLE.sub(value_of, raw))
    return "".join(parts)


def variables_in(formula: str) -> list[str]:
    """The ``$names`` a formula reads, outside quotes and backticks."""
    try:
        segments = _segments(formula)
    except FormulaError:
        return []
    return [m.group(1) for kind, raw in segments if kind == "code" for m in _VARIABLE.finditer(raw)]


# ---------------------------------------------------------------------------
# Aggregates: one value per table, or one per group
# ---------------------------------------------------------------------------

# name -> allowed argument counts
AGGREGATES: dict[str, tuple[int, ...]] = {
    "sum": (1,),
    "mean": (1,),
    "median": (1,),
    "min": (1,),
    "max": (1,),
    "std": (1,),
    "count": (0, 1),
    "count_distinct": (1,),
    "count_if": (1,),
    "percentile": (2,),
    "first": (1,),
    "last": (1,),
}

_NUMERIC_AGGREGATES = frozenset({"sum", "mean", "median", "std", "percentile"})

AGGREGATE_ALLOWED = (
    "aggregates "
    + ", ".join(AGGREGATES)
    + " over any row formula, combined with + - * / // % ** and numbers, "
    + "e.g. 'sum(clicks) / sum(impressions) * 100', 'percentile(net, 95)', 'count_if(net > 0)'"
)


class _Aggregator:
    def __init__(self, df: pd.DataFrame, names: dict[str, str], formula: str, by: list[str] | None) -> None:
        self.rows = _Evaluator(df, names, formula)
        self.df = df
        self.formula = formula
        self.keys = [df[c] for c in by] if by else None

    def refuse(self, what: str) -> FormulaError:
        return FormulaError(
            f"Formula {self.formula!r} uses {what}, which an aggregate does not allow. Allowed: {AGGREGATE_ALLOWED}."
        )

    def visit(self, node: ast.AST) -> Any:
        match node:
            case ast.Constant(value=value) if isinstance(value, (int, float)) and not isinstance(value, bool):
                return np.float64(value)
            case ast.BinOp(left=left, op=op, right=right):
                fn = _ARITHMETIC.get(type(op))
                if fn is None:
                    raise self.refuse(_NODE_NAMES.get(type(op).__name__, type(op).__name__))
                try:
                    return fn(self.visit(left), self.visit(right))
                except TypeError:
                    raise FormulaError(
                        f"Formula {self.formula!r} does arithmetic on a value that is not a number "
                        "(the min, max, first or last of a text or date column?)."
                    ) from None
            case ast.UnaryOp(op=ast.USub(), operand=operand):
                return -self.visit(operand)
            case ast.UnaryOp(op=ast.UAdd(), operand=operand):
                return self.visit(operand)
            case ast.Call(func=ast.Name(id=name)) if name in AGGREGATES:
                return self.reduce(node, name)
            case ast.Call(func=ast.Name(id="round"), args=args) if len(args) in (1, 2) and not node.keywords:
                return _round(*[self.visit(a) for a in args])
            case ast.Call(func=ast.Name(id="abs"), args=[arg]) if not node.keywords:
                return np.abs(self.visit(arg))
            case ast.Name(id=ident):
                col = self.rows.names.get(ident, ident)
                shown = f"`{col}`" if ident in self.rows.names else col
                raise FormulaError(
                    f"{col!r} in {self.formula!r} is a column, and an aggregate is one value per "
                    f"{'group' if self.keys else 'table'}: wrap it, e.g. sum({shown}), mean({shown}), "
                    f"count_distinct({shown}). Aggregates: {', '.join(AGGREGATES)}."
                )
            case ast.Call(func=ast.Name(id=name)):
                raise FormulaError(
                    f"Formula {self.formula!r} calls {name}() outside an aggregate. "
                    f"Put row functions inside one, e.g. sum({name}(...)); aggregates: {', '.join(AGGREGATES)}."
                )
            case _:
                kind = type(node).__name__
                raise self.refuse(_NODE_NAMES.get(kind, kind))

    def reduce(self, node: ast.Call, name: str) -> Any:
        if node.keywords or any(isinstance(a, ast.Starred) for a in node.args):
            raise FormulaError(f"{name}() takes positional arguments only, in {self.formula!r}.")
        arities = AGGREGATES[name]
        if len(node.args) not in arities:
            want = " or ".join(str(n) for n in arities)
            raise FormulaError(f"{name}() takes {want} argument(s), got {len(node.args)}, in {self.formula!r}.")
        for arg in node.args:
            for inner in ast.walk(arg):
                if isinstance(inner, ast.Call) and isinstance(inner.func, ast.Name) and inner.func.id in AGGREGATES:
                    raise FormulaError(
                        f"{inner.func.id}() inside {name}() in {self.formula!r}: aggregates do not nest. "
                        "Compute the inner one in a scalar step and use it here as $its_id."
                    )
        if not node.args:
            return self._reduce(pd.Series(1.0, index=self.df.index), "size")
        values = self.rows.visit(node.args[0])
        if not isinstance(values, pd.Series):
            values = pd.Series([values] * len(self.df), index=self.df.index)
        if name == "count_if":
            if not (pd.api.types.is_bool_dtype(values) or values.dropna().map(type).isin([bool, np.bool_]).all()):
                raise FormulaError(
                    f"count_if() takes a condition, true or false per row, e.g. count_if(net > 0); "
                    f"in {self.formula!r} it got {values.dtype} values."
                )
            return self._reduce(values.fillna(False).astype(bool).astype(int), "sum")
        if name in _NUMERIC_AGGREGATES:
            values = _num(values, self.formula)
        if name == "percentile":
            q = node.args[1]
            if not (
                isinstance(q, ast.Constant)
                and isinstance(q.value, (int, float))
                and not isinstance(q.value, bool)
                and 0 <= q.value <= 100
            ):
                raise FormulaError(
                    f"percentile(x, q) takes q as a number from 0 to 100, e.g. percentile(net, 95), in {self.formula!r}."
                )
            return self._reduce(values, "quantile", float(q.value) / 100)
        return self._reduce(values, {"count_distinct": "nunique"}.get(name, name))

    def _reduce(self, values: pd.Series, method: str, *args: Any) -> Any:
        if self.keys is None:
            if method == "size":
                return len(values)
            if method in ("first", "last"):
                present = values.dropna()
                return present.iloc[0 if method == "first" else -1] if len(present) else np.nan
            return getattr(values, method)(*args)
        grouped = values.groupby(self.keys, dropna=False, sort=True)
        return getattr(grouped, method)(*args)


def aggregate(formula: str, df: pd.DataFrame, by: list[str] | None = None) -> Any:
    """Evaluate an aggregate formula: one value over ``df``, or a Series with one value per group of ``by``.

    ``sum(amount * (1 - discount))``, ``count_if(net > 0) / count()``,
    ``percentile(net, 95)``: every aggregate reads a row formula -- the same
    language as ``evaluate`` -- and the aggregates combine with arithmetic.
    A bare column outside an aggregate is refused, naming the wrapping it needs.
    """
    tree, names = _parse(formula, df, AGGREGATE_ALLOWED)
    with np.errstate(all="ignore"):
        return _Aggregator(df, names, formula, by).visit(tree.body)
