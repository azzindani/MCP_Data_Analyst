"""Ring-1 pure utility — named column derivations. No I/O, no MCP imports.

Every aggregation tool in this repo takes a `file_path` and groups by columns
that are already in the file. Nothing could make a new one, so a question as
ordinary as "cargo tonnage by year" was unreachable whenever the year was not
already a column: on the SFO air-cargo file the period is the integer `199907`,
and `feature_engineering`'s automatic `date_parts` only fires on columns that
already parse as dates.

The measured cost of that gap: the model gave up on the tools and wrote five
pandas heredocs, deriving `Period_dt`, `year` and `month` by hand. Everything
downstream of those three columns — yearly totals, the era comparison, the
seasonality curve — left the tool surface with them.

These ops are a fixed dispatch table, never `eval`. Each spec is a dict::

    {"name": "year", "op": "text", "column": "Activity Period",
     "how": "slice", "start": 0, "stop": 4, "as": "int"}

`name` is the new column, `op` picks the family, and the remaining keys are that
family's arguments. Specs apply in order, so a later one can read an earlier
one's output.
"""

from __future__ import annotations

import pandas as pd

from shared.column_utils import parse_dates
from shared.progress import info, warn

DERIVE_OPS: frozenset[str] = frozenset({"parse_date", "date_part", "arith", "compare", "text"})

_DATE_PARTS: frozenset[str] = frozenset({"year", "month", "day", "quarter", "weekday", "week", "yearmonth", "date"})
_ARITH: frozenset[str] = frozenset({"add", "sub", "mul", "div", "floordiv", "mod"})
_COMPARE: frozenset[str] = frozenset({"eq", "ne", "gt", "gte", "lt", "lte"})
_TEXT: frozenset[str] = frozenset({"upper", "lower", "strip", "len", "slice", "combine"})
_CASTS: frozenset[str] = frozenset({"int", "float", "str", "bool"})

# How many derived columns one call may add. A derivation is cheap, but a
# caller looping over every column pair is not what this is for.
MAX_DERIVATIONS = 20


# The whole grammar of each op, so one refusal is enough to write a correct
# spec. It used to take five: each error named the single next missing key, and
# the `_need` message listed the keys the CALLER had sent -- which reads as
# confirmation that they were the right ones. A caller who wrote `other_column`
# was shown "Its keys are: name, op, column, operator, other_column" and had no
# way to see that the real key is `other`. Measured round trips to derive one
# ratio: expr -> op -> how -> how value -> other. Five.
#
# Keys here are read off the implementations below and must stay that way: a
# grammar line that documents a key the dispatch does not read would be the
# same defect one level up.
_OP_HELP: dict[str, str] = {
    "parse_date": (
        "{'name': new_column, 'op': 'parse_date', 'column': source} "
        "+ optional 'format', 'dayfirst': auto|true|false, 'as': int|float|str|bool"
    ),
    "date_part": (
        "{'name': new_column, 'op': 'date_part', 'column': source, "
        "'part': year|month|day|quarter|weekday|week|yearmonth|date} "
        "+ optional 'format', 'dayfirst', 'as'"
    ),
    "arith": (
        "{'name': new_column, 'op': 'arith', 'column': left, "
        "'how': add|sub|mul|div|floordiv|mod, and ONE of 'other': another column "
        "or 'value': a literal} + optional 'as'"
    ),
    "compare": (
        "{'name': new_column, 'op': 'compare', 'column': left, "
        "'how': eq|ne|gt|gte|lt|lte, and ONE of 'other' or 'value'} + optional 'as'"
    ),
    "text": (
        "{'name': new_column, 'op': 'text', 'column': source, "
        "'how': upper|lower|strip|len|slice|combine} ; slice adds 'start'/'stop', "
        "combine adds ONE of 'other'/'value' and optional 'separator' ; + optional 'as'"
    ),
}


def grammar_for(op: str) -> str:
    """The full spec line for one op, or every op when the name is unknown."""
    if op in _OP_HELP:
        return f"'{op}' takes {_OP_HELP[op]}."
    return "Specs: " + " | ".join(f"{name}: {help_}" for name, help_ in sorted(_OP_HELP.items()))


class DeriveError(ValueError):
    """A derivation spec that cannot be carried out, named by index."""


def _need(spec: dict, key: str, where: str, op: str = "") -> object:
    if key not in spec or spec[key] is None:
        # Say what the op needs, not what the caller happened to send.
        raise DeriveError(f"{where} needs a '{key}' key. {grammar_for(op)}")
    return spec[key]


def _source(df: pd.DataFrame, spec: dict, where: str) -> pd.Series:
    column = str(_need(spec, "column", where, str(spec.get("op", ""))))
    if column not in df.columns:
        raise DeriveError(f"{where} names column '{column}', which is not in the file. Available: {list(df.columns)}")
    return df[column]


def _operand(df: pd.DataFrame, spec: dict, where: str) -> pd.Series | float | str:
    """The right-hand side: another column when 'other' is given, else 'value'."""
    other = spec.get("other")
    if isinstance(other, str) and other:
        if other not in df.columns:
            raise DeriveError(f"{where} names column '{other}', which is not in the file.")
        return df[other]
    if "value" not in spec or spec["value"] is None:
        raise DeriveError(
            f"{where} needs either 'other' (a column name) or 'value' (a literal). "
            f"{grammar_for(str(spec.get('op', '')))}"
        )
    return spec["value"]


def _cast(series: pd.Series, spec: dict, where: str) -> pd.Series:
    want = spec.get("as")
    if not want:
        return series
    want = str(want).lower()
    if want not in _CASTS:
        raise DeriveError(f"{where} has as='{want}'. Valid casts: {', '.join(sorted(_CASTS))}.")
    if want == "int":
        # Through float first so "1999" and 1999.0 both land, and so a stray
        # non-numeric becomes NaN rather than raising halfway through the frame.
        return pd.to_numeric(series, errors="coerce").astype("Int64")
    if want == "float":
        return pd.to_numeric(series, errors="coerce")
    if want == "bool":
        return series.astype(bool)
    return series.astype(str)


def _as_datetime(series: pd.Series, spec: dict, where: str) -> tuple[pd.Series, dict]:
    if pd.api.types.is_datetime64_any_dtype(series):
        return series, {"dayfirst": False, "reason": "already a datetime column", "ambiguous": False}
    fmt = spec.get("format")
    if fmt:
        parsed = pd.to_datetime(series.astype(str), format=str(fmt), errors="coerce")
        if parsed.notna().sum() == 0:
            raise DeriveError(f"{where}: no value matched format '{fmt}'. Check the format string against the data.")
        return parsed, {"dayfirst": False, "reason": f"format '{fmt}'", "ambiguous": False}
    return parse_dates(series, str(spec.get("dayfirst", "auto")))


def _op_parse_date(df: pd.DataFrame, spec: dict, where: str) -> tuple[pd.Series, dict | None]:
    parsed, fmt = _as_datetime(_source(df, spec, where), spec, where)
    return parsed, fmt


def _op_date_part(df: pd.DataFrame, spec: dict, where: str) -> tuple[pd.Series, dict | None]:
    part = str(_need(spec, "part", where, "date_part")).lower()
    if part not in _DATE_PARTS:
        raise DeriveError(
            f"{where} has part='{part}'. Valid parts: {', '.join(sorted(_DATE_PARTS))}. {grammar_for('date_part')}"
        )
    parsed, fmt = _as_datetime(_source(df, spec, where), spec, where)
    if part == "yearmonth":
        return parsed.dt.to_period("M").astype(str), fmt
    if part == "date":
        return parsed.dt.date.astype(str), fmt
    if part == "week":
        return parsed.dt.isocalendar().week.astype("Int64"), fmt
    if part == "weekday":
        return parsed.dt.dayofweek, fmt
    return getattr(parsed.dt, part), fmt


def _op_arith(df: pd.DataFrame, spec: dict, where: str) -> tuple[pd.Series, dict | None]:
    how = str(_need(spec, "how", where, "arith")).lower()
    if how not in _ARITH:
        raise DeriveError(f"{where} has how='{how}'. Valid: {', '.join(sorted(_ARITH))}. {grammar_for('arith')}")
    left = pd.to_numeric(_source(df, spec, where), errors="coerce")
    right_raw = _operand(df, spec, where)
    right = pd.to_numeric(right_raw, errors="coerce") if isinstance(right_raw, pd.Series) else float(right_raw)
    if how == "add":
        return left + right, None
    if how == "sub":
        return left - right, None
    if how == "mul":
        return left * right, None
    if how == "div":
        return left / right, None
    if how == "floordiv":
        return left // right, None
    return left % right, None


def _op_compare(df: pd.DataFrame, spec: dict, where: str) -> tuple[pd.Series, dict | None]:
    how = str(_need(spec, "how", where, "compare")).lower()
    if how not in _COMPARE:
        raise DeriveError(f"{where} has how='{how}'. Valid: {', '.join(sorted(_COMPARE))}. {grammar_for('compare')}")
    left = _source(df, spec, where)
    right = _operand(df, spec, where)
    if how in {"eq", "ne"}:
        # Equality compares as-is so two text columns work without a cast.
        return (left == right) if how == "eq" else (left != right), None
    lnum = pd.to_numeric(left, errors="coerce")
    rnum = pd.to_numeric(right, errors="coerce") if isinstance(right, pd.Series) else float(right)
    if how == "gt":
        return lnum > rnum, None
    if how == "gte":
        return lnum >= rnum, None
    if how == "lt":
        return lnum < rnum, None
    return lnum <= rnum, None


def _op_text(df: pd.DataFrame, spec: dict, where: str) -> tuple[pd.Series, dict | None]:
    how = str(_need(spec, "how", where, "text")).lower()
    if how not in _TEXT:
        raise DeriveError(f"{where} has how='{how}'. Valid: {', '.join(sorted(_TEXT))}. {grammar_for('text')}")
    text = _source(df, spec, where).astype(str)
    if how == "upper":
        return text.str.upper(), None
    if how == "lower":
        return text.str.lower(), None
    if how == "strip":
        return text.str.strip(), None
    if how == "len":
        return text.str.len(), None
    if how == "combine":
        right = _operand(df, spec, where)
        sep = str(spec.get("separator", ""))
        right_text = right.astype(str) if isinstance(right, pd.Series) else str(right)
        return text + sep + right_text, None
    start = spec.get("start", 0)
    stop = spec.get("stop")
    try:
        start_i = int(start)
        stop_i = None if stop is None else int(stop)
    except TypeError, ValueError:
        raise DeriveError(f"{where}: slice needs integer 'start' and 'stop'. Got {start!r} and {stop!r}.") from None
    return text.str[start_i:stop_i], None


_DISPATCH = {
    "parse_date": _op_parse_date,
    "date_part": _op_date_part,
    "arith": _op_arith,
    "compare": _op_compare,
    "text": _op_text,
}


def apply_derivations(df: pd.DataFrame, specs: list[dict]) -> tuple[list[str], list[dict]]:
    """Add each derived column to `df` in order, in place.

    Returns ``(new_column_names, progress_entries)``. Raises :class:`DeriveError`
    with the offending index for any spec that cannot be carried out — a
    half-applied frame written to disk would be worse than a refusal.
    """
    if not specs:
        return [], []
    if not isinstance(specs, list):
        raise DeriveError("derive must be a list of specs, e.g. [{'name': ..., 'op': ..., 'column': ...}].")
    if len(specs) > MAX_DERIVATIONS:
        raise DeriveError(f"{len(specs)} derivations requested; {MAX_DERIVATIONS} is the limit for one call.")

    added: list[str] = []
    progress: list[dict] = []
    for index, spec in enumerate(specs):
        where = f"Derivation {index}"
        if not isinstance(spec, dict):
            raise DeriveError(f"{where} is {type(spec).__name__}, not a dict.")
        op = str(spec.get("op", "")).lower()
        if op not in DERIVE_OPS:
            raise DeriveError(f"{where} has op='{op}'. Valid ops: {', '.join(sorted(DERIVE_OPS))}. {grammar_for(op)}")
        name = str(_need(spec, "name", where, op))
        if name in df.columns and name not in added:
            progress.append(warn(f"'{name}' already exists — overwriting it", where))

        series, date_fmt = _DISPATCH[op](df, spec, where)
        df[name] = _cast(series, spec, where)
        added.append(name)

        detail = f"{op}" + (f" ({spec['how']})" if spec.get("how") else "")
        progress.append(info(f"Derived '{name}'", detail))
        if date_fmt and date_fmt.get("ambiguous"):
            progress.append(
                warn(
                    f"{where}: date order is ambiguous — read as "
                    + ("day-first" if date_fmt["dayfirst"] else "month-first"),
                    f"{date_fmt['reason']}. Add dayfirst='true' or 'false' to the spec.",
                )
            )
    return added, progress
