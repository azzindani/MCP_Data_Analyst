"""Ring-1 pure utility — column inference across all tiers. No I/O, no MCP imports."""

from __future__ import annotations

import re

import pandas as pd

# Keywords that suggest mean is the right aggregation
_AGG_MEAN = frozenset(
    {
        "rate",
        "ratio",
        "pct",
        "percent",
        "percentage",
        "score",
        "avg",
        "average",
        "mean",
        "index",
        "idx",
        "temperature",
        "temp",
        "speed",
        "density",
        "grade",
        "gpa",
        "weight",
        "proportion",
        "fraction",
        "growth",
        "margin",
        "efficiency",
        "utilization",
        "utilisation",
        "yield",
        "conversion",
        "accuracy",
        "precision",
        "recall",
        "f1",
        "satisfaction",
        "rating",
        "probability",
        "prob",
        "likelihood",
    }
)

# Keywords that suggest max
_AGG_MAX = frozenset(
    {
        "max",
        "maximum",
        "peak",
        "high",
        "highest",
        "ceiling",
        "top",
        "upper",
        "limit",
        "cap",
        "best",
    }
)

# Keywords that suggest min
_AGG_MIN = frozenset(
    {
        "min",
        "minimum",
        "low",
        "lowest",
        "floor",
        "bottom",
        "base",
        "lower",
        "worst",
    }
)


def is_numeric_col(series: pd.Series) -> bool:
    """True for numeric columns excluding boolean dtype.

    pd.api.types.is_numeric_dtype returns True for bool, which causes
    numpy boolean subtract errors in corr/std/skew/quantile operations.
    """
    return pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series)


def infer_agg(col: str, series: pd.Series | None = None) -> str:
    """
    Infer the best aggregation function for a numeric column.

    Returns one of: "sum", "mean", "max", "min".

    Priority order:
    1. Name-keyword match (most reliable)
    2. Distribution heuristic: values in [0, 1] → mean (likely a rate/ratio)
    3. Default: sum
    """
    lower = col.lower()
    words = set(re.split(r"[^a-zA-Z]+", lower))
    words.discard("")

    if words & _AGG_MEAN or any(k in lower for k in _AGG_MEAN):
        return "mean"
    if words & _AGG_MAX or any(k in lower for k in _AGG_MAX):
        return "max"
    if words & _AGG_MIN or any(k in lower for k in _AGG_MIN):
        return "min"

    # Distribution heuristic: proportion/rate columns sit in [0, 1]
    if series is not None:
        try:
            valid = series.dropna()
            if len(valid) > 0:
                mn, mx = float(valid.min()), float(valid.max())
                if mn >= 0.0 and mx <= 1.0:
                    return "mean"
        except Exception:
            pass

    return "sum"


def agg_label(agg: str) -> str:
    """Human-readable label prefix for an aggregation function."""
    return {"sum": "Total", "mean": "Avg", "max": "Max", "min": "Min"}.get(agg, "Total")


def parse_agg_overrides(overrides: list[str] | None) -> dict[str, str]:
    """
    Parse a list of "column:agg" strings into a dict.

    Example input: ["revenue:sum", "rate:mean", "temperature:mean"]
    """
    result: dict[str, str] = {}
    if not overrides:
        return result
    valid = {"sum", "mean", "max", "min"}
    for item in overrides:
        if ":" in item:
            col, agg = item.split(":", 1)
            col, agg = col.strip(), agg.strip().lower()
            if agg in valid:
                result[col] = agg
    return result


# ---------------------------------------------------------------------------
# Filter conditions
# ---------------------------------------------------------------------------

# `conditions` is a bare list[dict] in every schema that takes one, so the key
# names live nowhere a caller can read them. `op` has always accepted `operator`
# as an alias; the column key accepted exactly one spelling, and a caller who
# wrote `variable` got "Column '' not found" -- the empty string being the
# tool's own default, quoted back as if the caller had asked for a column named
# "". The same shape as delete_paragraph answering with an index nobody sent.
COLUMN_KEYS: tuple[str, ...] = ("column", "col", "field", "variable", "name", "column_name")


def condition_column(cond: dict) -> str:
    """The column a filter condition names, under any of its spellings."""
    for key in COLUMN_KEYS:
        value = cond.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return ""


def missing_column_error(cond: dict) -> tuple[str, str]:
    """(error, hint) for a condition that names no column at all.

    Kept separate from "names a column that does not exist" because the two
    need different advice and used to share one misleading message.
    """
    keys = ", ".join(str(k) for k in cond) or "none"
    return (
        f"This filter condition names no column. Its keys are: {keys}",
        f'Give the column under "column" — {", ".join(COLUMN_KEYS[1:])} are accepted too. '
        'A condition looks like {"column": "spends", "op": "gt", "value": 0}.',
    )


# The operand each filter op compares against, and the key(s) it may arrive
# under. Ops absent from this map compare against nothing (is_null, not_null).
#
# An earlier round taught `between`, `isin` and `regex` to name the key they
# were missing, because those three read unusually-named keys (min/max, values,
# pattern) and a caller could not guess them. The ten ops that read plain
# `value` were left alone -- they were not the ones being debugged -- and they
# are the ops everybody uses. So they kept doing this:
#
#     filter_dataset(f, [{"column": "spend", "op": "gt"}])
#     -> error: 'value'
#
# The entire error is one quoted word. And in data-medium's filter_rows, which
# reads the operand with cond.get("value") instead, it was worse than an
# unhelpful error -- there was no error:
#
#     filter_rows(f, [{"column": "region", "op": "equals"}])
#     -> success: true, rows_kept: 0
#
# Every value compared against None, nothing matched, and the tool wrote an
# empty CSV over the caller's filtered output and reported it as a filter that
# worked. A condition missing its operand is not a condition that excludes
# everything; it is a condition nobody finished writing.
FILTER_OPERANDS: dict[str, tuple[str, ...]] = {
    "equals": ("value",),
    "not_equals": ("value",),
    "contains": ("value",),
    "not_contains": ("value",),
    "starts_with": ("value",),
    "ends_with": ("value",),
    "gt": ("value",),
    "lt": ("value",),
    "gte": ("value",),
    "lte": ("value",),
    "isin": ("values", "value"),
    "not_isin": ("values", "value"),
    "regex": ("pattern", "value"),
    "between": ("min", "max", "value"),
    "quantile_between": ("min", "max", "min_q", "max_q", "value"),
    "date_range": ("start", "end"),
}

# Ops whose operand goes through float(); a non-numeric one raised
# "float() argument must be a string or a real number, not 'NoneType'",
# which names neither the column nor the condition it came from.
_NUMERIC_FILTER_OPS: frozenset[str] = frozenset({"gt", "lt", "gte", "lte", "between", "quantile_between"})
_RANGE_FILTER_OPS: frozenset[str] = frozenset({"between", "quantile_between"})


def _is_number(value) -> bool:
    if isinstance(value, bool):
        return False
    try:
        float(value)
    except TypeError, ValueError:
        return False
    return True


def filter_operand_error(cond: dict, op: str, index: int = -1) -> str:
    """Why this condition cannot be evaluated, or "" if it can.

    Checked before dispatch so a half-written condition is refused by name
    rather than reaching float() or a bare subscript.
    """
    wanted = FILTER_OPERANDS.get(op)
    if not wanted:
        return ""  # is_null / not_null take no operand
    where = f"Condition {index}" if index >= 0 else "This condition"
    column = condition_column(cond) or "?"

    # A column-to-column condition carries no literal by design; without this
    # it was refused for "having nothing to compare against".
    if other_column(cond):
        if op in _COLUMN_PAIR_OPS:
            return ""
        return (
            f"{where} ('{column}' {op}) names another column, but {op} compares against a value. "
            f"Column-to-column ops: {', '.join(sorted(_COLUMN_PAIR_OPS))}."
        )

    present = [k for k in wanted if cond.get(k) is not None]

    if op in _RANGE_FILTER_OPS:
        pair = [k for k in wanted if k != "value"]
        lo, hi = pair[0], pair[1]
        both = cond.get(lo) is not None and cond.get(hi) is not None
        value = cond.get("value")
        as_pair = isinstance(value, list | tuple) and len(value) == 2
        if not both and not as_pair:
            return (
                f"{where} ('{column}' {op}) needs both '{lo}' and '{hi}'. Its keys are: "
                f"{', '.join(str(k) for k in cond) or 'none'}. "
                f"Write it as {{'column': '{column}', 'op': '{op}', '{lo}': ..., '{hi}': ...}}, "
                "or give 'value' as a two-item list."
            )
        bounds = list(value) if as_pair and not both else [cond.get(lo), cond.get(hi)]
        bad = [b for b in bounds if not _is_number(b)]
        if bad:
            return f"{where} ('{column}' {op}) needs numeric bounds; got {bad!r}."
        return ""

    if op == "date_range":
        if not present:
            return (
                f"{where} ('{column}' date_range) names neither 'start' nor 'end', so it would keep every row. "
                f"Give at least one, as an ISO date."
            )
        return ""

    if not present:
        names = " or ".join(f"'{k}'" for k in wanted)
        return (
            f"{where} ('{column}' {op}) has no {names} to compare against. Its keys are: "
            f"{', '.join(str(k) for k in cond) or 'none'}. "
            f"Write it as {{'column': '{column}', 'op': '{op}', '{wanted[0]}': ...}}."
        )

    if op in _NUMERIC_FILTER_OPS and not _is_number(cond.get(present[0])):
        return f"{where} ('{column}' {op}) needs a number to compare against; got {cond.get(present[0])!r}."

    return ""


def paired_numeric(df: pd.DataFrame, col_a: str, col_b: str) -> tuple[pd.Series, pd.Series]:
    """Two numeric columns as aligned pairs, dropping rows either one is null in.

    A paired test compares row i of one column against row i of the other, so
    the two series have to come out of the same rows. Dropping the nulls from
    each column *separately* and then cutting both to the shorter length looks
    equivalent and is not: after the first null, every pair is offset by one,
    and the offset grows with each null after it.

        a = to_numeric(df[col_a]).dropna()
        b = to_numeric(df[col_b]).dropna()
        n = min(len(a), len(b))
        pearsonr(a.iloc[:n], b.iloc[:n])

    On the reference dataset, clicks against link_clicks -- 546 nulls out of
    16,834, the first at row 2,011:

        as written           r = 0.0015   p = 0.847257   "not significant"
        pairwise deletion    r = 0.9256   p < 1e-300     n = 16,288

    A near-perfect correlation reported as no correlation at all, deterministic,
    under success: true. Pairwise deletion is what every stats package means by
    dropping missing values from a paired test.
    """
    pair = pd.DataFrame(
        {
            "a": pd.to_numeric(df[col_a], errors="coerce"),
            "b": pd.to_numeric(df[col_b], errors="coerce"),
        }
    ).dropna()
    return pair["a"], pair["b"]


# --- Date orientation -------------------------------------------------------

# Two integers then a year, separated by - / or . -- the only shape where
# day-first and month-first disagree. ISO (4-digit first field) is unambiguous.
_DATE_TRIPLE = re.compile(r"^\s*(\d{1,4})[-/.](\d{1,2})[-/.](\d{2,4})")

_MAX_DATE_SAMPLE = 5000

# The `dayfirst` vocabulary, in one place because five tools document it in
# their own 80-character description and the words have to agree. These are
# EXACTLY the documented three, case-insensitively, plus "" for an argument the
# caller omitted. "yes", "no", "1" and "0" were accepted here for one draft of
# this fix and that was the original defect in miniature: an undocumented
# spelling, silently coerced, choosing a date interpretation on the caller's
# behalf. A model that types "yes" should be corrected, not guessed at, and a
# client sending a real JSON boolean is refused earlier by the type contract.
DAYFIRST_CHOICES: frozenset[str] = frozenset({"auto", "true", "false"})
_DAYFIRST_TRUE: frozenset[str] = frozenset({"true"})
_DAYFIRST_FALSE: frozenset[str] = frozenset({"false"})
_DAYFIRST_AUTO: frozenset[str] = frozenset({"auto", ""})


def detect_dayfirst(series: pd.Series, sample: int = _MAX_DATE_SAMPLE) -> tuple[bool, str, bool]:
    """Decide day-first vs month-first from the column itself.

    Returns ``(dayfirst, reason, ambiguous)``.

    Every date parse in this repo used to hardcode ``dayfirst=False``, which is
    pandas' default and correct for US-style files. Handed a day-first column it
    does not fail -- it transposes. On the SFO air-cargo dataset every value is
    the first of a month::

        01-07-1999  ->  1999-01-07   (read as 7 January)
        truth       ->  1999-07-01   (1 July)

    Every row parsed, ``errors="coerce"`` dropped nothing, and 291 distinct
    months collapsed into 25 Januaries with the real month hiding in the day
    field. Yearly totals stayed right; seasonality and every MoM/QoQ comparison
    were silently wrong under ``success: true``.

    The rules below are ordered decisive-first. A value above 12 in either
    position settles it outright and is the standard test. The constant-field
    rule catches the case that bites monthly data: a column spanning several
    years cannot sit inside one calendar month, so a first field that never
    changes while the second cycles is a day, not a month.

    When nothing is decisive the answer stays ``False`` -- pandas' default, so
    no existing behaviour moves -- and ``ambiguous`` comes back True so the
    caller can say so instead of guessing quietly.
    """
    text = series.dropna().astype(str)
    if len(text) > sample:
        text = text.iloc[:sample]
    if text.empty:
        return False, "no values to inspect", False

    first: list[int] = []
    second: list[int] = []
    years: set[str] = set()
    for value in text:
        match = _DATE_TRIPLE.match(value)
        if not match:
            continue
        a, b, c = match.group(1), match.group(2), match.group(3)
        if len(a) == 4:
            # 1999-07-01 -- year first, nothing to disambiguate.
            return False, "ISO year-first dates", False
        first.append(int(a))
        second.append(int(b))
        years.add(c)

    if not first:
        return False, "no day/month/year triples found", False

    first_over = any(v > 12 for v in first)
    second_over = any(v > 12 for v in second)
    if first_over and not second_over:
        return True, "field 1 exceeds 12, so it is the day", False
    if second_over and not first_over:
        return False, "field 2 exceeds 12, so it is the day", False
    if first_over and second_over:
        return False, "both fields exceed 12 -- the column mixes formats", True

    n_first, n_second = len(set(first)), len(set(second))
    if len(years) > 1:
        if n_first == 1 and n_second >= 3:
            return True, "field 1 is constant across years, so it is the day", False
        if n_second == 1 and n_first >= 3:
            return False, "field 2 is constant across years, so it is the day", False

    return False, "no value above 12 -- day and month are interchangeable here", True


def parse_dates(series: pd.Series, dayfirst: str = "auto") -> tuple[pd.Series, dict]:
    """Parse a date column, choosing the orientation from the data.

    ``dayfirst`` is a tristate string so it survives an MCP tool signature:
    ``"auto"`` detects, ``"true"``/``"false"`` force it. See
    :func:`detect_dayfirst` for what silent misdetection costs.

    The second return value is metadata for the caller's ``progress`` list --
    ``{"dayfirst": bool, "reason": str, "ambiguous": bool}``. Callers must
    surface ``ambiguous`` rather than swallow it; that is the whole point.
    """
    choice = str(dayfirst).strip().lower()
    if choice in _DAYFIRST_TRUE:
        flag, reason, ambiguous = True, "caller passed dayfirst=true", False
    elif choice in _DAYFIRST_FALSE:
        flag, reason, ambiguous = False, "caller passed dayfirst=false", False
    elif choice in _DAYFIRST_AUTO:
        flag, reason, ambiguous = detect_dayfirst(series)
    else:
        # Anything unrecognised used to fall through to auto-detect in silence,
        # which made the parameter's documented vocabulary a suggestion:
        #
        #     dayfirst="yes"     -> day-first   (truthy alias, undocumented)
        #     dayfirst="banana"  -> month-first (fell through to auto)
        #
        # Both answered success: true with different dates, so a typo -- ture,
        # flase, Yes -- silently chose an interpretation for the caller and the
        # response said nothing. The dates then flow into trend, seasonality,
        # rolling stats and the chart. Office's bold/italic is the same
        # tri-state string and refuses a wrong value naming the accepted forms;
        # this is that contract, applied to the copy that drifted.
        raise ValueError(
            f"dayfirst='{dayfirst}' is not a value this tool takes. "
            f"Use one of: {', '.join(sorted(DAYFIRST_CHOICES))}. "
            "'auto' reads the orientation off the data, which is the default."
        )

    parsed = pd.to_datetime(series, format="mixed", dayfirst=flag, errors="coerce")
    return parsed, {"dayfirst": flag, "reason": reason, "ambiguous": ambiguous}


_COLUMN_PAIR_OPS: frozenset[str] = frozenset({"equals", "not_equals", "gt", "gte", "lt", "lte"})

# Spellings a caller reaches for when comparing one column against another.
OTHER_COLUMN_KEYS: tuple[str, ...] = ("other_column", "other_col", "compare_column", "value_column")


def other_column(cond: dict) -> str:
    """The second column named by a condition, or "" when it compares a literal."""
    for key in OTHER_COLUMN_KEYS:
        name = cond.get(key)
        if isinstance(name, str) and name:
            return name
    return ""


def column_pair_mask(df: pd.DataFrame, cond: dict, col: str, op: str) -> pd.Series | None:
    """Mask for a column-against-column condition, or None if it is not one.

    Filter conditions could only ever compare a column to a *literal*, so
    "which rows disagree between these two columns" had no expression at all --
    the codeshare count on the SFO cargo file (``Operating Airline`` vs
    ``Published Airline``, 1,498 rows, quoted in the report that shipped) came
    out of a pandas heredoc because no tool could say it.

    Numeric ordering coerces both sides; equality compares them as they are, so
    two string columns work without the caller casting anything.
    """
    other = other_column(cond)
    if not other:
        return None
    if op not in _COLUMN_PAIR_OPS:
        raise ValueError(
            f"Filter op '{op}' compares a column to a value, not to another column. "
            f"Column-to-column ops: {', '.join(sorted(_COLUMN_PAIR_OPS))}."
        )
    if other not in df.columns:
        raise ValueError(f"Column '{other}' not found. Available: {list(df.columns)}")

    left, right = df[col], df[other]
    if op == "equals":
        return left == right
    if op == "not_equals":
        return left != right

    lnum = pd.to_numeric(left, errors="coerce")
    rnum = pd.to_numeric(right, errors="coerce")
    if op == "gt":
        return lnum > rnum
    if op == "gte":
        return lnum >= rnum
    if op == "lt":
        return lnum < rnum
    return lnum <= rnum


def date_note(info: dict, column: str) -> dict:
    """One ``progress`` entry naming the orientation chosen and why.

    Ambiguity comes back as a ``warn`` on purpose. A caller that cannot tell
    day-first from month-first has a 50% chance of reporting transposed months
    under ``success: true``, and the only way out is to say so.
    """
    from shared.progress import info as _info
    from shared.progress import warn as _warn

    order = "day-first (DD-MM-YYYY)" if info["dayfirst"] else "month-first (MM-DD-YYYY)"
    if info["ambiguous"]:
        return _warn(
            f"'{column}' date order is ambiguous — read as {order}",
            f"{info['reason']}. Pass dayfirst='true' or 'false' to settle it.",
        )
    return _info(f"Read '{column}' as {order}", info["reason"])


# ---------------------------------------------------------------------------
# One date-detection rule
# ---------------------------------------------------------------------------

# How much of a sample must parse before a column counts as dates. Shared with
# the numeric guess in auto_detect_schema, which had always used 0.9 while the
# date guess beside it used `errors="raise"` -- all or nothing, on the first
# fifty values. Three DD-MM-YYYY columns of one file were typed datetime and a
# fourth, identically formatted, was typed text, because one value near the top
# of that column would not parse.
DATE_MATCH_THRESHOLD = 0.9

# Values a type guess reads, spread across the column rather than taken from
# its head. `head(50)` sees whatever the file happens to open with: on the
# reference dataset the first null in one column is at row 2,011.
TYPE_SAMPLE_SIZE = 200


def type_sample(series: pd.Series) -> pd.Series:
    """Values a type guess is made from -- spread across the whole column."""
    non_null = series.dropna()
    if len(non_null) <= TYPE_SAMPLE_SIZE:
        return non_null
    step = max(1, len(non_null) // TYPE_SAMPLE_SIZE)
    return non_null.iloc[::step][:TYPE_SAMPLE_SIZE]


def looks_like_dates(series: pd.Series, dayfirst: str = "auto") -> tuple[bool, float, dict]:
    """Is this column dates? Returns (verdict, match rate, dayfirst metadata).

    Four call sites in this repo asked this question four different ways --
    `head(10)` twice, `head(50)` twice, all with `errors="raise"` -- so the
    same column could be a date column to `cohort_analysis` and not to
    `auto_detect_schema`. They all read this now.

    `parse_dates` does the parsing, so orientation is chosen from the data
    rather than assumed, and an ambiguous orientation comes back in the third
    return value instead of being swallowed.
    """
    sample = type_sample(series)
    if len(sample) == 0:
        return False, 0.0, {"dayfirst": False, "reason": "no values", "ambiguous": False}
    parsed, meta = parse_dates(sample, dayfirst)
    rate = float(parsed.notna().mean())
    return rate >= DATE_MATCH_THRESHOLD, rate, meta
