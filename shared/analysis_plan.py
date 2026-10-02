"""What each column is, decided before any chart is drawn.

The sweep's dashboard drew one chart per column pair. It did not know that
campaign_platform, campaign_type and communication_medium are the same split
under three names, so six bars and three identical donuts showed one fact; that
audience_type is "'-" in 90% of rows; that subchannel nests inside platform; or
that the rows are one per day per ad. An analyst works these out first, and a
dashboard planned from them can ask questions of the data instead of pairing
its columns.

`plan(df)` returns, for the page and for the caller:

- a role per column: date, calendar (a number that is a piece of a date: year, week, day of
  month), id, measure (additive or not), dimension, flag, constant, text; a unit (currency, count, percent, number); the share of
  placeholders ("-", "N/A", "Undetermined", its own name);
- alias groups: dimensions that split the rows identically;
- hierarchies: a dimension whose every value sits inside one value of another;
- the grain: the date column, its frequency, span and rows per period;
- measures in headline order, and dimensions ranked by how much of the
  headline measure they explain (between-group share of its variance).
"""

from __future__ import annotations

import re
from typing import Any

import numpy as np
import pandas as pd

from shared.column_utils import calendar_part, is_identifier, is_numeric_col, parse_date_column
from shared.data_alerts import placeholder_counts
from shared.metrics import better_of, unit_of

# A measure whose parts do not add up to its whole: summing ages or rates is
# not a rough answer but a wrong one, so these are averaged.
_NON_ADDITIVE = frozenset(
    "age rate ratio pct percent percentage price score rating avg average mean median temperature temp "
    "lat lon lng latitude longitude year month day hour index rank level grade share ctr cvr cpc cpm cpa "
    "roas margin probability prob duration tenure balance "
    "lead latency wait delay elapsed interval speed adr arpu aov per".split()
)
# The column a page counts rows with when a table has nothing to add up (labels, text, codes).
ROWS_COLUMN = "Rows"


# A measure adds up only when its name says so. Summing a property of a row (an age, a BMI, a blood
# pressure, a bedroom count, an area) is not a rough total but a meaningless one, and the sweep's corpus
# showed "Total BMI" and "Total Blood pressure" as headline KPIs; an average of a quantity that does add up
# is merely modest. So the default is the mean, and these words earn the sum.
_ADDITIVE = frozenset(
    "revenue sales income gmv profit spend spends cost costs expense expenses amount amt total quantity qty "
    "units unit count counts clicks impressions views visits sessions users orders conversions installs "
    "downloads sold volume kwh mwh energy production output generated consumption emissions fare tip tax "
    "fees fee payment payments budget bill rows records".split()
)
_COORDINATE = frozenset("lat lon lng long latitude longitude".split())
# A number with this few values is a code (a sex, a class, a grade), not an amount: it groups rows.
MAX_CODE_LEVELS = 4
MIN_SENTENCE_CHARS = 40
_CODE_WORDS = frozenset("category type status label group segment class kind tier grade level cluster".split())
_ID_WORDS = frozenset("id key code uuid guid sku ref number no".split())
# Measures in the order a reader asks about them.
_HEADLINE = ("revenue", "sales", "income", "gmv", "profit", "spend", "spends", "cost", "conversions", "orders",
             "clicks", "impressions", "users", "sessions", "units", "quantity")  # fmt: skip

MAX_DIMENSION_LEVELS = 50
PLACEHOLDER_HEAVY = 0.5


def _words(name: Any) -> set[str]:
    return {w for w in re.split(r"[^a-z0-9]+", str(name).lower()) if w}


def _frequency(dates: pd.Series) -> str:
    unique = pd.Series(dates.dropna().unique()).sort_values()
    if len(unique) < 2:
        return ""
    step = float(unique.diff().dropna().dt.total_seconds().median()) / 86400
    for limit, name in ((0.9, "sub-day"), (1.5, "day"), (8, "week"), (32, "month"), (95, "quarter")):
        if step <= limit:
            return name
    return "year"


def _role(col: Any, s: pd.Series, rows: int) -> tuple[str, pd.Series | None]:
    if col == ROWS_COLUMN and bool((s == 1).all()):
        return "measure", None  # the page's own row counter: all 1s, and exactly what a count adds up
    present = s.dropna()
    if present.nunique() <= 1:
        return "constant", None
    if pd.api.types.is_bool_dtype(s):
        return "flag", None
    if pd.api.types.is_datetime64_any_dtype(s):
        return "date", s
    if is_numeric_col(s):
        if is_identifier(str(col), s):
            return "id", None
        if set(pd.unique(present)) <= {0, 1}:
            return "flag", None
        if calendar_part(str(col), s):
            return "calendar", None
        if _words(col) & _COORDINATE:
            return "coordinate", None
        levels = present.nunique()
        # A number named for a kind of thing (a type, a status, a segment) is a category held as a code.
        if (
            _words(col) & _CODE_WORDS
            and 2 <= levels <= MAX_DIMENSION_LEVELS
            and bool((present == present.round()).all())
        ):
            return "dimension", None
        if (
            2 <= levels <= MAX_CODE_LEVELS
            and bool((present == present.round()).all())
            and not _words(col) & _ADDITIVE
            and len(present) >= 20
        ):
            return "dimension", None
        return "measure", None
    parsed = parse_date_column(s)
    if parsed is not None:
        return "date", parsed
    levels = present.nunique()
    if levels > MAX_DIMENSION_LEVELS and levels > 0.5 * max(len(present), 1):
        return ("id" if _words(col) & _ID_WORDS else "text"), None
    if _written_in_sentences(present):
        return "text", None
    return "dimension", None


def _written_in_sentences(present: pd.Series) -> bool:
    """A few long sentences repeated down the rows (a footnote, a remark) is a note, not a category to split by:
    as a filter it is a wall of pills, and as a segment it headlines a page with a paragraph."""
    values = pd.Series(present.astype(str).unique()[:500])
    lengths = values.str.len()
    return bool(len(values) and lengths.median() >= MIN_SENTENCE_CHARS and values.str.count(" ").median() >= 4)


def _last_day_is_complete(parsed: pd.Series) -> bool:
    """False when timestamped data stops partway through its last day: the latest time on that day is well short of
    the latest time the days before it reached. Dates alone (no times) are whole days."""
    present = parsed.dropna()
    if present.empty:
        return True
    day = present.dt.normalize()
    seconds = (present - day).dt.total_seconds()
    if not bool((seconds > 0).any()):
        return True
    latest = seconds.groupby(day).max()
    if len(latest) < 3:
        return True
    last = day.max()
    return bool(latest[last] >= 0.9 * float(latest.drop(last).quantile(0.9)))


def _first_day_is_complete(parsed: pd.Series) -> bool:
    """False when timestamped data starts partway through its first day: the earliest time on that day is well
    after the earliest time the days that follow it reached. Dates alone (no times) are whole days."""
    present = parsed.dropna()
    if present.empty:
        return True
    day = present.dt.normalize()
    seconds = (present - day).dt.total_seconds()
    if not bool((seconds > 0).any()):
        return True
    earliest = seconds.groupby(day).min()
    if len(earliest) < 3:
        return True
    first = day.min()
    return bool(earliest[first] <= float(earliest.drop(first).quantile(0.1)) + 0.1 * 86400)


def needs_row_count(df: pd.DataFrame) -> bool:
    """True when no column of `df` is a measure: a dashboard of it would have nothing to chart but the rows."""
    if ROWS_COLUMN in df.columns:
        return False
    rows = len(df)
    return not any(_role(c, df[c], rows)[0] == "measure" for c in df.columns)


def with_row_counter(df: pd.DataFrame) -> pd.DataFrame:
    """`df`, with a `Rows` column of 1s when it has nothing to add up (the page counts rows with it)."""
    return df.assign(**{ROWS_COLUMN: 1}) if needs_row_count(df) else df


def _variance_explained(values: pd.Series, groups: pd.Series) -> float:
    """Between-group share of the variance of `values` (eta squared)."""
    frame = pd.DataFrame({"v": pd.to_numeric(values, errors="coerce"), "g": groups.astype(str)}).dropna()
    if len(frame) < 3 or frame["v"].var() == 0:
        return 0.0
    grand = frame["v"].mean()
    stats = frame.groupby("g")["v"].agg(["mean", "count"])
    between = float((stats["count"] * (stats["mean"] - grand) ** 2).sum())
    total = float(((frame["v"] - grand) ** 2).sum())
    return between / total if total else 0.0


def plan(df: pd.DataFrame) -> dict[str, Any]:
    rows = len(df)
    columns: dict[str, dict[str, Any]] = {}
    dates: dict[str, pd.Series] = {}
    for col in df.columns:
        s = df[col]
        role, parsed = _role(col, s, rows)
        info: dict[str, Any] = {"role": role, "levels": int(s.nunique(dropna=True))}
        if role == "date" and parsed is not None:
            dates[str(col)] = parsed
        if role == "measure":
            words = _words(col)
            info["unit"] = unit_of(col)
            values = pd.to_numeric(s, errors="coerce").dropna()
            if info["unit"] == "percent" and len(values) and float(values.abs().max()) > 1:
                # Already in points: 5.2 is 5.2%, and drawn as a share it would read 520%.
                info["unit"] = "number"
            info["additive"] = (
                bool(words & _ADDITIVE)
                and not (words & _NON_ADDITIVE)
                and info["unit"] != "percent"
                and bool((pd.to_numeric(s, errors="coerce").dropna() >= 0).all())
            )
            info["better"] = better_of(col)
        if role in ("dimension", "flag", "text"):
            found = placeholder_counts(s, col)
            if found and rows:
                info["placeholder_share"] = round(sum(found.values()) / rows, 4)
                info["placeholders"] = found
        columns[str(col)] = info

    measures = [c for c, i in columns.items() if i["role"] == "measure"]

    def headline_rank(col: str) -> tuple[int, int]:
        # "clicks (sales)" is clicks, from the sales dataset: not a sales measure.
        words = _words(re.sub(r"\s*\([^)]*\)$", "", col))
        hits = [i for i, w in enumerate(_HEADLINE) if w in words]
        return (min(hits) if hits else len(_HEADLINE), list(columns).index(col))

    measures.sort(key=headline_rank)

    # Dimensions that split the rows identically are one dimension.
    candidates = [c for c, i in columns.items() if i["role"] in ("dimension", "flag")]
    by_partition: dict[bytes, list[str]] = {}
    for col in candidates:
        key = pd.factorize(df[col].astype(str))[0].tobytes()
        by_partition.setdefault(key, []).append(col)
    aliases = [group for group in by_partition.values() if len(group) > 1]
    for group in aliases:
        for other in group[1:]:
            columns[other]["alias_of"] = group[0]
    representatives = [g[0] for g in by_partition.values()]

    # A dimension whose every value sits inside one value of another.
    hierarchies = []
    small = [c for c in representatives if 2 <= columns[c]["levels"] <= 200][:20]
    for child in small:
        for parent in small:
            if child == parent or columns[child]["levels"] <= columns[parent]["levels"]:
                continue
            spread = df[[child, parent]].dropna().groupby(child)[parent].nunique()
            if len(spread) and int(spread.max()) == 1:
                hierarchies.append({"child": child, "parent": parent})
    for h in hierarchies:
        columns[h["child"]].setdefault("parents", []).append(h["parent"])

    headline = measures[0] if measures else ""
    ranked = []
    for col in representatives:
        info = columns[col]
        if info.get("placeholder_share", 0) >= PLACEHOLDER_HEAVY or info["levels"] > MAX_DIMENSION_LEVELS:
            continue
        explained = _variance_explained(df[headline], df[col]) if headline else 0.0
        info["explains"] = round(explained, 4)
        ranked.append(col)
    # The top of a hierarchy first -- the platform its subchannels and devices
    # sit inside -- then coarser before finer, then what explains more.
    parents = {h["parent"] for h in hierarchies}
    ranked.sort(
        key=lambda c: (
            0 if c in parents else 1,
            len(columns[c].get("parents", [])),
            columns[c]["levels"],
            -columns[c].get("explains", 0.0),
        )
    )

    grain: dict[str, Any] = {}
    if dates:
        date_col = max(dates, key=lambda c: dates[c].nunique())
        parsed = dates[date_col]
        per_period = parsed.value_counts()
        grain = {
            "date": date_col,
            "frequency": _frequency(parsed),
            "start": str(parsed.min().date()) if pd.notna(parsed.min()) else "",
            "end": str(parsed.max().date()) if pd.notna(parsed.max()) else "",
            "last_day_complete": _last_day_is_complete(parsed),
            "first_day_complete": _first_day_is_complete(parsed),
            "periods": int(parsed.nunique()),
            "rows_per_period": float(np.median(per_period)) if len(per_period) else 0.0,
        }
        keyed = [date_col, *ranked]
        grain["unique_rows"] = bool(not df.duplicated(subset=keyed).any()) if keyed else False

    notes = []
    for group in aliases:
        notes.append(f"{', '.join(group)} split the rows identically: one dimension, shown as {group[0]}.")
    for col, info in columns.items():
        share = info.get("placeholder_share", 0)
        if share >= PLACEHOLDER_HEAVY:
            notes.append(f"{col} is a placeholder in {share:.0%} of rows, so it is left out of the segment views.")
    coords = [c for c, i in columns.items() if i["role"] == "coordinate"]
    if coords:
        notes.append(f"{', '.join(coords)} {'is a' if len(coords) == 1 else 'are'} map coordinate(s), not measured or summed.")
    parts = [c for c, i in columns.items() if i["role"] == "calendar"]
    if parts:
        notes.append(
            f"{', '.join(parts)} {'is a part' if len(parts) == 1 else 'are parts'} of a date, so "
            f"{'it is' if len(parts) == 1 else 'they are'} not summed, averaged or ranked as measures."
        )
    for h in hierarchies:
        # A column already left out as placeholders adds nothing by nesting.
        if columns[h["child"]].get("placeholder_share", 0) < PLACEHOLDER_HEAVY:
            notes.append(f"{h['child']} nests inside {h['parent']}.")

    return {
        "rows": rows,
        "columns": columns,
        "measures": measures,
        "dimensions": ranked,
        "dates": list(dates),
        "aliases": aliases,
        "hierarchies": hierarchies,
        "grain": grain,
        "notes": notes,
    }


def parsed_dates(df: pd.DataFrame, planned: dict[str, Any]) -> pd.DataFrame:
    """`df` with every planned date column read as dates."""
    out = df.copy()
    for col in planned.get("dates", []):
        if not pd.api.types.is_datetime64_any_dtype(out[col]):
            parsed = parse_date_column(out[col])
            if parsed is not None:
                out[col] = parsed
    return out
