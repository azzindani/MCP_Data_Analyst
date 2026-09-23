"""The dashboard as a document you can edit, not a button you can press.

The review's S1, verbatim:

    `generate_*(spec)` + `customize_*` on every generator. Spec:
    `{title, theme, layout:[{slot, chart, cols, agg}], kpis:[], filters:[],
    tabs:[], interactions:{}}`

and, from its dashboard section:

    Split shell/data... Then customization = small JSON edit, not full rebuild.

`generate_dashboard` auto-detects everything: which charts, from which columns,
with which aggregate. The detection is good, and it is the only way in. A caller
who wants the same dashboard with one chart swapped has exactly two options --
accept what they got, or write the page themselves -- and an agent asked to
"make that bar chart a line chart" cannot express the request at all.

**A spec makes the detection an opening offer.** `resolve_spec` runs the same
auto-detection and returns it as a document; a caller edits the parts they care
about and passes it back. Nothing is required: an absent key means "decide for
me", which keeps the zero-argument call exactly as it was.

**The resolved spec ships in the response and in the page.** That is what makes
`customize_dashboard` possible without a rebuild path of its own -- it reads the
spec the page was built from, applies a change, and regenerates. Without that,
"customize" would mean re-deriving the caller's intent from HTML.

**Every override is validated against the frame, and a bad one is refused by
name.** A spec naming a column that does not exist, or a bar chart with no
category, is a caller who believes they configured something. Silently falling
back to auto-detect there produces a dashboard that looks like it worked.
"""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

from shared.column_utils import is_numeric_col

# Charts the dashboard knows how to draw. `generate_dashboard` detects a subset
# of these from the data; a spec may name any of them, and is refused for the
# rest rather than quietly given a different chart.
CHART_KINDS: tuple[str, ...] = (
    "bar",
    "line",
    "time_series",
    "pie",
    "scatter",
    "histogram",
    "box",
    "geo_scatter",
    "choropleth",
    # Panels that are not plots: a heading across the grid, a note, one
    # number, and a table of grouped totals -- the last two follow the filters.
    "section",
    "text",
    "kpi",
    "table",
)

# What each chart needs before it can be drawn. Stated once, so a refusal can
# name the missing piece instead of failing at render time with a KeyError.
CHART_NEEDS: dict[str, tuple[str, ...]] = {
    "bar": ("category", "value"),
    "line": ("date", "value"),
    "time_series": ("date", "value"),
    "pie": ("category", "value"),
    "scatter": ("x", "y"),
    "histogram": ("value",),
    "box": ("value",),
    "geo_scatter": ("lat", "lon"),
    "choropleth": ("location", "value"),
    "section": (),
    "text": (),
    "kpi": ("value",),
    "table": ("category", "value"),
}

THEMES: tuple[str, ...] = ("device", "light", "dark")

# Interaction switches, with the defaults that reproduce today's dashboard.
# `table` and `tabs` are new, so they default off: a spec parameter must not
# change what a zero-argument call returns.
DEFAULT_INTERACTIONS: dict[str, Any] = {
    "cross_filter": True,
    "table": False,
    "table_page_size": 25,
    "hover": True,
    # The review's "5k-row default + `Load full`", offered rather than
    # defaulted. 0 means every row, which is what the tool has always done.
    # Setting it samples, and the page then says on its face that every number
    # on it is an estimate -- because the KPI cards and chart heights are
    # computed in the browser from exactly these rows. See the comment above
    # EMBED_LIMIT in _adv_dashboard.py for why the default cannot be 5000.
    "embed_rows": 0,
}

SPEC_KEYS: tuple[str, ...] = ("title", "theme", "layout", "kpis", "filters", "tabs", "interactions", "style")

# What a panel may carry. Anything else -- "width", "title", a typo of "cols" --
# used to be accepted and dropped, which is the failure this module exists to
# refuse.
PANEL_KEYS: tuple[str, ...] = ("slot", "chart", "cols", "agg", "title", "style", "place", "text")
MAX_TEXT = 2000

# How a panel looks, field by field, and which charts read each field. A field
# a chart does not draw is refused by name rather than accepted and dropped.
STYLE_ENUMS: dict[str, tuple[str, ...]] = {
    "sort": ("desc", "asc", "label"),
    "legend": ("top", "bottom", "right", "none"),
    "y_scale": ("linear", "log"),
    "format": ("compact", "integer", "decimal", "percent"),
    # Plotly.js's own names; "RdBu_r" and the like are Python-only and draw the default.
    "colorscale": (
        "Blues",
        "Greens",
        "Greys",
        "Reds",
        "YlGnBu",
        "YlOrRd",
        "Viridis",
        "Cividis",
        "Hot",
        "Blackbody",
        "Earth",
        "Electric",
        "Jet",
        "Rainbow",
        "Portland",
        "Picnic",
        "Bluered",
        "RdBu",
    ),
}
STYLE_INTS: dict[str, tuple[int, int]] = {"top_n": (1, 500), "bins": (2, 500), "ma": (0, 60)}
STYLE_TEXT: dict[str, int] = {"prefix": 8, "suffix": 8}
CHART_STYLE: dict[str, tuple[str, ...]] = {
    "bar": ("color", "colors", "top_n", "sort", "value_labels", "y_scale", "format", "prefix", "suffix"),
    "line": ("color", "accent", "ma", "legend", "y_scale", "format", "prefix", "suffix"),
    "time_series": ("color", "accent", "ma", "legend", "y_scale", "format", "prefix", "suffix"),
    "pie": ("palette", "colors", "top_n", "legend"),
    "scatter": ("color", "accent", "legend", "y_scale"),
    "histogram": ("color", "accent", "bins"),
    "box": ("palette", "colors", "top_n", "y_scale", "format", "prefix", "suffix"),
    "geo_scatter": ("color",),
    "choropleth": ("colorscale", "format", "prefix", "suffix"),
    "section": (),
    "text": (),
    "kpi": ("color", "format", "prefix", "suffix"),
    "table": ("top_n", "sort", "format", "prefix", "suffix"),
}
# The page's own style: a palette, and colours by category value that every
# panel uses -- so "North" is one colour wherever it is drawn.
PAGE_STYLE_KEYS: tuple[str, ...] = ("palette", "colors")
PLACE_LIMITS: dict[str, tuple[int, int]] = {"span": (1, 12), "height": (160, 1200)}
MAX_TITLE = 120
MAX_PALETTE = 30
MAX_COLOR_MAP = 200

# Roles a chart can take beyond the ones it needs.
CHART_OPTIONAL: dict[str, tuple[str, ...]] = {"box": ("category",)}

# Roles whose column has to hold numbers, and the one that has to hold dates.
NUMERIC_ROLES = frozenset({"value", "x", "y", "lat", "lon"})
DATE_ROLES = frozenset({"date"})

# A panel's `agg` means something only where values are grouped; a pie shows
# shares of a total, so the only aggregate it can draw is a sum.
PANEL_AGGS: tuple[str, ...] = ("sum", "mean", "median", "max", "min", "count", "count_distinct")
AGG_CHARTS: tuple[str, ...] = ("bar", "line", "time_series", "pie", "choropleth", "kpi", "table")

# Set by the generator when the layout is its own detection rather than the
# caller's, so a round-trip through customize_dashboard redraws the detected
# page instead of reading the detection as a hand-written layout.
LAYOUT_SOURCE_KEY = "_layout_source"

# Distinct values above which a text column is not offered as a filter.
MAX_FILTER_VALUES = 100

# A filter is a column name, or {column, control, default, scope}: the control
# the reader gets, what it selects when the page opens, and the panels it
# narrows. The control follows what the column holds -- a text column is pills
# or a dropdown, a number a min-max range (or pills, with few enough values), a
# date a from-to range over its days.
FILTER_KEYS: tuple[str, ...] = ("column", "control", "default", "scope")
FILTER_CONTROLS: dict[str, tuple[str, ...]] = {
    "text": ("pills", "dropdown"),
    "number": ("range", "pills", "dropdown"),
    "date": ("date_range",),
}
LISTED_CONTROLS = ("pills", "dropdown")
_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class SpecError(ValueError):
    """A spec that cannot be honoured, named precisely enough to fix."""


def filter_kind(series: pd.Series) -> str:
    """What a filter on this column compares: text, number or date."""
    if pd.api.types.is_datetime64_any_dtype(series):
        return "date"
    return "number" if is_numeric_col(series) else "text"


def page_text(value: Any) -> str:
    """A cell as the page's script reads it: String() of the value embedded as JSON.

    A pill has to carry exactly that text or it matches nothing. Python's str()
    wrote a boolean as 'True' and a whole float as '3.0', where the page reads
    'true' and '3' -- so those pills filtered every row away.
    """
    if isinstance(value, bool) or type(value).__name__ == "bool_":
        return "true" if value else "false"
    if pd.api.types.is_integer(value):
        return str(int(value))
    if pd.api.types.is_float(value):
        v = float(value)
        if v.is_integer() and abs(v) < 1e21:
            return str(int(v))
        return repr(v).replace("e-0", "e-").replace("e+0", "e+")
    return str(value)


def filter_values(series: pd.Series) -> list[str]:
    """A listed filter's values, in the page's own text, numbers in numeric order."""
    values = series.dropna().unique().tolist()
    if is_numeric_col(series):
        return [page_text(v) for v in sorted(values)]
    return sorted({page_text(v) for v in values})


def filter_entry(entry: Any) -> dict[str, Any]:
    """A filter as a dict: a bare column name is {"column": name}."""
    return dict(entry) if isinstance(entry, dict) else {"column": entry}


def _filter_mask(series: pd.Series, kind: str, listed: bool, default: Any) -> pd.Series:
    """The rows a filter's default keeps -- the same test the page's script makes."""
    if listed:
        chosen = {page_text(v) for v in default}
        if len(chosen) >= series.dropna().nunique():
            return pd.Series(True, index=series.index)  # every value selected is no filter
        return series.map(lambda v: not pd.isna(v) and page_text(v) in chosen)
    if kind == "date":
        # The page holds each date as its YYYY-MM-DD day, and compares that text.
        values = pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
        keep = values != ""
    else:
        values = pd.to_numeric(series, errors="coerce")
        keep = values.notna()
    if default.get("min") is not None:
        keep &= values >= default["min"]
    if default.get("max") is not None:
        keep &= values <= default["max"]
    return keep


def _check_default(at: str, name: str, series: pd.Series, kind: str, listed: bool, default: Any) -> None:
    if listed:
        have = filter_values(series)
        if not isinstance(default, list) or not default:
            raise SpecError(
                f"{at}.default is what is selected when the page opens: a list of values of {name!r}, e.g. {have[:2]}"
            )
        held = set(have)
        unknown = [v for v in default if isinstance(v, (dict, list)) or page_text(v) not in held]
        if unknown:
            shown = ", ".join(have[:12]) + (", ..." if len(have) > 12 else "")
            raise SpecError(
                f"{at}.default names value(s) {name!r} does not hold: {', '.join(map(repr, unknown[:5]))}. "
                f"It holds: {shown}"
            )
        return
    what = "a date written YYYY-MM-DD" if kind == "date" else "a number"
    if not isinstance(default, dict) or not default or set(default) - {"min", "max"}:
        raise SpecError(f"{at}.default of a range is {{'min': ..., 'max': ...}}, each {what}, either one optional")
    for bound, value in default.items():
        if kind == "date":
            ok = isinstance(value, str) and bool(_DAY.match(value))
            if ok:
                try:
                    pd.Timestamp(value)
                except ValueError:
                    ok = False
        else:
            ok = not isinstance(value, bool) and isinstance(value, (int, float)) and value == value
            ok = ok and abs(value) != float("inf")
        if not ok:
            raise SpecError(f"{at}.default.{bound} must be {what}; got {value!r}")
    lo, hi = default.get("min"), default.get("max")
    if lo is not None and hi is not None and lo > hi:
        raise SpecError(f"{at}.default has min {lo!r} above max {hi!r}")


def _check_scope(at: str, scope: Any, layout: Any, detected: bool) -> list[int]:
    if scope == "page":
        return []
    if not isinstance(scope, list) or not scope or any(isinstance(s, bool) or not isinstance(s, int) for s in scope):
        raise SpecError(f"{at}.scope is 'page' or a list of the layout slots it narrows, e.g. [0, 2]")
    if not isinstance(layout, list) or detected:
        raise SpecError(
            f"{at}.scope names panels by layout slot, and this spec has no layout of its own; "
            "pass a layout, or scope 'page'"
        )
    out_of_range = [s for s in scope if not 0 <= s < len(layout)]
    if out_of_range:
        raise SpecError(f"{at}.scope refers to slot(s) {out_of_range} but layout has {len(layout)} panel(s)")
    for s in scope:
        chart = layout[s].get("chart")
        if chart in ("section", "text"):
            raise SpecError(f"{at}.scope names slot {s}, a {chart} panel, which draws no rows to filter")
    return sorted(set(scope))


def validate_filters(filters: Any, df, layout: Any = None, detected: bool = False) -> None:
    """Each filter's column, control, default and scope -- and that the page opens on some rows."""
    if not isinstance(filters, list):
        raise SpecError("filters must be a list of column names or {column, control, default, scope} entries")
    entries = []
    for i, raw in enumerate(filters):
        if isinstance(raw, dict):
            extra = sorted(str(k) for k in raw if k not in FILTER_KEYS)
            if extra:
                raise SpecError(
                    f"filters[{i}] has unknown key(s): {', '.join(extra)}. A filter takes: {', '.join(FILTER_KEYS)}"
                )
            if not isinstance(raw.get("column"), str) or not raw["column"]:
                raise SpecError(f"filters[{i}] needs a column: {{'column': 'region', ...}}")
        elif not isinstance(raw, str):
            raise SpecError(f"filters[{i}] must be a column name or a dict with keys {', '.join(FILTER_KEYS)}")
        entries.append(filter_entry(raw))
    cols = set(_valid_columns(df))
    missing = [e["column"] for e in entries if e["column"] not in cols]
    if missing:
        raise SpecError(
            f"filters names column(s) not in the file: {', '.join(map(str, missing))}. "
            f"Available: {', '.join(sorted(cols))}"
        )
    seen: set[str] = set()
    page = pd.Series(True, index=df.index)
    by_slot: dict[int, pd.Series] = {}
    for i, e in enumerate(entries):
        at, name = f"filters[{i}]", e["column"]
        if name in seen:
            raise SpecError(f"filters name {name!r} twice; a column takes one filter")
        seen.add(name)
        series = df[name]
        kind = filter_kind(series)
        distinct = series.dropna().nunique()
        if distinct < 2:
            raise SpecError(f"filters: {name!r} has {distinct} distinct value(s), so a filter on it would do nothing")
        control = e.get("control")
        if control is not None and control not in FILTER_CONTROLS[kind]:
            raise SpecError(
                f"{at}.control={control!r} does not fit {name!r}, a {kind} column; "
                f"valid: {', '.join(FILTER_CONTROLS[kind])}"
            )
        listed = control in LISTED_CONTROLS or (control is None and kind == "text")
        if listed and distinct > MAX_FILTER_VALUES:
            raise SpecError(
                f"filters: {name!r} has {distinct} distinct values; a list filter offers at most {MAX_FILTER_VALUES}"
                + (". Use control 'range'" if kind == "number" else "")
            )
        slots = _check_scope(at, e["scope"], layout, detected) if "scope" in e else []
        if "default" not in e:
            continue
        _check_default(at, name, series, kind, listed, e["default"])
        keep = _filter_mask(series, kind, listed, e["default"])
        if not keep.any():
            raise SpecError(f"{at}.default keeps no rows of {name!r}, so the page would open empty")
        if slots:
            for s in slots:
                by_slot[s] = by_slot.get(s, pd.Series(True, index=df.index)) & keep
        else:
            page &= keep
    if not page.any():
        raise SpecError("filters: the defaults together keep no rows, so the page would open empty")
    for s, keep in sorted(by_slot.items()):
        if not (page & keep).any():
            raise SpecError(f"filters: the defaults on slot {s} together keep no rows, so that panel would open empty")


_HEX = re.compile(r"^#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{6}|[0-9a-fA-F]{8})$")
_RGB = re.compile(r"^rgba?\(\s*\d{1,3}\s*,\s*\d{1,3}\s*,\s*\d{1,3}\s*(?:,\s*(?:0|1|0?\.\d+)\s*)?\)$")


def _colour(where: str, value: Any) -> None:
    if not (isinstance(value, str) and (_HEX.match(value) or _RGB.match(value))):
        raise SpecError(f"{where} {value!r} is not a colour; write '#58a6ff' or 'rgb(88,166,255)'")


def _palette(where: str, value: Any) -> None:
    if not isinstance(value, list) or not 1 <= len(value) <= MAX_PALETTE:
        raise SpecError(f"{where} must be a list of 1 to {MAX_PALETTE} colours")
    for j, c in enumerate(value):
        _colour(f"{where}[{j}]", c)


def _colour_map(where: str, value: Any) -> None:
    if not isinstance(value, dict) or len(value) > MAX_COLOR_MAP:
        raise SpecError(f"{where} must map category values to colours, e.g. {{'North': '#2f81f7'}}")
    for key, c in value.items():
        _colour(f"{where}[{key!r}]", c)


def validate_page_style(style: Any) -> None:
    """The page's palette and category colours."""
    if not isinstance(style, dict):
        raise SpecError(f"style must be a dict with keys {', '.join(PAGE_STYLE_KEYS)}")
    unknown = sorted(str(k) for k in style if k not in PAGE_STYLE_KEYS)
    if unknown:
        raise SpecError(
            f"style has unknown key(s): {', '.join(unknown)}. The page's style takes: {', '.join(PAGE_STYLE_KEYS)}"
        )
    if "palette" in style:
        _palette("style.palette", style["palette"])
    if "colors" in style:
        _colour_map("style.colors", style["colors"])


def validate_panel_style(where: str, chart: str, style: Any) -> None:
    """A panel's style: only the fields its chart draws, each with a value it can draw."""
    if not isinstance(style, dict):
        raise SpecError(f"{where}.style must be a dict")
    allowed = CHART_STYLE.get(chart, ())
    for key, value in style.items():
        at = f"{where}.style.{key}"
        if key not in allowed:
            raise SpecError(f"{at}: a {chart} chart does not draw {key!r}. Its style takes: {', '.join(allowed)}")
        if key in ("color", "accent"):
            _colour(at, value)
        elif key == "palette":
            _palette(at, value)
        elif key == "colors":
            _colour_map(at, value)
        elif key == "value_labels":
            if not isinstance(value, bool):
                raise SpecError(f"{at} must be true or false")
        elif key in STYLE_ENUMS:
            if value not in STYLE_ENUMS[key]:
                raise SpecError(f"{at}={value!r}; valid: {', '.join(STYLE_ENUMS[key])}")
        elif key in STYLE_INTS:
            lo, hi = STYLE_INTS[key]
            if isinstance(value, bool) or not isinstance(value, int) or not lo <= value <= hi:
                raise SpecError(f"{at} must be a whole number from {lo} to {hi}")
        elif key in STYLE_TEXT:
            if not isinstance(value, str) or len(value) > STYLE_TEXT[key]:
                raise SpecError(f"{at} must be text of at most {STYLE_TEXT[key]} characters, e.g. '$' or ' kg'")


def validate_place(where: str, place: Any) -> None:
    """Where a panel sits on a 12-column grid, and how tall it is."""
    if not isinstance(place, dict):
        raise SpecError(f"{where}.place must be a dict with keys {', '.join(PLACE_LIMITS)}")
    for key, value in place.items():
        if key not in PLACE_LIMITS:
            raise SpecError(f"{where}.place has unknown key {key!r}. It takes: {', '.join(PLACE_LIMITS)}")
        lo, hi = PLACE_LIMITS[key]
        if isinstance(value, bool) or not isinstance(value, int) or not lo <= value <= hi:
            unit = " columns of 12" if key == "span" else " pixels"
            raise SpecError(f"{where}.place.{key} must be a whole number from {lo} to {hi}{unit}")


def _valid_columns(df) -> list[str]:
    return [str(c) for c in df.columns]


def validate(spec: dict[str, Any] | None, df) -> dict[str, Any]:
    """Check a caller's spec against the frame. Returns it unchanged, or raises.

    Refuses rather than falls back. A spec naming a column that is not there is
    a caller who believes they configured something, and a dashboard that
    quietly ignored them looks exactly like one that obeyed.
    """
    if spec is None:
        return {}
    if not isinstance(spec, dict):
        raise SpecError(f"spec must be a dict with keys {', '.join(SPEC_KEYS)}; got {type(spec).__name__}")

    # Underscore keys are recorded by the generator, not sent by a caller --
    # `_source_path` is the one that exists, and it is what lets
    # customize_dashboard find the data when the output directory is not the
    # directory the CSV lives in. That is the deployed layout, so a spec that
    # round-trips has to carry them rather than have validation reject its own
    # output.
    unknown = sorted(k for k in set(spec) - set(SPEC_KEYS) if not str(k).startswith("_"))
    if unknown:
        raise SpecError(f"spec has unknown key(s): {', '.join(unknown)}. Valid: {', '.join(SPEC_KEYS)}")

    cols = set(_valid_columns(df))
    available = ", ".join(sorted(cols))

    theme = spec.get("theme")
    if theme is not None and theme not in THEMES:
        raise SpecError(f"theme must be one of {', '.join(THEMES)}; got {theme!r}")
    if spec.get("style") is not None:
        validate_page_style(spec["style"])

    kpis = spec.get("kpis")
    if kpis is not None:
        if not isinstance(kpis, list):
            raise SpecError("kpis must be a list of column names")
        missing = [n for n in kpis if n not in cols]
        if missing:
            raise SpecError(f"kpis names column(s) not in the file: {', '.join(map(str, missing))}. Available: {available}")

    numeric = [c for c in df.columns if is_numeric_col(df[c])]
    if kpis:
        not_numeric = [k for k in kpis if k not in numeric]
        if not_numeric:
            raise SpecError(
                f"kpis must be numeric columns; not numeric: {', '.join(map(str, not_numeric))}. "
                f"Numeric: {', '.join(map(str, numeric))}"
            )

    layout = spec.get("layout")
    if layout is not None:
        if not isinstance(layout, list):
            raise SpecError("layout must be a list of {slot, chart, cols, agg, title, style, place} panels")
        for i, panel in enumerate(layout):
            if not isinstance(panel, dict):
                raise SpecError(f"layout[{i}] must be a dict, got {type(panel).__name__}")
            extra = sorted(str(k) for k in panel if k not in PANEL_KEYS)
            if extra:
                raise SpecError(
                    f"layout[{i}] has unknown key(s): {', '.join(extra)}. A panel takes: {', '.join(PANEL_KEYS)}"
                )
            chart = panel.get("chart")
            if chart not in CHART_KINDS:
                raise SpecError(f"layout[{i}] chart={chart!r} is not drawable. Valid: {', '.join(CHART_KINDS)}")
            if "title" in panel and not (
                isinstance(panel["title"], str) and 0 < len(panel["title"].strip()) <= MAX_TITLE
            ):
                raise SpecError(f"layout[{i}].title must be text of 1 to {MAX_TITLE} characters")
            if "text" in panel and chart != "text":
                raise SpecError(f"layout[{i}] is a {chart} panel; only a text panel takes text")
            if chart == "text" and not (
                isinstance(panel.get("text"), str) and 0 < len(panel["text"].strip()) <= MAX_TEXT
            ):
                raise SpecError(f"layout[{i}] is a text panel and needs text: 1 to {MAX_TEXT} characters")
            if chart == "section" and not panel.get("title"):
                raise SpecError(f"layout[{i}] is a section, a heading across the grid, and needs a title")
            if panel.get("style") is not None:
                validate_panel_style(f"layout[{i}]", chart, panel["style"])
            if panel.get("place") is not None:
                validate_place(f"layout[{i}]", panel["place"])
            panel_cols = panel.get("cols") or {}
            if not isinstance(panel_cols, dict):
                raise SpecError(f"layout[{i}] cols must be a dict of role -> column name")
            bad = [str(v) for v in panel_cols.values() if v and str(v) not in cols]
            if bad:
                raise SpecError(
                    f"layout[{i}] names column(s) not in the file: {', '.join(bad)}. Available: {available}"
                )
            # No cols at all means "you pick" -- which is both a reasonable
            # request ("give me a bar chart of something sensible") and the
            # shape the detector emits. The resolved spec has to be valid input
            # to this same validator, or the round-trip `customize_dashboard`
            # depends on cannot work: it reads the spec a page was built from
            # and hands it straight back.
            #
            # Cols that are *partly* filled are refused, because there the
            # caller plainly meant to choose and named one role short. Silently
            # detecting the rest would give them a chart they did not ask for
            # under a spec that says they did.
            needs = CHART_NEEDS.get(chart, ())
            if panel_cols:
                roles = needs + CHART_OPTIONAL.get(chart, ())
                unknown_roles = sorted(str(r) for r in panel_cols if r not in roles)
                if unknown_roles:
                    raise SpecError(
                        f"layout[{i}] is a {chart} chart, which has no role(s) {', '.join(unknown_roles)}. "
                        f"Its roles: {', '.join(roles)}"
                    )
                absent = [role for role in needs if not panel_cols.get(role)]
                if absent:
                    raise SpecError(
                        f"layout[{i}] is a {chart} chart and needs cols for: {', '.join(absent)}. "
                        f"Got: {', '.join(sorted(panel_cols))}. Pass no cols at all to let the "
                        "detector choose them."
                    )
                for role, col in panel_cols.items():
                    if not col:
                        continue
                    if role in NUMERIC_ROLES and col not in numeric:
                        raise SpecError(
                            f"layout[{i}] {role}={col!r} is not numeric. Numeric columns: {', '.join(map(str, numeric))}"
                        )
                    if role in DATE_ROLES and not pd.api.types.is_datetime64_any_dtype(df[col]):
                        raise SpecError(
                            f"layout[{i}] date={col!r} is not read as a date. "
                            "Convert it with apply_patch cast_column dtype=datetime first."
                        )
            agg = panel.get("agg") or ""
            if agg:
                if agg not in PANEL_AGGS:
                    raise SpecError(f"layout[{i}] agg={agg!r} is not one of {', '.join(PANEL_AGGS)}")
                if chart not in AGG_CHARTS:
                    raise SpecError(
                        f"layout[{i}] is a {chart} chart, which draws values as they are; agg applies to "
                        f"{', '.join(AGG_CHARTS)}"
                    )
                if chart == "pie" and agg != "sum":
                    raise SpecError(f"layout[{i}] is a pie, which shows shares of a total, so its only agg is sum")

    # After the layout, which a filter's scope names by slot.
    if spec.get("filters") is not None:
        validate_filters(spec["filters"], df, layout, spec.get(LAYOUT_SOURCE_KEY) == "detected")

    tabs = spec.get("tabs")
    if tabs is not None:
        if not isinstance(tabs, list):
            raise SpecError("tabs must be a list of {name, slots} entries")
        slot_count = len(layout) if isinstance(layout, list) else None
        for i, tab in enumerate(tabs):
            if not isinstance(tab, dict) or not tab.get("name"):
                raise SpecError(f"tabs[{i}] needs a name")
            slots = tab.get("slots") or []
            if not isinstance(slots, list):
                raise SpecError(f"tabs[{i}] slots must be a list of layout indexes")
            if slot_count is not None:
                out_of_range = [s for s in slots if not isinstance(s, int) or s < 0 or s >= slot_count]
                if out_of_range:
                    raise SpecError(f"tabs[{i}] refers to slot(s) {out_of_range} but layout has {slot_count} panel(s)")
        # A tab shows its own panels and hides every other, so a panel in no
        # tab is never on screen -- it was drawn, and passed, and hidden.
        if slot_count is not None and tabs and spec.get(LAYOUT_SOURCE_KEY) != "detected":
            shown = {s for tab in tabs for s in (tab.get("slots") or [])}
            unshown = [s for s in range(slot_count) if s not in shown]
            if unshown:
                raise SpecError(
                    f"layout slot(s) {unshown} are in no tab, and a tab hides every panel it does not list, "
                    "so nothing would show them. Add each to a tab's slots."
                )

    interactions = spec.get("interactions")
    if interactions is not None:
        if not isinstance(interactions, dict):
            raise SpecError("interactions must be a dict")
        unknown_i = sorted(set(interactions) - set(DEFAULT_INTERACTIONS))
        if unknown_i:
            raise SpecError(
                f"interactions has unknown key(s): {', '.join(unknown_i)}. Valid: {', '.join(DEFAULT_INTERACTIONS)}"
            )
    return spec


def resolve(
    spec: dict[str, Any] | None,
    *,
    title: str,
    theme: str,
    detected_layout: list[dict[str, Any]],
    kpi_columns: list[str],
    filter_columns: list[str],
) -> dict[str, Any]:
    """The detection, as a document, with the caller's edits applied over it.

    An absent key means "decide for me" -- so a caller who passes `{"title":
    "Q3"}` gets today's dashboard with a different heading, and one who passes
    nothing gets today's dashboard exactly.
    """
    spec = spec or {}
    interactions = dict(DEFAULT_INTERACTIONS)
    interactions.update(spec.get("interactions") or {})
    return {
        "title": spec.get("title") or title,
        "theme": spec.get("theme") or theme,
        "layout": spec.get("layout") if spec.get("layout") is not None else detected_layout,
        "kpis": spec.get("kpis") if spec.get("kpis") is not None else list(kpi_columns),
        "filters": spec.get("filters") if spec.get("filters") is not None else list(filter_columns),
        "tabs": spec.get("tabs") or [],
        "interactions": interactions,
        "style": dict(spec.get("style") or {}),
    }


PANEL_OPS: dict[str, tuple[str, ...]] = {
    "set_panel": ("slot", "chart", "cols", "agg", "title", "style", "place", "text"),
    "add_panel": ("panel", "at", "tab"),
    "remove_panel": ("slot",),
    "move_panel": ("slot", "to"),
}
# A panel field that is a dict is edited key by key: set_panel {style:
# {color}} changes the colour and keeps the rest of the style.
_MERGED_FIELDS = ("cols", "style", "place")
MAX_OPS = 50


def apply_panel_ops(spec: dict[str, Any], ops: Any) -> tuple[dict[str, Any], list[str]]:
    """Edit a dashboard's layout one panel at a time. Returns the new spec and what each op did.

    `merge` replaces `layout` whole, so changing one panel meant resending all
    of them -- and a tab or a filter scope names panels by slot, so a panel
    removed or moved by hand left them pointing at the wrong cards. Here every
    slot reference follows its panel: tabs and filter scopes are renumbered,
    and an edit that would leave one pointing at nothing is refused by name.

    set_panel replaces the fields it names (null removes one), except cols,
    style and place, which change key by key. The panels themselves are
    checked afterwards by `validate`, like any layout.
    """
    if not isinstance(ops, list) or not ops:
        raise SpecError("ops must be a list of {op, ...}; ops: " + ", ".join(PANEL_OPS))
    if len(ops) > MAX_OPS:
        raise SpecError(f"ops has {len(ops)} edits; one call takes at most {MAX_OPS}")
    layout = spec.get("layout")
    if spec.get(LAYOUT_SOURCE_KEY) == "detected" or not isinstance(layout, list):
        raise SpecError(
            "this page's layout is the detector's: each of its slots is a chart kind that may draw several cards, "
            "so there is no panel to edit. Make the layout yours first -- changes={'layout': [...]}; the page's "
            "spec.layout, handed back, draws one panel per kind -- and edit that"
        )
    out = {**spec, "layout": [dict(p) if isinstance(p, dict) else p for p in layout]}
    panels: list = out["layout"]
    # Which original slot each position holds; None for a panel added here.
    origin: list[int | None] = list(range(len(panels)))
    tab_names = [str(t.get("name")) for t in (out.get("tabs") or []) if isinstance(t, dict)]
    joins: list[str | None] = [None] * len(panels)  # the tab an added panel joins
    applied: list[str] = []

    def slot_at(i: int, op: dict, key: str, size: int) -> int:
        value = op.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < size:
            raise SpecError(f"ops[{i}].{key} must be a slot from 0 to {size - 1}; got {value!r}")
        return value

    for i, op in enumerate(ops):
        if not isinstance(op, dict) or op.get("op") not in PANEL_OPS:
            got = op.get("op") if isinstance(op, dict) else op
            raise SpecError(f"ops[{i}] op={got!r} is not an edit. Valid: {', '.join(PANEL_OPS)}")
        name = op["op"]
        extra = sorted(str(k) for k in op if k != "op" and k not in PANEL_OPS[name])
        if extra:
            raise SpecError(f"ops[{i}] {name} has unknown key(s): {', '.join(extra)}. It takes: {', '.join(PANEL_OPS[name])}")
        if name == "set_panel":
            s = slot_at(i, op, "slot", len(panels))
            fields = [k for k in op if k not in ("op", "slot")]
            if not fields:
                raise SpecError(f"ops[{i}] set_panel names no field to set. It takes: {', '.join(PANEL_OPS[name][1:])}")
            panel = dict(panels[s])
            before = panel.get("chart")
            for key in fields:
                value = op[key]
                if key in _MERGED_FIELDS and isinstance(value, dict):
                    merged = dict(panel.get(key) or {})
                    for k, v in value.items():
                        if v is None:
                            merged.pop(k, None)
                        else:
                            merged[k] = v
                    panel[key] = merged
                elif value is None:
                    panel.pop(key, None)
                else:
                    panel[key] = value
            panels[s] = panel
            what = f"slot {s}: set {', '.join(fields)}"
            if panel.get("chart") != before:
                what += f" ({before} -> {panel.get('chart')})"
            applied.append(what)
        elif name == "add_panel":
            panel = op.get("panel")
            if not isinstance(panel, dict) or not panel.get("chart"):
                raise SpecError(f"ops[{i}] add_panel needs a panel with a chart, e.g. {{'chart': 'bar', 'cols': {{...}}}}")
            at = len(panels) if op.get("at") is None else slot_at(i, op, "at", len(panels) + 1)
            tab = op.get("tab")
            if tab_names and tab is None:
                raise SpecError(
                    f"ops[{i}] adds a panel to a page with tabs ({', '.join(tab_names)}); name the tab that shows it"
                )
            if tab is not None and tab not in tab_names:
                raise SpecError(
                    f"ops[{i}].tab={tab!r} is not a tab on this page. Tabs: {', '.join(tab_names) or 'none'}"
                )
            panel = dict(panel)
            panels.insert(at, panel)
            origin.insert(at, None)
            joins.insert(at, tab)
            applied.append(f"added a {panel.get('chart')} panel at slot {at}" + (f" in tab {tab!r}" if tab else ""))
        elif name == "remove_panel":
            s = slot_at(i, op, "slot", len(panels))
            if len(panels) == 1:
                raise SpecError(f"ops[{i}] removes the last panel; a dashboard needs one")
            gone = panels.pop(s)
            origin.pop(s)
            joins.pop(s)
            applied.append(f"removed slot {s} ({gone.get('title') or gone.get('chart')})")
        else:
            s = slot_at(i, op, "slot", len(panels))
            to = slot_at(i, op, "to", len(panels))
            panels.insert(to, panels.pop(s))
            origin.insert(to, origin.pop(s))
            joins.insert(to, joins.pop(s))
            applied.append(f"moved slot {s} to {to}")

    # Every reference to an original slot now points at where that panel went.
    where = {o: n for n, o in enumerate(origin) if o is not None}
    tabs = []
    for tab in out.get("tabs") or []:
        if not isinstance(tab, dict):
            tabs.append(tab)
            continue
        slots = sorted(where[s] for s in tab.get("slots") or [] if isinstance(s, int) and s in where)
        slots += [n for n, joined in enumerate(joins) if joined is not None and joined == str(tab.get("name"))]
        if not slots:
            raise SpecError(f"the ops leave tab {tab.get('name')!r} with no panels; remove the tab or give it one")
        tabs.append({**tab, "slots": sorted(slots)})
    if out.get("tabs"):
        out["tabs"] = tabs
    if isinstance(out.get("filters"), list):
        filters = []
        for entry in out["filters"]:
            if isinstance(entry, dict) and isinstance(entry.get("scope"), list):
                scope = sorted(where[s] for s in entry["scope"] if isinstance(s, int) and s in where)
                if not scope:
                    raise SpecError(
                        f"the ops remove every panel the filter on {entry.get('column')!r} narrows; "
                        "change its scope or remove it (changes={'filters': [...]})"
                    )
                entry = {**entry, "scope": scope}
            filters.append(entry)
        out["filters"] = filters
    for n, panel in enumerate(panels):
        if isinstance(panel, dict) and "slot" in panel:
            panel["slot"] = n
    return out, applied


def merge(base: dict[str, Any], changes: dict[str, Any]) -> dict[str, Any]:
    """Apply an edit to a resolved spec. Top-level replace, interactions merge.

    Replace rather than deep-merge for lists, because "here are the three
    panels I want" and "add these three panels" are different requests and a
    merge that guesses will eventually guess wrong. `interactions` is the one
    dict, and merging it is unambiguous.
    """
    out = dict(base)
    for key, value in (changes or {}).items():
        # Underscore keys are the generator's record of how the page was built.
        # A caller changing `_source_path` is asking this dashboard to be
        # rebuilt from a different file, which is a new dashboard.
        if key not in SPEC_KEYS:
            raise SpecError(f"cannot change unknown key {key!r}. Valid: {', '.join(SPEC_KEYS)}")
        if key == "interactions" and isinstance(value, dict):
            merged = dict(out.get("interactions") or {})
            merged.update(value)
            out["interactions"] = merged
        else:
            out[key] = value
        if key == "layout":
            # The caller has written the layout now, so it is no longer the
            # detector's and every panel in it is drawn as given.
            out.pop(LAYOUT_SOURCE_KEY, None)
    return out
