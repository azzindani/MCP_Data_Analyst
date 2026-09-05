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

from typing import Any

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

SPEC_KEYS: tuple[str, ...] = ("title", "theme", "layout", "kpis", "filters", "tabs", "interactions")


class SpecError(ValueError):
    """A spec that cannot be honoured, named precisely enough to fix."""


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

    for key in ("kpis", "filters"):
        names = spec.get(key)
        if names is None:
            continue
        if not isinstance(names, list):
            raise SpecError(f"{key} must be a list of column names")
        missing = [n for n in names if n not in cols]
        if missing:
            raise SpecError(f"{key} names column(s) not in the file: {', '.join(map(str, missing))}. Available: {available}")

    layout = spec.get("layout")
    if layout is not None:
        if not isinstance(layout, list):
            raise SpecError("layout must be a list of {slot, chart, cols, agg} panels")
        for i, panel in enumerate(layout):
            if not isinstance(panel, dict):
                raise SpecError(f"layout[{i}] must be a dict, got {type(panel).__name__}")
            chart = panel.get("chart")
            if chart not in CHART_KINDS:
                raise SpecError(
                    f"layout[{i}] chart={chart!r} is not drawable. Valid: {', '.join(CHART_KINDS)}"
                )
            panel_cols = panel.get("cols") or {}
            if not isinstance(panel_cols, dict):
                raise SpecError(f"layout[{i}] cols must be a dict of role -> column name")
            bad = [str(v) for v in panel_cols.values() if v and str(v) not in cols]
            if bad:
                raise SpecError(f"layout[{i}] names column(s) not in the file: {', '.join(bad)}. Available: {available}")
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
                absent = [role for role in needs if not panel_cols.get(role)]
                if absent:
                    raise SpecError(
                        f"layout[{i}] is a {chart} chart and needs cols for: {', '.join(absent)}. "
                        f"Got: {', '.join(sorted(panel_cols))}. Pass no cols at all to let the "
                        "detector choose them."
                    )

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
                    raise SpecError(
                        f"tabs[{i}] refers to slot(s) {out_of_range} but layout has {slot_count} panel(s)"
                    )

    interactions = spec.get("interactions")
    if interactions is not None:
        if not isinstance(interactions, dict):
            raise SpecError("interactions must be a dict")
        unknown_i = sorted(set(interactions) - set(DEFAULT_INTERACTIONS))
        if unknown_i:
            raise SpecError(
                f"interactions has unknown key(s): {', '.join(unknown_i)}. "
                f"Valid: {', '.join(DEFAULT_INTERACTIONS)}"
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
    }


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
    return out
