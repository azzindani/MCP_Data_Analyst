"""generate_dashboard sub-module. No MCP imports."""

from __future__ import annotations

import html as _html_esc
import json as _json
import logging
import re as _re
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_HERE = str(Path(__file__).resolve().parent)
for _p in (str(_ROOT), _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import pandas as pd
from _adv_helpers import (
    _BACK_TO_TOP_HTML,
    _BACK_TO_TOP_JS,
    VIEWPORT_META,
    _detect_location_mode,
    _find_geo_cols,
    _open_file,
    _read_csv,
    _token_estimate,
    agg_label,
    css_dashboard,
    css_vars,
    device_mode_js,
    extension_note,
    fail,
    get_output_path,
    infer_agg,
    info,
    is_numeric_col,
    ok,
    parse_agg_overrides,
    plotly_script_tag,
    theme_plot_colors,
    warn,
)

from shared.column_utils import is_identifier, parse_date_column
from shared.dashboard_spec import CHART_KINDS, LAYOUT_SOURCE_KEY, MAX_FILTER_VALUES, SPEC_KEYS, SpecError
from shared.dashboard_spec import merge as merge_spec
from shared.dashboard_spec import resolve as resolve_spec
from shared.dashboard_spec import validate as validate_spec
from shared.data_alerts import alerts_for_frame, alerts_html, quality_score
from shared.file_utils import embed_content, error_text, hint_for_error, no_rows_error, resolve_path
from shared.geo_names import unrecognised_locations
from shared.provenance import frame_hash, provenance, provenance_script, read_provenance, read_spec, spec_script
from shared.table_payload import json_for_script, records_js

logger = logging.getLogger(__name__)


def _bad_source(name: str, why: str) -> dict:
    """Refuse an extra source by name, before anything is written.

    A tab that opens onto nothing is worse than a dashboard that was not built:
    the page looks complete, and the missing dataset is discovered by whoever
    clicks the tab rather than by whoever called the tool.
    """
    return {
        "success": False,
        "op": "generate_dashboard",
        "error": f"sources entry {name!r} {why}",
        "hint": "Every entry in sources must be a readable CSV with at least one row. Nothing was written.",
        "progress": [fail("Unusable source", name)],
        "token_estimate": 40,
    }


def _safe(s: str) -> str:
    return _re.sub(r"[^a-zA-Z0-9]", "_", str(s))


_JS_ESCAPES = {
    "\\": "\\\\",
    "'": "\\'",
    '"': '\\"',
    "\n": "\\n",
    "\r": "\\r",
    " ": "\\u2028",
    " ": "\\u2029",
    "<": "\\x3c",
    ">": "\\x3e",
    "&": "\\x26",
}


def _js(s: object) -> str:
    """Text safe inside a quoted JavaScript string in an inline <script>.

    Column names come from the CSV, and the chart templates put them in string
    literals (``r['{cc}']``). Raw, a ``'`` ended the literal and broke every
    chart on the page, and ``</script>`` ended the block, so a crafted header ran
    as script for whoever opened the dashboard. The escaped text decodes to the
    same string, so it still reads the same key out of each row. Inside an HTML
    attribute (an ``onclick``), wrap it in ``html.escape`` as well.
    """
    return "".join(_JS_ESCAPES.get(ch, ch) for ch in str(s))


# ---------------------------------------------------------------------------
# Panels: every card is a complete description, drawn by one renderer
# ---------------------------------------------------------------------------

# The page's categorical palette, and the style each kind of panel starts from.
# Each panel carries its own copy in the page's _PANELS document, so a colour,
# a cap or a bin count is a field the renderer reads -- never a value baked
# into one chart's template, which is how nine kinds each grew their own.
PALETTE = [
    "#58a6ff",
    "#3fb950",
    "#f0883e",
    "#f85149",
    "#bc8cff",
    "#79c0ff",
    "#7ee787",
    "#ffa657",
    "#ff7b72",
    "#d2a8ff",
    "#a5d6ff",
    "#aff5b4",
    "#ffd6a5",
    "#ffabab",
    "#e0b0ff",
]
PANEL_STYLE: dict[str, dict] = {
    "bar": {"color": "#58a6ff", "top_n": 25},
    "pie": {"top_n": 15},
    "scatter": {"color": "#58a6ff", "accent": "#f0883e"},
    "grouped_bar": {"top_n": 20, "series": 10},
    "cscat": {"series": 15},
    "box": {"top_n": 20},
    "corr": {"colorscale": "RdBu_r"},
    "agg_hm": {"top_n": 30, "colorscale": "YlOrRd"},
    "ts": {"color": "#3fb950", "accent": "#f0883e", "ma": 3},
    "dist": {"color": "#58a6ff", "accent": "#f0883e", "bins": 50},
    "geo_scatter": {"color": "#58a6ff"},
    "geo_choro": {"colorscale": "YlOrRd"},
}


def _panel(spec: dict, title: str) -> dict:
    """A card's panel: its columns and aggregate, its title, and its own style."""
    return {**spec, "title": title, "style": dict(PANEL_STYLE.get(spec["type"], {}))}


def _theme(theme: str) -> dict:
    """The colours every panel is drawn over, for the page's theme.

    A "device" page follows the reader's light/dark setting, so it carries both
    and the renderer draws with the one in force. It used to carry one set, and
    the script meant to recolour the charts looked for a class these charts do
    not have: in dark mode every chart was a light panel on a dark page.
    """
    if theme == "device":
        return {"device": True, "light": _theme("light"), "dark": _theme("dark")}
    bg, font_c, _ = theme_plot_colors(theme)
    dark = theme == "dark"
    land, ocean, coast = ("#1a2332", "#0d1117", "#3d4f60") if dark else ("#e8ede6", "#c8ddef", "#aabbc8")
    return {
        "bg": bg,
        "font": font_c,
        "grid": "rgba(255,255,255,0.07)" if dark else "rgba(0,0,0,0.07)",
        "land": land,
        "ocean": ocean,
        "coast": coast,
        "palette": PALETTE,
    }


def generate_dashboard(
    file_path: str,
    output_path: str = "",
    title: str = "",
    chart_types: list[str] = None,
    agg_overrides: list[str] = None,
    geo_file_path: str = "",
    theme: str = "device",
    dry_run: bool = False,
    open_after: bool = True,
    return_content: bool = False,
    spec: dict | None = None,
    sources: list[str] = None,
) -> dict:
    """Generate interactive HTML dashboard with auto-detected charts. Opens HTML.

    `spec` overrides any part of the auto-detection:
    `{title, theme, layout:[{slot, chart, cols, agg}], kpis, filters, tabs,
    interactions}`. An absent key means "decide for me", so passing nothing
    returns exactly what this returned before the parameter existed.

    `sources` adds a tab per extra CSV -- the review's "`chargedoff.csv` as tab
    2, `anomalies_only.csv` as tab 3". Each extra tab carries that file's exact
    totals, computed server-side over all of its rows, and a paged table of
    them. The primary tab keeps the interactive charts and the cross-filter;
    those are client-side and belong to one dataset.

    The **resolved** spec -- detection plus the caller's edits -- comes back in
    the response and is embedded in the page, which is what lets
    `customize_dashboard` change one panel without re-deriving intent from HTML.
    A spec naming a column that is not in the file, or a chart missing a role it
    needs, is refused by name rather than quietly falling back to detection: a
    dashboard that ignored its configuration looks exactly like one that obeyed.
    """
    progress = []
    # geo_file_path was declared on the tool, forwarded by the wrapper, and read
    # nowhere. The dashboard does build geo panels -- from geo columns it finds
    # in the dataset itself, via _find_geo_cols below -- so an external geojson
    # handed to it was accepted and dropped, and the map that appeared or did
    # not had nothing to do with the file the caller passed. Refused with the
    # route that works rather than silently ignored.
    if geo_file_path:
        return {
            "success": False,
            "op": "generate_dashboard",
            "error": "generate_dashboard does not read an external geo file",
            "hint": (
                "It maps geo columns found in file_path itself. Use enrich_with_geo() to join the "
                "geojson into your dataset first, or generate_geo_map() to map the geojson directly."
            ),
            "progress": [fail("Unsupported argument", "geo_file_path")],
            "token_estimate": 40,
        }
    try:
        try:
            import plotly.graph_objects as _go  # noqa: F401
        except ImportError:
            return {
                "success": False,
                "error": "plotly not installed",
                "hint": "Install: uv add plotly",
                "progress": [fail("Missing dependency", "plotly")],
                "token_estimate": 20,
            }

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
        if err := no_rows_error("generate_dashboard", df, path.name, "Building a dashboard"):
            return err
        dashboard_title = title if title else path.stem
        _parse_dates(df, spec if isinstance(spec, dict) else None)

        numeric_all = [c for c in df.columns if is_numeric_col(df[c])]
        # An override names what a column is, so it is checked against every
        # numeric column, identifiers included: "store_no:sum" is the caller
        # saying store_no is a quantity after all.
        try:
            overrides = parse_agg_overrides(agg_overrides, [str(c) for c in numeric_all])
        except ValueError as exc:
            return {
                "success": False,
                "op": "generate_dashboard",
                "error": str(exc),
                "hint": "Fix the agg_overrides named above and call again. Nothing was written.",
                "progress": [fail("Invalid agg_overrides", str(exc))],
                "token_estimate": 60,
            }
        # Identifiers are never summed or averaged: no KPI, no chart value, no
        # correlation. A spec may still name one explicitly.
        identifiers = [c for c in numeric_all if str(c) not in overrides and is_identifier(str(c), df[c])]
        numeric_cols = [c for c in numeric_all if c not in identifiers]
        datetime_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
        cat_cols = [
            c for c in df.columns if c not in numeric_cols and c not in datetime_cols and df[c].nunique() <= 100
        ]
        # A column with one value groups into one bar, one pie slice and a 1x1
        # heatmap -- the total, drawn as a rectangle. The alert panel keeps the
        # full cat_cols so it can still say the column is constant; charts get
        # this list so they do not spend the top of the page proving it. The
        # filter bar and the numeric range inputs already made this exclusion
        # (1 < len(uniq), mn < mx); the chart builder was the one place that did
        # not, which is how a dataset flagged "'product' has only 1 unique
        # value" got four full-size charts of product.
        chart_cat_cols = [c for c in cat_cols if df[c].nunique() > 1]

        col_agg: dict[str, str] = {nc: infer_agg(nc, df[nc]) for nc in numeric_cols}
        col_agg.update(overrides)
        # What each column was taken to be, returned so a wrong guess is seen in
        # the response and fixed with one override, not found on the page.
        column_roles = {
            str(c): "identifier"
            if c in identifiers
            else "date"
            if c in datetime_cols
            else "measure"
            if c in numeric_cols
            else "dimension"
            if c in cat_cols
            else "text"
            for c in df.columns[:60]
        }

        _d_geo_lat, _d_geo_lon, _d_geo_loc = _find_geo_cols(df)
        _d_geo_loc_mode = _detect_location_mode(df, _d_geo_loc) if _d_geo_loc else ""

        detected: list[str] = []
        if numeric_cols and chart_cat_cols:
            detected.append("bar")
        if datetime_cols and numeric_cols:
            detected.append("time_series")
        if len(numeric_cols) >= 2:
            detected.append("scatter")
        if chart_cat_cols:
            detected.append("pie")
        if _d_geo_lat and _d_geo_lon:
            detected.append("geo_scatter")
        # The column is found by name ("country", "state", "iso3"), which says
        # nothing about what is in it -- a `country` column holding "Domestic"
        # and "International" adds a card containing an unshaded world map.
        # generate_geo_map refuses that outright; a dashboard panel is one of
        # many, so it is simply left out and the rest of the dashboard is drawn.
        if _d_geo_loc:
            _d_geo_values = [str(v) for v in df[_d_geo_loc].dropna().unique().tolist()]
            _d_geo_placeable = len(unrecognised_locations(_d_geo_values, _d_geo_loc_mode)) < len(_d_geo_values)
        else:
            _d_geo_placeable = False
        if _d_geo_loc and numeric_cols and _d_geo_placeable:
            detected.append("geo_choropleth")
        charts = chart_types if chart_types else detected

        # The detection, as a document a caller can edit. Auto-detect stays the
        # default and the zero-argument call is unchanged; a spec turns the
        # detection from the only way in into an opening offer. `resolved` ships
        # in the response and in the page, which is what lets
        # customize_dashboard change one panel without re-deriving a caller's
        # intent from HTML.
        try:
            validate_spec(spec, df)
        except SpecError as exc:
            return {
                "success": False,
                "op": "generate_dashboard",
                "error": str(exc),
                "hint": f"spec keys: {', '.join(SPEC_KEYS)}. Charts: {', '.join(CHART_KINDS)}.",
                "progress": [fail("Invalid spec", str(exc))],
                "token_estimate": 60,
            }
        # The detected layout is written in the spec's own vocabulary, so it can
        # be handed straight back: it used to say "geo_choropleth", which the
        # validator refuses, and a geo dashboard could not be customised at all.
        detected_layout = [
            {"slot": i, "chart": _SPEC_KIND.get(name, name), "cols": {}, "agg": ""} for i, name in enumerate(charts)
        ]
        # The defaults describe the page that is actually drawn. They used to
        # say eight KPIs over a row of seven, and every text column as a filter
        # while the bar offered only those with 2-50 values and no numeric ranges.
        default_controls = _build_filter_controls(df, cat_cols)
        default_ranges = _build_num_ranges(df, numeric_cols)
        resolved = resolve_spec(
            spec,
            title=dashboard_title,
            theme=theme,
            detected_layout=detected_layout,
            kpi_columns=numeric_cols[:7],
            filter_columns=[fc["col"] for fc in default_controls] + [nr["col"] for nr in default_ranges],
        )
        # The build document records where the data came from. The provenance
        # block records only the file NAME -- deliberately, since that block
        # travels with the page -- and that left customize_dashboard unable to
        # find the source whenever MCP_OUTPUT_DIR is not the directory the CSV
        # lives in, which is the deployed layout. Reproduced before fixing.
        resolved["_source_path"] = str(path)
        # A layout the caller wrote is drawn panel by panel, exactly as written:
        # one card per panel, from its own columns. It used to be read for its
        # chart kinds only, and the detector then drew what it always drew --
        # layout=[pie] came back as a pie plus a grouped bar, a box plot, a
        # correlation matrix, a heatmap and a histogram per numeric column, with
        # charts_included saying ["pie"].
        caller_layout = bool(spec) and spec.get("layout") is not None and spec.get(LAYOUT_SOURCE_KEY) != "detected"
        panel_plan: list[tuple[dict, str, bool, int]] | None = None
        if caller_layout:
            try:
                panel_plan = _plan_panels(
                    resolved["layout"],
                    df,
                    chart_cat_cols,
                    numeric_cols,
                    datetime_cols,
                    (_d_geo_lat, _d_geo_lon, _d_geo_loc, _d_geo_loc_mode),
                    col_agg,
                )
            except SpecError as exc:
                return {
                    "success": False,
                    "op": "generate_dashboard",
                    "error": str(exc),
                    "hint": "Name the missing column in that panel's cols, or pick a chart this file can draw.",
                    "progress": [fail("Invalid spec", str(exc))],
                    "token_estimate": 60,
                }
            charts = [p["chart"] for p in resolved["layout"]]
        else:
            resolved[LAYOUT_SOURCE_KEY] = "detected"
        kpi_cols = [str(c) for c in resolved["kpis"]]
        filter_names = [str(c) for c in resolved["filters"]]
        filter_controls = _build_filter_controls(df, [c for c in filter_names if c not in numeric_cols], limit=None)
        num_ranges = _build_num_ranges(df, [c for c in filter_names if c in numeric_cols], limit=None)
        dashboard_title = resolved["title"]
        theme = resolved["theme"]

        if dry_run:
            progress.append(info("Dry run — no file written", path.name))
            result: dict = {
                "success": True,
                "dry_run": True,
                "op": "generate_dashboard",
                "file_path": str(path),
                "would_generate": {
                    "title": dashboard_title,
                    "charts": charts,
                    "kpi_columns": kpi_cols,
                    "column_roles": column_roles,
                    "filter_columns": [fc["col"] for fc in filter_controls] + [nr["col"] for nr in num_ranges],
                },
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        # The ceiling that has always been here: above it a page stops loading
        # at all. `interactions.embed_rows` is the caller's own, lower cap.
        #
        # The review asked for a 5,000-row *default* ("5k-row default + `Load
        # full`"). It is offered, not defaulted, and the reason is the review's
        # own standard. Every figure on this page -- the KPI cards, the bar
        # heights, the pie shares -- is computed in the browser from the rows
        # embedded here, so a 5k default would have silently divided every
        # number on the 38,576-row dashboard it reviewed by about eight, under
        # the same headings. The saving is also smaller than it looks: of that
        # 8.9 MB, 4.86 MB is the Plotly runtime the page carries so it renders
        # anywhere, which capping rows does not touch.
        #
        # So the lever exists, the page says loudly when it is pulled, and the
        # default still tells the truth.
        EMBED_LIMIT = 500_000
        requested = int(resolved["interactions"].get("embed_rows") or 0)
        cap = min(EMBED_LIMIT, requested) if requested > 0 else EMBED_LIMIT
        was_sampled = len(df) > cap
        embed_df = df.sample(cap, random_state=42) if was_sampled else df.copy()
        embed_clean = embed_df.copy()
        for c in datetime_cols:
            if c in embed_clean.columns:
                embed_clean[c] = pd.to_datetime(embed_clean[c], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
        # Columnar + dictionary-encoded; the page rebuilds the same array of
        # row objects, so everything downstream of _RAW is unchanged.
        raw_json = records_js(embed_clean)

        sparklines = _build_sparklines(df, kpi_cols)

        # Computed before the KPI row so the score can see them: the headline
        # number and the panel underneath it must describe the same dataset.
        alerts = alerts_for_frame(df, numeric_cols, cat_cols)

        null_pct = float(df.isnull().mean().mean() * 100)
        dup_pct = float(df.duplicated().sum() / max(len(df), 1) * 100)
        quality = _quality_score(null_pct, dup_pct, alerts)
        qual_clr = "var(--green)" if quality >= 80 else "var(--orange)" if quality >= 60 else "var(--red)"

        _css = css_vars(theme)

        # Resolved first: the output path decides where the page is written,
        # and the <head> is assembled around it.
        out = get_output_path(output_path, path, "dashboard", "html")
        if note := extension_note(output_path, out):
            progress.append(warn("Output extension changed", note))

        h: list[str] = []
        page_header = provenance(
            rows_plotted=len(embed_df),
            rows_total=len(df),
            source=path.name,
            data_hash=frame_hash(embed_df),
            tool="generate_dashboard",
        )
        h.append(_dash_head(_css, dashboard_title, out.parent, page_header, resolved))
        full_call = f'generate_dashboard(file_path="{path.name}", spec={{"interactions": {{"embed_rows": 0}}}})'
        h.append(_dash_header(dashboard_title, embed_df, was_sampled, len(df), full_call))
        # Extra datasets are read before anything is written, so a missing or
        # unreadable one is a refusal rather than a half-built page with a tab
        # that opens onto nothing.
        source_frames: list[tuple[str, object, dict]] = []
        primary_columns = [str(c) for c in df.columns]
        for raw in sources or []:
            try:
                src_path = resolve_path(str(raw))
            except Exception as exc:
                return _bad_source(str(raw), f"path could not be resolved: {exc}")
            if not src_path.exists():
                return _bad_source(str(raw), "file not found")
            try:
                src_df = _read_csv(str(src_path))
            except Exception as exc:
                return _bad_source(str(raw), f"could not be read as CSV: {error_text(exc)}")
            if src_df.empty:
                return _bad_source(str(raw), "has no rows, so its tab would be empty")
            source_frames.append((src_path.name, src_df, _source_summary(src_df, primary_columns, len(df))))

        if source_frames:
            h.append(_dash_source_tabs([path.name] + [n for n, _, _ in source_frames]))
            h.append('<section class="src-sec" data-src="0">')
        h.append(_dash_filterbar(filter_controls, num_ranges))
        h.append(_dash_kpi_row(df, kpi_cols, sparklines, quality, qual_clr, col_agg))
        # The dashboard is the artifact people actually send to a colleague, and
        # it used to show 26 charts of a dataset without mentioning that two of
        # its columns were constant. Same alert engine the EDA report leads with.
        h.append(_dash_alerts(alerts))

        chart_specs: list[dict] = []
        h.append('<div class="sec-hdr">Charts</div><div class="cgrid">')
        if panel_plan is not None:
            for chart_spec, title, full, height in panel_plan:
                _card(h, chart_spec["id"], title, full, height)
                chart_specs.append(_panel(chart_spec, title))
        else:
            _build_chart_cards(
                h,
                chart_specs,
                charts,
                chart_cat_cols,
                numeric_cols,
                datetime_cols,
                _d_geo_lat,
                _d_geo_lon,
                _d_geo_loc,
                _d_geo_loc_mode,
                col_agg,
            )
        h.append("</div>")
        # Off by default: a spec parameter must not change what a
        # zero-argument call returns. `interactions.table` turns it on.
        if resolved["interactions"].get("table"):
            h.append(_dash_table(embed_df, int(resolved["interactions"].get("table_page_size") or 25)))
        if resolved.get("tabs"):
            h.append(_dash_tabs(resolved["tabs"], chart_specs))
        if source_frames:
            h.append("</section>")
            page_size = int(resolved["interactions"].get("table_page_size") or 25)
            for i, (name, src_df, summary) in enumerate(source_frames, start=1):
                h.append(_dash_source_section(i, name, src_df, summary, page_size))
        h.append(_dash_modal())

        # The page's whole drawing state, as data: every card's panel, every KPI,
        # and the theme. One renderer in _dash_js reads them; nothing about a
        # chart is written into code, column names included.
        kpis = [{"col": str(nc), "agg": col_agg.get(nc, "sum"), "el": f"kv-{_safe(nc)}"} for nc in kpi_cols]
        h.append(_dash_js(raw_json, chart_specs, kpis, _theme(theme)))
        if source_frames:
            h.append(_dash_source_js())

        if theme == "device":
            h.append(device_mode_js())
        h.append(_BACK_TO_TOP_JS)
        h.append("</body></html>")

        html_content = "\n".join(h)

        out.write_text(html_content, encoding="utf-8")
        size_kb = round(out.stat().st_size / 1024)

        if open_after:
            _open_file(out)

        if was_sampled:
            progress.append(
                warn(
                    "Large dataset sampled",
                    f"{EMBED_LIMIT:,} of {len(df):,} rows embedded",
                )
            )
        progress.append(ok("Dashboard saved", f"{out.name} ({size_kb:,} KB)"))

        result = {
            "success": True,
            "op": "generate_dashboard",
            "file_path": str(path),
            "output_path": str(out.resolve()),
            "output_name": out.name,
            "dashboard_title": dashboard_title,
            "charts_included": charts,
            "kpi_columns": kpi_cols,
            "column_roles": column_roles,
            "filter_columns": [fc["col"] for fc in filter_controls] + [nr["col"] for nr in num_ranges],
            "rows_embedded": len(embed_df),
            "rows_total": len(df),
            "was_sampled": was_sampled,
            "report_size_kb": size_kb,
            "sources": [
                {
                    "name": name,
                    "rows_shown": min(len(src_df), SOURCE_ROW_CAP),
                    **summary,
                }
                for name, src_df, summary in source_frames
            ],
            # The document this page was built from. Returned so a caller can
            # edit one field and hand it back, rather than describing the change
            # in prose to a tool that only auto-detects.
            "spec": resolved,
            "progress": progress,
        }
        embed_content(result, out, return_content)
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("generate_dashboard error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check file_path is absolute and the file is a valid CSV."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# Dashboard helpers
# ---------------------------------------------------------------------------


def _build_sparklines(df, numeric_cols):
    sparklines: dict = {}
    for nc in numeric_cols:
        n_pts = min(30, len(df))
        step = max(1, len(df) // n_pts)
        sv = df[nc].iloc[::step].head(n_pts).fillna(0).tolist()
        sparklines[nc] = [0 if (isinstance(v, float) and v != v) else v for v in sv]
    return sparklines


def _build_filter_controls(df, cat_cols, limit: int | None = 8):
    """Detected filters: the first `limit` text columns with 2-50 values.

    With `limit=None` the columns are the caller's own, already checked by the
    spec validator, and each gets a control with every value (up to the
    validator's ceiling) -- a named filter that quietly produced no control
    would be the configuration-ignored failure all over again.
    """
    controls: list[dict] = []
    max_values = 50 if limit is not None else MAX_FILTER_VALUES
    for cc in cat_cols[:limit]:
        uniq = sorted(df[cc].dropna().astype(str).unique().tolist())
        if 1 < len(uniq) <= max_values:
            controls.append(
                {
                    "col": cc,
                    "values": uniq,
                    "style": "pills" if len(uniq) <= 10 else "dropdown",
                }
            )
    return controls


def _build_num_ranges(df, numeric_cols, limit: int | None = 3):
    ranges: list[dict] = []
    for nc in numeric_cols[:limit]:
        mn, mx = float(df[nc].min()), float(df[nc].max())
        if mn < mx:
            ranges.append({"col": nc, "min": mn, "max": mx})
    return ranges


def _trend(df, col: str) -> tuple[str, str]:
    mid = len(df) // 2
    if mid == 0:
        return "→", "trend-flat"
    a = df[col].iloc[:mid].mean()
    b = df[col].iloc[mid:].mean()
    if pd.isna(a) or pd.isna(b):
        return "→", "trend-flat"
    if b > a * 1.02:
        return "↑", "trend-up"
    if b < a * 0.98:
        return "↓", "trend-down"
    return "→", "trend-flat"


def _dash_head(_css, dashboard_title, output_dir, header=None, spec=None):
    import html as _html

    full_css = css_dashboard(_css)
    plotly_script = plotly_script_tag(output_dir)
    # The page carries what it is a picture of, and the document it was built
    # from. The second is what makes customize_dashboard possible: without it,
    # "change that bar chart to a line" would mean re-deriving the caller's
    # intent out of rendered HTML.
    blocks = provenance_script(header or {}) + spec_script(spec or {})
    return (
        "<!DOCTYPE html>"
        "<html lang='en'><head>"
        "<meta charset='utf-8'>"
        f"{VIEWPORT_META}"
        f"<title>{_html.escape(dashboard_title)} \u2014 Dashboard</title>"
        f"{blocks}"
        f"{plotly_script}"
        f"<style>{full_css}</style>"
        f"</head><body>"
        f"{_BACK_TO_TOP_HTML}"
    )


def _dash_header(dashboard_title, embed_df, was_sampled, rows_total: int = 0, full_call: str = ""):
    sampled_note = " (sampled)" if was_sampled else ""
    banner = ""
    if was_sampled and rows_total:
        # Every figure on this page -- KPI cards, bar heights, pie shares -- is
        # computed in the browser from the rows embedded here. Sampling makes
        # all of them estimates at once, so the page says so where a reader
        # cannot miss it rather than only in the response body. This is the
        # "Load full" the review asked for: the call that rebuilds the page at
        # full fidelity, spelled out, because a button in a standalone file has
        # nothing to fetch.
        banner = (
            '<div class="sample-banner" style="grid-column:1/-1;font-size:12px;padding:7px 11px;'
            'border-radius:8px;background:rgba(240,136,62,.14);border:1px solid rgba(240,136,62,.5)">'
            f"<b>Estimates.</b> Drawn from {len(embed_df):,} of {rows_total:,} rows, so every number on "
            "this page is computed from the sample, not the whole table."
            + (f" Load full: <code>{_html_esc.escape(full_call)}</code>" if full_call else "")
            + "</div>"
        )
    return f"""<header>
  <h1>{dashboard_title}</h1>
  <span class="row-ctr" id="row-ctr">{len(embed_df):,} of {len(embed_df):,} rows{sampled_note}</span>
  <button class="btn" onclick="clearAll()">Clear Filters</button>
  <button class="btn btn-p" onclick="exportCSV()">&#x2193; Export CSV</button>
  <button class="btn btn-print" onclick="window.print()">&#x2399; Print</button>
  {banner}
</header>"""


def _dash_filterbar(filter_controls, num_ranges):
    if not filter_controls and not num_ranges:
        return ""
    h = ['<div class="filter-bar">']
    for fc in filter_controls:
        col, vals, style = fc["col"], fc["values"], fc["style"]
        # escape(), not a quote-only replace: a column named "<script>" used to
        # reach the page intact, and both column names and cell values here come
        # straight from whatever CSV was loaded.
        lbl = _html_esc.escape(col)
        # A JS string inside a double-quoted HTML attribute: escaped for JS,
        # then for the attribute. Escaping only \ and ' left a " free to end the
        # onchange="..." attribute and open a new one.
        col_js = _html_esc.escape(_js(col))
        h.append(f'<div class="fgrp"><div class="flbl">{lbl}</div>')
        if style == "pills":
            h.append(f'<div class="pills" data-col="{lbl}">')
            for v in vals:
                ve = _html_esc.escape(str(v))
                h.append(f'<button class="pill active" data-val="{ve}" onclick="pilClick(this)">{ve}</button>')
            h.append("</div>")
        else:
            opts = "".join(
                f'<label class="optlbl"><input type="checkbox" data-val="{_html_esc.escape(str(v))}"'
                f" checked onchange=\"ddChange('{col_js}')\">{_html_esc.escape(str(v))}</label>"
                for v in vals
            )
            h.append(
                f'<div class="ddw" data-col="{lbl}">'
                f'<button class="ddbtn" onclick="ddToggle(this)">All &#x25BE;</button>'
                f'<div class="ddmenu hid">'
                f'<input class="ddsrch" placeholder="Search..." oninput="ddSrch(this,\'{col_js}\')">'
                f'<div class="ddacts"><button class="btn" onclick="ddAll(\'{col_js}\',true)">All</button>'
                f'<button class="btn" onclick="ddAll(\'{col_js}\',false)">None</button></div>{opts}</div></div>'
            )
        h.append("</div>")
    for nr in num_ranges:
        nc = nr["col"]
        nc_js = _html_esc.escape(_js(nc))
        mn_s = _compact_num(nr["min"])
        mx_s = _compact_num(nr["max"])
        h.append(
            f'<div class="fgrp"><div class="flbl">{_html_esc.escape(nc)}</div>'
            f'<div class="nrng">'
            f'<input type="number" class="ninp" placeholder="Min ({mn_s})" onchange="numCh(\'{nc_js}\',\'min\',this.value)">'
            f'<span class="nsep">–</span>'
            f'<input type="number" class="ninp" placeholder="Max ({mx_s})" onchange="numCh(\'{nc_js}\',\'max\',this.value)">'
            f"</div></div>"
        )
    h.append("</div>")
    return "\n".join(h)


def _compact_num(v: float) -> str:
    """Short enough to survive inside a filter input.

    These are placeholders showing a column's bounds, and "Max (67,454)" was
    being clipped mid-number to "Max (67,4" -- a hint the reader cannot finish
    is worse than a rounder one they can, so magnitudes are abbreviated.
    """
    a = abs(v)
    if a < 1:
        return f"{v:.3f}".rstrip("0").rstrip(".") or "0"
    for cutoff, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if a >= cutoff:
            scaled = v / cutoff
            return f"{scaled:.0f}{suffix}" if abs(scaled) >= 10 else f"{scaled:.1f}{suffix}"
    return f"{v:,.0f}"


# Shared with the EDA report: both describe the same frames, and when each
# kept its own formula they disagreed by 57 points on one dataset.
_quality_score = quality_score


def _dash_alerts(alerts: list[dict]) -> str:
    """Render the data-quality panel, collapsed when there is nothing to say."""
    if not alerts:
        return ""
    errors = sum(1 for a in alerts if a["sev"] == "error")
    label = f"Data quality — {len(alerts)} alert{'s' if len(alerts) != 1 else ''}"
    if errors:
        label += f", {errors} serious"
    return (
        f'<div class="sec-hdr">{label}</div>'
        f'<div style="padding:0 clamp(.875rem,3vw,1.75rem) .5rem">{alerts_html(alerts)}</div>'
    )


def _dash_kpi_row(df, numeric_cols, sparklines, quality, qual_clr, col_agg):
    h = ['<div class="kpi-row">']
    h.append(
        f'<div class="kpi-card"><div class="kpi-val" style="color:{qual_clr}">{quality}</div><div class="kpi-lbl">Quality Score</div></div>'
    )
    # Every column passed is drawn: the detected default is already cut to
    # seven, and a caller's `kpis` list is theirs.
    for nc in numeric_cols:
        agg = col_agg.get(nc, "sum")
        arrow, acls = _trend(df, nc)
        sc = _safe(nc)
        sv = sparklines.get(nc, [])
        series = df[nc].dropna()
        if agg == "median":
            init_val = float(series.median()) if len(series) else 0.0
        elif agg == "count":
            init_val = float(series.count())
        elif agg == "count_distinct":
            init_val = float(series.nunique())
        elif agg == "mean":
            init_val = float(series.mean()) if len(series) else 0.0
        elif agg == "max":
            init_val = float(series.max()) if len(series) else 0.0
        elif agg == "min":
            init_val = float(series.min()) if len(series) else 0.0
        else:
            init_val = float(series.sum())
        lbl = f"{agg_label(agg)} {nc}"
        if abs(init_val) >= 1_000_000:
            iv = f"{init_val / 1_000_000:.1f}M"
        elif abs(init_val) >= 1_000:
            iv = f"{init_val / 1_000:.1f}K"
        else:
            iv = f"{init_val:,.0f}"
        h.append(
            f'<div class="kpi-card">'
            f'<div class="kpi-val" id="kv-{sc}">{iv}</div>'
            f'<div class="kpi-lbl">{_html_esc.escape(lbl)}</div>'
            f'<div class="kpi-trend {acls}">{arrow}</div>'
            f'<div class="kpi-spark" id="ks-{sc}"></div>'
            f"</div>"
        )
        h.append(
            f"<script>(function(){{"
            f"Plotly.newPlot('ks-{sc}',"
            f"[{{y:{_json.dumps(sv)},type:'scatter',mode:'lines',"
            f"line:{{color:'var(--accent)',width:1.5}},"
            f"fill:'tozeroy',fillcolor:'rgba(88,166,255,0.08)'}}],"
            f"{{paper_bgcolor:'rgba(0,0,0,0)',plot_bgcolor:'rgba(0,0,0,0)',"
            f"margin:{{l:0,r:0,t:0,b:0}},xaxis:{{visible:false}},"
            f"yaxis:{{visible:false}},showlegend:false}},"
            f"{{responsive:true,displayModeBar:false,staticPlot:true}});"
            f"}})();</script>"
        )
    h.append("</div>")
    return "\n".join(h)


def _card(h, cid: str, ttl: str, full: bool, height: int) -> None:
    cls = "cc full" if full else "cc"
    # Use CSS class for height — tall (>380 px original) gets cc-body--tall
    body_cls = "cc-body--tall" if height > 380 else "cc-body"
    te = _html_esc.escape(ttl)
    h.append(
        f'<div class="{cls}">'
        f'<div class="cc-hdr"><h3>{te}</h3>'
        f'<button class="exp" data-expand="{cid}" data-expand-title="{te}">&#x2922;</button>'
        f'</div><div class="{body_cls}">'
        f'<div id="{cid}" style="width:100%;height:100%"></div>'
        f"</div></div>"
    )


# The detector's internal names for the kinds the spec vocabulary calls
# something else.
_SPEC_KIND = {"geo_choropleth": "choropleth"}


def _parse_dates(df, spec) -> None:
    """Read date columns as dates, in place.

    The CSV loader leaves "2024-01-05" as text, so a date column was never seen
    as one: no CSV ever got a time series, and a pie of 90 dates took its place.
    Every text column that holds dates is now read as dates, and so is any
    column a panel names as its `date` (the caller has said what it is, so the
    shape check is skipped for it).
    """
    named = set()
    for panel in (spec or {}).get("layout") or []:
        cols = panel.get("cols") if isinstance(panel, dict) else None
        if isinstance(cols, dict) and cols.get("date"):
            named.add(str(cols["date"]))
    for col in list(df.columns):
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue
        parsed = parse_date_column(df[col], named=str(col) in named)
        if parsed is not None:
            df[col] = parsed


def _plan_panels(layout, df, cat_cols, numeric_cols, datetime_cols, geo, col_agg):
    """One card per panel of a caller's layout, drawn from that panel's columns.

    Returns (chart_spec, title, full_width, height) per panel, in layout order,
    so tab slot N is card N. A role the panel leaves empty is filled the way
    the detected page fills it; a role nothing can fill is refused by name
    rather than swapped for a different chart.
    """
    lat_d, lon_d, loc_d, loc_mode = geo
    plan: list[tuple[dict, str, bool, int]] = []
    for i, panel in enumerate(layout):
        kind = panel["chart"]
        cols = dict(panel.get("cols") or {})
        named_agg = panel.get("agg") or ""
        cid = f"p{i}_{_safe(kind)}"

        def pick(role: str, candidates: list, what: str) -> str:
            if cols.get(role):
                return str(cols[role])
            if candidates:
                return str(candidates[0])
            raise SpecError(
                f"layout[{i}] is a {kind} chart and this file has no {what} for its {role}; name one in cols.{role}"
            )

        if kind == "bar":
            cc = pick("category", cat_cols, "text column with 2-100 values")
            nc = pick("value", numeric_cols, "numeric column")
            agg = named_agg or col_agg.get(nc, "sum")
            plan.append(
                (
                    {"id": cid, "type": "bar", "category": cc, "value": nc, "agg": agg},
                    f"{agg_label(agg)} {nc} by {cc}",
                    False,
                    340,
                )
            )
        elif kind == "pie":
            cc = pick("category", cat_cols, "text column with 2-100 values")
            nc = str(cols.get("value") or "")
            title = f"{nc} share by {cc}" if nc else f"{cc} Distribution"
            plan.append(({"id": cid, "type": "pie", "category": cc, "value": nc}, title, False, 340))
        elif kind in ("line", "time_series"):
            dc = pick("date", datetime_cols, "date column")
            nc = pick("value", numeric_cols, "numeric column")
            agg = named_agg or col_agg.get(nc, "sum")
            plan.append(
                (
                    {"id": cid, "type": "ts", "date": dc, "value": nc, "agg": agg},
                    f"{agg_label(agg)} {nc} Over Time",
                    True,
                    380,
                )
            )
        elif kind == "scatter":
            x = pick("x", numeric_cols, "numeric column")
            y = pick("y", [c for c in numeric_cols if c != x], "second numeric column")
            plan.append(({"id": cid, "type": "scatter", "x": x, "y": y}, f"{x} vs {y}", False, 340))
        elif kind == "histogram":
            nc = pick("value", numeric_cols, "numeric column")
            plan.append(({"id": cid, "type": "dist", "value": nc}, f"{nc} Distribution", False, 320))
        elif kind == "box":
            nc = pick("value", numeric_cols, "numeric column")
            # Named cols without a category ask for one box; no cols at all
            # asks for the detected page's box, grouped by the first category.
            cc = str(cols.get("category") or ("" if cols or not cat_cols else cat_cols[0]))
            title = f"{nc} distribution by {cc}" if cc else f"{nc} distribution"
            plan.append(({"id": cid, "type": "box", "value": nc, "category": cc}, title, True, 380))
        elif kind == "geo_scatter":
            lat = pick("lat", [lat_d] if lat_d else [], "latitude column")
            lon = pick("lon", [lon_d] if lon_d else [], "longitude column")
            val = str(numeric_cols[0]) if numeric_cols else ""
            cc = str(cat_cols[0]) if cat_cols else ""
            spec = {"id": cid, "type": "geo_scatter", "lat": lat, "lon": lon, "value": val, "category": cc}
            plan.append((spec, "Geographic Distribution (Scatter)", True, 500))
        elif kind == "choropleth":
            loc = pick("location", [loc_d] if loc_d else [], "location column")
            nc = pick("value", numeric_cols, "numeric column")
            agg = named_agg or col_agg.get(nc, "sum")
            mode = loc_mode if loc == loc_d else _detect_location_mode(df, loc)
            values = [str(v) for v in df[loc].dropna().unique().tolist()]
            if values and len(unrecognised_locations(values, mode)) == len(values):
                raise SpecError(
                    f"layout[{i}] choropleth location={loc!r} holds no place names a map can shade "
                    f"(e.g. {values[0]!r}); use a bar chart for it instead"
                )
            spec = {
                "id": cid,
                "type": "geo_choro",
                "location": loc,
                "value": nc,
                "mode": mode or "country names",
                "agg": agg,
            }
            plan.append((spec, f"{agg_label(agg)} {nc} by {loc} (Choropleth)", True, 500))
        else:  # pragma: no cover - the validator refuses every other kind first
            raise SpecError(f"layout[{i}] chart={kind!r} is not drawable. Valid: {', '.join(CHART_KINDS)}")
    return plan


def _build_chart_cards(
    h,
    chart_specs,
    charts,
    cat_cols,
    numeric_cols,
    datetime_cols,
    _d_geo_lat,
    _d_geo_lon,
    _d_geo_loc,
    _d_geo_loc_mode,
    col_agg,
):
    """The detected page: a card and a panel for every chart this file supports."""

    def add(spec: dict, title: str, full: bool, height: int) -> None:
        _card(h, spec["id"], title, full, height)
        chart_specs.append(_panel(spec, title))

    if "bar" in charts and cat_cols and numeric_cols:
        for cc in cat_cols[:3]:
            for nc in numeric_cols[:2]:
                agg = col_agg.get(nc, "sum")
                spec = {"id": f"bar_{_safe(cc)}_{_safe(nc)}", "type": "bar", "category": cc, "value": nc, "agg": agg}
                add(spec, f"{agg_label(agg)} {nc} by {cc}", False, 340)
    if "pie" in charts and cat_cols:
        for cc in cat_cols[:3]:
            add(
                {"id": f"pie_{_safe(cc)}", "type": "pie", "category": cc, "value": ""}, f"{cc} Distribution", False, 340
            )
    if "scatter" in charts and len(numeric_cols) >= 2:
        pairs = [
            (numeric_cols[i], numeric_cols[j])
            for i in range(min(2, len(numeric_cols)))
            for j in range(i + 1, min(i + 3, len(numeric_cols)))
        ]
        for nc1, nc2 in pairs:
            spec = {"id": f"scat_{_safe(nc1)}_{_safe(nc2)}", "type": "scatter", "x": nc1, "y": nc2}
            add(spec, f"{nc1} vs {nc2}", False, 340)
    if len(cat_cols) >= 2 and numeric_cols:
        cc1, cc2, nc = cat_cols[0], cat_cols[1], numeric_cols[0]
        agg = col_agg.get(nc, "sum")
        spec = {
            "id": f"grp_{_safe(cc1)}_{_safe(cc2)}",
            "type": "grouped_bar",
            "category": cc1,
            "group": cc2,
            "value": nc,
            "agg": agg,
        }
        add(spec, f"{agg_label(agg)} {nc} by {cc1}, grouped by {cc2}", True, 380)
    if len(numeric_cols) >= 2 and cat_cols:
        nc1, nc2, cc = numeric_cols[0], numeric_cols[1], cat_cols[0]
        spec = {"id": f"cscat_{_safe(nc1)}_{_safe(nc2)}", "type": "cscat", "x": nc1, "y": nc2, "group": cc}
        add(spec, f"{nc1} vs {nc2} by {cc}", True, 380)
    if numeric_cols and cat_cols:
        nc, cc = numeric_cols[0], cat_cols[0]
        add(
            {"id": f"box_{_safe(nc)}_{_safe(cc)}", "type": "box", "value": nc, "category": cc},
            f"{nc} distribution by {cc}",
            True,
            380,
        )
    if len(numeric_cols) >= 2:
        add(
            {"id": "corr_hm", "type": "corr", "columns": [str(c) for c in numeric_cols[:15]]},
            "Correlation Matrix",
            True,
            480,
        )
    if len(cat_cols) >= 2 and numeric_cols:
        cc1, cc2, nc = cat_cols[0], cat_cols[1], numeric_cols[0]
        agg = col_agg.get(nc, "sum")
        spec = {
            "id": f"aghm_{_safe(cc1)}_{_safe(cc2)}",
            "type": "agg_hm",
            "category": cc1,
            "group": cc2,
            "value": nc,
            "agg": agg,
        }
        add(spec, f"{agg_label(agg)} {nc}: {cc1} \u00d7 {cc2}", True, 460)
    if "time_series" in charts and datetime_cols and numeric_cols:
        for dc in datetime_cols[:2]:
            for nc in numeric_cols[:2]:
                agg = col_agg.get(nc, "sum")
                spec = {"id": f"ts_{_safe(dc)}_{_safe(nc)}", "type": "ts", "date": dc, "value": nc, "agg": agg}
                add(spec, f"{agg_label(agg)} {nc} Over Time", True, 380)
    for nc in numeric_cols[:6]:
        add({"id": f"dist_{_safe(nc)}", "type": "dist", "value": nc}, f"{nc} Distribution", False, 320)
    if "geo_scatter" in charts and _d_geo_lat and _d_geo_lon:
        spec = {
            "id": f"geo_scat_{_safe(_d_geo_lat)}",
            "type": "geo_scatter",
            "lat": _d_geo_lat,
            "lon": _d_geo_lon,
            "value": numeric_cols[0] if numeric_cols else "",
            "category": cat_cols[0] if cat_cols else "",
        }
        add(spec, "Geographic Distribution (Scatter)", True, 500)
    if "geo_choropleth" in charts and _d_geo_loc and numeric_cols:
        nc = numeric_cols[0]
        agg = col_agg.get(nc, "sum")
        spec = {
            "id": f"geo_choro_{_safe(_d_geo_loc)}",
            "type": "geo_choro",
            "location": _d_geo_loc,
            "value": nc,
            "mode": _d_geo_loc_mode or "country names",
            "agg": agg,
        }
        add(spec, f"{agg_label(agg)} {nc} by {_d_geo_loc} (Choropleth)", True, 500)


def _dash_modal():
    return (
        '<div id="modal" class="modal"><div class="mbox">'
        '<div class="mhdr"><h3 id="mttl"></h3>'
        '<button class="mclose" onclick="closeM()">&#x2715;</button></div>'
        '<div id="mdiv"></div></div></div>'
    )


def _close_tag_safe(js: str) -> str:
    """Neutralise the one sequence that can end a <script> block early.

    The HTML parser looks for `</script>` before any JavaScript is parsed, so a
    column name or cell value containing it ends the block and everything after
    becomes markup. In generated code that sequence can only have come from data,
    and inside a string literal `<\\/script` is the identical string.
    """
    return _re.sub(r"</(script)", r"<\\/\1", js, flags=_re.IGNORECASE)


_RENDERER_JS = r"""
// --- one renderer --------------------------------------------------------
// Every card is a panel in _PANELS: its columns, aggregate, title and style.
// FIG[type] turns a panel and the filtered rows into Plotly traces and layout,
// and renderPanel lays the panel's style over the theme. A new option is a
// field of the panel read here -- never a new template -- and a column name is
// data in _PANELS, never text pasted into code.
const PCFG={responsive:true,displayModeBar:true,scrollZoom:true};
// The theme in force. A device page holds a light and a dark one and follows
// the reader's setting, at every draw.
function _dark(){return typeof window!=='undefined'&&!!window.matchMedia&&window.matchMedia('(prefers-color-scheme: dark)').matches;}
function _T(){return _THEME.device?(_dark()?_THEME.dark:_THEME.light):_THEME;}
const _ARRAY={median:1,count:1,count_distinct:1};
function _fmt(v){return v>=1e6?(v/1e6).toFixed(1)+'M':v>=1e3?(v/1e3).toFixed(1)+'K':Math.round(v).toString();}
function _key(r,c){return String(r[c]??'');}
function _isObj(v){return v!==null&&typeof v==='object'&&!Array.isArray(v);}
function _merge(a,b){
  var o=Object.assign({},a);
  Object.keys(b||{}).forEach(function(k){o[k]=(_isObj(b[k])&&_isObj(o[k]))?_merge(o[k],b[k]):b[k];});
  return o;
}
function _min(v){return v.reduce(function(a,b){return a<b?a:b;});}
function _max(v){return v.reduce(function(a,b){return a>b?a:b;});}
// The values of `col` grouped by `keyOf`, in the order groups are first seen.
// A missing value never makes a group -- only a present one does.
function _groups(d,keyOf,col){
  var m=new Map();
  d.forEach(function(r){var k=keyOf(r),v=_num(r[col]);if(k===null||isNaN(v))return;if(!m.has(k))m.set(k,[]);m.get(k).push(v);});
  return m;
}
function _kpi(d,col,how){return _agg(d.map(function(r){return _num(r[col]);}),how);}
function _axes(extra){return _merge({margin:{l:55,r:20,t:10,b:65},xaxis:{gridcolor:_T().grid,tickangle:'auto'},yaxis:{gridcolor:_T().grid}},extra);}
function _geo(){return{showland:true,landcolor:_T().land,showocean:true,oceancolor:_T().ocean,showcoastlines:true,coastlinecolor:_T().coast,showcountries:true,countrycolor:_T().coast,showframe:false,bgcolor:_T().bg};}
function _color(i){return _T().palette[i%_T().palette.length];}

const FIG={
  bar:function(p,d){
    var s=p.style,how=p.agg||'sum';
    var e=Array.from(_groups(d,function(r){return _key(r,p.category);},p.value),function(g){return[g[0],_agg(g[1],how)];});
    e.sort(how==='min'?function(x,y){return x[1]-y[1];}:function(x,y){return y[1]-x[1];});
    e=e.slice(0,s.top_n);
    return{data:[{x:e.map(function(i){return i[0];}),y:e.map(function(i){return i[1];}),type:'bar',marker:{color:s.color,opacity:0.85},text:e.map(function(i){return _fmt(i[1]);}),textposition:'outside'}],layout:_axes()};
  },
  pie:function(p,d){
    // With a value column the slices are its sums per category; without one
    // they are row counts.
    var c=new Map();
    d.forEach(function(r){var k=_key(r,p.category),w=p.value?(_num(r[p.value])||0):1;c.set(k,(c.get(k)||0)+w);});
    var e=Array.from(c).sort(function(x,y){return y[1]-x[1];}).slice(0,p.style.top_n);
    // Past a handful of slices, labels drawn outside on leader lines overlap
    // and spill out of the card, repeating names the legend already lists.
    var ti=e.length>6?'percent':'label+percent';
    return{data:[{values:e.map(function(i){return i[1];}),labels:e.map(function(i){return i[0];}),type:'pie',hole:0.38,marker:{colors:_T().palette},textinfo:ti,textposition:'inside',insidetextorientation:'horizontal',textfont:{size:11},pull:e.map(function(_,i){return i===0?0.04:0;})}],
           layout:{margin:{l:20,r:20,t:10,b:20},showlegend:true,legend:{orientation:'h',y:-0.14}}};
  },
  scatter:function(p,d){
    var s=p.style,xs=[],ys=[];
    d.forEach(function(r){var x=_num(r[p.x]),y=_num(r[p.y]);if(!isNaN(x)&&!isNaN(y)){xs.push(x);ys.push(y);}});
    var t=[{x:xs,y:ys,type:'scatter',mode:'markers',marker:{color:s.color,opacity:0.5,size:5},name:'data'}];
    if(xs.length>1){
      var n=xs.length,sx=0,sy=0,sxy=0,sxx=0,syy=0;
      for(var i=0;i<n;i++){sx+=xs[i];sy+=ys[i];sxy+=xs[i]*ys[i];sxx+=xs[i]*xs[i];syy+=ys[i]*ys[i];}
      var sl=(n*sxy-sx*sy)/(n*sxx-sx*sx||1),ic=(sy-sl*sx)/n;
      var r=(n*sxy-sx*sy)/Math.sqrt(((n*sxx-sx*sx)*(n*syy-sy*sy))||1);
      var lo=_min(xs),hi=_max(xs);
      t.push({x:[lo,hi],y:[sl*lo+ic,sl*hi+ic],type:'scatter',mode:'lines',line:{color:s.accent,width:2,dash:'dash'},name:'r='+r.toFixed(2)});
    }
    return{data:t,layout:_axes({showlegend:true,legend:{x:0,y:1.1,orientation:'h'},xaxis:{title:p.x},yaxis:{title:p.y}})};
  },
  grouped_bar:function(p,d){
    var s=p.style,how=p.agg||'sum',a=new Map(),gs=[],seen=new Set();
    d.forEach(function(r){
      var k1=_key(r,p.category),k2=_key(r,p.group),v=_num(r[p.value]);
      if(!seen.has(k1)){seen.add(k1);gs.push(k1);}
      if(isNaN(v))return;
      if(!a.has(k2))a.set(k2,new Map());
      var m=a.get(k2);if(!m.has(k1))m.set(k1,[]);m.get(k1).push(v);
    });
    gs=gs.slice(0,s.top_n);
    var t=Array.from(a.keys()).slice(0,s.series).map(function(k,i){
      var m=a.get(k);
      return{x:gs,y:gs.map(function(g){return m.has(g)?_agg(m.get(g),how):0;}),type:'bar',name:k,marker:{color:_color(i),opacity:0.85}};
    });
    return{data:t,layout:_axes({barmode:'group',showlegend:true,legend:{orientation:'h',x:0,y:1.12}})};
  },
  cscat:function(p,d){
    var g=new Map();
    d.forEach(function(r){var x=_num(r[p.x]),y=_num(r[p.y]),k=_key(r,p.group);if(!isNaN(x)&&!isNaN(y)){if(!g.has(k))g.set(k,{x:[],y:[]});g.get(k).x.push(x);g.get(k).y.push(y);}});
    var t=Array.from(g.keys()).slice(0,p.style.series).map(function(k,i){return{x:g.get(k).x,y:g.get(k).y,type:'scatter',mode:'markers',name:k,marker:{color:_color(i),opacity:0.6,size:5}};});
    return{data:t,layout:_axes({showlegend:true,legend:{orientation:'h',x:0,y:1.12},xaxis:{title:p.x},yaxis:{title:p.y}})};
  },
  box:function(p,d){
    var g=_groups(d,function(r){return _key(r,p.category);},p.value);
    var t=Array.from(g.keys()).sort().slice(0,p.style.top_n).map(function(k,i){return{y:g.get(k),type:'box',name:k,marker:{color:_color(i),size:3},boxpoints:'outliers'};});
    return{data:t,layout:_axes({showlegend:false,yaxis:{title:p.value}})};
  },
  corr:function(p,d){
    var cols=p.columns,n=d.length;if(n<2)return null;
    var z=cols.map(function(a){return cols.map(function(b){
      var pr=[];
      d.forEach(function(row){var x=_num(row[a]),y=_num(row[b]);if(!isNaN(x)&&!isNaN(y))pr.push([x,y]);});
      if(pr.length<2)return 0;
      var mx=0,my=0;pr.forEach(function(q){mx+=q[0];my+=q[1];});mx/=pr.length;my/=pr.length;
      var num=0,dx=0,dy=0;pr.forEach(function(q){num+=(q[0]-mx)*(q[1]-my);dx+=(q[0]-mx)*(q[0]-mx);dy+=(q[1]-my)*(q[1]-my);});
      return dx&&dy?num/Math.sqrt(dx*dy):0;
    });});
    return{data:[{z:z,x:cols,y:cols,type:'heatmap',colorscale:p.style.colorscale,zmid:0,zmin:-1,zmax:1,text:z.map(function(r){return r.map(function(v){return v.toFixed(2);});}),texttemplate:'%{text}',textfont:{size:10}}],
           layout:{font:{size:11},margin:{l:120,r:20,t:10,b:120}}};
  },
  agg_hm:function(p,d){
    var s=p.style,how=p.agg||'sum',a=new Map(),R=new Set(),C=new Set();
    d.forEach(function(r){
      var k1=_key(r,p.category),k2=_key(r,p.group),v=_num(r[p.value]);if(isNaN(v))return;
      R.add(k1);C.add(k2);
      var key=k1+'\u0000'+k2;if(!a.has(key))a.set(key,[]);a.get(key).push(v);
    });
    var rl=Array.from(R).sort().slice(0,s.top_n),cl=Array.from(C).sort().slice(0,s.top_n);
    var z=rl.map(function(r){return cl.map(function(c){var v=a.get(r+'\u0000'+c);return v?_agg(v,how):0;});});
    return{data:[{z:z,x:cl,y:rl,type:'heatmap',colorscale:s.colorscale,text:z.map(function(r){return r.map(_fmt);}),texttemplate:'%{text}',textfont:{size:9}}],
           layout:{font:{size:11},margin:{l:130,r:20,t:10,b:130}}};
  },
  ts:function(p,d){
    var s=p.style,how=p.agg||'sum',w=s.ma;
    var bm=_groups(d,function(r){var dt=r[p.date];return dt?String(dt).substring(0,7):null;},p.value);
    var dates=Array.from(bm.keys()).sort(),vals=dates.map(function(k){return _agg(bm.get(k),how);});
    var ma=vals.map(function(_,i){if(i<w-1)return null;var t=0;for(var j=i-w+1;j<=i;j++)t+=vals[j];return t/w;});
    var t=[{x:dates,y:vals,type:'scatter',mode:'lines+markers',name:p.value,line:{color:s.color,width:2},marker:{size:4}},
           {x:dates.slice(w-1),y:ma.slice(w-1),type:'scatter',mode:'lines',name:w+'-period MA',line:{color:s.accent,width:2,dash:'dot'}}];
    return{data:t,layout:_axes({showlegend:true,legend:{x:0,y:1.1,orientation:'h'},xaxis:{title:'Date'},yaxis:{title:p.value}})};
  },
  dist:function(p,d){
    var s=p.style,vals=d.map(function(r){return _num(r[p.value]);}).filter(function(v){return!isNaN(v);});
    if(!vals.length)return null;
    var g=_T().grid;
    return{data:[{x:vals,type:'histogram',nbinsx:s.bins,marker:{color:s.color,opacity:0.75},xaxis:'x',yaxis:'y',name:'hist'},
                 {y:vals,type:'box',marker:{color:s.accent,size:3},xaxis:'x2',yaxis:'y2',boxpoints:'outliers',name:'box'}],
           layout:{margin:{l:50,r:20,t:10,b:30},grid:{rows:1,columns:2,pattern:'independent'},xaxis:{gridcolor:g},yaxis:{title:'Count',gridcolor:g},xaxis2:{gridcolor:g},yaxis2:{gridcolor:g}}};
  },
  geo_scatter:function(p,d){
    var lts=[],lns=[],txts=[];
    d.forEach(function(r){var lt=_num(r[p.lat]),ln=_num(r[p.lon]);if(!isNaN(lt)&&!isNaN(ln)){lts.push(lt);lns.push(ln);txts.push(p.value?p.value+': '+String(r[p.value]):lt.toFixed(4)+', '+ln.toFixed(4));}});
    if(!lts.length)return null;
    return{data:[{type:'scattergeo',lat:lts,lon:lns,mode:'markers',marker:{color:p.style.color,size:6,opacity:0.75,line:{color:'rgba(255,255,255,0.25)',width:0.5}},text:txts,hovertemplate:'%{text}<extra></extra>'}],
           layout:{geo:_merge(_geo(),{projection:{type:'natural earth'}}),margin:{l:0,r:0,t:10,b:0}}};
  },
  geo_choro:function(p,d){
    var how=p.agg||'sum',a=_groups(d,function(r){var k=_key(r,p.location);return k?k:null;},p.value);
    var locs=Array.from(a.keys());if(!locs.length)return null;
    return{data:[{type:'choropleth',locations:locs,z:locs.map(function(k){return _agg(a.get(k),how);}),locationmode:p.mode,coloraxis:'coloraxis',hovertemplate:'%{location}: %{z:.2f}<extra></extra>'}],
           layout:{geo:_geo(),margin:{l:0,r:0,t:10,b:0},coloraxis:{colorscale:p.style.colorscale,showscale:true,colorbar:{thickness:14,len:0.7,tickfont:{color:_T().font,size:10}}}}};
  }
};

// The panel's figure, drawn over the theme's frame, with the panel's own
// layout fields last so they win.
function figure(p,d){
  var f=FIG[p.type](p,d);if(!f)return null;
  var frame={paper_bgcolor:_T().bg,plot_bgcolor:_T().bg,font:{color:_T().font,size:12},autosize:true};
  return{data:f.data,layout:am(_merge(_merge(frame,f.layout),p.style.layout||{}))};
}
function renderPanel(p,d){var f=figure(p,d);if(f)Plotly.react(p.id,f.data,f.layout,PCFG);}

function updKPIs(d){
  _KPIS.forEach(function(k){
    var s=_kpi(d,k.col,k.agg),el=document.getElementById(k.el);
    if(el)el.textContent=s>=1e6?(s/1e6).toFixed(1)+'M':s>=1e3?(s/1e3).toFixed(1)+'K':Math.round(s).toLocaleString();
  });
}

function renderAll(d){
  updKPIs(d);
  _PANELS.forEach(function(p){try{renderPanel(p,d);}catch(_e){console.warn('chart '+p.id,_e);}});
}
// A device page redraws when the reader switches light and dark.
if(_THEME.device&&typeof window!=='undefined'&&window.matchMedia){
  window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change',function(){renderAll(getFilt());});
}
"""


def _dash_js(raw_json, panels: list[dict], kpis: list[dict], theme: dict):
    # json_for_script escapes <, > and &, so no name or value in the panels can
    # end the <script> block -- and none is ever read as code.
    state = (
        f"const _PANELS={json_for_script(panels)};\n"
        f"const _KPIS={json_for_script(kpis)};\n"
        f"const _THEME={json_for_script(theme)};\n"
    )
    return f"""<script>
let _RAW={raw_json};
{state}const _TOTAL=_RAW.length;
let _CF={{}};
let _NF={{}};

// A missing cell arrives as null, and +null is 0 -- so every chart and KPI
// counted a missing value as a real zero: a group with [10, missing] averaged
// to 5, its minimum became 0, the KPI mean sank. _num keeps missing missing.
function _num(v){{return(v===null||v===undefined||v==='')?NaN:+v;}}
// The aggregates that need every value, not a running total. Missing values
// (NaN from _num) are never counted or ranked.
function _agg(v,how){{var x=v.filter(function(a){{return!isNaN(a);}});
  if(how==='count')return x.length;
  if(how==='count_distinct')return new Set(x).size;
  if(!x.length)return 0;
  if(how==='median'){{x.sort(function(a,b){{return a-b;}});var m=x.length>>1;return x.length%2?x[m]:(x[m-1]+x[m])/2;}}
  if(how==='mean')return x.reduce(function(a,b){{return a+b;}},0)/x.length;
  if(how==='max')return x.reduce(function(a,b){{return a>b?a:b;}});
  if(how==='min')return x.reduce(function(a,b){{return a<b?a:b;}});
  return x.reduce(function(a,b){{return a+b;}},0);}}

(function(){{
  const _SAVED=sessionStorage.getItem('dash-filters');
  if(_SAVED){{try{{const s=JSON.parse(_SAVED);if(s.cf)Object.assign(_CF,s.cf);if(s.nf)Object.assign(_NF,s.nf);}}catch(e){{}}}}
}})();

// Every axis grows its own margin to fit the labels it draws. At 390px the
// dashboard's fixed margins sheared the y-axis ticks off: '2000', '4000',
// '6000' and '8000' were all cut. Applied here rather than in each of the
// thirteen hand-written layouts, which is where it would rot.
function am(l){{
  var found=false;
  Object.keys(l).forEach(function(k){{
    if(/^[xy]axis/.test(k) && l[k] && typeof l[k]==='object'){{ l[k].automargin=true; found=true; }}
  }});
  if(!found && !l.geo && !l.mapbox){{ l.xaxis={{automargin:true}}; l.yaxis={{automargin:true}}; }}
  return l;
}}

function getFilt(){{
  return _RAW.filter(function(row){{
    for(var c in _CF){{var s=_CF[c];if(s&&s.size>0&&!s.has(String(row[c]??'')))return false;}}
    for(var c in _NF){{var r=_NF[c],v=_num(row[c]);if(isNaN(v)){{if(r.min!==null||r.max!==null)return false;}}else{{if(r.min!==null&&v<r.min)return false;if(r.max!==null&&v>r.max)return false;}}}}
    return true;
  }});
}}

function applyF(){{
  try{{renderTable(getFilt());}}catch(_e){{}}
  const d=getFilt();
  document.getElementById('row-ctr').textContent=d.length.toLocaleString()+' of '+_TOTAL.toLocaleString()+' rows';
  sessionStorage.setItem('dash-filters',JSON.stringify({{cf:Object.fromEntries(Object.entries(_CF).map(([k,v])=>[k,v instanceof Set?Array.from(v):v])),nf:_NF}}));
  renderAll(d);
}}

function pilClick(btn){{
  btn.classList.toggle('active');
  var ct=btn.closest('.pills');if(!ct)return;
  var col=ct.dataset.col,all=ct.querySelectorAll('.pill'),act=ct.querySelectorAll('.pill.active');
  if(act.length===all.length||act.length===0){{delete _CF[col];}}
  else{{_CF[col]=new Set(Array.from(act).map(p=>p.dataset.val));}}
  applyF();
}}

function ddToggle(btn){{
  var m=btn.nextElementSibling;m.classList.toggle('hid');
  document.querySelectorAll('.ddmenu').forEach(function(x){{if(x!==m)x.classList.add('hid');}});
}}

function ddChange(col){{
  var ct=document.querySelector('.ddw[data-col="'+CSS.escape(col)+'"]');if(!ct)return;
  var cbs=ct.querySelectorAll('input[data-val]'),chk=Array.from(cbs).filter(c=>c.checked);
  var btn=ct.querySelector('.ddbtn');
  if(chk.length===cbs.length||chk.length===0){{delete _CF[col];if(btn)btn.textContent='All \u25be';}}
  else{{_CF[col]=new Set(chk.map(c=>c.dataset.val));if(btn)btn.textContent=chk.length+' selected \u25be';}}
  applyF();
}}

function ddAll(col,val){{
  var ct=document.querySelector('.ddw[data-col="'+CSS.escape(col)+'"]');if(!ct)return;
  ct.querySelectorAll('input[data-val]').forEach(function(cb){{cb.checked=val;}});
  ddChange(col);
}}

function ddSrch(inp,col){{
  var q=inp.value.toLowerCase(),ct=document.querySelector('.ddw[data-col="'+CSS.escape(col)+'"]');if(!ct)return;
  ct.querySelectorAll('.optlbl').forEach(function(el){{el.style.display=el.textContent.toLowerCase().includes(q)?'':'none';}});
}}

function numCh(col,bound,val){{
  if(!_NF[col])_NF[col]={{min:null,max:null}};
  _NF[col][bound]=val===''?null:+val;
  applyF();
}}

function clearAll(){{
  _CF={{}};_NF={{}};
  document.querySelectorAll('.pill').forEach(p=>p.classList.add('active'));
  document.querySelectorAll('.ddw input[data-val]').forEach(cb=>{{cb.checked=true;}});
  document.querySelectorAll('.ddbtn').forEach(btn=>{{btn.textContent='All \u25be';}});
  document.querySelectorAll('.ninp').forEach(inp=>{{inp.value='';}});
  applyF();
}}

function exportCSV(){{
  var d=getFilt();if(!d.length)return;
  var cols=Object.keys(d[0]);
  var lines=[cols.map(c=>'"'+c.replace(/"/g,'""')+'"').join(',')];
  d.forEach(function(row){{
    lines.push(cols.map(function(c){{
      var v=row[c];if(v===null||v===undefined)return'';
      var s=String(v);return(s.includes(',')||s.includes('"')||s.includes('\\n'))?'"'+s.replace(/"/g,'""')+'"':s;
    }}).join(','));
  }});
  var b=new Blob([lines.join('\\n')],{{type:'text/csv'}});
  var u=URL.createObjectURL(b);var a=document.createElement('a');a.href=u;a.download='export.csv';a.click();URL.revokeObjectURL(u);
}}

document.addEventListener('click',function(e){{
  const btn=e.target.closest('[data-expand]');
  if(btn)expand(btn.dataset.expand,btn.dataset.expandTitle||'');
}});

function expand(id,ttl){{
  const src=document.getElementById(id);if(!src||!src.data)return;
  document.getElementById('mttl').textContent=ttl;
  document.getElementById('modal').classList.add('open');
  Plotly.newPlot('mdiv',src.data,Object.assign({{}},src.layout,{{height:null,autosize:true}}),{{responsive:true}});
}}
function closeM(){{document.getElementById('modal').classList.remove('open');Plotly.purge('mdiv');}}
document.getElementById('modal').addEventListener('click',function(e){{if(e.target===this)closeM();}});
document.addEventListener('keydown',function(e){{if(e.key==='Escape')closeM();}});
document.addEventListener('click',function(e){{if(!e.target.closest('.ddw'))document.querySelectorAll('.ddmenu').forEach(m=>m.classList.add('hid'));}});

{_RENDERER_JS}

// --- rows table: sortable and paged, over the same filtered rows ---------
// Rendered from getFilt() so the table and the charts can never disagree about
// what is being shown. Paged in the browser because the rows are already in the
// page: truncating at write time would shrink nothing and lose the answer.
var _SORT={{col:null,dir:1}}, _PAGE=0;
function renderTable(rows){{
  var tbl=document.getElementById('dash-table'); if(!tbl) return;
  var size=+tbl.getAttribute('data-page-size')||25;
  var cols=[].map.call(tbl.querySelectorAll('thead th'),function(th){{return th.getAttribute('data-col');}});
  var data=rows.slice();
  if(_SORT.col!==null){{
    var c=_SORT.col, d=_SORT.dir;
    data.sort(function(a,b){{
      var x=a[c], y=b[c], nx=+x, ny=+y;
      if(!isNaN(nx)&&!isNaN(ny)) return (nx-ny)*d;
      return String(x??'').localeCompare(String(y??''))*d;
    }});
  }}
  var pages=Math.max(1,Math.ceil(data.length/size));
  if(_PAGE>=pages)_PAGE=pages-1; if(_PAGE<0)_PAGE=0;
  var slice=data.slice(_PAGE*size,(_PAGE+1)*size);
  var body=tbl.querySelector('tbody'); body.innerHTML='';
  slice.forEach(function(row){{
    var tr=document.createElement('tr');
    cols.forEach(function(c){{var td=document.createElement('td');td.textContent=String(row[c]??'');tr.appendChild(td);}});
    body.appendChild(tr);
  }});
  var cnt=document.getElementById('tbl-count');
  // The same honesty the responses carry: how many of how many, never one
  // number that could be either.
  if(cnt)cnt.textContent=slice.length+' of '+data.length+' row'+(data.length===1?'':'s');
  var pg=document.getElementById('tbl-page'); if(pg)pg.textContent=(_PAGE+1)+' / '+pages;
}}
(function(){{
  var tbl=document.getElementById('dash-table'); if(!tbl) return;
  tbl.querySelectorAll('thead th').forEach(function(th){{
    function toggle(){{
      var c=th.getAttribute('data-col');
      _SORT.dir=(_SORT.col===c)?-_SORT.dir:1; _SORT.col=c; _PAGE=0;
      tbl.querySelectorAll('thead th').forEach(function(o){{o.setAttribute('aria-sort','none');o.querySelector('.sort-ind').textContent='';}});
      th.setAttribute('aria-sort',_SORT.dir>0?'ascending':'descending');
      th.querySelector('.sort-ind').textContent=_SORT.dir>0?' \u25b2':' \u25bc';
      renderTable(getFilt());
    }}
    th.addEventListener('click',toggle);
    th.addEventListener('keydown',function(e){{if(e.key==='Enter'||e.key===' '){{e.preventDefault();toggle();}}}});
  }});
  var prev=document.getElementById('tbl-prev'), next=document.getElementById('tbl-next');
  if(prev)prev.addEventListener('click',function(){{_PAGE--;renderTable(getFilt());}});
  if(next)next.addEventListener('click',function(){{_PAGE++;renderTable(getFilt());}});
}})();

// --- tabs: show and hide the cards, never re-plot them -------------------
// A tab switch that re-rendered every chart would make the cheapest
// interaction on the page the most expensive one.
(function(){{
  var btns=[].slice.call(document.querySelectorAll('.tab-btn'));
  if(!btns.length) return;
  function show(btn){{
    var keep=(btn.getAttribute('data-cards')||'').split(',').filter(Boolean);
    btns.forEach(function(b){{b.setAttribute('aria-selected',b===btn?'true':'false');}});
    document.querySelectorAll('.cgrid > *').forEach(function(card){{
      var id=(card.querySelector('[id]')||{{}}).id||card.id||'';
      card.style.display=(!keep.length||keep.indexOf(id)>=0)?'':'none';
    }});
  }}
  btns.forEach(function(b){{b.addEventListener('click',function(){{show(b);}});}});
  show(btns[0]);
}})();

applyF();
</script>"""


def customize_dashboard(
    dashboard_path: str,
    changes: dict,
    output_path: str = "",
    open_after: bool = True,
    return_content: bool = False,
) -> dict:
    """Rebuild an existing dashboard with part of its spec changed.

    The review's phrasing was "customization = small JSON edit, not full
    rebuild". This is the edit half: `generate_dashboard` embeds the spec it
    built from, so changing one panel means reading that document, applying the
    change, and regenerating -- rather than describing the change in prose to a
    tool that only auto-detects, which is what a caller had to do before.

    `changes` replaces top-level keys and merges `interactions`. Replace rather
    than deep-merge for lists, because "here are the three panels I want" and
    "add these three panels" are different requests, and a merge that guesses
    between them will eventually guess wrong.

    The source file is the one the dashboard names in its own provenance block,
    so a caller does not have to remember it.
    """
    progress: list = []
    try:
        page = resolve_path(dashboard_path)
        if not page.exists():
            return {
                "success": False,
                "op": "customize_dashboard",
                "error": f"Dashboard not found: {page.name}",
                "hint": "Pass the output_path that generate_dashboard returned.",
                "progress": [fail("File not found", page.name)],
                "token_estimate": 30,
            }
        html = page.read_text(encoding="utf-8", errors="replace")
        base = read_spec(html)
        if not base:
            return {
                "success": False,
                "op": "customize_dashboard",
                "error": f"{page.name} carries no spec to customize",
                "hint": (
                    "Only dashboards written by generate_dashboard embed one. Regenerate it "
                    "with generate_dashboard(file_path) and customize that."
                ),
                "progress": [fail("No embedded spec", page.name)],
                "token_estimate": 40,
            }
        header = read_provenance(html)
        # The recorded absolute path first, the provenance name second. The
        # second is a fallback for pages written before the path was recorded,
        # and only works when the data sits beside the page.
        recorded = str(base.get("_source_path") or "")
        source = recorded or (header.get("source") or "")
        if not source:
            return {
                "success": False,
                "op": "customize_dashboard",
                "error": f"{page.name} does not name the file it was built from",
                "hint": "Call generate_dashboard(file_path, spec=...) directly instead.",
                "progress": [fail("No source in provenance", page.name)],
                "token_estimate": 40,
            }
        # The dashboard records only the file's name, so it is looked for beside
        # the page. A dashboard and its data living apart is a real case, and
        # saying which name could not be found beats a bare "file not found".
        data_path = resolve_path(source) if Path(source).is_absolute() else page.parent / source
        if not Path(data_path).exists():
            return {
                "success": False,
                "op": "customize_dashboard",
                "error": f"source data {source!r} no longer exists",
                "hint": (
                    f"{page.name} was built from {source}. Restore it, or call "
                    "generate_dashboard(file_path, spec=...) with the data's current path."
                ),
                "progress": [fail("Source data missing", source)],
                "token_estimate": 40,
            }

        try:
            merged = merge_spec(base, changes or {})
        except SpecError as exc:
            return {
                "success": False,
                "op": "customize_dashboard",
                "error": str(exc),
                "hint": f"Changeable keys: {', '.join(SPEC_KEYS)}.",
                "progress": [fail("Invalid change", str(exc))],
                "token_estimate": 50,
            }

        progress.append(
            info("Customizing dashboard", f"{page.name} — {', '.join(sorted(changes or {})) or 'no change'}")
        )
        result = generate_dashboard(
            str(data_path),
            output_path=output_path or str(page),
            theme=merged.get("theme", "device"),
            open_after=open_after,
            return_content=return_content,
            spec=merged,
        )
        if result.get("success"):
            result["op"] = "customize_dashboard"
            result["changed_keys"] = sorted(changes or {})
            result["previous_spec"] = base
            result["progress"] = progress + list(result.get("progress", []))
        return result
    except Exception as exc:
        logger.exception("customize_dashboard error")
        return {
            "success": False,
            "op": "customize_dashboard",
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Pass the dashboard's output_path and a dict of changes."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


def _dash_table(embed_df, page_size: int) -> str:
    """A sortable, paged view of the rows the charts were drawn from.

    The review asked for "sortable paged table" among the components a real
    dashboard has. The point is not the widget: every chart on this page is an
    aggregate, and the question a reader reaches next is almost always "which
    rows are those" -- which previously meant leaving the dashboard and opening
    the CSV.

    It renders from the same embedded rows the charts use, so the table and the
    charts cannot disagree, and it re-renders under the filter bar like
    everything else. Paged in the browser rather than truncated at write time:
    the rows are already in the page, so cutting them would shrink nothing and
    lose the answer.
    """
    import html as _html

    cols = [str(c) for c in embed_df.columns]
    heads = "".join(
        f'<th data-col="{_html.escape(c)}" role="columnheader" tabindex="0" '
        f'aria-sort="none">{_html.escape(c)}<span class="sort-ind"></span></th>'
        for c in cols
    )
    size = max(5, min(int(page_size), 200))
    return (
        '<div class="sec-hdr">Rows</div>'
        '<div class="card tbl-card">'
        '<div class="tbl-bar">'
        f'<span class="tbl-count" id="tbl-count"></span>'
        '<span class="tbl-pager">'
        '<button type="button" id="tbl-prev" aria-label="Previous page">&#8592;</button>'
        '<span id="tbl-page"></span>'
        '<button type="button" id="tbl-next" aria-label="Next page">&#8594;</button>'
        "</span></div>"
        f'<div class="tbl-scroll"><table id="dash-table" data-page-size="{size}">'
        f"<thead><tr>{heads}</tr></thead><tbody></tbody></table></div>"
        "</div>"
    )


# ---------------------------------------------------------------------------
# multi-source tabs
# ---------------------------------------------------------------------------

# Rows embedded per extra source. These tabs are reference views -- "which rows
# are the charged-off ones", "which rows got flagged" -- not the interactive
# surface, and 38,576 rows of each would put four copies of the dataset in one
# page. The section header states the two numbers, so a truncated view is never
# mistaken for a complete one.
SOURCE_ROW_CAP = 2000

_SOURCE_CSS = """<style>
.src-tabs{display:flex;gap:6px;flex-wrap:wrap;margin:10px 0 4px}
.src-tab-btn{font:inherit;font-size:13px;padding:6px 14px;border-radius:8px;cursor:pointer;
 border:1px solid var(--bd,rgba(127,127,127,.35));background:transparent;color:inherit}
.src-tab-btn[aria-selected="true"]{background:rgba(88,166,255,.16);border-color:#58a6ff;font-weight:600}
.src-sec[hidden]{display:none}
.src-meta{font-size:12px;opacity:.75;margin:2px 0 10px}
.src-kpis{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:10px}
.src-kpi{border:1px solid var(--bd,rgba(127,127,127,.28));border-radius:10px;padding:8px 14px;min-width:120px}
.src-kpi b{display:block;font-size:19px;line-height:1.3}
.src-kpi span{font-size:11px;opacity:.7}
</style>"""


def _source_summary(df, primary_columns: list[str], primary_rows: int) -> dict:
    """The facts about an extra source that a reader needs before its table.

    `share_of_primary` is only computed when the column sets match, because
    "5,333 of 38,576 rows" is a meaningful sentence about a subset of the same
    table and a meaningless one about a different table that happens to be
    smaller.
    """
    cols = [str(c) for c in df.columns]
    same_schema = set(cols) == set(primary_columns)
    out: dict = {
        "rows": len(df),
        "columns": len(cols),
        "same_schema_as_primary": same_schema,
    }
    if same_schema and primary_rows > 0:
        out["share_of_primary_pct"] = round(len(df) / primary_rows * 100, 2)
    return out


def _dash_source_section(idx: int, name: str, df, summary: dict, page_size: int) -> str:
    """One extra dataset as a tab: what it is, its exact totals, and its rows.

    The KPI numbers here are computed from **all** of the source's rows before
    any capping, so a capped table never drags the totals down with it. The
    charts on the primary tab are client-side and cannot make that promise;
    these are server-side, and saying which is which is the difference between
    a second dataset and a second dataset you can trust.
    """
    numeric = [c for c in df.columns if is_numeric_col(df[c])]
    kpis = []
    for col in numeric[:6]:
        agg = infer_agg(col, df[col])
        try:
            value = float(getattr(df[col], agg)())
        except Exception:
            continue
        kpis.append(
            f'<div class="src-kpi"><b>{_html_esc.escape(_compact_num(value))}</b>'
            f"<span>{_html_esc.escape(f'{agg_label(agg)} {col}')}</span></div>"
        )

    shown = df.head(SOURCE_ROW_CAP)
    meta = f"{summary['rows']:,} row(s) &times; {summary['columns']} column(s)"
    if summary.get("share_of_primary_pct") is not None:
        meta += f" &mdash; {summary['share_of_primary_pct']}% of the primary dataset, same schema"
    if len(shown) < len(df):
        meta += f". Table below shows the first {len(shown):,}; totals above are from all {summary['rows']:,}."

    cols = [str(c) for c in shown.columns]
    heads = "".join(
        f'<th data-col="{_html_esc.escape(c)}" role="columnheader">{_html_esc.escape(c)}</th>' for c in cols
    )
    body_rows = []
    for _, row in shown.iterrows():
        cells = "".join(f"<td>{_html_esc.escape('' if pd.isna(v) else str(v))}</td>" for v in row.to_list())
        body_rows.append(f"<tr>{cells}</tr>")

    size = max(5, min(int(page_size), 200))
    return (
        f'<section class="src-sec" data-src="{idx}" hidden>'
        f'<div class="sec-hdr">{_html_esc.escape(name)}</div>'
        f'<div class="src-meta">{meta}</div>'
        f'<div class="src-kpis">{"".join(kpis)}</div>'
        '<div class="card tbl-card"><div class="tbl-bar">'
        f'<span class="tbl-count" data-src-count="{idx}"></span>'
        '<span class="tbl-pager">'
        f'<button type="button" data-src-prev="{idx}" aria-label="Previous page">&#8592;</button>'
        f'<span data-src-page="{idx}"></span>'
        f'<button type="button" data-src-next="{idx}" aria-label="Next page">&#8594;</button>'
        "</span></div>"
        f'<div class="tbl-scroll"><table class="src-table" data-src-table="{idx}" data-page-size="{size}">'
        f"<thead><tr>{heads}</tr></thead><tbody>{''.join(body_rows)}</tbody></table></div>"
        "</div></section>"
    )


def _dash_source_tabs(names: list[str]) -> str:
    """The tab strip that switches whole sections, primary first."""
    buttons = []
    for i, name in enumerate(names):
        active = " aria-selected='true'" if i == 0 else " aria-selected='false'"
        buttons.append(
            f'<button type="button" role="tab" class="src-tab-btn" data-src-tab="{i}"{active}>'
            f"{_html_esc.escape(name)}</button>"
        )
    return _SOURCE_CSS + '<div class="src-tabs" role="tablist">' + "".join(buttons) + "</div>"


def _dash_source_js() -> str:
    """Section switching and per-source paging.

    Deliberately separate from the chart-card tab script: that one shows and
    hides cards inside the primary section, this one swaps whole sections, and
    a single script trying to be both would have to guess which a click meant.
    The rows are already in the DOM, so paging hides and shows `<tr>`s rather
    than re-rendering -- the same reason the card tabs do not re-plot.
    """
    return """<script>
(function(){
  var tabs=[].slice.call(document.querySelectorAll('.src-tab-btn'));
  if(!tabs.length) return;
  var secs=[].slice.call(document.querySelectorAll('.src-sec'));
  var pages={};
  function rows(i){
    var t=document.querySelector('[data-src-table="'+i+'"]');
    return t?[].slice.call(t.tBodies[0].rows):[];
  }
  function size(i){
    var t=document.querySelector('[data-src-table="'+i+'"]');
    return t?Math.max(5,parseInt(t.getAttribute('data-page-size'),10)||25):25;
  }
  function draw(i){
    var rs=rows(i), n=size(i), total=rs.length;
    var pageCount=Math.max(1,Math.ceil(total/n));
    var p=Math.min(Math.max(pages[i]||0,0),pageCount-1); pages[i]=p;
    rs.forEach(function(r,k){ r.hidden = (k<p*n || k>=(p+1)*n); });
    var c=document.querySelector('[data-src-count="'+i+'"]');
    if(c)c.textContent=total?((p*n+1)+'-'+Math.min((p+1)*n,total)+' of '+total.toLocaleString()+' rows'):'no rows';
    var pg=document.querySelector('[data-src-page="'+i+'"]');
    if(pg)pg.textContent=(p+1)+' / '+pageCount;
  }
  document.querySelectorAll('[data-src-prev]').forEach(function(b){
    var i=b.getAttribute('data-src-prev');
    b.addEventListener('click',function(){pages[i]=(pages[i]||0)-1;draw(i);});
  });
  document.querySelectorAll('[data-src-next]').forEach(function(b){
    var i=b.getAttribute('data-src-next');
    b.addEventListener('click',function(){pages[i]=(pages[i]||0)+1;draw(i);});
  });
  function show(i){
    tabs.forEach(function(t){t.setAttribute('aria-selected',t.getAttribute('data-src-tab')===i?'true':'false');});
    secs.forEach(function(s){s.hidden = s.getAttribute('data-src')!==i;});
    if(i!=='0')draw(i);
    // Plotly sizes to a container that had no width while hidden.
    if(window.Plotly&&i==='0')setTimeout(function(){window.dispatchEvent(new Event('resize'));},0);
  }
  tabs.forEach(function(t){t.addEventListener('click',function(){show(t.getAttribute('data-src-tab'));});});
  secs.forEach(function(s){ if(s.getAttribute('data-src')!=='0') draw(s.getAttribute('data-src')); });
  show('0');
})();
</script>"""


def _dash_tabs(tabs: list[dict], chart_specs: list[dict]) -> str:
    """Group the chart cards into named tabs, without moving them.

    The cards are already in the DOM and already wired to the filter bar, so
    tabs show and hide rather than re-render: a tab switch that re-plotted every
    chart would make the cheapest interaction on the page the most expensive.

    A tab naming a slot that does not exist was refused at validation, so
    anything here addresses a real card.
    """
    import html as _html

    ids = [s["id"] for s in chart_specs]
    buttons, panels = [], []
    for i, tab in enumerate(tabs):
        name = _html.escape(str(tab.get("name", f"Tab {i + 1}")))
        slots = [s for s in (tab.get("slots") or []) if isinstance(s, int) and 0 <= s < len(ids)]
        members = ",".join(ids[s] for s in slots)
        active = " aria-selected='true'" if i == 0 else ""
        buttons.append(
            f'<button type="button" role="tab" class="tab-btn" data-tab="{i}" '
            f'data-cards="{members}"{active}>{name}</button>'
        )
        panels.append(f'<span class="tab-meta" data-tab="{i}" data-cards="{members}"></span>')
    return '<div class="tabs" role="tablist">' + "".join(buttons) + "</div>" + "".join(panels)
