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

from shared.dashboard_spec import CHART_KINDS, SPEC_KEYS, SpecError
from shared.dashboard_spec import merge as merge_spec
from shared.dashboard_spec import resolve as resolve_spec
from shared.dashboard_spec import validate as validate_spec
from shared.data_alerts import alerts_for_frame, alerts_html, quality_score
from shared.file_utils import embed_content, error_text, hint_for_error, no_rows_error, resolve_path
from shared.geo_names import unrecognised_locations
from shared.provenance import frame_hash, provenance, provenance_script, read_provenance, read_spec, spec_script
from shared.table_payload import records_js

logger = logging.getLogger(__name__)


def _safe(s: str) -> str:
    return _re.sub(r"[^a-zA-Z0-9]", "_", str(s))


# ---------------------------------------------------------------------------
# JS aggregation code-generators
# ---------------------------------------------------------------------------


def _js_agg_block(agg: str, key_expr: str, val_expr: str, top_n: int = 25) -> str:
    """Return JS that builds sorted entries `e` using the given agg function."""
    if agg == "mean":
        return (
            f"var a={{}},cnt={{}};\n"
            f"  d.forEach(function(r){{var k={key_expr},v={val_expr};"
            f"if(!isNaN(v)){{a[k]=(a[k]||0)+v;cnt[k]=(cnt[k]||0)+1;}}}});\n"
            f"  var e=Object.entries(a)"
            f".map(function(p){{return[p[0],p[1]/(cnt[p[0]]||1)];}})"
            f".sort((x,y)=>y[1]-x[1]).slice(0,{top_n});\n"
        )
    if agg == "max":
        return (
            f"var a={{}};\n"
            f"  d.forEach(function(r){{var k={key_expr},v={val_expr};"
            f"if(!isNaN(v))a[k]=(a[k]===undefined||v>a[k])?v:a[k];}});\n"
            f"  var e=Object.entries(a).sort((x,y)=>y[1]-x[1]).slice(0,{top_n});\n"
        )
    if agg == "min":
        return (
            f"var a={{}};\n"
            f"  d.forEach(function(r){{var k={key_expr},v={val_expr};"
            f"if(!isNaN(v))a[k]=(a[k]===undefined||v<a[k])?v:a[k];}});\n"
            f"  var e=Object.entries(a).sort((x,y)=>x[1]-y[1]).slice(0,{top_n});\n"
        )
    # sum (default)
    return (
        f"var a={{}};\n"
        f"  d.forEach(function(r){{var k={key_expr},v={val_expr};"
        f"if(!isNaN(v))a[k]=(a[k]||0)+v;}});\n"
        f"  var e=Object.entries(a).sort((x,y)=>y[1]-x[1]).slice(0,{top_n});\n"
    )


def _js_kpi_expr(nc: str, agg: str) -> str:
    """Return a JS expression (no semicolon) that computes the KPI scalar."""
    v = f"d.map(function(r){{return+r['{nc}'];}}).filter(function(v){{return!isNaN(v);}})"
    if agg == "mean":
        return f"(function(){{var v={v};return v.length?v.reduce(function(a,b){{return a+b;}},0)/v.length:0;}})()"
    if agg == "max":
        return f"(function(){{var v={v};return v.length?Math.max.apply(null,v):0;}})()"
    if agg == "min":
        return f"(function(){{var v={v};return v.length?Math.min.apply(null,v):0;}})()"
    # sum
    return f"{v}.reduce(function(a,b){{return a+b;}},0)"


def _js_ts_block(dc: str, nc: str, agg: str) -> tuple[str, str]:
    """Return (accumulation_js, vals_expr) for a time-series render function."""
    if agg == "mean":
        acc = (
            f"var bm={{}};\n"
            f"  d.forEach(function(r){{var dt=r['{dc}'],v=+r['{nc}'];"
            f"if(dt&&!isNaN(v)){{var ym=String(dt).substring(0,7);"
            f"if(!bm[ym])bm[ym]={{s:0,n:0}};bm[ym].s+=v;bm[ym].n++;}}}});\n"
        )
        vals = "dates.map(function(d){return bm[d]?bm[d].s/bm[d].n:0;})"
    elif agg == "max":
        acc = (
            f"var bm={{}};\n"
            f"  d.forEach(function(r){{var dt=r['{dc}'],v=+r['{nc}'];"
            f"if(dt&&!isNaN(v)){{var ym=String(dt).substring(0,7);"
            f"bm[ym]=(bm[ym]===undefined||v>bm[ym])?v:bm[ym];}}}});\n"
        )
        vals = "dates.map(function(d){return bm[d]!==undefined?bm[d]:0;})"
    elif agg == "min":
        acc = (
            f"var bm={{}};\n"
            f"  d.forEach(function(r){{var dt=r['{dc}'],v=+r['{nc}'];"
            f"if(dt&&!isNaN(v)){{var ym=String(dt).substring(0,7);"
            f"bm[ym]=(bm[ym]===undefined||v<bm[ym])?v:bm[ym];}}}});\n"
        )
        vals = "dates.map(function(d){return bm[d]!==undefined?bm[d]:0;})"
    else:  # sum
        acc = (
            f"var bm={{}};\n"
            f"  d.forEach(function(r){{var dt=r['{dc}'],v=+r['{nc}'];"
            f"if(dt&&!isNaN(v)){{var ym=String(dt).substring(0,7);bm[ym]=(bm[ym]||0)+v;}}}});\n"
        )
        vals = "dates.map(function(d){return bm[d]||0;})"
    return acc, vals


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
) -> dict:
    """Generate interactive HTML dashboard with auto-detected charts. Opens HTML.

    `spec` overrides any part of the auto-detection:
    `{title, theme, layout:[{slot, chart, cols, agg}], kpis, filters, tabs,
    interactions}`. An absent key means "decide for me", so passing nothing
    returns exactly what this returned before the parameter existed.

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

        numeric_cols = [c for c in df.columns if is_numeric_col(df[c])]
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
        col_agg.update(parse_agg_overrides(agg_overrides))

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
        detected_layout = [{"slot": i, "chart": name, "cols": {}, "agg": ""} for i, name in enumerate(charts)]
        resolved = resolve_spec(
            spec,
            title=dashboard_title,
            theme=theme,
            detected_layout=detected_layout,
            kpi_columns=numeric_cols[:8],
            filter_columns=cat_cols[:8],
        )
        # The build document records where the data came from. The provenance
        # block records only the file NAME -- deliberately, since that block
        # travels with the page -- and that left customize_dashboard unable to
        # find the source whenever MCP_OUTPUT_DIR is not the directory the CSV
        # lives in, which is the deployed layout. Reproduced before fixing.
        resolved["_source_path"] = str(path)
        if spec and spec.get("layout") is not None:
            charts = [p["chart"] for p in resolved["layout"]]
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
                    "kpi_columns": numeric_cols[:8],
                    "filter_columns": cat_cols[:8],
                },
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        EMBED_LIMIT = 500_000
        was_sampled = len(df) > EMBED_LIMIT
        embed_df = df.sample(EMBED_LIMIT, random_state=42) if was_sampled else df.copy()
        embed_clean = embed_df.copy()
        for c in datetime_cols:
            if c in embed_clean.columns:
                embed_clean[c] = pd.to_datetime(embed_clean[c], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
        # Columnar + dictionary-encoded; the page rebuilds the same array of
        # row objects, so everything downstream of _RAW is unchanged.
        raw_json = records_js(embed_clean)

        sparklines = _build_sparklines(df, numeric_cols)
        filter_controls = _build_filter_controls(df, cat_cols)
        num_ranges = _build_num_ranges(df, numeric_cols)

        # Computed before the KPI row so the score can see them: the headline
        # number and the panel underneath it must describe the same dataset.
        alerts = alerts_for_frame(df, numeric_cols, cat_cols)

        null_pct = float(df.isnull().mean().mean() * 100)
        dup_pct = float(df.duplicated().sum() / max(len(df), 1) * 100)
        quality = _quality_score(null_pct, dup_pct, alerts)
        qual_clr = "var(--green)" if quality >= 80 else "var(--orange)" if quality >= 60 else "var(--red)"

        _css = css_vars(theme)
        bg, font_c, _ = theme_plot_colors(theme)
        grid_c = "rgba(255,255,255,0.07)" if theme == "dark" else "rgba(0,0,0,0.07)"
        if theme == "dark":
            geo_land_c, geo_ocean_c, geo_coast_c = "#1a2332", "#0d1117", "#3d4f60"
        else:
            geo_land_c, geo_ocean_c, geo_coast_c = "#e8ede6", "#c8ddef", "#aabbc8"

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
        h.append(_dash_header(dashboard_title, embed_df, was_sampled))
        h.append(_dash_filterbar(filter_controls, num_ranges))
        h.append(_dash_kpi_row(df, numeric_cols, sparklines, quality, qual_clr, col_agg))
        # The dashboard is the artifact people actually send to a colleague, and
        # it used to show 26 charts of a dataset without mentioning that two of
        # its columns were constant. Same alert engine the EDA report leads with.
        h.append(_dash_alerts(alerts))

        chart_specs: list[dict] = []
        h.append('<div class="sec-hdr">Charts</div><div class="cgrid">')
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
        h.append(_dash_modal())

        COLORS = "['#58a6ff','#3fb950','#f0883e','#f85149','#bc8cff','#79c0ff','#7ee787','#ffa657','#ff7b72','#d2a8ff','#a5d6ff','#aff5b4','#ffd6a5','#ffabab','#e0b0ff']"
        PCFG = "{responsive:true,displayModeBar:true,scrollZoom:true}"

        def _lyt(_h_px: int = 0, extra: str = "") -> str:
            # Height is intentionally omitted — CSS (.cc-body / .cc-body--tall) controls it
            # autosize:true makes Plotly fill the CSS-sized container div
            return (
                f"{{paper_bgcolor:'{bg}',plot_bgcolor:'{bg}',"
                f"font:{{color:'{font_c}',size:12}},"
                f"autosize:true,margin:{{l:55,r:20,t:10,b:65}},"
                # 'auto' rotates only when labels would collide. The fixed -38
                # tilted "Google Ads" and "Facebook Ads" diagonally across an
                # otherwise empty axis, which is harder to read than level text
                # and bought nothing. (automargin is not set here on purpose --
                # am() applies it to every axis of every layout.)
                f"xaxis:{{gridcolor:'{grid_c}',tickangle:'auto'}},"
                f"yaxis:{{gridcolor:'{grid_c}'}}{extra}}}"
            )

        rfns = _build_render_functions(
            chart_specs,
            bg,
            font_c,
            grid_c,
            geo_land_c,
            geo_ocean_c,
            geo_coast_c,
            numeric_cols,
            COLORS,
            PCFG,
            _lyt,
            col_agg,
        )
        kpi_upd = "\n".join(
            f"  (function(){{var s={_js_kpi_expr(nc, col_agg.get(nc, 'sum'))};"
            f"var el=document.getElementById('kv-{_safe(nc)}');"
            f"if(el)el.textContent=s>=1e6?(s/1e6).toFixed(1)+'M':s>=1e3?(s/1e3).toFixed(1)+'K':Math.round(s).toLocaleString();}})();"
            for nc in numeric_cols[:7]
        )
        render_calls = "\n".join(
            "  try{rf_" + s["id"] + "(d);}catch(_e){console.warn('chart " + s["id"] + "',_e);}" for s in chart_specs
        )
        rfns_str = "\n\n".join(rfns)

        h.append(_dash_js(raw_json, kpi_upd, rfns_str, render_calls))

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
            "kpi_columns": numeric_cols[:7],
            "filter_columns": [fc["col"] for fc in filter_controls],
            "rows_embedded": len(embed_df),
            "rows_total": len(df),
            "was_sampled": was_sampled,
            "report_size_kb": size_kb,
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
    for nc in numeric_cols[:8]:
        n_pts = min(30, len(df))
        step = max(1, len(df) // n_pts)
        sv = df[nc].iloc[::step].head(n_pts).fillna(0).tolist()
        sparklines[nc] = [0 if (isinstance(v, float) and v != v) else v for v in sv]
    return sparklines


def _build_filter_controls(df, cat_cols):
    controls: list[dict] = []
    for cc in cat_cols[:8]:
        uniq = sorted(df[cc].dropna().astype(str).unique().tolist())
        if 1 < len(uniq) <= 50:
            controls.append(
                {
                    "col": cc,
                    "values": uniq[:50],
                    "style": "pills" if len(uniq) <= 10 else "dropdown",
                }
            )
    return controls


def _build_num_ranges(df, numeric_cols):
    ranges: list[dict] = []
    for nc in numeric_cols[:3]:
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


def _dash_header(dashboard_title, embed_df, was_sampled):
    sampled_note = " (sampled)" if was_sampled else ""
    return f"""<header>
  <h1>{dashboard_title}</h1>
  <span class="row-ctr" id="row-ctr">{len(embed_df):,} of {len(embed_df):,} rows{sampled_note}</span>
  <button class="btn" onclick="clearAll()">Clear Filters</button>
  <button class="btn btn-p" onclick="exportCSV()">&#x2193; Export CSV</button>
  <button class="btn btn-print" onclick="window.print()">&#x2399; Print</button>
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
        col_js = col.replace("\\", "\\\\").replace("'", "\\'")
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
        nc_js = nc.replace("\\", "\\\\").replace("'", "\\'")
        mn_s = _compact_num(nr["min"])
        mx_s = _compact_num(nr["max"])
        h.append(
            f'<div class="fgrp"><div class="flbl">{nc}</div>'
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
    for nc in numeric_cols[:7]:
        agg = col_agg.get(nc, "sum")
        arrow, acls = _trend(df, nc)
        sc = _safe(nc)
        sv = sparklines.get(nc, [])
        series = df[nc].dropna()
        if agg == "mean":
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
    if "bar" in charts and cat_cols and numeric_cols:
        for cc in cat_cols[:3]:
            for nc in numeric_cols[:2]:
                agg = col_agg.get(nc, "sum")
                cid = f"bar_{_safe(cc)}_{_safe(nc)}"
                _card(h, cid, f"{agg_label(agg)} {nc} by {cc}", False, 340)
                chart_specs.append({"id": cid, "type": "bar", "cc": cc, "nc": nc, "agg": agg})
    if "pie" in charts and cat_cols:
        for cc in cat_cols[:3]:
            cid = f"pie_{_safe(cc)}"
            _card(h, cid, f"{cc} Distribution", False, 340)
            chart_specs.append({"id": cid, "type": "pie", "cc": cc})
    if "scatter" in charts and len(numeric_cols) >= 2:
        pairs = [
            (numeric_cols[i], numeric_cols[j])
            for i in range(min(2, len(numeric_cols)))
            for j in range(i + 1, min(i + 3, len(numeric_cols)))
        ]
        for nc1, nc2 in pairs:
            cid = f"scat_{_safe(nc1)}_{_safe(nc2)}"
            _card(h, cid, f"{nc1} vs {nc2}", False, 340)
            chart_specs.append({"id": cid, "type": "scatter", "nc1": nc1, "nc2": nc2})
    if len(cat_cols) >= 2 and numeric_cols:
        cc1, cc2, nc = cat_cols[0], cat_cols[1], numeric_cols[0]
        agg = col_agg.get(nc, "sum")
        cid = f"grp_{_safe(cc1)}_{_safe(cc2)}"
        _card(h, cid, f"{agg_label(agg)} {nc} by {cc1}, grouped by {cc2}", True, 380)
        chart_specs.append(
            {
                "id": cid,
                "type": "grouped_bar",
                "cc1": cc1,
                "cc2": cc2,
                "nc": nc,
                "agg": agg,
            }
        )
    if len(numeric_cols) >= 2 and cat_cols:
        nc1, nc2, cc = numeric_cols[0], numeric_cols[1], cat_cols[0]
        cid = f"cscat_{_safe(nc1)}_{_safe(nc2)}"
        _card(h, cid, f"{nc1} vs {nc2} by {cc}", True, 380)
        chart_specs.append({"id": cid, "type": "cscat", "nc1": nc1, "nc2": nc2, "cc": cc})
    if numeric_cols and cat_cols:
        nc, cc = numeric_cols[0], cat_cols[0]
        cid = f"box_{_safe(nc)}_{_safe(cc)}"
        _card(h, cid, f"{nc} distribution by {cc}", True, 380)
        chart_specs.append({"id": cid, "type": "box", "nc": nc, "cc": cc})
    if len(numeric_cols) >= 2:
        _card(h, "corr_hm", "Correlation Matrix", True, 480)
        chart_specs.append({"id": "corr_hm", "type": "corr"})
    if len(cat_cols) >= 2 and numeric_cols:
        cc1, cc2, nc = cat_cols[0], cat_cols[1], numeric_cols[0]
        agg = col_agg.get(nc, "sum")
        cid = f"aghm_{_safe(cc1)}_{_safe(cc2)}"
        _card(h, cid, f"{agg_label(agg)} {nc}: {cc1} \u00d7 {cc2}", True, 460)
        chart_specs.append({"id": cid, "type": "agg_hm", "cc1": cc1, "cc2": cc2, "nc": nc, "agg": agg})
    if "time_series" in charts and datetime_cols and numeric_cols:
        for dc in datetime_cols[:2]:
            for nc in numeric_cols[:2]:
                agg = col_agg.get(nc, "sum")
                cid = f"ts_{_safe(dc)}_{_safe(nc)}"
                _card(h, cid, f"{agg_label(agg)} {nc} Over Time", True, 380)
                chart_specs.append({"id": cid, "type": "ts", "dc": dc, "nc": nc, "agg": agg})
    for nc in numeric_cols[:6]:
        cid = f"dist_{_safe(nc)}"
        _card(h, cid, f"{nc} Distribution", False, 320)
        chart_specs.append({"id": cid, "type": "dist", "nc": nc})
    if "geo_scatter" in charts and _d_geo_lat and _d_geo_lon:
        _val_c = numeric_cols[0] if numeric_cols else ""
        _cc_c = cat_cols[0] if cat_cols else ""
        cid = f"geo_scat_{_safe(_d_geo_lat)}"
        _card(h, cid, "Geographic Distribution (Scatter)", True, 500)
        chart_specs.append(
            {
                "id": cid,
                "type": "geo_scatter",
                "lat": _d_geo_lat,
                "lon": _d_geo_lon,
                "val": _val_c,
                "cc": _cc_c,
            }
        )
    if "geo_choropleth" in charts and _d_geo_loc and numeric_cols:
        nc = numeric_cols[0]
        agg = col_agg.get(nc, "sum")
        cid = f"geo_choro_{_safe(_d_geo_loc)}"
        _card(h, cid, f"{agg_label(agg)} {nc} by {_d_geo_loc} (Choropleth)", True, 500)
        chart_specs.append(
            {
                "id": cid,
                "type": "geo_choro",
                "loc": _d_geo_loc,
                "nc": nc,
                "mode": _d_geo_loc_mode or "country names",
                "agg": agg,
            }
        )


def _dash_modal():
    return (
        '<div id="modal" class="modal"><div class="mbox">'
        '<div class="mhdr"><h3 id="mttl"></h3>'
        '<button class="mclose" onclick="closeM()">&#x2715;</button></div>'
        '<div id="mdiv"></div></div></div>'
    )


def _build_render_functions(
    chart_specs,
    bg,
    font_c,
    grid_c,
    geo_land_c,
    geo_ocean_c,
    geo_coast_c,
    numeric_cols,
    COLORS,
    PCFG,
    _lyt,
    col_agg,
):
    rfns: list[str] = []
    for s in chart_specs:
        cid, t = s["id"], s["type"]
        if t == "bar":
            cc, nc = s["cc"], s["nc"]
            agg = s.get("agg", "sum")
            agg_blk = _js_agg_block(agg, f"String(r['{cc}']??'')", f"+r['{nc}']", 25)
            rfns.append(
                f"function rf_{cid}(d){{\n  {agg_blk}  var fmt=function(v){{return v>=1e6?(v/1e6).toFixed(1)+'M':v>=1e3?(v/1e3).toFixed(1)+'K':Math.round(v).toString();}};\n  Plotly.react('{cid}',[{{x:e.map(i=>i[0]),y:e.map(i=>i[1]),type:'bar',marker:{{color:'#58a6ff',opacity:0.85}},text:e.map(i=>fmt(i[1])),textposition:'outside'}}],{_lyt(340)},{PCFG});\n}}"
            )
        elif t == "pie":
            cc = s["cc"]
            rfns.append(
                f"function rf_{cid}(d){{\n  var c={{}};\n  d.forEach(function(r){{var k=String(r['{cc}']??'');c[k]=(c[k]||0)+1;}});\n  var e=Object.entries(c).sort((x,y)=>y[1]-x[1]).slice(0,15);\n  var layout={{paper_bgcolor:'{bg}',plot_bgcolor:'{bg}',font:{{color:'{font_c}',size:12}},autosize:true,margin:{{l:20,r:20,t:10,b:20}},showlegend:true,legend:{{orientation:'h',y:-0.14}}}};\n  // Past a handful of slices, per-slice labels are drawn outside on\n  // leader lines that overlap each other and spill out of the card, while\n  // repeating names the legend already lists. Keep the percent inside the\n  // slice and let the legend carry the names.\n  var ti=e.length>6?'percent':'label+percent';\n  Plotly.react('{cid}',[{{values:e.map(i=>i[1]),labels:e.map(i=>i[0]),type:'pie',hole:0.38,marker:{{colors:{COLORS}}},textinfo:ti,textposition:'inside',insidetextorientation:'horizontal',textfont:{{size:11}},pull:e.map((_,i)=>i===0?0.04:0)}}],am(layout),{{responsive:true,displayModeBar:true,scrollZoom:true}});\n}}"
            )
        elif t == "scatter":
            nc1, nc2 = s["nc1"], s["nc2"]
            rfns.append(
                f"function rf_{cid}(d){{\n  var xs=[],ys=[];\n  d.forEach(function(r){{var x=+r['{nc1}'],y=+r['{nc2}'];if(!isNaN(x)&&!isNaN(y)){{xs.push(x);ys.push(y);}}}});\n  var traces=[{{x:xs,y:ys,type:'scatter',mode:'markers',marker:{{color:'#58a6ff',opacity:0.5,size:5}},name:'data'}}];\n  if(xs.length>1){{\n    var n=xs.length,sx=xs.reduce((a,b)=>a+b,0),sy=ys.reduce((a,b)=>a+b,0),sxy=0,sxx=0,syy=0;\n    for(var i=0;i<n;i++){{sxy+=xs[i]*ys[i];sxx+=xs[i]*xs[i];syy+=ys[i]*ys[i];}}\n    var sl=(n*sxy-sx*sy)/(n*sxx-sx*sx||1),ic=(sy-sl*sx)/n;\n    var r=(n*sxy-sx*sy)/Math.sqrt(((n*sxx-sx*sx)*(n*syy-sy*sy))||1);\n    var xmn=Math.min(...xs),xmx=Math.max(...xs);\n    traces.push({{x:[xmn,xmx],y:[sl*xmn+ic,sl*xmx+ic],type:'scatter',mode:'lines',line:{{color:'#f0883e',width:2,dash:'dash'}},name:'r='+r.toFixed(2)}});\n  }}\n  var layout=Object.assign({{}},{_lyt(340)},{{showlegend:true,legend:{{x:0,y:1.1,orientation:'h'}},xaxis:{{title:'{nc1}',gridcolor:'{grid_c}'}},yaxis:{{title:'{nc2}',gridcolor:'{grid_c}'}}}});\n  Plotly.react('{cid}',traces,am(layout),{PCFG});\n}}"
            )
        elif t == "grouped_bar":
            cc1, cc2, nc = s["cc1"], s["cc2"], s["nc"]
            agg = s.get("agg", "sum")
            if agg == "mean":
                inner_acc = (
                    "if(!isNaN(v)){if(!a[k2])a[k2]={};if(!a[k2][k1])a[k2][k1]={s:0,n:0};a[k2][k1].s+=v;a[k2][k1].n++;}"
                )
                val_expr = "a[k]&&a[k][g]?a[k][g].s/a[k][g].n:0"
            elif agg == "max":
                inner_acc = (
                    "if(!isNaN(v)){if(!a[k2])a[k2]={};a[k2][k1]=(a[k2][k1]===undefined||v>a[k2][k1])?v:a[k2][k1];}"
                )
                val_expr = "(a[k]&&a[k][g]!==undefined)?a[k][g]:0"
            elif agg == "min":
                inner_acc = (
                    "if(!isNaN(v)){if(!a[k2])a[k2]={};a[k2][k1]=(a[k2][k1]===undefined||v<a[k2][k1])?v:a[k2][k1];}"
                )
                val_expr = "(a[k]&&a[k][g]!==undefined)?a[k][g]:0"
            else:
                inner_acc = "if(!isNaN(v)){if(!a[k2])a[k2]={};a[k2][k1]=(a[k2][k1]||0)+v;}"
                val_expr = "(a[k]&&a[k][g])||0"
            rfns.append(
                f"function rf_{cid}(d){{\n  var a={{}};\n  d.forEach(function(r){{var k1=String(r['{cc1}']??''),k2=String(r['{cc2}']??''),v=+r['{nc}'];{inner_acc}}});\n  var gs=Array.from(new Set(d.map(r=>String(r['{cc1}']??'')))).slice(0,20);\n  var ks=Object.keys(a).slice(0,10),C={COLORS};\n  var traces=ks.map(function(k,i){{return{{x:gs,y:gs.map(g=>{val_expr}),type:'bar',name:k,marker:{{color:C[i%15],opacity:0.85}}}};}});\n  var layout=Object.assign({{}},{_lyt(380)},{{barmode:'group',showlegend:true,legend:{{orientation:'h',x:0,y:1.12}}}});\n  Plotly.react('{cid}',traces,am(layout),{PCFG});\n}}"
            )
        elif t == "cscat":
            nc1, nc2, cc = s["nc1"], s["nc2"], s["cc"]
            rfns.append(
                f"function rf_{cid}(d){{\n  var g={{}};\n  d.forEach(function(r){{var x=+r['{nc1}'],y=+r['{nc2}'],k=String(r['{cc}']??'');if(!isNaN(x)&&!isNaN(y)){{if(!g[k])g[k]={{x:[],y:[]}};g[k].x.push(x);g[k].y.push(y);}}}});\n  var ks=Object.keys(g).slice(0,15),C={COLORS};\n  var traces=ks.map(function(k,i){{return{{x:g[k].x,y:g[k].y,type:'scatter',mode:'markers',name:k,marker:{{color:C[i%15],opacity:0.6,size:5}}}};}});\n  var layout=Object.assign({{}},{_lyt(380)},{{showlegend:true,legend:{{orientation:'h',x:0,y:1.12}},xaxis:{{title:'{nc1}',gridcolor:'{grid_c}'}},yaxis:{{title:'{nc2}',gridcolor:'{grid_c}'}}}});\n  Plotly.react('{cid}',traces,am(layout),{PCFG});\n}}"
            )
        elif t == "box":
            nc, cc = s["nc"], s["cc"]
            rfns.append(
                f"function rf_{cid}(d){{\n  var g={{}};\n  d.forEach(function(r){{var v=+r['{nc}'],k=String(r['{cc}']??'');if(!isNaN(v)){{if(!g[k])g[k]=[];g[k].push(v);}}}});\n  var ks=Object.keys(g).sort().slice(0,20),C={COLORS};\n  var traces=ks.map(function(k,i){{return{{y:g[k],type:'box',name:k,marker:{{color:C[i%15],size:3}},boxpoints:'outliers'}};}});\n  var layout=Object.assign({{}},{_lyt(380)},{{showlegend:false,yaxis:{{title:'{nc}',gridcolor:'{grid_c}'}}}});\n  Plotly.react('{cid}',traces,am(layout),{PCFG});\n}}"
            )
        elif t == "corr":
            nc_list = _json.dumps(numeric_cols[:15])
            rfns.append(
                f"function rf_{cid}(d){{\n  var cols={nc_list},n=d.length;if(n<2)return;\n  var z=cols.map(function(r){{return cols.map(function(c){{\n    var xv=d.map(row=>+row[r]),yv=d.map(row=>+row[c]),pr=[];\n    for(var i=0;i<n;i++)if(!isNaN(xv[i])&&!isNaN(yv[i]))pr.push([xv[i],yv[i]]);\n    if(pr.length<2)return 0;\n    var mx=pr.reduce((s,p)=>s+p[0],0)/pr.length,my=pr.reduce((s,p)=>s+p[1],0)/pr.length;\n    var num=0,dx=0,dy=0;pr.forEach(p=>{{num+=(p[0]-mx)*(p[1]-my);dx+=(p[0]-mx)**2;dy+=(p[1]-my)**2;}});\n    return dx&&dy?num/Math.sqrt(dx*dy):0;\n  }});}});\n  var layout={{paper_bgcolor:'{bg}',plot_bgcolor:'{bg}',font:{{color:'{font_c}',size:11}},autosize:true,margin:{{l:120,r:20,t:10,b:120}}}};\n  Plotly.react('{cid}',[{{z:z,x:cols,y:cols,type:'heatmap',colorscale:'RdBu_r',zmid:0,zmin:-1,zmax:1,text:z.map(r=>r.map(v=>v.toFixed(2))),texttemplate:'%{{text}}',textfont:{{size:10}}}}],am(layout),{{responsive:true,displayModeBar:true,scrollZoom:true}});\n}}"
            )
        elif t == "agg_hm":
            cc1, cc2, nc = s["cc1"], s["cc2"], s["nc"]
            agg = s.get("agg", "sum")
            if agg == "mean":
                inner_acc = "Rs.add(r1);Cs.add(c1);if(!a[r1])a[r1]={};if(!a[r1][c1])a[r1][c1]={s:0,n:0};a[r1][c1].s+=v;a[r1][c1].n++;"
                z_val = "a[r]&&a[r][c]?a[r][c].s/a[r][c].n:0"
            elif agg == "max":
                inner_acc = "Rs.add(r1);Cs.add(c1);if(!a[r1])a[r1]={};a[r1][c1]=(a[r1][c1]===undefined||v>a[r1][c1])?v:a[r1][c1];"
                z_val = "(a[r]&&a[r][c]!==undefined)?a[r][c]:0"
            elif agg == "min":
                inner_acc = "Rs.add(r1);Cs.add(c1);if(!a[r1])a[r1]={};a[r1][c1]=(a[r1][c1]===undefined||v<a[r1][c1])?v:a[r1][c1];"
                z_val = "(a[r]&&a[r][c]!==undefined)?a[r][c]:0"
            else:
                inner_acc = "Rs.add(r1);Cs.add(c1);if(!a[r1])a[r1]={};a[r1][c1]=(a[r1][c1]||0)+v;"
                z_val = "(a[r]&&a[r][c])||0"
            rfns.append(
                f"function rf_{cid}(d){{\n  var a={{}},Rs=new Set(),Cs=new Set();\n  d.forEach(function(r){{var r1=String(r['{cc1}']??''),c1=String(r['{cc2}']??''),v=+r['{nc}'];if(!isNaN(v)){{{inner_acc}}}}});\n  var rl=Array.from(Rs).sort().slice(0,30),cl=Array.from(Cs).sort().slice(0,30);\n  var z=rl.map(function(r){{return cl.map(function(c){{return {z_val};}});}});\n  var fmt=function(v){{return v>=1e6?(v/1e6).toFixed(1)+'M':v>=1e3?(v/1e3).toFixed(1)+'K':Math.round(v).toString();}};\n  var layout={{paper_bgcolor:'{bg}',plot_bgcolor:'{bg}',font:{{color:'{font_c}',size:11}},autosize:true,margin:{{l:130,r:20,t:10,b:130}}}};\n  Plotly.react('{cid}',[{{z:z,x:cl,y:rl,type:'heatmap',colorscale:'YlOrRd',text:z.map(r=>r.map(fmt)),texttemplate:'%{{text}}',textfont:{{size:9}}}}],am(layout),{{responsive:true,displayModeBar:true,scrollZoom:true}});\n}}"
            )
        elif t == "ts":
            dc, nc = s["dc"], s["nc"]
            agg = s.get("agg", "sum")
            acc, vals_expr = _js_ts_block(dc, nc, agg)
            rfns.append(
                f"function rf_{cid}(d){{\n  {acc}  var dates=Object.keys(bm).sort(),vals={vals_expr};\n  var ma=vals.map(function(_,i){{if(i<2)return null;return(vals[i]+vals[i-1]+vals[i-2])/3;}});\n  var traces=[{{x:dates,y:vals,type:'scatter',mode:'lines+markers',name:'{nc}',line:{{color:'#3fb950',width:2}},marker:{{size:4}}}},{{x:dates.slice(2),y:ma.slice(2),type:'scatter',mode:'lines',name:'3-period MA',line:{{color:'#f0883e',width:2,dash:'dot'}}}}];\n  var layout=Object.assign({{}},{_lyt(380)},{{showlegend:true,legend:{{x:0,y:1.1,orientation:'h'}},xaxis:{{title:'Date',gridcolor:'{grid_c}'}},yaxis:{{title:'{nc}',gridcolor:'{grid_c}'}}}});\n  Plotly.react('{cid}',traces,am(layout),{PCFG});\n}}"
            )
        elif t == "dist":
            nc = s["nc"]
            rfns.append(
                f"function rf_{cid}(d){{\n  var vals=d.map(r=>+r['{nc}']).filter(v=>!isNaN(v));if(!vals.length)return;\n  var layout={{paper_bgcolor:'{bg}',plot_bgcolor:'{bg}',font:{{color:'{font_c}',size:12}},autosize:true,margin:{{l:50,r:20,t:10,b:30}},grid:{{rows:1,columns:2,pattern:'independent'}},xaxis:{{gridcolor:'{grid_c}'}},yaxis:{{title:'Count',gridcolor:'{grid_c}'}},xaxis2:{{gridcolor:'{grid_c}'}},yaxis2:{{gridcolor:'{grid_c}'}}}};\n  Plotly.react('{cid}',[{{x:vals,type:'histogram',nbinsx:50,marker:{{color:'#58a6ff',opacity:0.75}},xaxis:'x',yaxis:'y',name:'hist'}},{{y:vals,type:'box',marker:{{color:'#f0883e',size:3}},xaxis:'x2',yaxis:'y2',boxpoints:'outliers',name:'box'}}],am(layout),{{responsive:true,displayModeBar:true,scrollZoom:true}});\n}}"
            )
        elif t == "geo_scatter":
            lat_c, lon_c = s["lat"], s["lon"]
            val_c = s.get("val", "")
            txt_expr = f"'{val_c}: '+String(r['{val_c}'])" if val_c else "lt.toFixed(4)+', '+ln.toFixed(4)"
            rfns.append(
                f"function rf_{cid}(d){{\n  var lts=[],lns=[],txts=[];\n  d.forEach(function(r){{var lt=+r['{lat_c}'],ln=+r['{lon_c}'];if(!isNaN(lt)&&!isNaN(ln)){{lts.push(lt);lns.push(ln);txts.push({txt_expr});}}}});\n  if(!lts.length)return;\n  var layout={{paper_bgcolor:'{bg}',plot_bgcolor:'{bg}',geo:{{showland:true,landcolor:'{geo_land_c}',showocean:true,oceancolor:'{geo_ocean_c}',showcoastlines:true,coastlinecolor:'{geo_coast_c}',showcountries:true,countrycolor:'{geo_coast_c}',showframe:false,bgcolor:'{bg}',projection:{{type:'natural earth'}}}},font:{{color:'{font_c}',size:12}},autosize:true,margin:{{l:0,r:0,t:10,b:0}}}};\n  Plotly.react('{cid}',[{{type:'scattergeo',lat:lts,lon:lns,mode:'markers',marker:{{color:'#58a6ff',size:6,opacity:0.75,line:{{color:'rgba(255,255,255,0.25)',width:0.5}}}},text:txts,hovertemplate:'%{{text}}<extra></extra>'}}],am(layout),{PCFG});\n}}"
            )
        elif t == "geo_choro":
            loc_c, nc, mode = s["loc"], s["nc"], s["mode"]
            agg = s.get("agg", "sum")
            if agg == "mean":
                choro_acc = f"var a={{}},cnt={{}};\n  d.forEach(function(r){{var k=String(r['{loc_c}']??''),v=+r['{nc}'];if(k&&!isNaN(v)){{a[k]=(a[k]||0)+v;cnt[k]=(cnt[k]||0)+1;}}}});\n  var locs=Object.keys(a),vals=locs.map(k=>a[k]/(cnt[k]||1));"
            elif agg == "max":
                choro_acc = f"var a={{}};\n  d.forEach(function(r){{var k=String(r['{loc_c}']??''),v=+r['{nc}'];if(k&&!isNaN(v))a[k]=(a[k]===undefined||v>a[k])?v:a[k];}});\n  var locs=Object.keys(a),vals=locs.map(k=>a[k]);"
            elif agg == "min":
                choro_acc = f"var a={{}};\n  d.forEach(function(r){{var k=String(r['{loc_c}']??''),v=+r['{nc}'];if(k&&!isNaN(v))a[k]=(a[k]===undefined||v<a[k])?v:a[k];}});\n  var locs=Object.keys(a),vals=locs.map(k=>a[k]);"
            else:
                choro_acc = f"var a={{}};\n  d.forEach(function(r){{var k=String(r['{loc_c}']??''),v=+r['{nc}'];if(k&&!isNaN(v))a[k]=(a[k]||0)+v;}});\n  var locs=Object.keys(a),vals=locs.map(k=>a[k]);"
            rfns.append(
                f"function rf_{cid}(d){{\n  {choro_acc}\n  if(!locs.length)return;\n  var layout={{paper_bgcolor:'{bg}',plot_bgcolor:'{bg}',geo:{{showland:true,landcolor:'{geo_land_c}',showocean:true,oceancolor:'{geo_ocean_c}',showcoastlines:true,coastlinecolor:'{geo_coast_c}',showcountries:true,countrycolor:'{geo_coast_c}',showframe:false,bgcolor:'{bg}'}},font:{{color:'{font_c}',size:12}},autosize:true,margin:{{l:0,r:0,t:10,b:0}},coloraxis:{{colorscale:'YlOrRd',showscale:true,colorbar:{{thickness:14,len:0.7,tickfont:{{color:'{font_c}',size:10}}}}}}}};\n  Plotly.react('{cid}',[{{type:'choropleth',locations:locs,z:vals,locationmode:'{mode}',coloraxis:'coloraxis',hovertemplate:'%{{location}}: %{{z:.2f}}<extra></extra>'}}],am(layout),{PCFG});\n}}"
            )
    return rfns


def _close_tag_safe(js: str) -> str:
    """Neutralise the one sequence that can end a <script> block early.

    The HTML parser looks for `</script>` before any JavaScript is parsed, so a
    column name or cell value containing it ends the block and everything after
    becomes markup. In generated code that sequence can only have come from data,
    and inside a string literal `<\\/script` is the identical string.
    """
    return _re.sub(r"</(script)", r"<\\/\1", js, flags=_re.IGNORECASE)


def _dash_js(raw_json, kpi_upd, rfns_str, render_calls):
    kpi_upd = _close_tag_safe(kpi_upd)
    rfns_str = _close_tag_safe(rfns_str)
    render_calls = _close_tag_safe(render_calls)
    return f"""<script>
let _RAW={raw_json};
const _TOTAL=_RAW.length;
let _CF={{}};
let _NF={{}};

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
    for(var c in _NF){{var r=_NF[c],v=+row[c];if(!isNaN(v)){{if(r.min!==null&&v<r.min)return false;if(r.max!==null&&v>r.max)return false;}}}}
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
  var ct=document.querySelector('.ddw[data-col="'+col+'"]');if(!ct)return;
  var cbs=ct.querySelectorAll('input[data-val]'),chk=Array.from(cbs).filter(c=>c.checked);
  var btn=ct.querySelector('.ddbtn');
  if(chk.length===cbs.length||chk.length===0){{delete _CF[col];if(btn)btn.textContent='All \u25be';}}
  else{{_CF[col]=new Set(chk.map(c=>c.dataset.val));if(btn)btn.textContent=chk.length+' selected \u25be';}}
  applyF();
}}

function ddAll(col,val){{
  var ct=document.querySelector('.ddw[data-col="'+col+'"]');if(!ct)return;
  ct.querySelectorAll('input[data-val]').forEach(function(cb){{cb.checked=val;}});
  ddChange(col);
}}

function ddSrch(inp,col){{
  var q=inp.value.toLowerCase(),ct=document.querySelector('.ddw[data-col="'+col+'"]');if(!ct)return;
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

function updKPIs(d){{
{kpi_upd}
}}

{rfns_str}

function renderAll(d){{
  updKPIs(d);
{render_calls}
}}

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
