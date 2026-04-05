"""generate_dashboard sub-module. No MCP imports."""
from __future__ import annotations

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
    _token_estimate,
    _read_csv,
    _open_file,
    _find_geo_cols,
    _detect_location_mode,
    css_vars,
    device_mode_js,
    VIEWPORT_META,
    fail,
    info,
    ok,
    warn,
    infer_agg,
    agg_label,
    parse_agg_overrides,
)
from shared.file_utils import resolve_path

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
    theme: str = "dark",
    dry_run: bool = False,
    open_after: bool = True,
) -> dict:
    """Generate interactive HTML dashboard with auto-detected charts. Opens HTML."""
    progress = []
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
        dashboard_title = title if title else path.stem

        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        datetime_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
        cat_cols = [
            c for c in df.columns
            if c not in numeric_cols and c not in datetime_cols
            and df[c].nunique() <= 100
        ]

        col_agg: dict[str, str] = {nc: infer_agg(nc, df[nc]) for nc in numeric_cols}
        col_agg.update(parse_agg_overrides(agg_overrides))

        _d_geo_lat, _d_geo_lon, _d_geo_loc = _find_geo_cols(df)
        _d_geo_loc_mode = _detect_location_mode(df, _d_geo_loc) if _d_geo_loc else ""

        detected: list[str] = []
        if numeric_cols and cat_cols:
            detected.append("bar")
        if datetime_cols and numeric_cols:
            detected.append("time_series")
        if len(numeric_cols) >= 2:
            detected.append("scatter")
        if cat_cols:
            detected.append("pie")
        if _d_geo_lat and _d_geo_lon:
            detected.append("geo_scatter")
        if _d_geo_loc and numeric_cols:
            detected.append("geo_choropleth")
        charts = chart_types if chart_types else detected

        if dry_run:
            progress.append(info("Dry run — no file written", path.name))
            result: dict = {
                "success": True,
                "dry_run": True,
                "op": "generate_dashboard",
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
                embed_clean[c] = (
                    pd.to_datetime(embed_clean[c], errors="coerce")
                    .dt.strftime("%Y-%m-%d")
                    .fillna("")
                )
        raw_json = embed_clean.to_json(orient="records", date_format="iso")

        sparklines = _build_sparklines(df, numeric_cols)
        filter_controls = _build_filter_controls(df, cat_cols)
        num_ranges = _build_num_ranges(df, numeric_cols)

        null_pct = float(df.isnull().mean().mean() * 100)
        dup_pct = float(df.duplicated().sum() / max(len(df), 1) * 100)
        quality = max(0, round(100 - null_pct * 2 - dup_pct * 0.5))
        qual_clr = "var(--green)" if quality >= 80 else "var(--orange)" if quality >= 60 else "var(--red)"

        _css = css_vars(theme)
        if theme == "dark":
            bg, font_c, grid_c = "#161b22", "#c9d1d9", "rgba(255,255,255,0.07)"
            geo_land_c, geo_ocean_c, geo_coast_c = "#1a2332", "#0d1117", "#3d4f60"
        elif theme == "light":
            bg, font_c, grid_c = "#ffffff", "#1f2328", "rgba(0,0,0,0.07)"
            geo_land_c, geo_ocean_c, geo_coast_c = "#e8ede6", "#c8ddef", "#aabbc8"
        else:
            bg, font_c, grid_c = "#f6f8fa", "#1f2328", "rgba(0,0,0,0.07)"
            geo_land_c, geo_ocean_c, geo_coast_c = "#e8ede6", "#c8ddef", "#aabbc8"

        h: list[str] = []
        h.append(_dash_head(_css, dashboard_title))
        h.append(_dash_header(dashboard_title, embed_df, was_sampled))
        h.append(_dash_filterbar(filter_controls, num_ranges))
        h.append(_dash_kpi_row(df, numeric_cols, sparklines, quality, qual_clr, col_agg))

        spec: list[dict] = []
        h.append('<div class="sec-hdr">Charts</div><div class="cgrid">')
        _build_chart_cards(h, spec, charts, cat_cols, numeric_cols, datetime_cols,
                           _d_geo_lat, _d_geo_lon, _d_geo_loc, _d_geo_loc_mode, col_agg)
        h.append("</div>")
        h.append(_dash_modal())

        COLORS = "['#58a6ff','#3fb950','#f0883e','#f85149','#bc8cff','#79c0ff','#7ee787','#ffa657','#ff7b72','#d2a8ff','#a5d6ff','#aff5b4','#ffd6a5','#ffabab','#e0b0ff']"
        PCFG = "{responsive:true,displayModeBar:true,scrollZoom:true}"

        def _lyt(h_px: int, extra: str = "") -> str:
            return (
                f"{{paper_bgcolor:'{bg}',plot_bgcolor:'{bg}',"
                f"font:{{color:'{font_c}',size:12}},"
                f"height:{h_px},margin:{{l:55,r:20,t:10,b:65}},"
                f"xaxis:{{gridcolor:'{grid_c}',tickangle:-38}},"
                f"yaxis:{{gridcolor:'{grid_c}'}}{extra}}}"
            )

        rfns = _build_render_functions(spec, bg, font_c, grid_c, geo_land_c, geo_ocean_c, geo_coast_c, numeric_cols, COLORS, PCFG, _lyt, col_agg)
        kpi_upd = "\n".join(
            f"  (function(){{var s={_js_kpi_expr(nc, col_agg.get(nc, 'sum'))};"
            f"var el=document.getElementById('kv-{_safe(nc)}');"
            f"if(el)el.textContent=s>=1e6?(s/1e6).toFixed(1)+'M':s>=1e3?(s/1e3).toFixed(1)+'K':Math.round(s).toLocaleString();}})();"
            for nc in numeric_cols[:7]
        )
        render_calls = "\n".join(
            "  try{rf_" + s['id'] + "(d);}catch(_e){console.warn('chart " + s['id'] + "',_e);}"
            for s in spec
        )
        rfns_str = "\n\n".join(rfns)

        h.append(_dash_js(raw_json, kpi_upd, rfns_str, render_calls))

        if theme == "device":
            h.append(device_mode_js())
        h.append("</body></html>")

        html_content = "\n".join(h)

        if output_path:
            out = Path(output_path)
        else:
            out = path.parent / f"{path.stem}_dashboard.html"

        out.write_text(html_content, encoding="utf-8")
        size_kb = round(out.stat().st_size / 1024)

        if open_after:
            _open_file(out)

        if was_sampled:
            progress.append(warn("Large dataset sampled", f"{EMBED_LIMIT:,} of {len(df):,} rows embedded"))
        progress.append(ok("Dashboard saved", f"{out.name} ({size_kb:,} KB)"))

        result = {
            "success": True,
            "op": "generate_dashboard",
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
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("generate_dashboard error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path is absolute and the file is a valid CSV.",
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
            controls.append({"col": cc, "values": uniq[:50], "style": "pills" if len(uniq) <= 10 else "dropdown"})
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


def _dash_head(_css, dashboard_title):
    return f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8">
{VIEWPORT_META}
<title>{dashboard_title} — Dashboard</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
{_css}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text);overflow-x:hidden}}
::-webkit-scrollbar{{width:6px}}::-webkit-scrollbar-thumb{{background:var(--border);border-radius:3px}}
header{{background:var(--surface);border-bottom:1px solid var(--border);padding:14px 28px;display:flex;flex-wrap:wrap;gap:10px;align-items:center}}
header h1{{color:var(--accent);font-size:20px;font-weight:700;flex:1 1 auto}}
.row-ctr{{color:var(--text-muted);font-size:12px}}
.btn{{background:var(--bg);border:1px solid var(--border);color:var(--text-muted);border-radius:6px;padding:5px 12px;font-size:12px;cursor:pointer}}
.btn:hover{{border-color:var(--accent);color:var(--accent)}}
.btn-p{{background:var(--accent);color:#fff;border-color:var(--accent)}}
.btn-p:hover{{opacity:0.88;color:#fff}}
.filter-bar{{background:var(--surface);border-bottom:1px solid var(--border);padding:12px 28px;display:flex;flex-wrap:wrap;gap:16px;align-items:flex-end}}
.fgrp{{display:flex;flex-direction:column;gap:5px}}
.flbl{{font-size:11px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.04em;font-weight:600}}
.pills{{display:flex;flex-wrap:wrap;gap:4px}}
.pill{{background:var(--bg);border:1px solid var(--border);color:var(--text-muted);border-radius:100px;padding:3px 11px;font-size:12px;cursor:pointer;transition:.12s}}
.pill.active{{background:var(--accent);border-color:var(--accent);color:#fff}}
.pill:hover:not(.active){{border-color:var(--accent);color:var(--text)}}
.ddw{{position:relative}}
.ddbtn{{background:var(--bg);border:1px solid var(--border);color:var(--text);border-radius:6px;padding:5px 10px;font-size:12px;cursor:pointer;min-width:140px;text-align:left;white-space:nowrap}}
.ddmenu{{position:absolute;top:calc(100% + 4px);left:0;z-index:200;background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px;min-width:200px;max-height:280px;overflow-y:auto;box-shadow:0 8px 28px rgba(0,0,0,.35)}}
.ddmenu.hid{{display:none}}
.ddsrch{{width:100%;background:var(--bg);border:1px solid var(--border);color:var(--text);border-radius:4px;padding:4px 8px;font-size:12px;margin-bottom:6px;outline:none}}
.ddacts{{display:flex;gap:6px;margin-bottom:8px}}
.ddacts .btn{{padding:2px 8px;font-size:11px}}
.optlbl{{display:flex;align-items:center;gap:7px;padding:3px 4px;border-radius:4px;font-size:12px;cursor:pointer;user-select:none}}
.optlbl:hover{{background:var(--bg)}}
.optlbl input{{accent-color:var(--accent)}}
.nrng{{display:flex;gap:6px;align-items:center}}
.ninp{{width:88px;background:var(--bg);border:1px solid var(--border);color:var(--text);border-radius:6px;padding:4px 8px;font-size:12px;outline:none}}
.ninp:focus{{border-color:var(--accent)}}
.nsep{{color:var(--text-muted);font-size:13px}}
.kpi-row{{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:12px;padding:18px 28px}}
.kpi-card{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:14px 16px}}
.kpi-val{{font-size:22px;font-weight:700;color:var(--accent);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.kpi-lbl{{font-size:10px;color:var(--text-muted);margin-top:2px;text-transform:uppercase;letter-spacing:.04em;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.kpi-trend{{font-size:12px;margin-top:3px;font-weight:600}}
.kpi-spark{{height:34px;margin-top:6px;pointer-events:none}}
.trend-up{{color:var(--green)}}.trend-down{{color:var(--red)}}.trend-flat{{color:var(--text-muted)}}
.sec-hdr{{padding:16px 28px 4px;font-size:11px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.06em;font-weight:600}}
.cgrid{{padding:0 28px 28px;display:grid;grid-template-columns:repeat(auto-fill,minmax(480px,1fr));gap:14px}}
.cc{{background:var(--surface);border:1px solid var(--border);border-radius:10px;overflow:hidden}}
.cc-hdr{{display:flex;align-items:center;padding:10px 14px;border-bottom:1px solid var(--border)}}
.cc-hdr h3{{font-size:12px;font-weight:500;color:var(--text);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.exp{{background:none;border:none;color:var(--text-muted);cursor:pointer;font-size:15px;line-height:1;padding:0 2px;flex-shrink:0}}
.exp:hover{{color:var(--accent)}}
.cc-body{{padding:4px}}
.full{{grid-column:1/-1}}
.modal{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.72);z-index:1000;align-items:center;justify-content:center}}
.modal.open{{display:flex}}
.mbox{{background:var(--surface);border:1px solid var(--border);border-radius:12px;width:92vw;max-width:1200px;height:88vh;display:flex;flex-direction:column}}
.mhdr{{display:flex;align-items:center;padding:14px 18px;border-bottom:1px solid var(--border)}}
.mhdr h3{{flex:1;font-size:14px;color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.mclose{{background:none;border:none;color:var(--text-muted);font-size:20px;cursor:pointer;line-height:1;flex-shrink:0}}
.mclose:hover{{color:var(--red)}}
#mdiv{{flex:1;min-height:0}}
@media(max-width:1100px){{.cgrid{{grid-template-columns:1fr}}}}
@media(max-width:600px){{header,.filter-bar,.kpi-row,.cgrid,.sec-hdr{{padding-left:14px;padding-right:14px}}.kpi-row{{grid-template-columns:repeat(2,1fr)}}}}
</style></head><body>"""


def _dash_header(dashboard_title, embed_df, was_sampled):
    sampled_note = " (sampled)" if was_sampled else ""
    return f"""<header>
  <h1>{dashboard_title}</h1>
  <span class="row-ctr" id="row-ctr">{len(embed_df):,} of {len(embed_df):,} rows{sampled_note}</span>
  <button class="btn" onclick="clearAll()">Clear Filters</button>
  <button class="btn btn-p" onclick="exportCSV()">&#x2193; Export CSV</button>
</header>"""


def _dash_filterbar(filter_controls, num_ranges):
    if not filter_controls and not num_ranges:
        return ""
    h = ['<div class="filter-bar">']
    for fc in filter_controls:
        col, vals, style = fc["col"], fc["values"], fc["style"]
        lbl = col.replace('"', "&quot;")
        col_js = col.replace("\\", "\\\\").replace("'", "\\'")
        h.append(f'<div class="fgrp"><div class="flbl">{lbl}</div>')
        if style == "pills":
            h.append(f'<div class="pills" data-col="{lbl}">')
            for v in vals:
                ve = str(v).replace('"', "&quot;").replace("'", "&#39;")
                h.append(f'<button class="pill active" data-val="{ve}" onclick="pilClick(this)">{ve}</button>')
            h.append("</div>")
        else:
            opts = "".join(
                f'<label class="optlbl"><input type="checkbox" data-val="{str(v).replace(chr(34), "&quot;")}" checked onchange="ddChange(\'{col_js}\')">{str(v).replace("<", "&lt;")}</label>'
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
        mn_s = f'{nr["min"]:,.0f}' if abs(nr["min"]) >= 1 else f'{nr["min"]:.3f}'
        mx_s = f'{nr["max"]:,.0f}' if abs(nr["max"]) >= 1 else f'{nr["max"]:.3f}'
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


def _dash_kpi_row(df, numeric_cols, sparklines, quality, qual_clr, col_agg):
    h = ['<div class="kpi-row">']
    h.append(f'<div class="kpi-card"><div class="kpi-val" style="color:{qual_clr}">{quality}</div><div class="kpi-lbl">Quality Score</div></div>')
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
            f'<div class="kpi-lbl">{lbl}</div>'
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
    te = ttl.replace("'", "&#39;").replace('"', "&quot;")
    h.append(
        f'<div class="{cls}">'
        f'<div class="cc-hdr"><h3>{ttl}</h3>'
        f'<button class="exp" onclick="expand(\'{cid}\',\'{te}\')">&#x2922;</button>'
        f'</div><div class="cc-body">'
        f'<div id="{cid}" style="height:{height}px"></div>'
        f"</div></div>"
    )


def _build_chart_cards(h, spec, charts, cat_cols, numeric_cols, datetime_cols,
                       _d_geo_lat, _d_geo_lon, _d_geo_loc, _d_geo_loc_mode, col_agg):
    if "bar" in charts and cat_cols and numeric_cols:
        for cc in cat_cols[:3]:
            for nc in numeric_cols[:2]:
                agg = col_agg.get(nc, "sum")
                cid = f"bar_{_safe(cc)}_{_safe(nc)}"
                _card(h, cid, f"{agg_label(agg)} {nc} by {cc}", False, 340)
                spec.append({"id": cid, "type": "bar", "cc": cc, "nc": nc, "agg": agg})
    if "pie" in charts and cat_cols:
        for cc in cat_cols[:3]:
            cid = f"pie_{_safe(cc)}"
            _card(h, cid, f"{cc} Distribution", False, 340)
            spec.append({"id": cid, "type": "pie", "cc": cc})
    if "scatter" in charts and len(numeric_cols) >= 2:
        pairs = [(numeric_cols[i], numeric_cols[j]) for i in range(min(2, len(numeric_cols))) for j in range(i + 1, min(i + 3, len(numeric_cols)))]
        for nc1, nc2 in pairs:
            cid = f"scat_{_safe(nc1)}_{_safe(nc2)}"
            _card(h, cid, f"{nc1} vs {nc2}", False, 340)
            spec.append({"id": cid, "type": "scatter", "nc1": nc1, "nc2": nc2})
    if len(cat_cols) >= 2 and numeric_cols:
        cc1, cc2, nc = cat_cols[0], cat_cols[1], numeric_cols[0]
        agg = col_agg.get(nc, "sum")
        cid = f"grp_{_safe(cc1)}_{_safe(cc2)}"
        _card(h, cid, f"{agg_label(agg)} {nc} by {cc1}, grouped by {cc2}", True, 380)
        spec.append({"id": cid, "type": "grouped_bar", "cc1": cc1, "cc2": cc2, "nc": nc, "agg": agg})
    if len(numeric_cols) >= 2 and cat_cols:
        nc1, nc2, cc = numeric_cols[0], numeric_cols[1], cat_cols[0]
        cid = f"cscat_{_safe(nc1)}_{_safe(nc2)}"
        _card(h, cid, f"{nc1} vs {nc2} by {cc}", True, 380)
        spec.append({"id": cid, "type": "cscat", "nc1": nc1, "nc2": nc2, "cc": cc})
    if numeric_cols and cat_cols:
        nc, cc = numeric_cols[0], cat_cols[0]
        cid = f"box_{_safe(nc)}_{_safe(cc)}"
        _card(h, cid, f"{nc} distribution by {cc}", True, 380)
        spec.append({"id": cid, "type": "box", "nc": nc, "cc": cc})
    if len(numeric_cols) >= 2:
        _card(h, "corr_hm", "Correlation Matrix", True, 480)
        spec.append({"id": "corr_hm", "type": "corr"})
    if len(cat_cols) >= 2 and numeric_cols:
        cc1, cc2, nc = cat_cols[0], cat_cols[1], numeric_cols[0]
        agg = col_agg.get(nc, "sum")
        cid = f"aghm_{_safe(cc1)}_{_safe(cc2)}"
        _card(h, cid, f"{agg_label(agg)} {nc}: {cc1} \u00d7 {cc2}", True, 460)
        spec.append({"id": cid, "type": "agg_hm", "cc1": cc1, "cc2": cc2, "nc": nc, "agg": agg})
    if "time_series" in charts and datetime_cols and numeric_cols:
        for dc in datetime_cols[:2]:
            for nc in numeric_cols[:2]:
                agg = col_agg.get(nc, "sum")
                cid = f"ts_{_safe(dc)}_{_safe(nc)}"
                _card(h, cid, f"{agg_label(agg)} {nc} Over Time", True, 380)
                spec.append({"id": cid, "type": "ts", "dc": dc, "nc": nc, "agg": agg})
    for nc in numeric_cols[:6]:
        cid = f"dist_{_safe(nc)}"
        _card(h, cid, f"{nc} Distribution", False, 320)
        spec.append({"id": cid, "type": "dist", "nc": nc})
    if "geo_scatter" in charts and _d_geo_lat and _d_geo_lon:
        _val_c = numeric_cols[0] if numeric_cols else ""
        _cc_c = cat_cols[0] if cat_cols else ""
        cid = f"geo_scat_{_safe(_d_geo_lat)}"
        _card(h, cid, "Geographic Distribution (Scatter)", True, 500)
        spec.append({"id": cid, "type": "geo_scatter", "lat": _d_geo_lat, "lon": _d_geo_lon, "val": _val_c, "cc": _cc_c})
    if "geo_choropleth" in charts and _d_geo_loc and numeric_cols:
        nc = numeric_cols[0]
        agg = col_agg.get(nc, "sum")
        cid = f"geo_choro_{_safe(_d_geo_loc)}"
        _card(h, cid, f"{agg_label(agg)} {nc} by {_d_geo_loc} (Choropleth)", True, 500)
        spec.append({"id": cid, "type": "geo_choro", "loc": _d_geo_loc, "nc": nc, "mode": _d_geo_loc_mode or "country names", "agg": agg})


def _dash_modal():
    return ('<div id="modal" class="modal"><div class="mbox">'
            '<div class="mhdr"><h3 id="mttl"></h3>'
            '<button class="mclose" onclick="closeM()">&#x2715;</button></div>'
            '<div id="mdiv"></div></div></div>')


def _build_render_functions(spec, bg, font_c, grid_c, geo_land_c, geo_ocean_c, geo_coast_c, numeric_cols, COLORS, PCFG, _lyt, col_agg):
    rfns: list[str] = []
    for s in spec:
        cid, t = s["id"], s["type"]
        if t == "bar":
            cc, nc = s["cc"], s["nc"]
            agg = s.get("agg", "sum")
            agg_blk = _js_agg_block(agg, f"String(r['{cc}']??'')", f"+r['{nc}']", 25)
            rfns.append(f"function rf_{cid}(d){{\n  {agg_blk}  var fmt=function(v){{return v>=1e6?(v/1e6).toFixed(1)+'M':v>=1e3?(v/1e3).toFixed(1)+'K':Math.round(v).toString();}};\n  Plotly.react('{cid}',[{{x:e.map(i=>i[0]),y:e.map(i=>i[1]),type:'bar',marker:{{color:'#58a6ff',opacity:0.85}},text:e.map(i=>fmt(i[1])),textposition:'outside'}}],{_lyt(340)},{PCFG});\n}}")
        elif t == "pie":
            cc = s["cc"]
            rfns.append(f"function rf_{cid}(d){{\n  var c={{}};\n  d.forEach(function(r){{var k=String(r['{cc}']??'');c[k]=(c[k]||0)+1;}});\n  var e=Object.entries(c).sort((x,y)=>y[1]-x[1]).slice(0,15);\n  var layout={{paper_bgcolor:'{bg}',plot_bgcolor:'{bg}',font:{{color:'{font_c}',size:12}},height:340,margin:{{l:20,r:20,t:10,b:20}},showlegend:true,legend:{{orientation:'h',y:-0.14}}}};\n  Plotly.react('{cid}',[{{values:e.map(i=>i[1]),labels:e.map(i=>i[0]),type:'pie',hole:0.38,marker:{{colors:{COLORS}}},textinfo:'label+percent',textfont:{{size:11}},pull:e.map((_,i)=>i===0?0.04:0)}}],layout,{{responsive:true,displayModeBar:true,scrollZoom:true}});\n}}")
        elif t == "scatter":
            nc1, nc2 = s["nc1"], s["nc2"]
            rfns.append(f"function rf_{cid}(d){{\n  var xs=[],ys=[];\n  d.forEach(function(r){{var x=+r['{nc1}'],y=+r['{nc2}'];if(!isNaN(x)&&!isNaN(y)){{xs.push(x);ys.push(y);}}}});\n  var traces=[{{x:xs,y:ys,type:'scatter',mode:'markers',marker:{{color:'#58a6ff',opacity:0.5,size:5}},name:'data'}}];\n  if(xs.length>1){{\n    var n=xs.length,sx=xs.reduce((a,b)=>a+b,0),sy=ys.reduce((a,b)=>a+b,0),sxy=0,sxx=0,syy=0;\n    for(var i=0;i<n;i++){{sxy+=xs[i]*ys[i];sxx+=xs[i]*xs[i];syy+=ys[i]*ys[i];}}\n    var sl=(n*sxy-sx*sy)/(n*sxx-sx*sx||1),ic=(sy-sl*sx)/n;\n    var r=(n*sxy-sx*sy)/Math.sqrt(((n*sxx-sx*sx)*(n*syy-sy*sy))||1);\n    var xmn=Math.min(...xs),xmx=Math.max(...xs);\n    traces.push({{x:[xmn,xmx],y:[sl*xmn+ic,sl*xmx+ic],type:'scatter',mode:'lines',line:{{color:'#f0883e',width:2,dash:'dash'}},name:'r='+r.toFixed(2)}});\n  }}\n  var layout=Object.assign({{}},{_lyt(340)},{{showlegend:true,legend:{{x:0,y:1.1,orientation:'h'}},xaxis:{{title:'{nc1}',gridcolor:'{grid_c}'}},yaxis:{{title:'{nc2}',gridcolor:'{grid_c}'}}}});\n  Plotly.react('{cid}',traces,layout,{PCFG});\n}}")
        elif t == "grouped_bar":
            cc1, cc2, nc = s["cc1"], s["cc2"], s["nc"]
            agg = s.get("agg", "sum")
            if agg == "mean":
                inner_acc = "if(!isNaN(v)){if(!a[k2])a[k2]={};if(!a[k2][k1])a[k2][k1]={s:0,n:0};a[k2][k1].s+=v;a[k2][k1].n++;}"
                val_expr = "a[k]&&a[k][g]?a[k][g].s/a[k][g].n:0"
            elif agg == "max":
                inner_acc = "if(!isNaN(v)){if(!a[k2])a[k2]={};a[k2][k1]=(a[k2][k1]===undefined||v>a[k2][k1])?v:a[k2][k1];}"
                val_expr = "(a[k]&&a[k][g]!==undefined)?a[k][g]:0"
            elif agg == "min":
                inner_acc = "if(!isNaN(v)){if(!a[k2])a[k2]={};a[k2][k1]=(a[k2][k1]===undefined||v<a[k2][k1])?v:a[k2][k1];}"
                val_expr = "(a[k]&&a[k][g]!==undefined)?a[k][g]:0"
            else:
                inner_acc = "if(!isNaN(v)){if(!a[k2])a[k2]={};a[k2][k1]=(a[k2][k1]||0)+v;}"
                val_expr = "(a[k]&&a[k][g])||0"
            rfns.append(f"function rf_{cid}(d){{\n  var a={{}};\n  d.forEach(function(r){{var k1=String(r['{cc1}']??''),k2=String(r['{cc2}']??''),v=+r['{nc}'];{inner_acc}}});\n  var gs=Array.from(new Set(d.map(r=>String(r['{cc1}']??'')))).slice(0,20);\n  var ks=Object.keys(a).slice(0,10),C={COLORS};\n  var traces=ks.map(function(k,i){{return{{x:gs,y:gs.map(g=>{val_expr}),type:'bar',name:k,marker:{{color:C[i%15],opacity:0.85}}}};}});\n  var layout=Object.assign({{}},{_lyt(380)},{{barmode:'group',showlegend:true,legend:{{orientation:'h',y:-0.3}}}});\n  Plotly.react('{cid}',traces,layout,{PCFG});\n}}")
        elif t == "cscat":
            nc1, nc2, cc = s["nc1"], s["nc2"], s["cc"]
            rfns.append(f"function rf_{cid}(d){{\n  var g={{}};\n  d.forEach(function(r){{var x=+r['{nc1}'],y=+r['{nc2}'],k=String(r['{cc}']??'');if(!isNaN(x)&&!isNaN(y)){{if(!g[k])g[k]={{x:[],y:[]}};g[k].x.push(x);g[k].y.push(y);}}}});\n  var ks=Object.keys(g).slice(0,15),C={COLORS};\n  var traces=ks.map(function(k,i){{return{{x:g[k].x,y:g[k].y,type:'scatter',mode:'markers',name:k,marker:{{color:C[i%15],opacity:0.6,size:5}}}};}});\n  var layout=Object.assign({{}},{_lyt(380)},{{showlegend:true,legend:{{orientation:'h',y:-0.3}},xaxis:{{title:'{nc1}',gridcolor:'{grid_c}'}},yaxis:{{title:'{nc2}',gridcolor:'{grid_c}'}}}});\n  Plotly.react('{cid}',traces,layout,{PCFG});\n}}")
        elif t == "box":
            nc, cc = s["nc"], s["cc"]
            rfns.append(f"function rf_{cid}(d){{\n  var g={{}};\n  d.forEach(function(r){{var v=+r['{nc}'],k=String(r['{cc}']??'');if(!isNaN(v)){{if(!g[k])g[k]=[];g[k].push(v);}}}});\n  var ks=Object.keys(g).sort().slice(0,20),C={COLORS};\n  var traces=ks.map(function(k,i){{return{{y:g[k],type:'box',name:k,marker:{{color:C[i%15],size:3}},boxpoints:'outliers'}};}});\n  var layout=Object.assign({{}},{_lyt(380)},{{showlegend:false,yaxis:{{title:'{nc}',gridcolor:'{grid_c}'}}}});\n  Plotly.react('{cid}',traces,layout,{PCFG});\n}}")
        elif t == "corr":
            nc_list = _json.dumps(numeric_cols[:15])
            rfns.append(f"function rf_{cid}(d){{\n  var cols={nc_list},n=d.length;if(n<2)return;\n  var z=cols.map(function(r){{return cols.map(function(c){{\n    var xv=d.map(row=>+row[r]),yv=d.map(row=>+row[c]),pr=[];\n    for(var i=0;i<n;i++)if(!isNaN(xv[i])&&!isNaN(yv[i]))pr.push([xv[i],yv[i]]);\n    if(pr.length<2)return 0;\n    var mx=pr.reduce((s,p)=>s+p[0],0)/pr.length,my=pr.reduce((s,p)=>s+p[1],0)/pr.length;\n    var num=0,dx=0,dy=0;pr.forEach(p=>{{num+=(p[0]-mx)*(p[1]-my);dx+=(p[0]-mx)**2;dy+=(p[1]-my)**2;}});\n    return dx&&dy?num/Math.sqrt(dx*dy):0;\n  }});}});\n  var layout={{paper_bgcolor:'{bg}',plot_bgcolor:'{bg}',font:{{color:'{font_c}',size:11}},height:480,margin:{{l:120,r:20,t:10,b:120}}}};\n  Plotly.react('{cid}',[{{z:z,x:cols,y:cols,type:'heatmap',colorscale:'RdBu_r',zmid:0,zmin:-1,zmax:1,text:z.map(r=>r.map(v=>v.toFixed(2))),texttemplate:'%{{text}}',textfont:{{size:10}}}}],layout,{{responsive:true,displayModeBar:true,scrollZoom:true}});\n}}")
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
            rfns.append(f"function rf_{cid}(d){{\n  var a={{}},Rs=new Set(),Cs=new Set();\n  d.forEach(function(r){{var r1=String(r['{cc1}']??''),c1=String(r['{cc2}']??''),v=+r['{nc}'];if(!isNaN(v)){{{inner_acc}}}}});\n  var rl=Array.from(Rs).sort().slice(0,30),cl=Array.from(Cs).sort().slice(0,30);\n  var z=rl.map(function(r){{return cl.map(function(c){{return {z_val};}});}});\n  var fmt=function(v){{return v>=1e6?(v/1e6).toFixed(1)+'M':v>=1e3?(v/1e3).toFixed(1)+'K':Math.round(v).toString();}};\n  var layout={{paper_bgcolor:'{bg}',plot_bgcolor:'{bg}',font:{{color:'{font_c}',size:11}},height:460,margin:{{l:130,r:20,t:10,b:130}}}};\n  Plotly.react('{cid}',[{{z:z,x:cl,y:rl,type:'heatmap',colorscale:'YlOrRd',text:z.map(r=>r.map(fmt)),texttemplate:'%{{text}}',textfont:{{size:9}}}}],layout,{{responsive:true,displayModeBar:true,scrollZoom:true}});\n}}")
        elif t == "ts":
            dc, nc = s["dc"], s["nc"]
            agg = s.get("agg", "sum")
            acc, vals_expr = _js_ts_block(dc, nc, agg)
            rfns.append(f"function rf_{cid}(d){{\n  {acc}  var dates=Object.keys(bm).sort(),vals={vals_expr};\n  var ma=vals.map(function(_,i){{if(i<2)return null;return(vals[i]+vals[i-1]+vals[i-2])/3;}});\n  var traces=[{{x:dates,y:vals,type:'scatter',mode:'lines+markers',name:'{nc}',line:{{color:'#3fb950',width:2}},marker:{{size:4}}}},{{x:dates.slice(2),y:ma.slice(2),type:'scatter',mode:'lines',name:'3-period MA',line:{{color:'#f0883e',width:2,dash:'dot'}}}}];\n  var layout=Object.assign({{}},{_lyt(380)},{{showlegend:true,legend:{{x:0,y:1.1,orientation:'h'}},xaxis:{{title:'Date',gridcolor:'{grid_c}'}},yaxis:{{title:'{nc}',gridcolor:'{grid_c}'}}}});\n  Plotly.react('{cid}',traces,layout,{PCFG});\n}}")
        elif t == "dist":
            nc = s["nc"]
            rfns.append(f"function rf_{cid}(d){{\n  var vals=d.map(r=>+r['{nc}']).filter(v=>!isNaN(v));if(!vals.length)return;\n  var layout={{paper_bgcolor:'{bg}',plot_bgcolor:'{bg}',font:{{color:'{font_c}',size:12}},height:320,margin:{{l:50,r:20,t:10,b:30}},grid:{{rows:1,columns:2,pattern:'independent'}},xaxis:{{gridcolor:'{grid_c}'}},yaxis:{{title:'Count',gridcolor:'{grid_c}'}},xaxis2:{{gridcolor:'{grid_c}'}},yaxis2:{{gridcolor:'{grid_c}'}}}};\n  Plotly.react('{cid}',[{{x:vals,type:'histogram',nbinsx:50,marker:{{color:'#58a6ff',opacity:0.75}},xaxis:'x',yaxis:'y',name:'hist'}},{{y:vals,type:'box',marker:{{color:'#f0883e',size:3}},xaxis:'x2',yaxis:'y2',boxpoints:'outliers',name:'box'}}],layout,{{responsive:true,displayModeBar:true,scrollZoom:true}});\n}}")
        elif t == "geo_scatter":
            lat_c, lon_c = s["lat"], s["lon"]
            val_c = s.get("val", "")
            txt_expr = f"'{val_c}: '+String(r['{val_c}'])" if val_c else "lt.toFixed(4)+', '+ln.toFixed(4)"
            rfns.append(f"function rf_{cid}(d){{\n  var lts=[],lns=[],txts=[];\n  d.forEach(function(r){{var lt=+r['{lat_c}'],ln=+r['{lon_c}'];if(!isNaN(lt)&&!isNaN(ln)){{lts.push(lt);lns.push(ln);txts.push({txt_expr});}}}});\n  if(!lts.length)return;\n  var layout={{paper_bgcolor:'{bg}',plot_bgcolor:'{bg}',geo:{{showland:true,landcolor:'{geo_land_c}',showocean:true,oceancolor:'{geo_ocean_c}',showcoastlines:true,coastlinecolor:'{geo_coast_c}',showcountries:true,countrycolor:'{geo_coast_c}',showframe:false,bgcolor:'{bg}',projection:{{type:'natural earth'}}}},font:{{color:'{font_c}',size:12}},height:500,margin:{{l:0,r:0,t:10,b:0}}}};\n  Plotly.react('{cid}',[{{type:'scattergeo',lat:lts,lon:lns,mode:'markers',marker:{{color:'#58a6ff',size:6,opacity:0.75,line:{{color:'rgba(255,255,255,0.25)',width:0.5}}}},text:txts,hovertemplate:'%{{text}}<extra></extra>'}}],layout,{PCFG});\n}}")
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
            rfns.append(f"function rf_{cid}(d){{\n  {choro_acc}\n  if(!locs.length)return;\n  var layout={{paper_bgcolor:'{bg}',plot_bgcolor:'{bg}',geo:{{showland:true,landcolor:'{geo_land_c}',showocean:true,oceancolor:'{geo_ocean_c}',showcoastlines:true,coastlinecolor:'{geo_coast_c}',showcountries:true,countrycolor:'{geo_coast_c}',showframe:false,bgcolor:'{bg}'}},font:{{color:'{font_c}',size:12}},height:500,margin:{{l:0,r:0,t:10,b:0}},coloraxis:{{colorscale:'YlOrRd',showscale:true,colorbar:{{thickness:14,len:0.7,tickfont:{{color:'{font_c}',size:10}}}}}}}};\n  Plotly.react('{cid}',[{{type:'choropleth',locations:locs,z:vals,locationmode:'{mode}',coloraxis:'coloraxis',hovertemplate:'%{{location}}: %{{z:.2f}}<extra></extra>'}}],layout,{PCFG});\n}}")
    return rfns


def _dash_js(raw_json, kpi_upd, rfns_str, render_calls):
    return f"""<script>
var _RAW={raw_json};
var _TOTAL=_RAW.length;
var _CF={{}};
var _NF={{}};

function getFilt(){{
  return _RAW.filter(function(row){{
    for(var c in _CF){{var s=_CF[c];if(s&&s.size>0&&!s.has(String(row[c]??'')))return false;}}
    for(var c in _NF){{var r=_NF[c],v=+row[c];if(!isNaN(v)){{if(r.min!==null&&v<r.min)return false;if(r.max!==null&&v>r.max)return false;}}}}
    return true;
  }});
}}

function applyF(){{
  var d=getFilt();
  document.getElementById('row-ctr').textContent=d.length.toLocaleString()+' of '+_TOTAL.toLocaleString()+' rows';
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

function expand(id,ttl){{
  var src=document.getElementById(id);if(!src||!src.data)return;
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

applyF();
</script>"""
