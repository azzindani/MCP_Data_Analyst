"""
Unified HTML layout engine for all MCP Data Analyst HTML outputs.

Best-practice rules enforced here:
  - No hard-coded px for layout or typography — rem + CSS clamp() only
  - CSS auto-fill grids scale from mobile to ultra-wide with zero JS
  - overflow-wrap / word-break on all user content — no text blowout
  - Plotly charts sized by their CSS container (autosize=true, no height in layout)
  - Viewport meta, scroll-behavior, box-sizing applied globally
  - Mobile-first breakpoints — sidebar collapses at 48 rem (~768 px)
  - Default output target is input file's directory; falls back to ~/Downloads
"""

from __future__ import annotations

from pathlib import Path

from shared.file_utils import resolve_path

# ---------------------------------------------------------------------------
# Output path — input-file-first
# ---------------------------------------------------------------------------


def get_output_path(
    output_path: str,
    input_path: Path | None,
    stem_suffix: str,
    ext: str = "html",
) -> Path:
    """Resolve output path.

    Priority:
      1. Explicit output_path argument if given
      2. Same directory as input file (when input_path is provided)
      3. ~/Downloads/<stem>_<suffix>.<ext>  (pure generation, no input file)
    """
    if output_path:
        return resolve_path(output_path)
    if input_path is not None:
        return input_path.parent / f"{input_path.stem}_{stem_suffix}.{ext}"
    downloads = Path.home() / "Downloads"
    return downloads / f"{stem_suffix}.{ext}"


# ---------------------------------------------------------------------------
# Plotly constants
# ---------------------------------------------------------------------------

_PLOTLYJS_SCRIPT: str = ""


def get_plotlyjs_script() -> str:
    """Return a <script> tag with the full Plotly bundle inlined (cached)."""
    global _PLOTLYJS_SCRIPT
    if not _PLOTLYJS_SCRIPT:
        import plotly

        _PLOTLYJS_SCRIPT = f"<script>{plotly.offline.get_plotlyjs()}</script>"
    return _PLOTLYJS_SCRIPT


# Standardised Plotly config — use in every Plotly.newPlot / fig.to_html call
PLOTLY_CFG_JS = '{"responsive":true,"displayModeBar":true,"scrollZoom":true}'


def plotly_config() -> dict:
    """Return standard Plotly config dict for Python (fig.to_html / fig.show)."""
    return {"responsive": True, "displayModeBar": True, "scrollZoom": True}


# ---------------------------------------------------------------------------
# Viewport meta
# ---------------------------------------------------------------------------

VIEWPORT_META = '<meta name="viewport" content="width=device-width,initial-scale=1">'

# ---------------------------------------------------------------------------
# Base CSS — shared by ALL layouts
# ---------------------------------------------------------------------------
# Rules:
#   • No px for spacing/typography — only rem
#   • clamp() for fluid card widths and chart heights
#   • overflow-wrap everywhere to prevent text blowout
#   • Tables always inside .tbl-wrap for horizontal scroll on mobile
# ---------------------------------------------------------------------------

_BASE_CSS = (
    # Reset
    "*{box-sizing:border-box;margin:0;padding:0}"
    # Text wrapping — prevents any user-supplied content from blowing the layout
    "*{overflow-wrap:break-word;word-break:break-word}"
    # Restore sane word-break inside code blocks
    "code,pre,kbd,samp{word-break:normal;overflow-wrap:normal;overflow-x:auto}"
    "html{scroll-behavior:smooth;font-size:16px}"
    "body{font-family:'Segoe UI',system-ui,-apple-system,sans-serif;"
    "background:var(--bg);color:var(--text);min-height:100vh;"
    "overflow-x:hidden;transition:background .2s,color .2s;line-height:1.5}"
    # Scrollbar
    "::-webkit-scrollbar{width:.375rem}"
    "::-webkit-scrollbar-track{background:var(--bg)}"
    "::-webkit-scrollbar-thumb{background:var(--border);border-radius:.1875rem}"
    # Headings — rem-based, clamp for fluid scaling
    "h1{font-size:clamp(1.25rem,2.5vw,1.625rem);font-weight:700;color:var(--accent)}"
    "h2{font-size:clamp(1rem,2vw,1.25rem);font-weight:600;color:var(--accent)}"
    "h3{font-size:clamp(.8125rem,1.5vw,.9375rem);font-weight:500;color:var(--text)}"
    # Semantic helpers
    ".text-muted{color:var(--text-muted)}"
    ".text-mono{font-family:'Cascadia Code','Fira Mono',monospace;font-size:.75rem}"
    ".good{color:var(--green)}.warn{color:var(--orange)}.bad{color:var(--red)}"
    # Badge
    ".badge{font-size:.6875rem;padding:.125rem .5rem;border-radius:.75rem;"
    "background:var(--border);color:var(--text-muted);font-weight:500;white-space:nowrap}"
    # Print button
    ".btn-print{background:none;border:1px solid var(--border);color:var(--text-muted);"
    "border-radius:var(--radius-sm);padding:.25rem .625rem;font-size:var(--font-xs);"
    "cursor:pointer;white-space:nowrap}"
    ".btn-print:hover{border-color:var(--accent);color:var(--accent)}"
    # ── Cards grid ────────────────────────────────────────────────────────────
    # auto-fill: browser decides how many columns fit; minmax clamps card width
    ".cards{display:grid;"
    "grid-template-columns:repeat(auto-fill,minmax(clamp(7rem,15vw,10rem),1fr));"
    "gap:clamp(.5rem,1.5vw,.875rem);margin-bottom:1.5rem}"
    ".card{background:var(--surface);border:1px solid var(--border);"
    "border-radius:.625rem;"
    "padding:clamp(.75rem,2vw,1.125rem) clamp(.625rem,2vw,1.25rem);"
    "text-align:center;transition:transform .15s,border-color .15s}"
    ".card:hover{transform:translateY(-.125rem);border-color:var(--accent)}"
    # Card number — never wraps; ellipsis if it overflows
    ".card .num{font-size:clamp(1.25rem,3vw,1.875rem);font-weight:700;"
    "color:var(--accent);line-height:1.2;"
    "white-space:nowrap;overflow:hidden;text-overflow:ellipsis}"
    ".card .label,.card .lbl{font-size:clamp(.5rem,1.2vw,.6875rem);"
    "color:var(--text-muted);margin-top:.375rem;text-transform:uppercase;"
    "letter-spacing:.05em;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}"
    ".card.good .num{color:var(--green)}"
    ".card.warn .num{color:var(--orange)}"
    ".card.bad .num{color:var(--red)}"
    # ── Section wrappers ───────────────────────────────────────────────────────
    ".section{margin-bottom:3rem}"
    ".section>h1{margin-bottom:1.5rem;padding-bottom:.75rem;border-bottom:2px solid var(--border)}"
    ".section>h2{margin-bottom:1.25rem;padding-bottom:.625rem;border-bottom:1px solid var(--border)}"
    # ── Chart containers ───────────────────────────────────────────────────────
    # .chart-box  = outer card shell (border, background)
    # .chart-div  = the actual Plotly mount div — has defined height so Plotly
    #               picks it up via autosize:true (no hardcoded height in layout!)
    ".chart-box{background:var(--surface);border:1px solid var(--border);"
    "border-radius:.625rem;margin:1rem 0;overflow:hidden;width:100%}"
    ".chart-note{padding:.375rem .75rem;font-size:.75rem;color:var(--text-muted)}"
    # Standard chart heights — CSS-controlled, no Plotly height param needed
    ".chart-div{width:100%;height:clamp(18rem,40vh,30rem)}"
    ".chart-div.heatmap{height:clamp(22rem,50vh,38rem)}"
    ".chart-div.compact{height:clamp(14rem,30vh,22rem)}"
    ".chart-div.network{height:clamp(20rem,45vh,34rem)}"
    # Legacy class kept for backward compat
    ".chart-container{background:var(--surface);border:1px solid var(--border);"
    "border-radius:.625rem;padding:.75rem;margin:1rem 0;overflow:hidden;width:100%}"
    # ── Insight list ───────────────────────────────────────────────────────────
    ".insights{list-style:none;padding:0}"
    ".insights li{padding:.625rem .875rem;margin:.375rem 0;background:var(--surface);"
    "border-radius:.5rem;border-left:.25rem solid var(--accent);"
    "font-size:.8125rem;line-height:1.6}"
    ".insights li.warn{border-left-color:var(--orange)}"
    ".insights li.bad{border-left-color:var(--red)}"
    ".insights li.good{border-left-color:var(--green)}"
    # ── Alerts ────────────────────────────────────────────────────────────────
    ".alert-panel{border-radius:.625rem;overflow:hidden;margin-bottom:1.25rem}"
    ".alert-item{padding:.625rem .875rem;margin:.1875rem 0;font-size:.8125rem;"
    "border-radius:.5rem;display:flex;align-items:flex-start;gap:.625rem;"
    "background:var(--surface);border:1px solid var(--border)}"
    ".alert-item.error{border-left:.25rem solid var(--red)}"
    ".alert-item.warning{border-left:.25rem solid var(--orange)}"
    ".alert-item.info{border-left:.25rem solid var(--green)}"
    ".alert-badge{font-size:.625rem;font-weight:700;padding:.125rem .5rem;"
    "border-radius:.625rem;white-space:nowrap;flex-shrink:0;margin-top:.0625rem}"
    ".alert-badge.error{background:var(--red);color:#fff}"
    ".alert-badge.warning{background:var(--orange);color:#fff}"
    ".alert-badge.info{background:var(--green);color:#fff}"
    # ── Tables ────────────────────────────────────────────────────────────────
    # Always wrap tables in .tbl-wrap for mobile horizontal scroll
    ".tbl-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch}"
    "table{width:100%;border-collapse:collapse;font-size:.8125rem;"
    "background:var(--surface);border-radius:.5rem;overflow:hidden}"
    "th,td{padding:.625rem 1rem;text-align:left;border-bottom:1px solid var(--border);"
    "min-width:0;overflow-wrap:break-word}"
    "th{background:rgba(88,166,255,.08);color:var(--accent);font-weight:600;"
    "font-size:.6875rem;text-transform:uppercase;letter-spacing:.03em;white-space:nowrap}"
    "tr:hover{background:rgba(88,166,255,.03)}"
    ".stats-cell{font-family:'Cascadia Code','Fira Mono',monospace;font-size:.75rem;"
    "color:var(--text-muted)}"
    # ── Missing-data bar ──────────────────────────────────────────────────────
    ".mbar{height:1.5rem;background:var(--border);border-radius:.375rem;"
    "overflow:hidden;margin:.25rem 0}"
    ".mbar-fill{height:100%;background:linear-gradient(90deg,var(--orange),var(--red));"
    "border-radius:.375rem}"
    # ── Two-column split ──────────────────────────────────────────────────────
    ".split{display:grid;"
    "grid-template-columns:repeat(auto-fit,minmax(min(100%,20rem),1fr));"
    "gap:1.5rem;margin:1rem 0}"
    ".split-left table{margin:0}"
    ".split-right .cc-card{background:var(--surface);border:1px solid var(--border);"
    "border-radius:.625rem;padding:.75rem}"
    # ── Column card ───────────────────────────────────────────────────────────
    ".cc-card{background:var(--surface);border:1px solid var(--border);"
    "border-radius:.625rem;margin:1rem 0;overflow:hidden;transition:border-color .15s}"
    ".cc-card:hover{border-color:var(--accent)}"
    ".cc-hdr{padding:.875rem 1.125rem;background:rgba(88,166,255,.04);"
    "border-bottom:1px solid var(--border);display:flex;justify-content:space-between;"
    "align-items:center;gap:.5rem;min-width:0}"
    ".cc-hdr h3{color:var(--text);font-size:.9375rem;margin:0;font-weight:600;"
    "overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0}"
    ".cc-body{padding:1.125rem}"
)

# ---------------------------------------------------------------------------
# Report layout CSS (sidebar + main content) — used by profile & EDA
# ---------------------------------------------------------------------------

_REPORT_CSS = (
    "body{display:flex}"
    ".sidebar{width:clamp(14rem,20vw,20rem);background:var(--surface);"
    "border-right:1px solid var(--border);position:fixed;top:0;left:0;bottom:0;"
    "overflow-y:auto;z-index:100;display:flex;flex-direction:column;flex-shrink:0}"
    ".sidebar-header{padding:1.5rem 1.25rem 1rem;border-bottom:1px solid var(--border)}"
    ".sidebar-header h2{color:var(--accent);font-size:1.125rem;margin-bottom:.25rem;"
    "font-weight:600}"
    ".sidebar-header .file-name{color:var(--text-muted);font-size:.8125rem;"
    "margin-bottom:.125rem;word-break:break-all}"
    ".sidebar-header .meta{color:var(--text-muted);font-size:.75rem}"
    ".sidebar-nav{padding:.75rem 0;flex:1}"
    ".sidebar-nav a{display:block;padding:.5rem 1.25rem;color:var(--text-muted);"
    "text-decoration:none;font-size:.8125rem;border-left:.1875rem solid transparent;"
    "transition:all .15s;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}"
    ".sidebar-nav a:hover,.sidebar-nav a.active{color:var(--accent);"
    "background:rgba(88,166,255,.06);border-left-color:var(--accent)}"
    ".sidebar-nav .st{padding:1rem 1.25rem .375rem;color:#484f58;font-size:.625rem;"
    "text-transform:uppercase;letter-spacing:.075em;font-weight:600}"
    # Main content area — margin matches sidebar width via CSS calc
    ".main{margin-left:clamp(14rem,20vw,20rem);padding:clamp(1rem,4vw,2rem);"
    "flex:1;min-width:0;max-width:min(87.5rem,100%)}"
    # Sticky section headings — only in report layout (sidebar + main)
    ".section>h2{position:sticky;top:0;z-index:10;background:var(--bg);padding-top:.5rem}"
    # EDA sidebar alternate class names
    ".sidebar-hdr{padding:1.25rem;border-bottom:1px solid var(--border)}"
    ".sidebar-hdr h2{color:var(--accent);font-size:1rem;margin-bottom:.25rem}"
    ".sidebar-hdr .meta{color:var(--text-muted);font-size:.75rem}"
    ".nav{padding:.5rem 0}"
    ".nav a{display:block;padding:.4375rem 1.25rem;color:var(--text-muted);"
    "text-decoration:none;font-size:.8125rem;border-left:.1875rem solid transparent;"
    "transition:all .15s;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}"
    ".nav a:hover,.nav a.active{color:var(--accent);background:rgba(88,166,255,.06);"
    "border-left-color:var(--accent)}"
    ".nav .st{padding:.875rem 1.25rem .25rem;color:var(--border);font-size:.625rem;"
    "text-transform:uppercase;letter-spacing:.0625rem;font-weight:600}"
    # Sidebar transition
    ".sidebar{transition:transform .22s ease}"
    # Hamburger button (hidden on desktop)
    "#sb-toggle{display:none;position:fixed;top:.75rem;left:.75rem;z-index:200;"
    "background:var(--surface);border:1px solid var(--border);"
    "border-radius:var(--radius-sm);padding:.375rem .625rem;cursor:pointer;"
    "color:var(--accent);font-size:var(--font-xl);line-height:1;"
    "box-shadow:0 2px 8px rgba(0,0,0,.15)}"
    "#sb-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.45);"
    "z-index:90;transition:opacity .2s}"
    "#sb-overlay.show{display:block}"
    # Responsive — breakpoints in rem (not px) so they scale with user font settings
    "@media(max-width:68.75rem){"
    ".sidebar{width:clamp(12rem,18vw,17rem)}"
    ".main{margin-left:clamp(12rem,18vw,17rem)}}"
    "@media(max-width:48rem){"
    "#sb-toggle{display:flex;align-items:center;justify-content:center}"
    ".sidebar{transform:translateX(-100%)}"
    ".sidebar.open{transform:translateX(0)}"
    ".main{margin-left:0;padding-top:3.5rem}"
    ".cards{grid-template-columns:repeat(auto-fill,minmax(7rem,1fr))}}"
    "@media(max-width:30rem){"
    ".cards{grid-template-columns:repeat(2,1fr)}"
    "th,td{padding:.5rem .625rem;font-size:.75rem}}"
)

# ---------------------------------------------------------------------------
# Dashboard layout CSS — used by generate_dashboard
# ---------------------------------------------------------------------------

_DASHBOARD_CSS = (
    # Header
    "header{background:var(--surface);border-bottom:1px solid var(--border);"
    "padding:clamp(.625rem,2vw,.875rem) clamp(.875rem,3vw,1.75rem);"
    "display:flex;flex-wrap:wrap;gap:.625rem;align-items:center}"
    "header h1{color:var(--accent);font-size:clamp(1rem,2vw,1.25rem);font-weight:700;"
    "flex:1 1 auto;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0}"
    ".row-ctr{color:var(--text-muted);font-size:.75rem;white-space:nowrap}"
    ".btn{background:var(--bg);border:1px solid var(--border);color:var(--text-muted);"
    "border-radius:.375rem;padding:.3125rem .75rem;font-size:.75rem;cursor:pointer;"
    "white-space:nowrap}"
    ".btn:hover{border-color:var(--accent);color:var(--accent)}"
    ".btn-p{background:var(--accent);color:#fff;border-color:var(--accent)}"
    ".btn-p:hover{opacity:.88;color:#fff}"
    # Filter bar
    ".filter-bar{background:var(--surface);border-bottom:1px solid var(--border);"
    "padding:clamp(.5rem,2vw,.75rem) clamp(.875rem,3vw,1.75rem);"
    "display:flex;flex-wrap:wrap;gap:1rem;align-items:flex-end}"
    ".fgrp{display:flex;flex-direction:column;gap:.3125rem}"
    ".flbl{font-size:.6875rem;color:var(--text-muted);text-transform:uppercase;"
    "letter-spacing:.04em;font-weight:600}"
    ".pills{display:flex;flex-wrap:wrap;gap:.25rem}"
    ".pill{background:var(--bg);border:1px solid var(--border);color:var(--text-muted);"
    "border-radius:6.25rem;padding:.1875rem .6875rem;font-size:.75rem;cursor:pointer;"
    "transition:.12s}"
    ".pill.active{background:var(--accent);border-color:var(--accent);color:#fff}"
    ".pill:hover:not(.active){border-color:var(--accent);color:var(--text)}"
    ".ddw{position:relative}"
    ".ddbtn{background:var(--bg);border:1px solid var(--border);color:var(--text);"
    "border-radius:.375rem;padding:.3125rem .625rem;font-size:.75rem;cursor:pointer;"
    "min-width:8.75rem;text-align:left;white-space:nowrap}"
    ".ddmenu{position:absolute;top:calc(100% + .25rem);left:0;z-index:200;"
    "background:var(--surface);border:1px solid var(--border);border-radius:.5rem;"
    "padding:.5rem;min-width:12.5rem;max-height:17.5rem;overflow-y:auto;"
    "box-shadow:0 .5rem 1.75rem rgba(0,0,0,.35)}"
    ".ddmenu.hid{display:none}"
    ".ddsrch{width:100%;background:var(--bg);border:1px solid var(--border);"
    "color:var(--text);border-radius:.25rem;padding:.25rem .5rem;font-size:.75rem;"
    "margin-bottom:.375rem;outline:none}"
    ".ddacts{display:flex;gap:.375rem;margin-bottom:.5rem}"
    ".ddacts .btn{padding:.125rem .5rem;font-size:.6875rem}"
    ".optlbl{display:flex;align-items:center;gap:.4375rem;padding:.1875rem .25rem;"
    "border-radius:.25rem;font-size:.75rem;cursor:pointer;user-select:none}"
    ".optlbl:hover{background:var(--bg)}"
    ".optlbl input{accent-color:var(--accent)}"
    ".nrng{display:flex;gap:.375rem;align-items:center}"
    ".ninp{width:5.5rem;background:var(--bg);border:1px solid var(--border);"
    "color:var(--text);border-radius:.375rem;padding:.25rem .5rem;font-size:.75rem;"
    "outline:none}"
    ".ninp:focus{border-color:var(--accent)}"
    ".nsep{color:var(--text-muted);font-size:.8125rem}"
    # KPI cards — fluid grid, no fixed column count
    ".kpi-row{display:grid;"
    "grid-template-columns:repeat(auto-fill,minmax(clamp(7.5rem,16vw,12rem),1fr));"
    "gap:clamp(.5rem,1.5vw,.75rem);"
    "padding:clamp(.75rem,2vw,1.125rem) clamp(.875rem,3vw,1.75rem)}"
    ".kpi-card{background:var(--surface);border:1px solid var(--border);"
    "border-radius:.625rem;padding:clamp(.625rem,2vw,.875rem) clamp(.625rem,2vw,1rem)}"
    ".kpi-val{font-size:clamp(1.1rem,2.5vw,1.375rem);font-weight:700;color:var(--accent);"
    "white-space:nowrap;overflow:hidden;text-overflow:ellipsis}"
    ".kpi-lbl{font-size:clamp(.5rem,1.2vw,.625rem);color:var(--text-muted);margin-top:.125rem;"
    "text-transform:uppercase;letter-spacing:.04em;"
    "overflow:hidden;text-overflow:ellipsis;white-space:nowrap}"
    ".kpi-trend{font-size:.75rem;margin-top:.1875rem;font-weight:600}"
    ".kpi-spark{height:2.125rem;margin-top:.375rem;pointer-events:none}"
    ".trend-up{color:var(--green)}.trend-down{color:var(--red)}.trend-flat{color:var(--text-muted)}"
    # Chart grid — auto-fill, min column ~clamp(20rem, 45vw, 36rem)
    ".sec-hdr{padding:1rem clamp(.875rem,3vw,1.75rem) .25rem;font-size:.6875rem;"
    "color:var(--text-muted);text-transform:uppercase;letter-spacing:.06em;font-weight:600}"
    ".cgrid{padding:0 clamp(.875rem,3vw,1.75rem) 1.75rem;display:grid;"
    "grid-template-columns:repeat(auto-fill,minmax(min(100%,clamp(18rem,42vw,34rem)),1fr));"
    "gap:clamp(.5rem,1.5vw,.875rem)}"
    # Chart card shell
    ".cc{background:var(--surface);border:1px solid var(--border);border-radius:.625rem;"
    "overflow:hidden;min-width:0}"
    ".cc-hdr{display:flex;align-items:center;padding:.625rem .875rem;"
    "border-bottom:1px solid var(--border)}"
    ".cc-hdr h3{font-size:.75rem;font-weight:500;color:var(--text);flex:1;"
    "overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0}"
    ".exp{background:none;border:none;color:var(--text-muted);cursor:pointer;"
    "font-size:.9375rem;line-height:1;padding:0 .125rem;flex-shrink:0}"
    ".exp:hover{color:var(--accent)}"
    # Chart body — CSS-controlled height; chart div fills it via height:100%
    ".cc-body{height:clamp(18rem,38vh,26rem)}"
    ".cc-body--tall{height:clamp(22rem,48vh,34rem)}"
    ".full{grid-column:1/-1}"
    # Expand modal
    ".modal{display:none;position:fixed;inset:0;background:rgba(0,0,0,.72);"
    "z-index:1000;align-items:center;justify-content:center}"
    ".modal.open{display:flex}"
    ".mbox{background:var(--surface);border:1px solid var(--border);border-radius:.75rem;"
    "width:92vw;max-width:75rem;height:88vh;display:flex;flex-direction:column}"
    ".mhdr{display:flex;align-items:center;padding:.875rem 1.125rem;"
    "border-bottom:1px solid var(--border)}"
    ".mhdr h3{flex:1;font-size:.875rem;color:var(--text);"
    "overflow:hidden;text-overflow:ellipsis;white-space:nowrap}"
    ".mclose{background:none;border:none;color:var(--text-muted);font-size:1.25rem;"
    "cursor:pointer;line-height:1;flex-shrink:0}"
    ".mclose:hover{color:var(--red)}"
    "#mdiv{flex:1;min-height:0}"
    # Responsive breakpoints
    "@media(max-width:68.75rem){.cgrid{grid-template-columns:1fr}}"
    "@media(max-width:37.5rem){"
    "header,.filter-bar,.kpi-row,.cgrid,.sec-hdr{padding-left:.875rem;padding-right:.875rem}"
    ".kpi-row{grid-template-columns:repeat(2,1fr)}}"
    "@media(max-width:25rem){.kpi-row{grid-template-columns:1fr}}"
)

# ---------------------------------------------------------------------------
# Public CSS builders
# ---------------------------------------------------------------------------


def css_report(theme_vars: str) -> str:
    """Full CSS for report pages (profile / EDA) — sidebar + main layout."""
    return theme_vars + _BASE_CSS + _REPORT_CSS


def css_dashboard(theme_vars: str) -> str:
    """Full CSS for dashboard pages — header + filter + KPI + chart grid."""
    return theme_vars + _BASE_CSS + _DASHBOARD_CSS


# ---------------------------------------------------------------------------
# Plotly layout helpers
# ---------------------------------------------------------------------------


def plotly_layout_base(plot_bg: str, font_color: str, margin: dict | None = None) -> dict:
    """Return a base Plotly layout dict.

    Does NOT include ``height`` — CSS controls height via the container div.
    autosize=True lets Plotly fill whatever the CSS sets.
    """
    m = margin or {"l": 50, "r": 20, "t": 20, "b": 40}
    return {
        "paper_bgcolor": plot_bg,
        "plot_bgcolor": plot_bg,
        "font": {"color": font_color},
        "margin": m,
        "autosize": True,
    }
