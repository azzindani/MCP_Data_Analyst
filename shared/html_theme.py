"""Shared HTML theme utilities for all tiers."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

from shared.html_layout import VIEWPORT_META, get_output_path, get_plotlyjs_script  # noqa: F401  (re-exported)

# ---------------------------------------------------------------------------
# Plotly template mapping
# ---------------------------------------------------------------------------

PLOTLY_TEMPLATE: dict[str, str] = {
    "dark": "plotly_dark",
    "light": "plotly_white",
    "device": "plotly_white",  # device starts light, JS switches it
}


def plotly_template(theme: str) -> str:
    return PLOTLY_TEMPLATE.get(theme, "plotly_dark")


# ---------------------------------------------------------------------------
# Device-mode JS (auto-switches Plotly template + body bg on system pref)
# ---------------------------------------------------------------------------

_DEVICE_JS = """<script>
(function(){
  const DARK_BG='#0d1117',LIGHT_BG='#ffffff';
  function applyTheme(){
    const dark=window.matchMedia('(prefers-color-scheme:dark)').matches;
    document.body.style.background=dark?DARK_BG:LIGHT_BG;
    document.documentElement.setAttribute('data-theme',dark?'dark':'light');
    document.querySelectorAll('.plotly-graph-div').forEach(function(d){
      try{Plotly.relayout(d,{template:dark?'plotly_dark':'plotly_white'});}catch(e){}
    });
  }
  if(typeof Plotly!=='undefined'){applyTheme();}
  else{window.addEventListener('load',applyTheme);}
  window.matchMedia('(prefers-color-scheme:dark)').addEventListener('change',applyTheme);
})();
</script>"""


def device_mode_js() -> str:
    return _DEVICE_JS


# ---------------------------------------------------------------------------
# CSS custom property blocks
# ---------------------------------------------------------------------------

_DARK_VARS = (
    "--bg:#0d1117;--surface:#161b22;--border:#21262d;--text:#c9d1d9;"
    "--text-muted:#8b949e;--accent:#58a6ff;--green:#3fb950;"
    "--orange:#f0883e;--red:#f85149;"
)
_LIGHT_VARS = (
    "--bg:#ffffff;--surface:#f6f8fa;--border:#d0d7de;--text:#1f2328;"
    "--text-muted:#636c76;--accent:#0969da;--green:#1a7f37;"
    "--orange:#9a6700;--red:#cf222e;"
)
_LAYOUT_VARS = (
    "--sidebar-w:16.25rem;"
    "--sidebar-w-md:13.75rem;"
    "--main-pad:2rem;"
    "--main-pad-sm:1rem;"
    "--section-gap:3rem;"
    "--card-gap:0.75rem;"
    "--card-min:8rem;"
    "--card-pad:1rem;"
    "--radius-sm:0.375rem;"
    "--radius-md:0.625rem;"
    "--radius-lg:0.75rem;"
    "--font-xs:0.6875rem;"
    "--font-sm:0.8125rem;"
    "--font-base:1rem;"
    "--font-lg:1.125rem;"
    "--font-xl:1.25rem;"
    "--font-2xl:clamp(1.125rem,2vw,1.5rem);"
    "--chart-radius:0.75rem;"
)


def css_vars(theme: str) -> str:
    """Return a CSS :root{} block (and optional media query) for the theme."""
    if theme == "light":
        return f":root{{{_LIGHT_VARS}{_LAYOUT_VARS}}}"
    elif theme == "device":
        return f":root{{{_LIGHT_VARS}{_LAYOUT_VARS}}}@media(prefers-color-scheme:dark){{:root{{{_DARK_VARS}}}}}"
    else:  # dark (default)
        return f":root{{{_DARK_VARS}{_LAYOUT_VARS}}}"


# ---------------------------------------------------------------------------
# Theme dicts and helpers
# ---------------------------------------------------------------------------

THEMES: dict[str, dict] = {
    "dark": {
        "plotly_template": "plotly_dark",
        "bg_color": "#0d1117",
        "paper_color": "#161b22",
        "text_color": "#c9d1d9",
        "accent": "#58a6ff",
    },
    "light": {
        "plotly_template": "plotly_white",
        "bg_color": "#ffffff",
        "paper_color": "#f6f8fa",
        "text_color": "#1f2328",
        "accent": "#0969da",
    },
}
THEMES["device"] = THEMES["light"]


def get_theme(theme: str = "dark") -> dict:
    return THEMES.get(theme, THEMES["dark"])


def theme_plot_colors(theme: str) -> tuple[str, str, str]:
    """Return (plot_bg, font_color, accent_color)."""
    t = get_theme(theme)
    return t["paper_color"], t["text_color"], t["accent"]


def apply_fig_theme(fig: object, theme: str) -> None:
    """Apply paper_bgcolor, plot_bgcolor, font color, template, autosize=True."""
    t = get_theme(theme)
    fig.update_layout(  # type: ignore[attr-defined]
        paper_bgcolor=t["paper_color"],
        plot_bgcolor=t["paper_color"],
        font=dict(color=t["text_color"]),
        template=plotly_template(theme),
        autosize=True,
    )


_PX_PER_ROW_SUBPLOT = 220
_PX_PER_ROW_BAR = 28
_PX_HEATMAP_PER_ITEM = 28
_PX_CHART_BASE = 80
_PX_CHART_MIN = 280
_PX_CHART_MAX = 1800


def calc_chart_height(n: int = 1, mode: str = "subplot", extra_base: int = 0) -> int:
    """Return chart height px clamped to [280, 1800].
    mode: subplot | bar | heatmap | fixed
    """
    base = _PX_CHART_BASE + extra_base
    if mode == "subplot":
        raw = _PX_PER_ROW_SUBPLOT * n + base
    elif mode == "bar":
        raw = _PX_PER_ROW_BAR * n + base
    elif mode == "heatmap":
        raw = _PX_HEATMAP_PER_ITEM * n + base
    else:
        raw = n
    return max(_PX_CHART_MIN, min(_PX_CHART_MAX, raw))


_SIDEBAR_JS = """<script>
(function(){
  const btn=document.getElementById('sb-toggle');
  const sb=document.querySelector('.sidebar');
  const overlay=document.getElementById('sb-overlay');
  function close(){sb.classList.remove('open');overlay.classList.remove('show');}
  if(btn&&sb&&overlay){
    btn.addEventListener('click',function(){
      const open=sb.classList.toggle('open');
      overlay.classList.toggle('show',open);
    });
    overlay.addEventListener('click',close);
  }
})();
</script>"""


# ---------------------------------------------------------------------------
# Scroll spy — sidebar active link follows viewport
# ---------------------------------------------------------------------------

_SCROLL_SPY_JS = """<script>
(function(){
  'use strict';
  const links=document.querySelectorAll('.nav a[href^="#"],.sidebar-nav a[href^="#"]');
  const sections=Array.from(document.querySelectorAll('.section[id]'));
  if(!sections.length||!links.length)return;
  const obs=new IntersectionObserver(function(entries){
    entries.forEach(function(e){
      if(e.isIntersecting){
        links.forEach(function(l){l.classList.remove('active');});
        const m=document.querySelector('.nav a[href="#'+e.target.id+'"],.sidebar-nav a[href="#'+e.target.id+'"]');
        if(m)m.classList.add('active');
      }
    });
  },{rootMargin:'-20% 0px -70% 0px'});
  sections.forEach(function(s){obs.observe(s);});
})();
</script>"""

# ---------------------------------------------------------------------------
# Sortable tables — click <th data-sort> to sort
# ---------------------------------------------------------------------------

_SORTABLE_TABLES_JS = """<script>
(function(){
  'use strict';
  document.querySelectorAll('th[data-sort]').forEach(function(th){
    th.style.cursor='pointer';
    let dir=1;
    th.addEventListener('click',function(){
      const idx=th.cellIndex;
      const tbody=th.closest('table').querySelector('tbody');
      if(!tbody)return;
      const rows=Array.from(tbody.rows);
      rows.sort(function(a,b){
        const av=a.cells[idx]?a.cells[idx].textContent.trim():'';
        const bv=b.cells[idx]?b.cells[idx].textContent.trim():'';
        const n=av-bv;
        return dir*(isNaN(n)?av.localeCompare(bv):n);
      });
      rows.forEach(function(r){tbody.appendChild(r);});
      th.closest('table').querySelectorAll('th').forEach(function(t){
        t.textContent=t.textContent.replace(/ [▲▼]$/,'');
      });
      th.textContent+=dir>0?' ▲':' ▼';
      dir*=-1;
    });
  });
})();
</script>"""

# ---------------------------------------------------------------------------
# Collapsible sections — click h2 to toggle, state in sessionStorage
# ---------------------------------------------------------------------------

_COLLAPSIBLE_SECTIONS_JS = """<script>
(function(){
  'use strict';
  document.querySelectorAll('.section>h2').forEach(function(h){
    const section=h.closest('.section');
    if(!section)return;
    const id=section.id;
    const body=h.nextElementSibling;
    if(!body)return;
    h.style.cursor='pointer';
    const key='sec-collapsed-'+id;
    if(sessionStorage.getItem(key)==='1'){
      body.style.display='none';
      h.textContent='▶ '+h.textContent;
    }else{
      h.textContent='▼ '+h.textContent;
    }
    h.addEventListener('click',function(){
      const hidden=body.style.display==='none';
      body.style.display=hidden?'':'none';
      sessionStorage.setItem(key,hidden?'0':'1');
      h.textContent=(hidden?'▼ ':'▶ ')+h.textContent.slice(2);
    });
  });
})();
</script>"""

# ---------------------------------------------------------------------------
# Animated KPI counters — requestAnimationFrame count-up
# ---------------------------------------------------------------------------

_KPI_COUNTER_JS = """<script>
(function(){
  'use strict';
  if(window.matchMedia('(prefers-reduced-motion:reduce)').matches)return;
  document.querySelectorAll('.card .num[data-val]').forEach(function(el){
    const target=parseFloat(el.dataset.val);
    if(isNaN(target))return;
    const fmt=el.dataset.fmt||'int';
    const start=performance.now();
    const dur=600;
    function step(now){
      const t=Math.min((now-start)/dur,1);
      const ease=1-Math.pow(1-t,3);
      const v=target*ease;
      el.textContent=fmt==='float2'?v.toFixed(2)
                    :fmt==='pct'?v.toFixed(1)+'%'
                    :Math.round(v).toLocaleString();
      if(t<1)requestAnimationFrame(step);
    }
    requestAnimationFrame(step);
  });
})();
</script>"""

# ---------------------------------------------------------------------------
# Copy to clipboard — data-copy attribute
# ---------------------------------------------------------------------------

_COPY_CLIPBOARD_JS = """<script>
(function(){
  'use strict';
  document.querySelectorAll('[data-copy]').forEach(function(btn){
    btn.addEventListener('click',function(){
      const text=btn.dataset.copy;
      navigator.clipboard.writeText(text).then(function(){
        const orig=btn.textContent;
        btn.textContent='Copied!';
        setTimeout(function(){btn.textContent=orig;},1500);
      }).catch(function(){});
    });
  });
})();
</script>"""

# ---------------------------------------------------------------------------
# Back-to-top — appears after 300 px scroll
# ---------------------------------------------------------------------------

_BACK_TO_TOP_HTML = (
    '<button id="back-top" aria-label="Back to top" style="display:none;position:fixed;'
    "bottom:1.5rem;right:1.5rem;z-index:300;background:var(--surface);"
    "border:1px solid var(--border);border-radius:var(--radius-md);"
    "padding:.5rem .75rem;cursor:pointer;color:var(--accent);font-size:1rem;"
    'box-shadow:0 2px 8px rgba(0,0,0,.2)" title="Back to top">▲</button>'
)

_BACK_TO_TOP_JS = """<script>
(function(){
  'use strict';
  const btn=document.getElementById('back-top');
  if(!btn)return;
  window.addEventListener('scroll',function(){
    btn.style.display=window.scrollY>300?'':'none';
  },{passive:true});
  btn.addEventListener('click',function(){
    window.scrollTo({top:0,behavior:'smooth'});
  });
})();
</script>"""


def metrics_cards_html(metrics: dict, styles: dict[str, str] | None = None) -> str:
    """Render dict as .cards grid of .card divs."""
    import html as _html

    if styles is None:
        styles = {}
    cards = []
    for k, v in metrics.items():
        cls = styles.get(k, "")
        cls_attr = f' class="card {cls}"' if cls else ' class="card"'
        label = _html.escape(k.replace("_", " ").title())
        if isinstance(v, float):
            val = f"{v:.4f}" if abs(v) < 10 else f"{v:,.2f}"
        else:
            val = _html.escape(str(v))
        cards.append(f'<div{cls_attr}><div class="num">{val}</div><div class="lbl">{label}</div></div>')
    return f'<div class="cards">{"".join(cards)}</div>'


def data_table_html(rows: list[dict], max_rows: int = 50) -> str:
    """Render list[dict] as .table-wrap > table."""
    import html as _html

    if not rows:
        return "<p>No data.</p>"
    headers = list(rows[0].keys())
    th = "".join(f"<th>{_html.escape(str(h).replace('_', ' '))}</th>" for h in headers)
    trs = ""
    for row in rows[:max_rows]:
        tds = ""
        for h in headers:
            val = row.get(h, "")
            tds += f"<td>{_html.escape(str(val) if val is not None else '')}</td>"
        trs += f"<tr>{tds}</tr>"
    if len(rows) > max_rows:
        remaining = len(rows) - max_rows
        trs += (
            f'<tr><td colspan="{len(headers)}" '
            f'style="text-align:center;color:var(--text-muted);font-style:italic">'
            f"&hellip; {remaining:,} more rows</td></tr>"
        )
    return f'<div class="table-wrap"><table><tr>{th}</tr>{trs}</table></div>'


# ---------------------------------------------------------------------------
# _ensure_plotly_js — copy plotly.min.js to output dir for offline use
# ---------------------------------------------------------------------------


def _ensure_plotly_js(output_dir: Path) -> str:
    """Copy plotly.min.js to output_dir once. Returns 'directory' or 'cdn' fallback."""
    target = output_dir / "plotly.min.js"
    if target.exists():
        return "directory"
    try:
        import plotly as _plotly

        src = Path(_plotly.__file__).parent / "package_data" / "plotly.min.js"
        if src.exists():
            shutil.copy2(str(src), str(target))
            return "directory"
    except Exception:
        pass
    return "cdn"


# ---------------------------------------------------------------------------
# save_chart — replaces _save_chart in both tiers
# ---------------------------------------------------------------------------


def save_chart(
    fig,
    output_path: str,
    stem_suffix: str,
    input_path: Path,
    theme: str,
    open_after: bool,
    open_func,  # _open_file callable from the calling engine
) -> tuple[str, str]:
    """Save Plotly fig as themed responsive HTML. Returns (abs_path, filename)."""
    apply_fig_theme(fig, theme)

    out = get_output_path(output_path, input_path, stem_suffix, "html")

    # Copy plotly.min.js to the output directory once (offline-first).
    # "directory" mode generates <script src="plotly.min.js"> instead of
    # embedding the 3.5 MB bundle inline — tiny HTML, no internet required.
    # Falls back to "cdn" only if the package file cannot be located.
    include_js = _ensure_plotly_js(out.parent)
    html = fig.to_html(
        include_plotlyjs=include_js,
        full_html=True,
        config={"responsive": True, "displayModeBar": True, "scrollZoom": True},
    )

    # Inject viewport meta
    html = html.replace("<head>", f"<head>\n{VIEWPORT_META}", 1)

    # Inject device-mode JS and CSS media query
    if theme == "device":
        style_block = (
            "<style>"
            "html,body{height:100vh;margin:0;padding:0;}"
            ".plotly-graph-div{min-height:clamp(20rem,60vh,50rem)}"
            "@media(prefers-color-scheme:dark){html,body{background:#0d1117!important;}}"
            "@media(prefers-color-scheme:light){html,body{background:#ffffff!important;}}"
            "</style>"
        )
        html = html.replace("</head>", f"{style_block}\n</head>", 1)
        html = html.replace("</body>", f"{device_mode_js()}\n</body>", 1)
    else:
        bg = "#0d1117" if theme == "dark" else "#ffffff"
        html = html.replace(
            "</head>",
            f"<style>html,body{{height:100vh;margin:0;padding:0;background:{bg}!important;}}"
            f".plotly-graph-div{{min-height:clamp(20rem,60vh,50rem)}}</style>\n</head>",
            1,
        )

    out.write_text(html, encoding="utf-8")

    if open_after:
        open_func(out)

    return str(out.resolve()), out.name
