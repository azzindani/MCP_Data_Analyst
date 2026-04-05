"""Shared HTML theme utilities for all tiers."""
from __future__ import annotations
import sys
from pathlib import Path

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
# Viewport meta tag
# ---------------------------------------------------------------------------

VIEWPORT_META = '<meta name="viewport" content="width=device-width,initial-scale=1">'

# ---------------------------------------------------------------------------
# Device-mode JS (auto-switches Plotly template + body bg on system pref)
# ---------------------------------------------------------------------------

_DEVICE_JS = """<script>
(function(){
  var DARK_BG='#0d1117',LIGHT_BG='#ffffff';
  function applyTheme(){
    var dark=window.matchMedia('(prefers-color-scheme:dark)').matches;
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


def css_vars(theme: str) -> str:
    """Return a CSS :root{} block (and optional media query) for the theme."""
    if theme == "light":
        return f":root{{{_LIGHT_VARS}}}"
    elif theme == "device":
        return (
            f":root{{{_LIGHT_VARS}}}"
            f"@media(prefers-color-scheme:dark){{:root{{{_DARK_VARS}}}}}"
        )
    else:  # dark (default)
        return f":root{{{_DARK_VARS}}}"


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
    open_func,          # _open_file callable from the calling engine
) -> tuple[str, str]:
    """Save Plotly fig as themed responsive HTML. Returns (abs_path, filename)."""
    tmpl = plotly_template(theme)
    fig.update_layout(template=tmpl, autosize=True)

    out = (
        Path(output_path)
        if output_path
        else input_path.parent / f"{input_path.stem}_{stem_suffix}.html"
    )

    # Generate HTML with responsive config
    html = fig.to_html(
        include_plotlyjs=True,
        full_html=True,
        config={"responsive": True, "displayModeBar": True, "scrollZoom": True},
    )

    # Inject viewport meta
    html = html.replace("<head>", f"<head>\n{VIEWPORT_META}", 1)

    # Inject device-mode JS and CSS media query
    if theme == "device":
        style_block = (
            "<style>"
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
            f"<style>html,body{{background:{bg}!important;}}</style>\n</head>",
            1,
        )

    out.write_text(html, encoding="utf-8")

    if open_after:
        open_func(out)

    return str(out.resolve()), out.name
