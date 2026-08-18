"""The standalone chart page, shared verbatim by Data Analyst and Machine Learning.

Both servers save single Plotly figures as their own HTML page, and both had
grown their own shell for it. Side by side at the same viewport they did not
look like they came from the same product:

    Data Analyst          full bleed, no padding, no card, no title,
                          plot 1440x900 at the window edge
    Machine Learning      card with border and shadow, 40px page padding,
                          plot 1166x450 with dead space below it

Neither was wrong on its own; having both is the defect. This module holds the
one shell, and the two repos vendor it byte for byte — the same arrangement
already used for the device-mode script, since they ship as separate
distributions with no package to share.

Keep this file identical in both repos. `md5sum */shared/chart_page.py` is the
check; a diff here is a visual regression in one of the two products.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Plotly margins
# ---------------------------------------------------------------------------
# A margin tight enough to look good on a chart with short tick labels clips a
# chart with long ones: "5000" rendered as "000" with 20px on the left, and an
# x-axis label sheared off at the bottom edge of the card. Rather than pick a
# number big enough for the worst case and waste it on every other chart, the
# base is modest and `automargin` grows it per axis to whatever the labels
# actually need. Plotly measures the rendered text, so this holds for rotated
# ticks, long category names and multi-line axis titles alike.

CHART_MARGIN: dict[str, int] = {"l": 56, "r": 24, "t": 48, "b": 48}


def apply_axis_automargin(fig: object) -> None:
    """Let every axis grow the margin to fit the labels it actually draws.

    Split out from apply_chart_margins because a figure embedded in a report
    keeps its own margin and height -- a subplot grid needs them -- but still has
    to stop shearing its tick labels off. A report's histogram rendered "20" as
    "0" until this ran on it, so every gridline read zero.
    """
    # title_automargin covers the one thing axis automargin cannot: a long or
    # wrapped figure title needs room at the top that no axis will ask for.
    fig.update_layout(title_automargin=True)  # type: ignore[attr-defined]
    # No-ops on figures without cartesian axes (pie, 3d, geo), which is why this
    # is safe to call unconditionally.
    fig.update_xaxes(automargin=True)  # type: ignore[attr-defined]
    fig.update_yaxes(automargin=True)  # type: ignore[attr-defined]


def apply_chart_margins(fig: object) -> None:
    """Set the shared margin and let each axis grow it to fit its own labels."""
    fig.update_layout(margin=dict(CHART_MARGIN), autosize=True)  # type: ignore[attr-defined]
    apply_axis_automargin(fig)


# ---------------------------------------------------------------------------
# Page CSS
# ---------------------------------------------------------------------------
# Height is the part worth reading twice, because the page has to serve two
# kinds of figure without a flag telling it which it has.
#
# A single chart carries no height and Plotly falls back to 450px, which is what
# left half of a 900px window empty. A multi-panel figure (a distribution grid,
# a stacked subplot) carries a real height that grows with its row count, and
# squashing that into the viewport would be a worse bug than the dead space.
#
# So height is a floor, never a cage: `--chart-h` gives a single chart a
# viewport-proportional height, and a figure that asks for more simply gets it
# and the page scrolls. Deliberately not a flex stretch — a flex child with
# `min-height:0` shrinks below its content, which is exactly what a tall subplot
# must not do.
#
# The narrow breakpoint tightens padding and type but deliberately leaves
# `--chart-h` alone: a phone has less width to spare, not less height, and an
# earlier mobile cap of 28rem left a third of the screen empty below the card.
#
# All spacing comes from the shared `--*` tokens, so the two repos cannot drift
# on padding again without drifting on their whole palette.

CHART_PAGE_CSS = """
*{box-sizing:border-box;margin:0;padding:0}
html{scroll-behavior:smooth;font-size:16px}
body{font-family:'Segoe UI',system-ui,-apple-system,sans-serif;
  background:var(--bg);color:var(--text);
  padding:clamp(0.75rem,2.5vw,1.75rem);
  min-height:100vh;overflow-x:hidden;
  --chart-h:clamp(20rem,72vh,46rem)}
.page{width:100%;max-width:90rem;margin:0 auto}
.chart-title{color:var(--accent);font-size:var(--font-2xl);
  font-weight:600;margin-bottom:0.875rem;padding-bottom:0.625rem;
  border-bottom:2px solid var(--border);
  overflow-wrap:break-word;word-break:break-word}
.chart-wrap{background:var(--surface);border:1px solid var(--border);
  border-radius:var(--chart-radius);padding:var(--card-pad);
  min-height:var(--chart-h)}
.chart-wrap .js-plotly-plot,.chart-wrap .plotly-graph-div{
  width:100%!important;min-height:var(--chart-h)}
::-webkit-scrollbar{width:0.375rem}
::-webkit-scrollbar-track{background:var(--bg)}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:var(--radius-sm)}
@media(max-width:30rem){
  body{padding:0.625rem}
  .chart-title{font-size:var(--font-xl);margin-bottom:0.625rem}
  .chart-wrap{padding:0.5rem}}
@media print{
  body{background:#fff;color:#000;padding:0}
  .chart-wrap{border:none;padding:0;background:#fff}}
"""


def chart_page_html(chart_html: str, title: str, vars_css: str, device_js: str = "") -> str:
    """Wrap a Plotly fragment in the shared standalone chart page.

    chart_html : the fragment from `to_html(full_html=False)`
    title      : heading shown above the chart
    vars_css   : `:root{}` block from css_vars(theme)
    device_js  : device-mode script, or "" for a page pinned to one theme
    """
    return f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title>
<style>
{vars_css}
{CHART_PAGE_CSS}
</style></head><body>
<div class="page">
  <h1 class="chart-title">{title}</h1>
  <div class="chart-wrap">{chart_html}</div>
</div>
{device_js}
</body></html>"""


def chart_title_from(stem_suffix: str) -> str:
    """Turn an output stem suffix like `roc_curve` into `Roc Curve`."""
    return stem_suffix.replace("_", " ").strip().title() or "Chart"


def take_page_title(fig: object, stem_suffix: str) -> str:
    """Return the page heading, moving the figure's own title up into it.

    A figure that already says "Sum of spends by day" under a page heading of
    "Line" is captioned twice, and the heading is the less informative of the
    two — it comes from the output filename. Where the figure has a real title
    it becomes the heading and is cleared from the figure, so the page has one
    caption and the chart gets the vertical space back.
    """
    title = ""
    try:
        raw = fig.layout.title.text  # type: ignore[attr-defined]
        title = (raw or "").strip()
    except AttributeError:
        title = ""
    if not title:
        return chart_title_from(stem_suffix)
    fig.update_layout(title_text=None)  # type: ignore[attr-defined]
    return title
