"""Where a generated page gets Plotly from, shared verbatim by DA and ML.

**A file this server writes has to render on its own.** That is the rule this
module exists to keep, and it was broken by the thing it used to do.

The library is 4.85 MB, so pages referenced it from a `plotly.min.js` sidecar
written once per output directory: a chart page was 12 KB instead of 4.86 MB.
That is right for a directory served whole and wrong for everything else. The
artifact is the deliverable, and the deliverable travelled alone constantly --
downloaded on its own, copied somewhere else, attached to a message, opened from
a path the sidecar was never in. Every one of those is a page with a title, an
empty bordered box where the chart should be, and `Plotly is not defined` in a
console nobody has open. It fails silently, and it failed on every chart this
fleet produced.

So the bundle is inlined again, unconditionally. A page costs 4.86 MB on disk
and works everywhere, including offline, which is this fleet's founding
constraint.

The size problem the sidecar was solving is real, but it is a problem about
**what travels in a tool result**, not about what is on disk, and it has its own
answer: `_self_contained` in file_utils swaps in the few-KB SVG drawing from
`svg_chart` for the copy returned inline, and `MAX_EMBED_BYTES` refuses anything
oversized outright. Those two keep working here -- they key off the page being a
Plotly page, not off how it loads the library, so the disk copy can be fat and
the response stays small.

Keep this file identical in both repos -- `md5sum */shared/plotly_bundle.py`.
"""

from __future__ import annotations

from pathlib import Path

# Anything larger than this is not worth base64-encoding into a tool result. A
# self-contained chart page is over it by definition, which is why the response
# path substitutes an SVG rendering rather than the file's own bytes.
MAX_EMBED_BYTES = 2_000_000

# What plotly's own to_html calls the mode, reused so callers can pass the
# return value straight through as include_plotlyjs.
INLINE = True

# The sidecar is no longer written. The name stays because pages produced before
# this change still reference it, and `references_sidecar` is how they are
# recognised.
_SIDECAR_NAME = "plotly.min.js"


def include_plotlyjs_for(output_dir: Path) -> bool:
    """Value to pass as plotly's `include_plotlyjs` for a page in output_dir.

    Always inline: the page must carry its own library. The argument is kept so
    that call sites read as a policy question rather than a hardcoded True, and
    so there is one place to change if this is ever revisited.
    """
    del output_dir
    return INLINE


def plotly_script_tag(output_dir: Path) -> str:
    """The `<script>` tag for a hand-assembled page written to output_dir.

    Reports build their own `<head>` rather than going through plotly's to_html,
    so they need the tag itself. One tag per page, with every figure rendered
    `include_plotlyjs=False`, so a twelve-figure report carries the library once.
    """
    del output_dir
    from plotly.offline import get_plotlyjs

    return f"<script>{get_plotlyjs()}</script>"


def references_sidecar(html: str) -> bool:
    """True when this page needs plotly.min.js sitting next to it to render.

    Only pages written before the sidecar was removed. Kept so that one of them
    is still recognised and handled rather than shipped as a blank chart.
    """
    return f'src="{_SIDECAR_NAME}"' in html


# Trace types whose *basemap* is fetched at view time, not carried in the page.
# Inlining the library does not help these: a `geo` trace pulls its country
# outlines from https://cdn.plot.ly/un/world_110m.json and a mapbox trace pulls
# raster tiles, so on a machine with no network both draw a colour bar beside an
# empty rectangle -- which is the same silent blank the sidecar produced, from a
# different cause.
REMOTE_BASEMAP_TRACES = frozenset(
    {
        "choropleth",
        "scattergeo",
        "choroplethmapbox",
        "scattermapbox",
        "densitymapbox",
        "choroplethmap",
        "scattermap",
        "densitymap",
    }
)

BASEMAP_NOTE = (
    "The map outlines are fetched by the browser when the page is opened "
    "(plotly loads its basemap from cdn.plot.ly, and tiled maps load tiles), so "
    "this file needs a network connection to draw the map itself. The data, the "
    "colour scale and everything else in the page are self-contained."
)


def remote_basemap_traces(fig) -> list[str]:
    """The trace types in `fig` that will need the network to draw a basemap."""
    kinds = []
    for trace in getattr(fig, "data", ()) or ():
        kind = str(getattr(trace, "type", "") or "")
        if kind in REMOTE_BASEMAP_TRACES and kind not in kinds:
            kinds.append(kind)
    return kinds


def is_plotly_page(html: str) -> bool:
    """True when this page draws its chart with Plotly, however it loads it.

    The test used to be `references_sidecar`, which meant the response-side
    substitution silently stopped applying the moment a page started inlining
    its library -- and inlining is now what every page does.
    """
    return "plotly-graph-div" in html or "Plotly.newPlot" in html
