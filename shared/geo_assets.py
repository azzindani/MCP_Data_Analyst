"""A map's outlines, carried in the page that draws it.

plotly.js draws a choropleth or a scatter_geo over outlines it fetches when the
page opens -- https://cdn.plot.ly/un/world_110m.json -- so a map written here
and opened offline drew a colour bar beside an empty rectangle: four failed
requests and a page error in the sweep's screenshot, under `success: true`.
plotly.js looks in `window.PlotlyGeoAssets.topojson` before it fetches, so a
page that fills that in first draws its map with no network at all.

`geo/world_110m.json` is that file, byte for byte, as plotly.js 3.8.2 fetches
it for the default world scope at the default resolution; countries, ISO-3
codes and US states (its `subunits`) all draw from it. It is plotly.js's own
asset (MIT). A figure that asks for another scope or resolution is not covered
here, and keeps saying it needs the network.
"""

from __future__ import annotations

import json
from functools import cache
from pathlib import Path
from typing import Any

GEO_TRACES = frozenset({"choropleth", "scattergeo"})
_DIR = Path(__file__).parent / "geo"


def _name(layout_geo: Any) -> str:
    scope = str(getattr(layout_geo, "scope", None) or "world").replace(" ", "_")
    return f"{scope}_{int(getattr(layout_geo, 'resolution', None) or 110)}m"


def topojson_names(fig: Any) -> list[str]:
    """The outline files the geo traces of `fig` will ask plotly.js for."""
    names: list[str] = []
    for trace in getattr(fig, "data", ()) or ():
        if str(getattr(trace, "type", "")) in GEO_TRACES:
            name = _name(fig.layout[getattr(trace, "geo", None) or "geo"])
            if name not in names:
                names.append(name)
    return names


def carried(name: str) -> bool:
    return (_DIR / f"{name}.json").is_file()


@cache
def _topojson(name: str) -> str:
    # Parsed and re-dumped rather than pasted: the text lands inside a
    # <script>, and a JSON document is JavaScript only once it is known to be one.
    text = json.dumps(json.loads((_DIR / f"{name}.json").read_text(encoding="utf-8")), separators=(",", ":"))
    return text.replace("</", "<\\/")


def assets_script(names: list[str]) -> str:
    """A <script> that hands plotly.js the named outlines before any map is drawn; "" when none is carried."""
    held = [n for n in names if carried(n)]
    if not held:
        return ""
    fill = "".join(f"a.topojson[{json.dumps(n)}]={_topojson(n)};" for n in held)
    return (
        "<script>(function(){var a=window.PlotlyGeoAssets=window.PlotlyGeoAssets||{};"
        f"a.topojson=a.topojson||{{}};{fill}}})();</script>"
    )


def inlined_traces(fig: Any) -> list[str]:
    """The geo trace types of `fig` whose outlines this page carries -- all of them or none."""
    names = topojson_names(fig)
    if not names or not all(carried(n) for n in names):
        return []
    return sorted({str(t.type) for t in fig.data if str(getattr(t, "type", "")) in GEO_TRACES})
