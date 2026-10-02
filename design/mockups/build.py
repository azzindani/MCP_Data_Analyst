"""Build the mockups into self-contained pages under design/mockups/build/.

    uv run python design/mockups/build.py            # all three
    uv run python design/mockups/build.py harbor     # one

Each page inlines the same plotly.min.js the server inlines (the installed plotly package),
the world outline from shared/geo, data.json and core.js -- the rule every page this server
writes keeps: it renders on its own, offline. build/<name>.html is the publishable body (no
<html>/<head>); build/preview/<name>.html wraps it in a full document for a browser or shoot.py.
"""

from __future__ import annotations

import sys
from pathlib import Path

import plotly

HERE = Path(__file__).parent
REPO = HERE.parents[1]
OUT = HERE / "build"
PAGES = {"lagoon": False, "harbor": True, "nocturne": True}  # name -> needs the world outline


def build(name: str, with_geo: bool) -> Path:
    bundle = (Path(plotly.__file__).parent / "package_data" / "plotly.min.js").read_text(encoding="utf-8")
    if "</script" in bundle.lower():
        raise SystemExit("plotly.min.js contains a closing script tag; it cannot be inlined as is")
    geo = (REPO / "shared" / "geo" / "world_110m.json").read_text(encoding="utf-8")
    blocks = {
        "<!--PLOTLY-->": f"<script>{bundle}</script>"
        + (f"<script>window.PlotlyGeoAssets={{topojson:{{world_110m:{geo}}}}};</script>" if with_geo else ""),
        "<!--DATA-->": f"<script>window.DATA={(HERE / 'data.json').read_text(encoding='utf-8')};</script>",
        "<!--CORE-->": f"<script>{(HERE / 'core.js').read_text(encoding='utf-8')}</script>",
    }
    page = (HERE / f"{name}.src.html").read_text(encoding="utf-8")
    for marker, block in blocks.items():
        if marker not in page:
            raise SystemExit(f"{name}.src.html has no {marker} marker")
        page = page.replace(marker, block)
    (OUT / "preview").mkdir(parents=True, exist_ok=True)
    body = OUT / f"{name}.html"
    body.write_text(page, encoding="utf-8")
    (OUT / "preview" / f"{name}.html").write_text(
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">'
        "<style>:root{color-scheme:light}body{margin:0}img{max-width:100%}[hidden]{display:none!important}</style>"
        f"</head><body>{page}</body></html>",
        encoding="utf-8",
    )
    print(f"{body.relative_to(REPO)}  {len(page) / 1e6:.2f} MB")
    return body


if __name__ == "__main__":
    names = sys.argv[1:] or list(PAGES)
    for n in names:
        if n not in PAGES:
            raise SystemExit(f"unknown mockup {n!r}; known: {', '.join(PAGES)}")
        build(n, PAGES[n])
