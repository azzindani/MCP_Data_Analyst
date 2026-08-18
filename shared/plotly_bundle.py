"""Where a generated page gets Plotly from, shared verbatim by DA and ML.

The library is 4.85 MB. Inlining it into every artifact made a one-chart page
4.86 MB and a report 6 MB, and the two repos did not even agree on when to do
it: Data Analyst inlined it in reports but referenced a sidecar from charts,
Machine Learning inlined it in both. One sweep of 124 artifacts carried the same
4.85 MB of JavaScript dozens of times over.

Worse than the disk cost, `return_content=True` base64-encodes whatever is on
disk, so asking a tool for the bytes of an ML chart put **6.21 MB of base64 in a
single tool result** -- measured against the live server, not estimated. No
client has room for that.

So every page in both repos now loads `plotly.min.js` from beside itself, and
the sidecar is written once per output directory. A page is a few KB; the
library is paid for once. For the caller that has no filesystem in common with
this server, `_self_contained` in file_utils swaps in a self-contained SVG
rendering, and `MAX_EMBED_BYTES` stops anything oversized being encoded at all.

Keep this file identical in both repos -- `md5sum */shared/plotly_bundle.py`.
"""

from __future__ import annotations

import shutil
from pathlib import Path

# Anything larger than this is not worth base64-encoding into a tool result.
# A sidecar page is a few KB, so this only ever trips on something unexpected --
# it is the backstop that keeps a 6 MB response from being possible at all.
MAX_EMBED_BYTES = 2_000_000

# What plotly's own to_html calls the two modes, reused so callers can pass the
# return value straight through as include_plotlyjs.
SIDECAR = "directory"
INLINE = True

_SIDECAR_NAME = "plotly.min.js"
_SIDECAR_TAG = f'<script src="{_SIDECAR_NAME}"></script>'


def ensure_plotly_js(output_dir: Path) -> bool:
    """Copy plotly.min.js beside the page, once. True when it is there.

    False means the package copy could not be found, and the caller must inline
    the bundle instead -- a page that references a script that is not there is a
    blank page, and this server has no network to fall back on.
    """
    target = Path(output_dir) / _SIDECAR_NAME
    if target.exists():
        return True
    try:
        import plotly as _plotly

        src = Path(_plotly.__file__).parent / "package_data" / _SIDECAR_NAME
        if not src.exists():
            return False
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(src), str(target))
        return True
    except Exception:
        return False


def include_plotlyjs_for(output_dir: Path):
    """Value to pass as plotly's `include_plotlyjs` for a page in output_dir."""
    return SIDECAR if ensure_plotly_js(output_dir) else INLINE


def plotly_script_tag(output_dir: Path) -> str:
    """Return the <script> tag for a hand-assembled page written to output_dir.

    Reports build their own <head> rather than going through plotly's to_html,
    so they need the tag itself. Falls back to the inline bundle on the same
    terms as include_plotlyjs_for.
    """
    if ensure_plotly_js(output_dir):
        return _SIDECAR_TAG
    from plotly.offline import get_plotlyjs

    return f"<script>{get_plotlyjs()}</script>"


def references_sidecar(html: str) -> bool:
    """True when this page needs plotly.min.js sitting next to it to render."""
    return f'src="{_SIDECAR_NAME}"' in html
