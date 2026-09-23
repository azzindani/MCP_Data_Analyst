#!/usr/bin/env python3
"""Render generated HTML in a real browser and report what a reader would see.

NOT part of pytest / CI, for the same reason remote_smoke_test.sh is not: it
needs a browser, and the offline-first suite deliberately drives nothing but
the engine. Run it by hand against artifacts a tool actually produced.

It exists because `success: true` and structurally-valid HTML say nothing about
whether a human can read the page. Every defect below shipped green:

  * y-axis labels sheared off, so "5000" rendered as "000" and a report's
    histogram read "0" at every gridline
  * a chart pinned at Plotly's default 450px, leaving a third of the window
    empty
  * a light chart panel on a dark page, because the figure bakes its colours
    into the layout and only the template was being switched
  * a WebGL scatter drawing nothing at all -- plotGlPixelRatio was 0, so the
    canvas was 0x0 while the axes and title rendered perfectly
  * a page 219px wider than a phone, because one table was not in a scroll
    wrapper

Checks per page, at each viewport:

  figures     every Plotly.newPlot in the source actually rendered
  clipped     no axis tick label escapes its own plot box
  overflow    document does not scroll sideways
  contrast    body text is not the same colour as the body background
  panels      no chart is a light panel on a dark page, or a dark one on a light page
  console     no page errors (a missing plotly.min.js sidecar shows up here)

Usage:
    uv run python visual_check.py <file-or-dir> [more...]
    uv run python visual_check.py --widths 1440,768,390 out/
    uv run python visual_check.py --scheme dark out/report.html
    uv run python visual_check.py --shots /tmp/shots out/     # also save PNGs

Exit status is non-zero if any check fails, so it can gate a release by hand.
Needs `playwright install chromium` once.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

_PROBE = """() => {
  const doc = document.documentElement;
  const plots = [...document.querySelectorAll('.js-plotly-plot, .plotly-graph-div')];
  let clipped = [];
  plots.forEach((p, i) => {
    const box = p.getBoundingClientRect();
    p.querySelectorAll('.ytick text, .xtick text').forEach(t => {
      const r = t.getBoundingClientRect();
      if (r.width > 0 && (r.left < box.left - 0.5 || r.right > box.right + 0.5 || r.bottom > box.bottom + 0.5)) {
        clipped.push({plot: i, text: t.textContent});
      }
    });
  });
  const cs = getComputedStyle(document.body);
  return {
    rendered: plots.length,
    clipped: clipped.slice(0, 8),
    overflow: doc.scrollWidth - doc.clientWidth,
    color: cs.color,
    background: cs.backgroundColor,
    panels: plots.map(p => (p._fullLayout || {}).paper_bgcolor).filter(Boolean),
    plotlyLoaded: typeof Plotly !== 'undefined',
  };
}"""


# Dashboard panels drawn as HTML or written once -- not Plotly figures.
_NOT_FIGURES = frozenset({"section", "text", "kpi", "table"})


def _declared(source: str) -> int:
    """How many figures the page will draw when it loads.

    Counting "Plotly.newPlot" in the whole source counted text inside an inlined
    Plotly bundle and the expand dialog's plot, which is only drawn on a click,
    while a dashboard draws its cards with Plotly.react from _PANELS -- so a
    page with few cards reported one it never meant to draw as missing. Count
    the page's own newPlot calls, and its figure panels.
    """
    own = [
        body
        for body in re.findall(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", source, flags=re.S | re.I)
        if "plotly.js v" not in body[:4000]
    ]
    count = sum(len(re.findall(r"Plotly\.newPlot\(\s*['\"](?!mdiv['\"])", body)) for body in own)
    panels = re.search(r"const _PANELS=(\[.*?\]);\n", source)
    if panels:
        count += sum(1 for p in json.loads(panels.group(1)) if p.get("type") not in _NOT_FIGURES)
    return count


def _pages(paths: list[str]) -> list[Path]:
    out: list[Path] = []
    for raw in paths:
        p = Path(raw)
        if p.is_dir():
            out.extend(sorted(q for q in p.rglob("*.html")))
        elif p.is_file():
            out.append(p)
        else:
            print(f"  ?  no such path: {p}")
    return out


def _same_colour(a: str, b: str) -> bool:
    """Body text the same colour as the body it sits on is invisible text."""

    def nums(s: str) -> tuple[int, ...]:
        return tuple(int(float(x)) for x in s.replace("rgba", "rgb").strip("rgb() ").split(",")[:3])

    try:
        return nums(a) == nums(b)
    except ValueError:
        return False


def _rgba(colour: str) -> tuple[float, float, float, float] | None:
    """'#rrggbb', 'rgb(...)' or 'rgba(...)' as 0-1 channels; None for anything else."""
    c = colour.strip().lower()
    try:
        if c.startswith("#") and len(c) == 7:
            return (int(c[1:3], 16) / 255, int(c[3:5], 16) / 255, int(c[5:7], 16) / 255, 1.0)
        if c.startswith("rgb"):
            parts = [float(x) for x in c[c.index("(") + 1 : c.index(")")].split(",")]
            return (parts[0] / 255, parts[1] / 255, parts[2] / 255, parts[3] if len(parts) > 3 else 1.0)
    except ValueError:
        return None
    return None


def _luminance(colour: str) -> float | None:
    rgba = _rgba(colour)
    if rgba is None or rgba[3] == 0:
        return None  # transparent: the page shows through, so it cannot clash
    return 0.2126 * rgba[0] + 0.7152 * rgba[1] + 0.0722 * rgba[2]


def _clashing_panels(background: str, panels: list[str]) -> list[str]:
    """Charts whose own background is the opposite of the page they sit on."""
    page = _luminance(background)
    if page is None:
        return []
    return [p for p in panels if (lum := _luminance(p)) is not None and abs(lum - page) > 0.5]


def check(paths: list[str], widths: list[int], scheme: str, shots: Path | None) -> int:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("playwright is not installed:  uv add --dev playwright && playwright install chromium")
        return 2

    pages = _pages(paths)
    if not pages:
        print("nothing to check")
        return 1

    failures = 0
    with sync_playwright() as pw:
        browser = pw.chromium.launch(args=["--no-sandbox"])
        ctx = browser.new_context(color_scheme=scheme)
        for page_path in pages:
            source = page_path.read_text(encoding="utf-8", errors="replace")
            declared = _declared(source)
            for width in widths:
                errors: list[str] = []
                pg = ctx.new_page()
                pg.set_viewport_size({"width": width, "height": 900})
                pg.on("pageerror", lambda e: errors.append(str(e)))
                pg.on("console", lambda m: errors.append(m.text) if m.type == "error" else None)
                pg.goto(f"file://{page_path.resolve()}", wait_until="load", timeout=120_000)
                pg.wait_for_timeout(2500)
                r = pg.evaluate(_PROBE)

                problems = []
                if declared and r["rendered"] < declared:
                    problems.append(f"{declared - r['rendered']} of {declared} figures did not render")
                if declared and not r["plotlyLoaded"]:
                    problems.append("Plotly never loaded (missing plotly.min.js sidecar?)")
                if r["clipped"]:
                    shown = ", ".join(repr(c["text"]) for c in r["clipped"][:4])
                    problems.append(f"{len(r['clipped'])} clipped axis label(s): {shown}")
                if r["overflow"] > 0:
                    problems.append(f"page scrolls sideways by {r['overflow']}px")
                if _same_colour(r["color"], r["background"]):
                    problems.append(f"body text and background are both {r['color']}")
                clash = _clashing_panels(r["background"], r["panels"])
                if clash:
                    problems.append(f"{len(clash)} chart(s) drawn {clash[0]} on a {r['background']} page")
                if errors:
                    problems.append(f"{len(errors)} console error(s): {errors[0][:70]}")

                if shots:
                    shots.mkdir(parents=True, exist_ok=True)
                    pg.screenshot(path=str(shots / f"{page_path.stem}_{scheme}_{width}.png"))
                pg.close()

                label = f"{page_path.name} @{width} {scheme}"
                if problems:
                    failures += 1
                    print(f"  FAIL  {label}")
                    for p in problems:
                        print(f"          {p}")
                else:
                    print(f"  ok    {label}  ({r['rendered']} figure(s))")
        browser.close()

    print(f"\n{len(pages)} page(s) x {len(widths)} width(s): {failures} failing check(s)")
    return 1 if failures else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("paths", nargs="+", help="HTML files, or directories to search")
    ap.add_argument("--widths", default="1440,768,390", help="viewport widths (default: 1440,768,390)")
    ap.add_argument("--scheme", default="light", choices=("light", "dark"), help="prefers-color-scheme")
    ap.add_argument("--shots", type=Path, default=None, help="directory to save screenshots into")
    args = ap.parse_args()
    return check(args.paths, [int(w) for w in args.widths.split(",")], args.scheme, args.shots)


if __name__ == "__main__":
    sys.exit(main())
