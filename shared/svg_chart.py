"""Render a chart as a self-contained SVG.

The interactive chart written to disk references `plotly.min.js` sitting beside
it — right for a directory that is served as a whole, and worthless to a caller
that received only the bytes. Handed to a chat client or pasted into an email,
that file is a blank page: "Plotly is not defined".

Inlining the library instead is not an answer either. It is 4.85 MB, which is
6.5 MB once base64-encoded — far past any budget for content returned in a tool
result.

So content handed back inline is drawn here instead: one `<svg>` element, a few
kilobytes, no script of any kind. It renders in anything that renders HTML,
including places that will not run JavaScript at all.

Only the trace types the chart tools actually produce are drawn — bar, scatter
in its line and marker forms, and pie. Anything else returns None, and the
caller falls back rather than showing a chart that misrepresents the data.
"""

from __future__ import annotations

import math
from html import escape
from typing import Any

from shared.plotly_payload import decode_array, load_figure

WIDTH = 900
HEIGHT = 500
MARGIN = {"left": 78, "right": 32, "top": 56, "bottom": 78}

_PALETTE = ["#636efa", "#EF553B", "#00cc96", "#ab63fa", "#FFA15A", "#19d3f3", "#FF6692", "#B6E880"]

_THEMES = {
    "dark": {"bg": "#0d1117", "panel": "#161b22", "grid": "#283442", "text": "#c9d1d9", "muted": "#8b949e"},
    "light": {"bg": "#ffffff", "panel": "#ffffff", "grid": "#e5e7eb", "text": "#1f2328", "muted": "#57606a"},
}


_CHAR_PX = 6.2  # approximate advance width of the 11px UI font used for ticks


def _left_margin(tick_labels: list[str], has_axis_title: bool) -> float:
    """Widen the left gutter to fit the tick labels actually drawn.

    A fixed gutter let a rotated axis title sit on top of a 2,000,000 tick.
    """
    widest = max((len(s) for s in tick_labels), default=0)
    return min(200.0, (24 if has_axis_title else 8) + 16 + widest * _CHAR_PX)


def _plot_box(left: float = MARGIN["left"]) -> tuple[float, float, float, float]:
    y0 = MARGIN["top"]
    return left, y0, WIDTH - MARGIN["right"] - left, HEIGHT - MARGIN["bottom"] - y0


def _nice_ticks(low: float, high: float, count: int = 5) -> list[float]:
    """Round tick values covering [low, high] — the axis should read cleanly."""
    if not math.isfinite(low) or not math.isfinite(high):
        return [0.0, 1.0]
    if high == low:
        high = low + 1.0
    raw = (high - low) / max(1, count)
    magnitude = 10 ** math.floor(math.log10(raw)) if raw > 0 else 1
    for multiple in (1, 2, 2.5, 5, 10):
        step = magnitude * multiple
        if raw <= step:
            break
    start = math.floor(low / step) * step
    ticks = []
    value = start
    while value <= high + step / 2 and len(ticks) < 24:
        ticks.append(round(value, 10))
        value += step
    return ticks or [low, high]


def _fmt(value: float) -> str:
    if value == int(value) and abs(value) < 1e15:
        n = int(value)
        if abs(n) >= 10_000:
            return f"{n:,}"
        return str(n)
    if abs(value) >= 10_000:
        return f"{value:,.0f}"
    return f"{value:.4g}"


def _title_text(node: Any) -> str:
    if isinstance(node, dict):
        return str(node.get("text", "") or "")
    return str(node or "")


def _axis_title(layout: dict, axis: str) -> str:
    return _title_text((layout.get(axis) or {}).get("title"))


def _trace_values(trace: dict, key: str) -> list | None:
    return decode_array(trace.get(key))


def _numeric(values: list) -> list[float] | None:
    out = []
    for v in values:
        if isinstance(v, bool) or v is None:
            return None
        if isinstance(v, (int, float)):
            out.append(float(v))
        else:
            return None
    return out


class _Canvas:
    def __init__(self, theme: dict):
        self.parts: list[str] = []
        self.t = theme
        self.left = MARGIN["left"]

    def add(self, markup: str) -> None:
        self.parts.append(markup)

    def text(
        self,
        x: float,
        y: float,
        body: str,
        size: float = 12,
        anchor: str = "middle",
        fill: str | None = None,
        weight: str = "normal",
        rotate: float = 0,
    ) -> None:
        if not body:
            return
        transform = f' transform="rotate({rotate} {x:.1f} {y:.1f})"' if rotate else ""
        self.add(
            f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" text-anchor="{anchor}" '
            f'fill="{fill or self.t["text"]}" font-weight="{weight}"'
            f' font-family="system-ui,-apple-system,Segoe UI,Roboto,sans-serif"{transform}>'
            f"{escape(body)}</text>"
        )


def _draw_axes(
    c: _Canvas,
    x_labels: list[str] | None,
    y_ticks: list[float],
    y_low: float,
    y_high: float,
    x_low: float,
    x_high: float,
    numeric_x: bool,
    layout: dict,
) -> None:
    x0, y0, w, h = _plot_box(c.left)
    span = (y_high - y_low) or 1.0

    for tick in y_ticks:
        y = y0 + h - (tick - y_low) / span * h
        if y < y0 - 1 or y > y0 + h + 1:
            continue
        c.add(f'<line x1="{x0}" y1="{y:.1f}" x2="{x0 + w}" y2="{y:.1f}" stroke="{c.t["grid"]}" stroke-width="1"/>')
        c.text(x0 - 10, y + 4, _fmt(tick), size=11, anchor="end", fill=c.t["muted"])

    c.add(f'<line x1="{x0}" y1="{y0 + h}" x2="{x0 + w}" y2="{y0 + h}" stroke="{c.t["grid"]}" stroke-width="1.5"/>')

    if numeric_x:
        xspan = (x_high - x_low) or 1.0
        for tick in _nice_ticks(x_low, x_high, 6):
            if tick < x_low or tick > x_high:
                continue
            x = x0 + (tick - x_low) / xspan * w
            c.text(x, y0 + h + 20, _fmt(tick), size=11, fill=c.t["muted"])
    elif x_labels:
        n = len(x_labels)
        step = max(1, n // 12)
        band = w / n
        rotate = -30 if max((len(s) for s in x_labels), default=0) > 6 and n > 6 else 0
        for i in range(0, n, step):
            x = x0 + band * (i + 0.5)
            label = x_labels[i]
            if len(label) > 18:
                label = label[:17] + "…"
            if rotate:
                c.text(x, y0 + h + 20, label, size=11, anchor="end", fill=c.t["muted"], rotate=rotate)
            else:
                c.text(x, y0 + h + 20, label, size=11, fill=c.t["muted"])

    c.text(x0 + w / 2, HEIGHT - 14, _axis_title(layout, "xaxis"), size=12, fill=c.t["muted"])
    ylabel = _axis_title(layout, "yaxis")
    if ylabel:
        c.text(16, y0 + h / 2, ylabel, size=12, fill=c.t["muted"], rotate=-90)


def _legend(c: _Canvas, names: list[str]) -> None:
    if len(names) < 2:
        return
    x = c.left
    y = HEIGHT - MARGIN["bottom"] + 46
    for i, name in enumerate(names[:8]):
        colour = _PALETTE[i % len(_PALETTE)]
        c.add(f'<rect x="{x}" y="{y - 9}" width="11" height="11" rx="2" fill="{colour}"/>')
        c.text(x + 17, y, name[:22], size=11, anchor="start", fill=c.t["muted"])
        x += 26 + 7 * min(len(name), 22)


def _render_pie(c: _Canvas, trace: dict) -> bool:
    labels = _trace_values(trace, "labels") or []
    values = _numeric(_trace_values(trace, "values") or [])
    if not labels or not values or len(labels) != len(values):
        return False
    total = sum(v for v in values if v > 0)
    if total <= 0:
        return False

    cx, cy, r = WIDTH / 2, HEIGHT / 2 + 6, min(WIDTH, HEIGHT) / 2 - 90
    angle = -math.pi / 2
    for i, (label, value) in enumerate(zip(labels, values)):
        if value <= 0:
            continue
        sweep = 2 * math.pi * value / total
        x1, y1 = cx + r * math.cos(angle), cy + r * math.sin(angle)
        angle += sweep
        x2, y2 = cx + r * math.cos(angle), cy + r * math.sin(angle)
        large = 1 if sweep > math.pi else 0
        colour = _PALETTE[i % len(_PALETTE)]
        c.add(
            f'<path d="M {cx:.1f} {cy:.1f} L {x1:.1f} {y1:.1f} '
            f'A {r:.1f} {r:.1f} 0 {large} 1 {x2:.1f} {y2:.1f} Z" fill="{colour}" '
            f'stroke="{c.t["bg"]}" stroke-width="2"/>'
        )
        mid = angle - sweep / 2
        pct = 100 * value / total
        if pct >= 4:
            lx, ly = cx + r * 0.68 * math.cos(mid), cy + r * 0.68 * math.sin(mid)
            c.text(lx, ly + 4, f"{pct:.0f}%", size=12, fill="#ffffff", weight="600")
    _legend(c, [str(x) for x in labels])
    return True


def _render_cartesian(c: _Canvas, traces: list[dict], layout: dict) -> bool:
    series = []
    for trace in traces:
        xs = _trace_values(trace, "x")
        ys = _numeric(_trace_values(trace, "y") or [])
        if xs is None or ys is None or len(xs) != len(ys) or not xs:
            continue
        series.append((trace, xs, ys))
    if not series:
        return False

    numeric_x = all(_numeric(xs) is not None for _, xs, _ in series)
    all_y = [v for _, _, ys in series for v in ys]
    y_low, y_high = min(all_y), max(all_y)
    if y_low > 0:
        y_low = 0.0
    if y_high < 0:
        y_high = 0.0
    ticks = _nice_ticks(y_low, y_high)
    y_low, y_high = min(ticks[0], y_low), max(ticks[-1], y_high)
    yspan = (y_high - y_low) or 1.0
    c.left = _left_margin([_fmt(t) for t in ticks], bool(_axis_title(layout, "yaxis")))
    x0, y0, w, h = _plot_box(c.left)

    if numeric_x:
        all_x = [v for _, xs, _ in series for v in (_numeric(xs) or [])]
        x_low, x_high = min(all_x), max(all_x)
        labels = None
    else:
        x_low, x_high = 0.0, 1.0
        labels = [str(v) for v in series[0][1]]

    _draw_axes(c, labels, ticks, y_low, y_high, x_low, x_high, numeric_x, layout)

    def sy(v: float) -> float:
        return y0 + h - (v - y_low) / yspan * h

    bars = [s for s in series if s[0].get("type") == "bar"]
    if bars:
        n = len(bars[0][1])
        band = w / max(1, n)
        group = band * 0.8 / len(bars)
        base = sy(0.0)
        for gi, (_, _, ys) in enumerate(bars):
            colour = _PALETTE[gi % len(_PALETTE)]
            for i, value in enumerate(ys):
                bx = x0 + band * i + band * 0.1 + group * gi
                top = sy(value)
                c.add(
                    f'<rect x="{bx:.1f}" y="{min(top, base):.1f}" width="{group:.1f}" '
                    f'height="{abs(base - top):.1f}" fill="{colour}" rx="2"/>'
                )

    for si, (trace, xs, ys) in enumerate(series):
        if trace.get("type") == "bar":
            continue
        colour = _PALETTE[si % len(_PALETTE)]
        if numeric_x:
            nx = _numeric(xs) or []
            xspan = (x_high - x_low) or 1.0
            points = [(x0 + (v - x_low) / xspan * w, sy(y)) for v, y in zip(nx, ys)]
        else:
            band = w / max(1, len(xs))
            points = [(x0 + band * (i + 0.5), sy(y)) for i, y in enumerate(ys)]

        mode = str(trace.get("mode") or "lines")
        if "lines" in mode or trace.get("type") == "scatter" and "markers" not in mode:
            path = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
            c.add(
                f'<polyline points="{path}" fill="none" stroke="{colour}" stroke-width="2" '
                f'stroke-linejoin="round" stroke-linecap="round"/>'
            )
        if "markers" in mode:
            radius = 3.5 if len(points) < 400 else 2
            for x, y in points[:3000]:
                c.add(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{radius}" fill="{colour}" fill-opacity="0.8"/>')

    _legend(c, [str(t.get("name") or f"series {i + 1}") for i, (t, _, _) in enumerate(series)])
    return True


def figure_to_svg(traces: list[dict], layout: dict, theme: str = "device") -> str | None:
    """Render `traces` as one self-contained SVG, or None if not drawable here."""
    palette = _THEMES.get(theme, _THEMES["dark"])
    c = _Canvas(palette)
    c.add(f'<rect width="{WIDTH}" height="{HEIGHT}" fill="{palette["bg"]}"/>')

    drawable = [t for t in traces if isinstance(t, dict)]
    if not drawable:
        return None

    kinds = {t.get("type") or "scatter" for t in drawable}
    if kinds <= {"pie"}:
        ok = _render_pie(c, drawable[0])
    elif kinds <= {"bar", "scatter", "scattergl"}:
        ok = _render_cartesian(c, drawable, layout)
    else:
        return None
    if not ok:
        return None

    title = _title_text(layout.get("title"))
    c.text(WIDTH / 2, 30, title, size=16, weight="600")

    body = "\n".join(c.parts)
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {WIDTH} {HEIGHT}" '
        f'width="100%" style="max-width:{WIDTH}px;height:auto;display:block" '
        f'role="img" aria-label="{escape(title or "chart")}">\n{body}\n</svg>'
    )


def standalone_html(chart_html: str, theme: str = "device") -> str | None:
    """Turn a generated Plotly page into one that renders with nothing beside it."""
    try:
        traces, layout = load_figure(chart_html)
    except (ValueError, TypeError) as exc:  # not a chart page
        del exc
        return None
    svg = figure_to_svg(traces, layout, theme)
    if svg is None:
        return None
    palette = _THEMES.get(theme, _THEMES["dark"])
    title = escape(_title_text(layout.get("title")) or "Chart")
    return (
        '<!DOCTYPE html>\n<html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        f"<title>{title}</title><style>"
        f"html,body{{margin:0;padding:1rem;background:{palette['bg']};color:{palette['text']};"
        "font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif}"
        "svg{width:100%;height:auto}</style></head><body>\n"
        f"{svg}\n</body></html>"
    )
