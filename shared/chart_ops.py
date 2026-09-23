"""Edit a chart's figure with ops over an allow-list of paths. No MCP imports.

customize_chart takes a fixed list of keywords -- title, labels, colours,
sort, highlight, annotations, value labels, size -- and anything outside it
(a log axis, the legend on the right, bars drawn as a line, a second y axis, a
target line) had no route but rewriting the page by hand. Opening the whole
figure to arbitrary JSON would let a caller write anything Plotly ignores, or
break the page. So each op names a path from an allow-list, and each path
takes the one kind of value it can draw:

    {"op": "set", "path": "layout.yaxis.type", "value": "log"}
    {"op": "set", "path": "data[1].yaxis", "value": "y2"}
    {"op": "set", "path": "data[*].line.width", "value": 3}
    {"op": "reference_line", "axis": "y", "value": 1000, "label": "Target"}

A path or a value outside the list is refused by name, with the paths that
are allowed.
"""

from __future__ import annotations

import re
from typing import Any

MAX_CHART_OPS = 50

_HEX = re.compile(r"^#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{6}|[0-9a-fA-F]{8})$")
_RGB = re.compile(r"^rgba?\(\s*\d{1,3}\s*,\s*\d{1,3}\s*,\s*\d{1,3}\s*(?:,\s*(?:0|1|0?\.\d+)\s*)?\)$")


class ChartOpError(ValueError):
    """An op that cannot be applied, named precisely enough to fix."""


# What a value may be. Each is (description, check).
def _enum(*values: Any):
    # Type and value: 0 == False in Python, and 0 is not a hovermode.
    return (" | ".join(map(repr, values)), lambda v: any(type(v) is type(x) and v == x for x in values))


def _num(lo: float, hi: float):
    return (
        f"a number from {lo:g} to {hi:g}",
        lambda v: not isinstance(v, bool) and isinstance(v, (int, float)) and lo <= v <= hi,
    )


def _int(lo: int, hi: int):
    return (
        f"a whole number from {lo} to {hi}",
        lambda v: not isinstance(v, bool) and isinstance(v, int) and lo <= v <= hi,
    )


def _text(n: int):
    return (f"text of at most {n} characters", lambda v: isinstance(v, str) and len(v) <= n)


_BOOL = ("true or false", lambda v: isinstance(v, bool))
_COLOUR = (
    "a colour such as '#58a6ff' or 'rgb(88,166,255)'",
    lambda v: isinstance(v, str) and bool(_HEX.match(v) or _RGB.match(v)),
)
_COLOURS = (
    "a list of 1 to 30 colours",
    lambda v: isinstance(v, list) and 1 <= len(v) <= 30 and all(_COLOUR[1](c) for c in v),
)
_RANGE = (
    "[low, high]: two numbers, or two dates as text",
    lambda v: (
        isinstance(v, list)
        and len(v) == 2
        and all((isinstance(x, (int, float)) and not isinstance(x, bool)) or isinstance(x, str) for x in v)
    ),
)

_AXIS: dict[str, tuple] = {
    "type": _enum("linear", "log", "date", "category"),
    "range": _RANGE,
    "autorange": ("true, false or 'reversed'", lambda v: isinstance(v, bool) or v == "reversed"),
    "title.text": _text(120),
    "tickformat": _text(40),
    "tickprefix": _text(8),
    "ticksuffix": _text(8),
    "tickangle": _num(-90, 90),
    "showgrid": _BOOL,
    "zeroline": _BOOL,
}
_LAYOUT: dict[str, tuple] = {
    "showlegend": _BOOL,
    "legend.orientation": _enum("h", "v"),
    "legend.x": _num(-2, 3),
    "legend.y": _num(-2, 3),
    "legend.xanchor": _enum("auto", "left", "center", "right"),
    "legend.yanchor": _enum("auto", "top", "middle", "bottom"),
    "barmode": _enum("group", "stack", "overlay", "relative"),
    "bargap": _num(0, 1),
    "hovermode": _enum("closest", "x", "y", "x unified", "y unified", False),
    "height": _int(160, 3000),
    "width": _int(200, 4000),
    "margin.l": _int(0, 400),
    "margin.r": _int(0, 400),
    "margin.t": _int(0, 400),
    "margin.b": _int(0, 400),
    "colorway": _COLOURS,
    "plot_bgcolor": _COLOUR,
    "paper_bgcolor": _COLOUR,
}
for _axis in ("xaxis", "yaxis", "yaxis2"):
    for _field, _check in _AXIS.items():
        _LAYOUT[f"{_axis}.{_field}"] = _check

_TRACE: dict[str, tuple] = {
    "type": _enum("bar", "scatter"),
    "mode": _enum("lines", "markers", "lines+markers", "lines+text", "markers+text", "lines+markers+text"),
    "name": _text(120),
    "marker.color": _COLOUR,
    "marker.size": _num(1, 60),
    "line.color": _COLOUR,
    "line.width": _num(0, 20),
    "line.dash": _enum("solid", "dot", "dash", "longdash", "dashdot"),
    "line.shape": _enum("linear", "spline", "hv", "vh"),
    "opacity": _num(0, 1),
    "yaxis": _enum("y", "y2"),
    "showlegend": _BOOL,
    "visible": ("true, false or 'legendonly'", lambda v: isinstance(v, bool) or v == "legendonly"),
    "texttemplate": _text(80),
    "textposition": _enum("inside", "outside", "auto", "none", "top center", "bottom center", "middle center"),
    "fill": _enum("none", "tozeroy", "tonexty"),
}

CHART_OPS = ("set", "reference_line")
_PATH = re.compile(r"^data\[(\d+|\*)\]\.(.+)$")
# Only these trace types can be switched: both are drawn from x and y. A pie,
# a heatmap or a histogram holds its data in fields the other does not read.
_SWITCHABLE = frozenset({"bar", "scatter"})
_AXISLESS = frozenset({"pie", "sunburst", "treemap", "funnelarea", "icicle", "indicator", "sankey"})
_THREE_D = frozenset({"scatter3d", "surface", "mesh3d", "cone", "streamtube", "volume", "isosurface"})


def _allowed(table: dict[str, tuple], prefix: str) -> str:
    groups: dict[str, list[str]] = {}
    for key in table:
        head, _, tail = key.partition(".")
        groups.setdefault(head, []).append(tail)
    parts = [f"{prefix}{h}" + (f".{{{','.join(t)}}}" if any(t) else "") for h, t in groups.items()]
    return ", ".join(parts)


def _put(obj: dict, dotted: str, value: Any) -> None:
    *heads, last = dotted.split(".")
    for head in heads:
        nxt = obj.get(head)
        if not isinstance(nxt, dict):
            nxt = {}
            obj[head] = nxt
        obj = nxt
    obj[last] = value


def _trace_type(trace: dict) -> str:
    return str(trace.get("type") or "scatter")


def apply_chart_ops(traces: list, layout: dict, ops: Any) -> list[str]:
    """Apply `ops` to the figure in place. Returns what each op did; raises ChartOpError."""
    if not isinstance(ops, list) or not ops:
        raise ChartOpError(f"ops must be a list of {{op, ...}}; ops: {', '.join(CHART_OPS)}")
    if len(ops) > MAX_CHART_OPS:
        raise ChartOpError(f"ops has {len(ops)} edits; one call takes at most {MAX_CHART_OPS}")
    types = {_trace_type(t) for t in traces if isinstance(t, dict)}
    axisless = bool(types) and types <= _AXISLESS
    three_d = bool(types & _THREE_D) or isinstance(layout.get("scene"), dict)
    applied: list[str] = []
    for i, op in enumerate(ops):
        at = f"ops[{i}]"
        if not isinstance(op, dict) or op.get("op") not in CHART_OPS:
            got = op.get("op") if isinstance(op, dict) else op
            raise ChartOpError(f"{at} op={got!r} is not an edit. Valid: {', '.join(CHART_OPS)}")
        if op["op"] == "reference_line":
            applied.append(_reference_line(at, op, layout, axisless or three_d))
            continue
        extra = sorted(str(k) for k in op if k not in ("op", "path", "value"))
        if extra:
            raise ChartOpError(f"{at} set has unknown key(s): {', '.join(extra)}. It takes: path, value")
        if "value" not in op:
            raise ChartOpError(f"{at} set needs a value")
        path, value = op.get("path"), op["value"]
        if not isinstance(path, str):
            raise ChartOpError(f"{at}.path must be text such as 'layout.yaxis.type' or 'data[0].marker.color'")
        if path.startswith("layout."):
            field = path[len("layout.") :]
            if field not in _LAYOUT:
                raise ChartOpError(f"{at}.path {path!r} is not editable. Layout paths: {_allowed(_LAYOUT, 'layout.')}")
            if (axisless or three_d) and field.split(".")[0] in ("xaxis", "yaxis", "yaxis2"):
                kind = "3D" if three_d else ", ".join(sorted(types))
                raise ChartOpError(f"{at}: a {kind} chart has no {field.split('.')[0]} to edit")
            _check(at, path, _LAYOUT[field], value)
            _put(layout, field, value)
            applied.append(f"{path} → {value!r}")
            continue
        match = _PATH.match(path)
        if not match:
            raise ChartOpError(
                f"{at}.path {path!r} is not editable. Paths start 'layout.' or 'data[N].' (N a trace, or * for every trace)"
            )
        which, field = match.groups()
        if field not in _TRACE:
            raise ChartOpError(f"{at}.path {path!r} is not editable. Trace paths: {_allowed(_TRACE, 'data[N].')}")
        if which == "*":
            targets = list(range(len(traces)))
        else:
            n = int(which)
            if n >= len(traces):
                raise ChartOpError(
                    f"{at}.path names trace {n}, and the chart has {len(traces)} (0 to {len(traces) - 1})"
                )
            targets = [n]
        _check(at, path, _TRACE[field], value)
        for n in targets:
            trace = traces[n]
            if field == "type" and _trace_type(trace) not in _SWITCHABLE:
                raise ChartOpError(
                    f"{at}: trace {n} is a {_trace_type(trace)}, which holds its data in fields a {value} does not "
                    "read; only bar and scatter (line) traces switch type"
                )
            if field in ("yaxis",) and (axisless or three_d):
                raise ChartOpError(f"{at}: this chart has no y axis to move trace {n} onto")
            _put(trace, field, value)
            if field == "type" and value == "scatter" and "mode" not in trace:
                trace["mode"] = "lines+markers"
            if field == "yaxis" and value == "y2":
                # A trace moved to y2 with no y2 defined is drawn against an
                # axis Plotly invents across the whole plot, labels over the
                # first. The second axis is the right-hand one, over the first.
                second = layout.get("yaxis2")
                layout["yaxis2"] = {
                    "overlaying": "y",
                    "side": "right",
                    "showgrid": False,
                    **(second if isinstance(second, dict) else {}),
                }
        applied.append(f"{path} → {value!r}")
    return applied


def _check(at: str, path: str, rule: tuple, value: Any) -> None:
    what, ok = rule
    if not ok(value):
        raise ChartOpError(f"{at}: {path} takes {what}; got {value!r}")


def _reference_line(at: str, op: dict, layout: dict, no_axes: bool) -> str:
    extra = sorted(str(k) for k in op if k not in ("op", "axis", "value", "label", "color", "dash"))
    if extra:
        raise ChartOpError(
            f"{at} reference_line has unknown key(s): {', '.join(extra)}. It takes: axis, value, label, color, dash"
        )
    if no_axes:
        raise ChartOpError(f"{at}: this chart has no axes to draw a reference line across")
    axis = op.get("axis", "y")
    if axis not in ("x", "y"):
        raise ChartOpError(f"{at}.axis must be 'x' or 'y'; got {axis!r}")
    value = op.get("value")
    if isinstance(value, bool) or not isinstance(value, (int, float, str)) or value == "":
        raise ChartOpError(
            f"{at}.value is where the line sits on the {axis} axis: a number, or a date or category as text"
        )
    label = op.get("label", "")
    _check(at, "label", _text(80), label)
    color = op.get("color", "#d62728")
    _check(at, "color", _COLOUR, color)
    dash = op.get("dash", "dash")
    _check(at, "dash", _enum("solid", "dot", "dash", "longdash", "dashdot"), dash)
    line: dict = {"type": "line", "line": {"color": color, "width": 2, "dash": dash}}
    if axis == "y":
        line.update({"xref": "paper", "x0": 0, "x1": 1, "yref": "y", "y0": value, "y1": value})
    else:
        line.update({"yref": "paper", "y0": 0, "y1": 1, "xref": "x", "x0": value, "x1": value})
    if label:
        # The label rides on the line. As an annotation it sat at y=value,
        # which a log axis reads as log10 -- 70000 put "Target" at 10^70000,
        # off the chart, with the line itself drawn in the right place.
        line["label"] = {
            "text": label,
            "textposition": "end",
            "yanchor": "bottom",
            "font": {"color": color, "size": 12},
        }
    shapes = layout.get("shapes")
    layout["shapes"] = [*(shapes if isinstance(shapes, list) else []), line]
    return f"reference line at {axis}={value!r}" + (f" ({label})" if label else "")
