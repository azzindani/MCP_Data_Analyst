"""customize_chart — modify existing Plotly chart HTML without regenerating. No MCP imports."""

from __future__ import annotations

import base64
import json
import logging
import re
import struct
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
_ADV = str(Path(__file__).resolve().parents[2] / "data_advanced")
for _p in (str(_ROOT), _ADV):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from shared.file_utils import atomic_write_text, embed_content, resolve_path
from shared.progress import fail, info, ok

logger = logging.getLogger(__name__)

_HIGHLIGHT_COLOR = "#EF553B"
_DEFAULT_TRACE_COLOR = "#636efa"


def _scan_balanced(text: str, start: int) -> int:
    """Return the index just past the balanced [...] or {...} beginning at `start`.

    A regex cannot do this: chart payloads nest brackets and embed base64 blobs
    and titles that contain braces, so `.*?\\]` stops at the first inner
    delimiter. Scanning tracks depth and skips over string literals.
    """
    opener = text[start]
    closer = {"[": "]", "{": "}"}.get(opener)
    if closer is None:
        raise ValueError(f"Expected '[' or '{{' at offset {start}, found {text[start]!r}.")

    depth = 0
    in_string = False
    quote = ""
    i = start
    while i < len(text):
        ch = text[i]
        if in_string:
            if ch == "\\":
                i += 2
                continue
            if ch == quote:
                in_string = False
        elif ch in ("'", '"'):
            in_string = True
            quote = ch
        elif ch == opener:
            depth += 1
        elif ch == closer:
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    raise ValueError("Unbalanced Plotly payload — the chart HTML is truncated or corrupt.")


def _split_newplot(html: str) -> tuple[str, str, str, str, str, str]:
    """Split chart HTML around the Plotly.newPlot call.

    Returns (before, call_prefix, data_str, separator, layout_str, after) such that
    concatenating all six reproduces `html` exactly.
    """
    call = re.search(r"Plotly\.newPlot\(", html)
    if call is None:
        raise ValueError("Could not find Plotly.newPlot call in HTML. Not a valid Plotly chart HTML.")

    data_start = html.find("[", call.end())
    if data_start == -1:
        raise ValueError("Plotly.newPlot call has no trace array.")
    data_end = _scan_balanced(html, data_start)

    layout_start = html.find("{", data_end)
    if layout_start == -1:
        raise ValueError("Plotly.newPlot call has no layout object.")
    layout_end = _scan_balanced(html, layout_start)

    return (
        html[: call.start()],
        html[call.start() : data_start],
        html[data_start:data_end],
        html[data_end:layout_start],
        html[layout_start:layout_end],
        html[layout_end:],
    )


def _extract_plotly_json(html: str) -> tuple[str, str, str, str, str]:
    """Extract the Plotly call. Returns (before, call_prefix, data_json, layout_part, after).

    `layout_part` keeps its leading separator so the five pieces still concatenate
    back into the original document.
    """
    before, call_prefix, data_str, separator, layout_str, after = _split_newplot(html)
    return before, call_prefix, data_str, separator + layout_str, after


# Plotly picks the narrowest dtype that fits the data, so an integer-valued bar
# chart arrives as "i1"/"i2" rather than "f8". Assuming float64 made the decode
# read zero elements out of the buffer and sorting such a chart failed outright.
_PLOTLY_DTYPES = {
    "f4": "f",
    "f8": "d",
    "i1": "b",
    "i2": "h",
    "i4": "i",
    "i8": "q",
    "u1": "B",
    "u2": "H",
    "u4": "I",
    "u8": "Q",
}


def _decode_plotly_y(y_field: object) -> list[float] | None:
    """Decode a trace's y values: either a plain list or Plotly's compact
    {"dtype": ..., "bdata": <base64>} binary-encoded numeric array."""
    if isinstance(y_field, list):
        return list(y_field)
    if isinstance(y_field, dict) and "bdata" in y_field:
        code = _PLOTLY_DTYPES.get(str(y_field.get("dtype", "f8")))
        if code is None:
            return None
        raw = base64.b64decode(y_field["bdata"])
        n = len(raw) // struct.calcsize(f"<{code}")
        return list(struct.unpack(f"<{n}{code}", raw))
    return None


def _encode_plotly_y(values: list[float], original_y_field: object) -> object:
    """Re-encode sorted y values, matching the original field's encoding."""
    if isinstance(original_y_field, dict) and "bdata" in original_y_field:
        dtype = str(original_y_field.get("dtype", "f8"))
        code = _PLOTLY_DTYPES.get(dtype)
        if code is None:
            dtype, code = "f8", "d"
        if code not in ("f", "d"):
            values = [int(v) for v in values]
        raw = struct.pack(f"<{len(values)}{code}", *values)
        return {"dtype": dtype, "bdata": base64.b64encode(raw).decode("ascii")}
    return values


def _title_dict(text: str) -> dict:
    return {"text": text}


def _apply_value_labels(trace: dict) -> None:
    """Turn on printed values for one trace, respecting its plot type."""
    trace["textposition"] = "outside" if trace.get("type") == "bar" else "top center"
    trace.setdefault("texttemplate", "%{y}")
    mode = trace.get("mode")
    if isinstance(mode, str) and "text" not in mode:
        trace["mode"] = f"{mode}+text"


def customize_chart(
    chart_path: str,
    title: str = "",
    x_label: str = "",
    y_label: str = "",
    color_scheme: list[str] = None,
    sort_bars: str = "",
    highlight: list[str] = None,
    annotations: list[dict] = None,
    show_value_labels: bool = False,
    width: int = 0,
    height: int = 0,
    output_path: str = "",
    return_content: bool = False,
) -> dict:
    """Modify existing Plotly HTML chart. Changes title labels colors annotations."""
    progress = []
    try:
        path = resolve_path(chart_path)
        if not path.exists():
            return {
                "success": False,
                "error": f"Chart file not found: {path.name}",
                "hint": "Provide the path to an HTML chart file generated by generate_chart().",
                "progress": [fail("File not found", path.name)],
                "token_estimate": 20,
            }
        if path.suffix.lower() != ".html":
            return {
                "success": False,
                "error": "chart_path must be an HTML file.",
                "hint": "Only HTML files from generate_chart() or generate_dashboard() can be customized.",
                "progress": [fail("Wrong file type", path.suffix)],
                "token_estimate": 20,
            }

        html = path.read_text(encoding="utf-8")

        # Every customization mutates the parsed trace/layout objects and the
        # document is re-serialised once at the end. Editing the JSON as text
        # silently produced unbalanced braces, so the chart parsed as valid HTML
        # but rendered as a blank page.
        try:
            before, call_prefix, data_str, separator, layout_str, after = _split_newplot(html)
            traces = json.loads(data_str)
            layout = json.loads(layout_str)
        except (ValueError, json.JSONDecodeError) as exc:
            return {
                "success": False,
                "error": f"Could not parse chart: {exc}",
                "hint": "Only HTML files from generate_chart() or generate_dashboard() can be customized.",
                "progress": [fail("Chart unparsable", str(exc))],
                "token_estimate": 20,
            }

        changes_applied: list[str] = []

        if title:
            layout["title"] = _title_dict(title)
            changes_applied.append(f"title → '{title}'")
            progress.append(info("Title updated", title))

        if x_label:
            axis = layout.get("xaxis")
            layout["xaxis"] = axis if isinstance(axis, dict) else {}
            layout["xaxis"]["title"] = _title_dict(x_label)
            changes_applied.append(f"x-axis label → '{x_label}'")
            progress.append(info("X-axis label", x_label))

        if y_label:
            axis = layout.get("yaxis")
            layout["yaxis"] = axis if isinstance(axis, dict) else {}
            layout["yaxis"]["title"] = _title_dict(y_label)
            changes_applied.append(f"y-axis label → '{y_label}'")
            progress.append(info("Y-axis label", y_label))

        if sort_bars:
            direction = sort_bars.lower()
            if direction not in ("asc", "desc"):
                return {
                    "success": False,
                    "error": f"Invalid sort_bars value: {sort_bars}",
                    "hint": "Use 'asc' or 'desc'.",
                    "progress": [fail("Invalid sort_bars value", sort_bars)],
                    "token_estimate": 20,
                }

        if sort_bars or highlight:
            try:
                trace = traces[0]
                categories = trace["x"]
                if not isinstance(categories, list):
                    raise ValueError("trace has no categorical x values")
            except (KeyError, IndexError, TypeError, ValueError) as exc:
                return {
                    "success": False,
                    "error": f"Could not parse chart data: {exc}",
                    "hint": "sort_bars/highlight only work on single-trace bar charts from generate_chart().",
                    "progress": [fail("Chart data unparsable", str(exc))],
                    "token_estimate": 20,
                }

            if sort_bars:
                direction = sort_bars.lower()
                values = _decode_plotly_y(trace.get("y"))
                if values is None or len(values) != len(categories):
                    return {
                        "success": False,
                        "error": "Could not parse chart values for sorting.",
                        "hint": "sort_bars only works on single-trace bar charts from generate_chart().",
                        "progress": [fail("Chart values unparsable", "")],
                        "token_estimate": 20,
                    }
                order = sorted(range(len(categories)), key=lambda i: values[i], reverse=(direction == "desc"))
                categories = [categories[i] for i in order]
                trace["x"] = categories
                trace["y"] = _encode_plotly_y([values[i] for i in order], trace.get("y"))
                changes_applied.append(f"bars sorted {direction}")
                progress.append(info("Bars sorted", direction))

            if highlight:
                marker = trace.get("marker", {}) if isinstance(trace.get("marker"), dict) else {}
                base_color = marker.get("color") if isinstance(marker.get("color"), str) else _DEFAULT_TRACE_COLOR
                highlight_set = {str(h) for h in highlight}
                marker["color"] = [_HIGHLIGHT_COLOR if str(c) in highlight_set else base_color for c in categories]
                trace["marker"] = marker
                changes_applied.append(f"{len(highlight)} categor{'y' if len(highlight) == 1 else 'ies'} highlighted")
                progress.append(info("Highlighted", str(highlight)))

            traces[0] = trace

        if color_scheme:
            # Per-point colours for a single categorical trace, palette otherwise —
            # a bar chart wants one colour per bar, a multi-trace chart one per trace.
            first = traces[0] if traces else {}
            categories = first.get("x") if isinstance(first, dict) else None
            if len(traces) == 1 and isinstance(categories, list) and not highlight:
                marker = first.get("marker", {}) if isinstance(first.get("marker"), dict) else {}
                marker["color"] = [color_scheme[i % len(color_scheme)] for i in range(len(categories))]
                first["marker"] = marker
                traces[0] = first
            layout["colorway"] = list(color_scheme)
            changes_applied.append(f"colors → {color_scheme[:3]}...")
            progress.append(info("Color scheme", str(color_scheme[:3])))

        if width or height:
            if width:
                layout["width"] = width
                changes_applied.append(f"width → {width}")
            if height:
                layout["height"] = height
                changes_applied.append(f"height → {height}")
            progress.append(info("Dimensions", f"{width or '?'}×{height or '?'}"))

        if show_value_labels:
            for trace in traces:
                if isinstance(trace, dict):
                    _apply_value_labels(trace)
            changes_applied.append("value labels → enabled")
            progress.append(info("Value labels", "enabled"))

        if annotations:
            layout["annotations"] = [
                {
                    "x": ann.get("x", 0),
                    "y": ann.get("y", 0),
                    "text": ann.get("text", ""),
                    "showarrow": ann.get("showarrow", True),
                    "arrowhead": 2,
                    "font": {"size": 12},
                }
                for ann in annotations
            ]
            changes_applied.append(f"{len(annotations)} annotation(s) added")
            progress.append(info("Annotations", f"{len(annotations)} added"))

        if not changes_applied:
            return {
                "success": False,
                "error": "No customization parameters provided.",
                "hint": "Provide at least one of: title, x_label, y_label, color_scheme, sort_bars, annotations, show_value_labels, width, height.",
                "progress": [fail("Nothing to change", "")],
                "token_estimate": 20,
            }

        html = before + call_prefix + json.dumps(traces) + separator + json.dumps(layout) + after
        if title:
            html = re.sub(r"<title>[^<]*</title>", f"<title>{title}</title>", html)

        # A chart that no longer parses is a blank page in the browser, and the
        # file would still look like valid HTML on disk. Fail loudly instead.
        try:
            _, _, check_data, _, check_layout, _ = _split_newplot(html)
            json.loads(check_data)
            json.loads(check_layout)
        except (ValueError, json.JSONDecodeError) as exc:  # pragma: no cover - defensive
            return {
                "success": False,
                "error": f"Customization produced an unrenderable chart: {exc}",
                "hint": "Report this chart_path — the source chart's payload could not be rewritten safely.",
                "progress": [fail("Output would not render", str(exc))],
                "token_estimate": 20,
            }

        if output_path:
            out_path = resolve_path(output_path)
        else:
            out_path = path.parent / f"{path.stem}_customized{path.suffix}"
        atomic_write_text(str(out_path), html)
        progress.append(ok("Chart customized", f"{len(changes_applied)} changes applied"))

        result = {
            "success": True,
            "op": "customize_chart",
            "input": path.name,
            "output_path": str(out_path),
            "changes_applied": changes_applied,
            "progress": progress,
        }
        embed_content(result, out_path, return_content)
        result["token_estimate"] = len(str(result)) // 4
        return result

    except Exception as exc:
        logger.exception("customize_chart error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Ensure chart_path points to an HTML file from generate_chart() or generate_dashboard().",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }
