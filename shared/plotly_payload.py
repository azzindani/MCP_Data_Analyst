"""Read the figure back out of a generated Plotly page.

A chart HTML file carries its own data: the traces and layout sit in the
`Plotly.newPlot(...)` call. Recovering them lets other code work with the figure
without the tool that made it having to hand anything over.

A regex cannot do the extraction — payloads nest brackets and embed base64 blobs
and titles containing braces, so `.*?\\]` stops at the first inner delimiter.
The scanner below tracks depth and steps over string literals.
"""

from __future__ import annotations

import base64
import json
import re
import struct

# Plotly picks the narrowest dtype that fits, so an integer-valued chart arrives
# as "i1"/"i2" rather than "f8".
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


def scan_balanced(text: str, start: int) -> int:
    """Return the index just past the balanced [...] or {...} beginning at `start`."""
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


def split_newplot(html: str) -> tuple[str, str, str, str, str, str]:
    """Split chart HTML around the Plotly.newPlot call.

    Returns (before, call_prefix, data_str, separator, layout_str, after); the six
    pieces concatenate back into `html` exactly.
    """
    # Search from the end. A self-contained page inlines plotly.min.js, and the
    # library's own source contains `Plotly.newPlot(` long before the page's
    # real call to it -- taking the first match parsed the minified bundle,
    # failed, and every caller of this silently fell back. The page's own call
    # is the last one, and each candidate is checked by actually scanning it, so
    # neither ordering nor a stray mention in a title can pick the wrong one.
    calls = list(re.finditer(r"Plotly\.newPlot\(", html))
    if not calls:
        raise ValueError("Could not find Plotly.newPlot call in HTML. Not a valid Plotly chart HTML.")

    last_error: Exception | None = None
    for call in reversed(calls):
        try:
            data_start = html.find("[", call.end())
            if data_start == -1:
                raise ValueError("Plotly.newPlot call has no trace array.")
            data_end = scan_balanced(html, data_start)

            layout_start = html.find("{", data_end)
            if layout_start == -1:
                raise ValueError("Plotly.newPlot call has no layout object.")
            layout_end = scan_balanced(html, layout_start)

            json.loads(html[data_start:data_end])
            json.loads(html[layout_start:layout_end])
        except (ValueError, json.JSONDecodeError) as exc:
            last_error = exc
            continue

        return (
            html[: call.start()],
            html[call.start() : data_start],
            html[data_start:data_end],
            html[data_end:layout_start],
            html[layout_start:layout_end],
            html[layout_end:],
        )

    raise ValueError(f"No parsable Plotly.newPlot payload in HTML: {last_error}")


def load_figure(html: str) -> tuple[list, dict]:
    """Return (traces, layout) exactly as the browser would parse them."""
    _, _, data_str, _, layout_str, _ = split_newplot(html)
    return json.loads(data_str), json.loads(layout_str)


def decode_array(field: object) -> list | None:
    """Decode a trace axis: a plain list, or Plotly's {"dtype","bdata"} form."""
    if isinstance(field, list):
        return list(field)
    if isinstance(field, dict) and "bdata" in field:
        code = _PLOTLY_DTYPES.get(str(field.get("dtype", "f8")))
        if code is None:
            return None
        raw = base64.b64decode(field["bdata"])
        n = len(raw) // struct.calcsize(f"<{code}")
        return list(struct.unpack(f"<{n}{code}", raw))
    return None


def encode_array(values: list, original: object) -> object:
    """Re-encode values, matching the original field's encoding."""
    if isinstance(original, dict) and "bdata" in original:
        dtype = str(original.get("dtype", "f8"))
        code = _PLOTLY_DTYPES.get(dtype)
        if code is None:
            dtype, code = "f8", "d"
        if code not in ("f", "d"):
            values = [int(v) for v in values]
        raw = struct.pack(f"<{len(values)}{code}", *values)
        return {"dtype": dtype, "bdata": base64.b64encode(raw).decode("ascii")}
    return values
