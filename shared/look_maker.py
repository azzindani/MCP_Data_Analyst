"""A look from a few choices: an accent, a mood and a mode -- how a designer starts, not how a token sheet ends.

A look is thirty-odd decisions (two sets of colour tokens, the type, the corners, the shadow, how the marks are
drawn, how the page is framed). Defining one by hand means making all of them and keeping the colours legible.
`look_from_brand` makes them from three: the accent colour, a mood, and light/dark/device. Every colour it writes
is checked for contrast against the card it sits on (text 12:1, secondary text and the accent 4.5:1) and moved
along its lightness until it reads, in light and in dark; the series colours are the colour-blind-safe set with the
accent first. What it returns is an ordinary look, so it can be changed key by key (`overrides`), saved, and named
in a page's style.
"""

from __future__ import annotations

import colorsys
from typing import Any

from shared.dashboard_looks import LookError, _rgb, validate_look

# Okabe-Ito: a reader with any common form of colour blindness can tell the series apart.
OKABE_ITO = ["#0072B2", "#E69F00", "#009E73", "#D55E00", "#56B4E9", "#CC79A7", "#F0E442", "#999999"]

# mood -> what it chooses. `neutral` is how much of the accent's hue the greys carry.
MOODS: dict[str, dict[str, Any]] = {
    "calm": {
        "about": "Soft and quiet: rounded raised cards, a gentle curve, light grid.",
        "neutral": 0.16,
        "look": {
            "font": "source-sans-3", "radius": 14, "shadow": "soft", "density": "comfortable", "card": "raised",
            "header": "plain", "kpi": "plain", "frame": "plain", "tabs": "pills", "bg": "solid",
            "chart": {"line_width": 2, "line_shape": "spline", "markers": "none", "area": 14, "bar_radius": 6, "bar_gap": 45,
                      "grid": "soft", "axis_line": False},
            "type": {"title_weight": 600},
        },
    },
    "bold": {
        "about": "Loud and confident: gradient banner and tiles, heavy lines, segmented tabs.",
        "neutral": 0.12,
        "look": {
            "font": "sora", "radius": 10, "shadow": "lift", "density": "comfortable", "card": "raised",
            "header": "gradient", "kpi": "gradient", "frame": "banner", "tabs": "segmented", "bg": "solid",
            "chart": {"line_width": 3, "line_shape": "linear", "markers": "all", "area": 22, "bar_radius": 3, "bar_gap": 30,
                      "grid": "soft"},
            "type": {"title_weight": 700},
        },
    },
    "editorial": {
        "about": "A printed page: serif headings in small capitals, hairlines, no shadows, nothing between the numbers.",
        "neutral": 0.10,
        "look": {
            "font": "source-sans-3", "display": "serif", "radius": 2, "shadow": "none", "density": "airy", "card": "outlined",
            "header": "plain", "kpi": "minimal", "frame": "plain", "tabs": "underline", "bg": "solid",
            "chart": {"line_width": 1.5, "line_shape": "linear", "markers": "ends", "area": 0, "bar_radius": 0, "bar_gap": 55,
                      "grid": "none", "axis_line": True},
            "type": {"title_case": "upper", "tracking": 0.06, "title_weight": 600, "numerals": "proportional"},
        },
    },
    "technical": {
        "about": "A console: compact, outlined, monospaced titles, a dotted grid, a sidebar of filters.",
        "neutral": 0.12,
        "look": {
            "font": "manrope", "display": "mono", "radius": 4, "shadow": "none", "density": "compact", "card": "outlined",
            "header": "bar", "kpi": "outline", "frame": "sidebar", "tabs": "underline", "bg": "grid",
            "chart": {"line_width": 1.5, "line_shape": "linear", "markers": "all", "area": 0, "bar_radius": 0, "bar_gap": 25,
                      "grid": "dotted", "axis_line": True},
            "type": {"numerals": "tabular", "scale": 0.95},
        },
    },
    "playful": {
        "about": "Round and friendly: big corners, tinted tiles, dotted ground, a curve with marked ends.",
        "neutral": 0.30,
        "look": {
            "font": "nunito", "radius": 22, "shadow": "soft", "density": "airy", "card": "raised", "header": "plain",
            "kpi": "tile", "frame": "plain", "tabs": "pills", "bg": "dots",
            "chart": {"line_width": 3, "line_shape": "spline", "markers": "ends", "area": 20, "bar_radius": 10, "bar_gap": 40,
                      "grid": "none"},
            "type": {"title_weight": 700},
        },
    },
}  # fmt: skip
BRAND_KEYS = ("accent", "mood", "mode", "font", "label")
_TEXT_RATIO, _MUTED_RATIO, _ACCENT_RATIO = 12.0, 4.5, 4.5


def _hex(rgb: tuple[float, float, float]) -> str:
    return "#" + "".join(f"{round(max(0.0, min(1.0, c)) * 255):02x}" for c in rgb)


def _hsl(hex_colour: str) -> tuple[float, float, float]:
    r, g, b = (c / 255 for c in _rgb(hex_colour) or (0, 0, 0))
    h, light, s = colorsys.rgb_to_hls(r, g, b)
    return h, s, light


def _from_hsl(h: float, s: float, light: float) -> str:
    return _hex(colorsys.hls_to_rgb(h % 1.0, max(0.0, min(1.0, light)), max(0.0, min(1.0, s))))


def _luminance(hex_colour: str) -> float:
    def channel(c: int) -> float:
        v = c / 255
        return v / 12.92 if v <= 0.03928 else ((v + 0.055) / 1.055) ** 2.4

    r, g, b = _rgb(hex_colour) or (0, 0, 0)
    return 0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b)


def contrast(a: str, b: str) -> float:
    """WCAG contrast ratio of two colours, 1 to 21."""
    hi, lo = sorted((_luminance(a), _luminance(b)), reverse=True)
    return (hi + 0.05) / (lo + 0.05)


def _legible(colour: str, on: str, ratio: float, *, darker: bool) -> str:
    """`colour`, moved along its lightness (darker or lighter) until it reads at `ratio` on `on`."""
    h, s, light = _hsl(colour)
    for _ in range(100):
        out = _from_hsl(h, s, light)
        if contrast(out, on) >= ratio:
            return out
        light += -0.01 if darker else 0.01
        if not 0.0 <= light <= 1.0:
            break
    return "#000000" if darker else "#ffffff"


def _ink_on(colour: str) -> str:
    return "#ffffff" if contrast("#ffffff", colour) >= contrast("#111111", colour) else "#111111"


def _hue_gap(a: str, b: str) -> float:
    d = abs(_hsl(a)[0] - _hsl(b)[0]) * 360
    return min(d, 360 - d)


def look_from_brand(
    accent: str, mood: str = "calm", mode: str = "device", font: str | None = None, label: str = ""
) -> tuple[dict[str, Any], dict[str, Any]]:
    """A complete, validated look from an accent colour, a mood and a mode, and the report of what it chose.

    The accent is a #rrggbb (or #rgb) colour; it is moved along its lightness, never its hue, until it reads on the
    card in each mode. `font` is one of the look's font names; it replaces the mood's body face.
    """
    if not isinstance(accent, str) or _rgb(accent) is None:
        raise LookError(f"brand.accent is a colour as #rrggbb (or #rgb); got {accent!r}.")
    if mood not in MOODS:
        raise LookError(f"brand.mood is one of {', '.join(MOODS)}; got {mood!r}.")
    if mode not in ("light", "dark", "device"):
        raise LookError(f"brand.mode is one of light, dark, device; got {mode!r}.")
    chosen = MOODS[mood]
    hue, sat, light = _hsl(accent)
    neutral = chosen["neutral"] if sat > 0.05 else 0.04  # a grey accent gets grey neutrals
    report: dict[str, Any] = {"mood": mood, "contrast": {}}

    surface = "#ffffff"
    light_tokens = {
        "ground": _from_hsl(hue, neutral, 0.96),
        "bg": _from_hsl(hue, neutral, 0.96),
        "surface": surface,
        "border": _from_hsl(hue, neutral, 0.89),
        "text": _legible(_from_hsl(hue, min(0.35, neutral + 0.15), 0.12), surface, _TEXT_RATIO, darker=True),
        "text-muted": _legible(_from_hsl(hue, neutral, 0.42), surface, _MUTED_RATIO, darker=True),
    }
    accent_light = _legible(_from_hsl(hue, sat, light), surface, _ACCENT_RATIO, darker=True)
    light_tokens.update(
        accent=accent_light,
        **{"accent-ink": _ink_on(accent_light), "rail": _legible(accent_light, "#ffffff", 4.5, darker=True), "rail-ink": "#ffffff"},
        header=light_tokens["bg"],
        **{"header-ink": light_tokens["text"]},
    )
    dark_surface = _from_hsl(hue, neutral + 0.06, 0.125)
    dark_tokens = {
        "ground": _from_hsl(hue, neutral + 0.06, 0.08),
        "bg": _from_hsl(hue, neutral + 0.06, 0.08),
        "surface": dark_surface,
        "border": _from_hsl(hue, neutral + 0.04, 0.21),
        "text": _legible(_from_hsl(hue, 0.2, 0.93), dark_surface, _TEXT_RATIO, darker=False),
        "text-muted": _legible(_from_hsl(hue, neutral, 0.66), dark_surface, _MUTED_RATIO, darker=False),
    }
    accent_dark = _legible(_from_hsl(hue, sat, max(light, 0.55)), dark_surface, _ACCENT_RATIO, darker=False)
    rail_dark = _from_hsl(hue, 0.35, 0.18)
    dark_tokens.update(
        accent=accent_dark,
        **{"accent-ink": _ink_on(accent_dark), "rail": rail_dark, "rail-ink": dark_tokens["text"]},
        header=dark_tokens["bg"],
        **{"header-ink": dark_tokens["text"]},
    )
    for name, tokens in (("light", light_tokens), ("dark", dark_tokens)):
        s_ = tokens["surface"]
        report["contrast"][name] = {
            "text": round(contrast(tokens["text"], s_), 1),
            "text-muted": round(contrast(tokens["text-muted"], s_), 1),
            "accent": round(contrast(tokens["accent"], s_), 1),
        }
    if accent_light.lower() != _from_hsl(hue, sat, light).lower():
        report["accent_moved"] = f"{accent} -> {accent_light} on light cards, {accent_dark} on dark, so it reads"

    palette = [accent_light] + [c for c in OKABE_ITO if _hue_gap(c, accent_light) > 25]
    base = {k: v for k, v in chosen["look"].items() if k not in ("chart", "type")}
    look: dict[str, Any] = {
        "label": label or f"{mood.title()} in {accent}",
        "about": f"{chosen['about']} Accent {accent}, made by look_from_brand.",
        "mode": mode,
        **base,
        "chart": dict(chosen["look"]["chart"]),
        "type": dict(chosen["look"]["type"]),
        "palette": palette[:8],
    }
    if font:
        look["font"] = font
    if mode in ("light", "device"):
        look["light"] = light_tokens
    if mode in ("dark", "device"):
        look["dark"] = dark_tokens
    report["chose"] = {k: look[k] for k in ("font", "radius", "shadow", "density", "card", "header", "kpi", "frame", "tabs", "bg")}
    return validate_look(look), report
