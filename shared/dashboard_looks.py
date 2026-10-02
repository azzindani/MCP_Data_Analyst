"""A dashboard's look is data: tokens, type, shape, spacing and a frame -- any number of them.

The engine's pages were drawn in three themes (device, light, dark) that differ only in a handful of
colours, so every dashboard looked like the same dashboard. A *look* is the rest of what makes one
design differ from another: the colour tokens of each mode, the type, the radius and shadow of a card,
the density of the grid, how the page is framed (a left rail, a console sidebar, a banner, a rounded
app frame) and what a KPI tile looks like. Plotly still draws every mark; the look only decides what
surrounds it and which colours it is handed.

A look is chosen in the page's style -- ``style.look`` is the name of a built-in (``lagoon``,
``harbor``, ``nocturne``, ``ledger``, ``slate``, ``signal``), the path of a saved look (.json), or the
look itself as a dict -- so it travels in the embedded spec and survives ``customize_dashboard``. A
look can also be *digested* from an HTML mockup: its CSS custom properties, radius, shadow and fonts
are read into this same vocabulary (``digest_html``), and what could not be read is said, not guessed.

Nothing here writes a caller's string into the page: every colour is checked against a strict
grammar, every number is clamped, every choice is one of a named set. A digested mockup therefore
cannot carry a rule, a URL or a script into the dashboard it styles.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

MODES = ("light", "dark", "device")
SHADOWS = ("none", "soft", "lift", "glow")
DENSITIES = ("compact", "comfortable", "airy")
CARDS = ("flat", "outlined", "raised", "glass")
FRAMES = ("plain", "rail", "sidebar", "banner")
HEADERS = ("bar", "plain", "gradient")
KPIS = ("plain", "tile", "gradient")
# System stacks only: a web font is a fetch, and the page is carried whole.
STACKS: dict[str, str] = {
    "system": "system-ui,-apple-system,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif",
    "humanist": "'Segoe UI',Candara,'Trebuchet MS',Optima,sans-serif",
    "serif": "Georgia,'Iowan Old Style','Times New Roman',serif",
    "mono": "ui-monospace,SFMono-Regular,Menlo,Consolas,monospace",
    "condensed": "'Arial Narrow','Roboto Condensed','Helvetica Neue',sans-serif",
    "rounded": "'Segoe UI Rounded','Nunito','Trebuchet MS','Segoe UI',system-ui,sans-serif",
}
REQUIRED_TOKENS = ("bg", "surface", "border", "text", "text-muted", "accent")
OPTIONAL_TOKENS = ("green", "orange", "red", "ground", "rail", "rail-ink", "header", "header-ink", "accent-ink")
TOKENS = REQUIRED_TOKENS + OPTIONAL_TOKENS
MAX_PALETTE = 16
MAX_TEXT = 200

_DEFAULTS = {
    "light": {"green": "#1a7f37", "orange": "#9a6700", "red": "#cf222e"},
    "dark": {"green": "#3fb950", "orange": "#f0883e", "red": "#f85149"},
}
_HEX = re.compile(r"^#(?:[0-9a-fA-F]{3,4}|[0-9a-fA-F]{6}|[0-9a-fA-F]{8})$")
_FN = re.compile(r"^(?:rgb|rgba|hsl|hsla)\(\s*[-+0-9.%\s,/deg]+\)$", re.IGNORECASE)

_SHADOW_CSS = {
    "none": "none",
    "soft": "0 1px 2px rgba(0,0,0,.06),0 8px 24px rgba(0,0,0,.07)",
    "lift": "0 2px 4px rgba(0,0,0,.10),0 14px 34px rgba(0,0,0,.16)",
    "glow": "0 0 0 1px color-mix(in srgb,var(--accent) 38%,transparent),0 10px 36px color-mix(in srgb,var(--accent) 26%,transparent)",
}
_DENSITY = {  # grid gap, card header padding, KPI padding
    "compact": (".5rem", ".4rem .7rem", ".5rem .75rem"),
    "comfortable": ("1rem", ".7rem 1rem", ".8rem 1rem"),
    "airy": ("1.5rem", ".95rem 1.25rem", "1.1rem 1.35rem"),
}


class LookError(ValueError):
    """A look that cannot be drawn; the message says which part and what is allowed."""


def is_colour(value: Any) -> bool:
    """A colour the page can be handed: hex, or rgb/rgba/hsl/hsla with plain numbers."""
    if not isinstance(value, str) or len(value) > 64:
        return False
    text = value.strip()
    return bool(_HEX.match(text) or _FN.match(text))


def _choice(where: str, value: Any, allowed: tuple[str, ...]) -> str:
    if value not in allowed:
        raise LookError(f"{where} is one of {', '.join(allowed)}; got {value!r}.")
    return str(value)


def _face(where: str, value: Any) -> str:
    """A named stack, or a short font-family list of plain names (quotes and commas only)."""
    if value in STACKS:
        return str(value)
    if isinstance(value, str) and 0 < len(value) <= 120 and re.fullmatch(r"[A-Za-z0-9 ,'\"-]+", value):
        return value
    raise LookError(f"{where} is one of {', '.join(STACKS)} or a list of font names (system fonts: a web font is a fetch).")


def _tokens(where: str, tokens: Any, mode: str, *, need_all: bool) -> dict[str, str]:
    if not isinstance(tokens, dict):
        raise LookError(f"{where} is a dict of colour tokens: {', '.join(TOKENS)}.")
    unknown = sorted(str(k) for k in tokens if k not in TOKENS)
    if unknown:
        raise LookError(f"{where} has unknown token(s): {', '.join(unknown)}. Tokens: {', '.join(TOKENS)}.")
    missing = [k for k in REQUIRED_TOKENS if k not in tokens]
    if need_all and missing:
        raise LookError(f"{where} needs {', '.join(missing)} (required: {', '.join(REQUIRED_TOKENS)}).")
    out: dict[str, str] = {}
    for key, value in tokens.items():
        if not is_colour(value):
            raise LookError(f"{where}.{key} is a colour (#rrggbb, rgb(...) or hsl(...)); got {value!r}.")
        out[str(key)] = str(value).strip()
    return out


def validate_look(look: Any) -> dict[str, Any]:
    """The look, checked and filled in. Raises LookError naming the part that cannot be drawn."""
    if not isinstance(look, dict):
        raise LookError("a look is a dict (or the name of a built-in look).")
    allowed = {
        "label", "about", "mode", "light", "dark", "font", "display", "radius", "shadow", "density", "card",
        "frame", "inset", "header", "kpi", "palette",
    }  # fmt: skip
    unknown = sorted(str(k) for k in look if k not in allowed)
    if unknown:
        raise LookError(f"a look has unknown key(s): {', '.join(unknown)}. It takes: {', '.join(sorted(allowed))}.")
    mode = _choice("look.mode", look.get("mode", "device"), MODES)
    out: dict[str, Any] = {"mode": mode}
    needs = {"light": mode in ("light", "device"), "dark": mode in ("dark", "device")}
    for name in ("light", "dark"):
        if name in look:
            out[name] = _tokens(f"look.{name}", look[name], name, need_all=needs[name])
        elif needs[name]:
            raise LookError(f"look.{name} is required when mode is {mode!r}: a dict of {', '.join(REQUIRED_TOKENS)}.")
    for key in ("label", "about"):
        if key in look:
            text = look[key]
            if not isinstance(text, str) or len(text) > MAX_TEXT:
                raise LookError(f"look.{key} is text of at most {MAX_TEXT} characters.")
            out[key] = text
    out["font"] = _face("look.font", look.get("font", "system"))
    out["display"] = _face("look.display", look.get("display", out["font"]))
    radius = look.get("radius", 12)
    if isinstance(radius, bool) or not isinstance(radius, (int, float)) or not 0 <= radius <= 40:
        raise LookError("look.radius is a card corner radius in pixels, 0 to 40.")
    out["radius"] = int(radius)
    out["shadow"] = _choice("look.shadow", look.get("shadow", "none"), SHADOWS)
    out["density"] = _choice("look.density", look.get("density", "comfortable"), DENSITIES)
    out["card"] = _choice("look.card", look.get("card", "outlined"), CARDS)
    out["frame"] = _choice("look.frame", look.get("frame", "plain"), FRAMES)
    out["header"] = _choice("look.header", look.get("header", "bar"), HEADERS)
    out["kpi"] = _choice("look.kpi", look.get("kpi", "plain"), KPIS)
    inset = look.get("inset", False)
    if not isinstance(inset, bool):
        raise LookError("look.inset is true or false: the page sits in a rounded frame on a ground colour.")
    out["inset"] = inset
    if "palette" in look:
        palette = look["palette"]
        if not isinstance(palette, list) or not 1 <= len(palette) <= MAX_PALETTE:
            raise LookError(f"look.palette is a list of 1 to {MAX_PALETTE} colours.")
        for i, c in enumerate(palette):
            if not is_colour(c):
                raise LookError(f"look.palette[{i}] is a colour; got {c!r}.")
        out["palette"] = [str(c).strip() for c in palette]
    return out


# ---------------------------------------------------------------------------
# Built-in looks
# ---------------------------------------------------------------------------

BUILTIN: dict[str, dict[str, Any]] = {
    "lagoon": {
        "label": "Lagoon",
        "about": "Pastel cards in a rounded app frame with a teal icon rail; light and dark.",
        "mode": "device",
        "light": {
            "ground": "#d7eeef", "bg": "#f3fafa", "surface": "#ffffff", "border": "#e2edf0", "text": "#1c3946",
            "text-muted": "#66838f", "accent": "#11a9ba", "green": "#18a56b", "orange": "#c98a10", "red": "#e8615e",
            "rail": "#15b0bf", "rail-ink": "#ffffff",
        },
        "dark": {
            "ground": "#0b181d", "bg": "#10232a", "surface": "#15303a", "border": "#224450", "text": "#e1f0f3",
            "text-muted": "#8eacb7", "accent": "#2ec6d3", "green": "#47d49a", "orange": "#f6c45b", "red": "#f38884",
            "rail": "#0d8995", "rail-ink": "#e9fbfc",
        },
        "font": "rounded", "display": "rounded", "radius": 20, "shadow": "soft", "density": "airy", "card": "raised",
        "frame": "rail", "inset": True, "header": "plain", "kpi": "tile",
        "palette": ["#11a9ba", "#5b59e0", "#f2b33d", "#e8615e", "#18a56b", "#8f8cff", "#2ec6d3", "#f38884"],
    },
    "harbor": {
        "label": "Harbor",
        "about": "An admin console: navy sidebar for the filters, crisp white cards, blue accent; light and dark.",
        "mode": "device",
        "light": {
            "ground": "#f2f4f8", "bg": "#f2f4f8", "surface": "#ffffff", "border": "#e5e9f0", "text": "#172133",
            "text-muted": "#66748a", "accent": "#1f7cf0", "green": "#1e9a61", "orange": "#c77f12", "red": "#d8474c",
            "rail": "#0d1f3e", "rail-ink": "#b6c4dc", "header": "#ffffff", "header-ink": "#172133",
        },
        "dark": {
            "ground": "#0a111c", "bg": "#0a111c", "surface": "#111a29", "border": "#1f2b3e", "text": "#e3e9f3",
            "text-muted": "#8b99af", "accent": "#3d94ff", "green": "#36bf80", "orange": "#e0a13a", "red": "#f06a6e",
            "rail": "#060c17", "rail-ink": "#8ea0bf", "header": "#111a29", "header-ink": "#e3e9f3",
        },
        "font": "system", "display": "system", "radius": 8, "shadow": "soft", "density": "comfortable",
        "card": "raised", "frame": "sidebar", "header": "bar", "kpi": "plain",
        "palette": ["#1f7cf0", "#6aa8f6", "#1e9a61", "#d8474c", "#c77f12", "#8b5cf6", "#0ea5a5", "#b9d7fb"],
    },
    "nocturne": {
        "label": "Nocturne",
        "about": "Dark gradient with a banner header, glowing gradient KPI tiles and glass cards; dark only.",
        "mode": "dark",
        "dark": {
            "ground": "#0f111c", "bg": "#0f111c", "surface": "#1f2337", "border": "#2a2f47", "text": "#eceefb",
            "text-muted": "#969cba", "accent": "#a855f7", "green": "#34d399", "orange": "#fbbf24", "red": "#fb7185",
            "header": "#3b1d6e", "header-ink": "#ffffff",
        },
        "font": "humanist", "display": "humanist", "radius": 18, "shadow": "glow", "density": "comfortable",
        "card": "glass", "frame": "banner", "header": "gradient", "kpi": "gradient",
        "palette": ["#a855f7", "#e046c4", "#22d3ee", "#3b82f6", "#34d399", "#fbbf24", "#fb7185", "#818cf8"],
    },
    "ledger": {
        "label": "Ledger",
        "about": "Warm paper, serif headings and thin rules: a printed management report; light and dark.",
        "mode": "device",
        "light": {
            "bg": "#faf7f2", "surface": "#fffdf9", "border": "#e6dfd2", "text": "#2b2620", "text-muted": "#7a6f60",
            "accent": "#9c3d1f", "green": "#3f7d4e", "orange": "#b7791f", "red": "#b3261e",
            "header": "#faf7f2", "header-ink": "#2b2620",
        },
        "dark": {
            "bg": "#1b1814", "surface": "#25211b", "border": "#3a342b", "text": "#efe8da", "text-muted": "#a89c88",
            "accent": "#e08a63", "green": "#79b88a", "orange": "#e0b04f", "red": "#e5736b",
            "header": "#1b1814", "header-ink": "#efe8da",
        },
        "font": "serif", "display": "serif", "radius": 3, "shadow": "none", "density": "comfortable",
        "card": "outlined", "frame": "plain", "header": "plain", "kpi": "plain",
        "palette": ["#9c3d1f", "#2f6f8f", "#3f7d4e", "#b7791f", "#6b4c9a", "#8a8a3a", "#c0576b", "#4d4d4d"],
    },
    "slate": {
        "label": "Slate",
        "about": "Neutral and compact: flat grey-blue cards, tight spacing, nothing decorative; light and dark.",
        "mode": "device",
        "light": {
            "bg": "#eef1f5", "surface": "#f9fafc", "border": "#d9dee7", "text": "#1d2733", "text-muted": "#5f6d7e",
            "accent": "#3b6fb6", "green": "#2f855a", "orange": "#b7791f", "red": "#c53030",
            "header": "#f9fafc", "header-ink": "#1d2733",
        },
        "dark": {
            "bg": "#141a22", "surface": "#1c242f", "border": "#2b3644", "text": "#dbe3ee", "text-muted": "#8a98ab",
            "accent": "#6c9bd8", "green": "#4caf82", "orange": "#d9a441", "red": "#e06666",
            "header": "#1c242f", "header-ink": "#dbe3ee",
        },
        "font": "system", "display": "system", "radius": 6, "shadow": "none", "density": "compact",
        "card": "flat", "frame": "plain", "header": "bar", "kpi": "plain",
        "palette": ["#3b6fb6", "#2f855a", "#b7791f", "#c53030", "#6b46c1", "#2c7a7b", "#97266d", "#718096"],
    },
    "signal": {
        "label": "Signal",
        "about": "High contrast and large type, colour-blind-safe palette: for a screen read from across a room; light and dark.",
        "mode": "device",
        "light": {
            "bg": "#ffffff", "surface": "#ffffff", "border": "#1a1a1a", "text": "#000000", "text-muted": "#3d3d3d",
            "accent": "#0057b8", "green": "#007a4d", "orange": "#b45f06", "red": "#c00000",
            "header": "#ffffff", "header-ink": "#000000",
        },
        "dark": {
            "bg": "#000000", "surface": "#0a0a0a", "border": "#f2f2f2", "text": "#ffffff", "text-muted": "#d0d0d0",
            "accent": "#56b4e9", "green": "#3ddc97", "orange": "#ffb000", "red": "#ff6b6b",
            "header": "#000000", "header-ink": "#ffffff",
        },
        "font": "system", "display": "condensed", "radius": 4, "shadow": "none", "density": "airy",
        "card": "outlined", "frame": "plain", "header": "bar", "kpi": "tile",
        "palette": ["#0072B2", "#E69F00", "#009E73", "#D55E00", "#56B4E9", "#CC79A7", "#F0E442", "#999999"],
    },
}


def builtin_names() -> list[str]:
    return list(BUILTIN)


def resolve_look(value: Any, resolve_path: Any = None) -> dict[str, Any]:
    """A look from a built-in name, a saved .json look, or a dict; validated either way."""
    if isinstance(value, dict):
        return validate_look(value)
    if not isinstance(value, str) or not value.strip():
        raise LookError("style.look is the name of a look, the path of a saved look (.json), or a look as a dict.")
    name = value.strip()
    if name.lower() in BUILTIN:
        return validate_look(BUILTIN[name.lower()])
    if name.lower().endswith(".json"):
        path = Path(resolve_path(name) if resolve_path else name)
        if not path.is_file():
            raise LookError(f"look file {path.name!r} does not exist. Built-in looks: {', '.join(BUILTIN)}.")
        try:
            return validate_look(json.loads(path.read_text(encoding="utf-8")))
        except json.JSONDecodeError as exc:
            raise LookError(f"look file {path.name!r} is not JSON: {exc}") from exc
    raise LookError(f"unknown look {name!r}. Built-in: {', '.join(BUILTIN)}; or a saved look (.json), or a look as a dict.")


def effective_theme(look: dict[str, Any] | None, theme: str) -> str:
    """The page's theme once the look has had its say: a dark-only look is dark whatever was asked."""
    if look is None:
        return theme
    if look["mode"] in ("light", "dark"):
        return look["mode"]
    return theme


def mode_tokens(look: dict[str, Any], mode: str) -> dict[str, str]:
    """Every token for one mode, with the optional ones filled in."""
    base = look.get(mode) or look.get("dark" if mode == "light" else "light") or {}
    tokens = {**_DEFAULTS[mode], **base}
    tokens.setdefault("ground", tokens["bg"])
    tokens.setdefault("rail", tokens["accent"])
    tokens.setdefault("rail-ink", "#ffffff")
    tokens.setdefault("header", tokens["surface"])
    tokens.setdefault("header-ink", tokens["text"])
    tokens.setdefault("accent-ink", "#ffffff")
    return tokens


def chart_theme(look: dict[str, Any], mode: str) -> dict[str, Any]:
    """What a Plotly figure needs to sit on this look's cards: their colour, the text colour, the grid."""
    t = mode_tokens(look, mode)
    return {"bg": t["surface"], "font": t["text"], "grid": t["border"], "palette": look.get("palette")}


def _vars(tokens: dict[str, str]) -> str:
    return "".join(f"--{k}:{v};" for k, v in tokens.items())


def _frame_css(look: dict[str, Any]) -> str:
    frame, radius = look["frame"], look["radius"]
    css = ""
    if look["inset"]:
        css += (
            "html{background:var(--ground)}"
            f"body{{max-width:92rem;margin:1.5rem auto;border-radius:{round(radius * 1.4)}px;overflow:hidden;"
            "box-shadow:var(--shadow);background:var(--bg)}"
            "@media(max-width:48rem){body{margin:0;border-radius:0}}"
        )
    if frame == "rail":
        css += (
            f"body:has(.tabs){{padding-left:5rem{';position:relative' if look['inset'] else ''}}}"
            f".tabs{{position:{'absolute' if look['inset'] else 'fixed'};top:0;left:0;bottom:0;width:5rem;"
            "flex-direction:column;flex-wrap:nowrap;margin:0;"
            "padding:1rem .375rem;gap:.375rem;overflow:auto;background:var(--rail);z-index:20;align-items:stretch}"
            ".tabs .tab-btn{background:transparent;border:0;border-radius:calc(var(--r)*.7);color:var(--rail-ink);"
            "padding:.625rem .25rem;font-size:.6875rem;line-height:1.2;white-space:normal;word-break:break-word;opacity:.85}"
            ".tabs .tab-btn:hover{opacity:1;background:color-mix(in srgb,var(--rail-ink) 14%,transparent)}"
            ".tabs .tab-btn[aria-selected=true]{background:color-mix(in srgb,var(--rail-ink) 24%,transparent);"
            "color:var(--rail-ink);opacity:1}"
            "@media(max-width:48rem){body:has(.tabs){padding-left:0}.tabs{position:static;width:auto;flex-direction:row;"
            "flex-wrap:wrap;padding:.5rem .875rem;background:var(--rail)}}"
        )
    elif frame == "sidebar":
        css += (
            "@media(min-width:68.75rem){body.sidebar .filter-bar{background:var(--rail);color:var(--rail-ink);"
            "border-right:0}body.sidebar .filter-bar .flbl{color:var(--rail-ink);opacity:.7}"
            "body.sidebar .filter-bar .pill{background:transparent;border-color:color-mix(in srgb,var(--rail-ink) 30%,transparent);"
            "color:var(--rail-ink)}body.sidebar .filter-bar .pill.active{background:var(--accent);border-color:var(--accent);"
            "color:var(--accent-ink)}body.sidebar .filter-bar .ddbtn,body.sidebar .filter-bar .ninp{background:"
            "color-mix(in srgb,var(--rail-ink) 10%,transparent);border-color:transparent;color:var(--rail-ink)}"
            "body.sidebar .filter-bar .nrng{flex-direction:column;align-items:stretch;gap:.25rem}"
            "body.sidebar .filter-bar .nsep{display:none}body.sidebar .filter-bar .dinp,body.sidebar .filter-bar .ninp"
            "{min-width:0;width:100%;flex:0 0 auto}}"
        )
    elif frame == "banner":
        css += (
            "header{padding-top:clamp(1.25rem,4vw,2.25rem);padding-bottom:clamp(2rem,5vw,3.25rem)}"
            "header h1{font-size:clamp(1.4rem,3.2vw,2.1rem);letter-spacing:-.01em}"
            ".kpi-row{margin-top:-1.75rem;position:relative;z-index:2}"
        )
    return css


def _header_css(look: dict[str, Any]) -> str:
    style = look["header"]
    if style == "plain":
        return (
            "header{background:transparent;border-bottom:0;color:var(--text)}header h1{color:var(--text)}"
            ".btn:not(.btn-p){background:var(--surface)}"
        )
    if style == "gradient":
        return (
            "header{background:linear-gradient(120deg,var(--header),color-mix(in srgb,var(--header) 55%,var(--accent)));"
            "border-bottom:0;color:var(--header-ink)}header h1{color:var(--header-ink)}"
            "header .row-ctr{color:color-mix(in srgb,var(--header-ink) 75%,transparent)}"
            "header .btn{background:color-mix(in srgb,var(--header-ink) 12%,transparent);border-color:transparent;"
            "color:var(--header-ink)}header .btn-p{background:var(--accent);color:var(--accent-ink)}"
        )
    return "header{background:var(--header);color:var(--header-ink)}header h1{color:var(--accent)}"


def _kpi_css(look: dict[str, Any]) -> str:
    style = look["kpi"]
    tones = (
        ".kpi-card:nth-child(4n+1),.kt0{--c:var(--accent)}.kpi-card:nth-child(4n+2),.kt1{--c:var(--green)}"
        ".kpi-card:nth-child(4n+3),.kt2{--c:var(--orange)}.kpi-card:nth-child(4n),.kt3{--c:var(--red)}"
    )
    tiles = ".kpi-card,.cc-kpi"
    if style == "tile":
        return (
            f"{tiles}{{border-color:transparent;background:color-mix(in srgb,var(--c,var(--accent)) 13%,var(--surface))}}"
            + tones
            + ".kpi-val,.cc-kpi .kpi-big{color:color-mix(in srgb,var(--c,var(--accent)) 82%,var(--text))}"
        )
    if style == "gradient":
        return (
            f"{tiles}{{border-color:transparent;color:#fff;background:linear-gradient(135deg,var(--c,var(--accent)),"
            "color-mix(in srgb,var(--c,var(--accent)) 55%,#000))}"
            + tones
            + ".kpi-val,.kpi-lbl,.kpi-trend,.cc-kpi .kpi-big,.cc-kpi h3,.cc-kpi .kpi-sub,.cc-kpi .kpi-delta,"
            ".cc-kpi .kpi-delta span{color:#fff}.kpi-lbl,.cc-kpi .kpi-sub{opacity:.8}"
            ".cc-kpi .cc-hdr{border-bottom:0}"
        )
    return ""


def _card_css(look: dict[str, Any]) -> str:
    kind = look["card"]
    shells = ".cc,.kpi-card,.tbl-card,.mbox"
    if kind == "flat":
        css = f"{shells}{{border:0;box-shadow:none}}.cc-hdr{{border-bottom:0}}"
    elif kind == "raised":
        css = f"{shells}{{border:0;box-shadow:var(--shadow)}}.cc-hdr{{border-bottom:0}}"
    elif kind == "glass":
        css = (
            f"{shells}{{background:color-mix(in srgb,var(--surface) 72%,transparent);"
            "-webkit-backdrop-filter:blur(14px);backdrop-filter:blur(14px);"
            "border:1px solid color-mix(in srgb,var(--border) 70%,transparent);box-shadow:var(--shadow)}"
            ".cc-hdr{border-bottom:0}"
        )
    else:
        css = f"{shells}{{border:1px solid var(--border);box-shadow:none}}"
    return css


def look_css(look: dict[str, Any]) -> str:
    """The stylesheet for a validated look, to follow the engine's own CSS."""
    mode = look["mode"]
    light, dark = mode_tokens(look, "light"), mode_tokens(look, "dark")
    if mode == "light":
        root = f":root{{{_vars(light)}color-scheme:light}}"
    elif mode == "dark":
        root = f":root{{{_vars(dark)}color-scheme:dark}}"
    else:
        root = (
            f":root{{{_vars(light)}color-scheme:light}}"
            f"@media(prefers-color-scheme:dark){{:root:not([data-theme=light]){{{_vars(dark)}color-scheme:dark}}}}"
            f":root[data-theme=dark]{{{_vars(dark)}color-scheme:dark}}"
            f":root[data-theme=light]{{{_vars(light)}color-scheme:light}}"
        )
    radius = look["radius"]
    gap, hdr_pad, kpi_pad = _DENSITY[look["density"]]
    body_font, display_font = STACKS.get(look["font"], look["font"]), STACKS.get(look["display"], look["display"])
    shape = (
        f":root{{--r:{radius}px;--shadow:{_SHADOW_CSS[look['shadow']]}}}"
        f".cc,.kpi-card,.tbl-card,.mbox{{border-radius:var(--r)}}"
        f".btn,.ddbtn,.ninp,.ddsrch,.tbl-pager button{{border-radius:calc(var(--r)*.45)}}"
        f".cgrid,.kpi-row{{gap:{gap}}}.cc-hdr{{padding:{hdr_pad}}}.kpi-card{{padding:{kpi_pad}}}"
        f"body{{font-family:{body_font}}}"
        f"header h1,.kpi-val,.cc-hdr h3,.cc-sec,.sec-hdr{{font-family:{display_font}}}"
        ".btn-p,.pill.active,.tabs .tab-btn[aria-selected=true]{color:var(--accent-ink)}"
    )
    return "/* look */" + root + shape + _card_css(look) + _header_css(look) + _kpi_css(look) + _frame_css(look)


def body_classes(look: dict[str, Any] | None, name: str = "") -> list[str]:
    """Classes for <body>: the engine's own `sidebar` where the look frames the page with one."""
    if look is None:
        return []
    classes = [f"frame-{look['frame']}"]
    if look["frame"] == "sidebar":
        classes.append("sidebar")
    if name and re.fullmatch(r"[a-z0-9_-]{1,32}", name):
        classes.append(f"look-{name}")
    return classes


# ---------------------------------------------------------------------------
# A look from an HTML mockup
# ---------------------------------------------------------------------------

_ROLE_WORDS: list[tuple[str, tuple[str, ...]]] = [
    ("rail-ink", ("rail-ink", "side-ink", "sidebar-ink", "nav-ink", "side-hi")),
    ("rail", ("rail", "side", "sidebar", "nav")),
    ("ground", ("ground", "backdrop", "canvas", "page")),
    ("surface", ("card", "surface", "panel", "tile", "paper", "box")),
    ("border", ("border", "line", "divider", "rule", "stroke", "outline", "hairline")),
    ("text-muted", ("muted", "subtle", "secondary", "dim", "faint", "caption", "ink-2", "text-2")),
    ("text", ("ink", "text", "fg", "foreground", "body")),
    ("accent", ("accent", "primary", "brand", "teal", "blue", "indigo", "violet", "purple")),
    ("green", ("green", "good", "success", "positive", "up", "mint")),
    ("red", ("red", "bad", "danger", "negative", "error", "down", "rose", "coral")),
    ("orange", ("orange", "amber", "warn", "warning", "yellow")),
    ("bg", ("bg", "background", "frame", "app", "base")),
]
_DECL = re.compile(r"--([A-Za-z0-9_-]+)\s*:\s*([^;}{]+)")
_RULE = re.compile(r"([^{}@]+)\{([^{}]*)\}")
_RADIUS_PX = re.compile(r"(\d+(?:\.\d+)?)\s*(px|rem|em)")


_NAMED = {
    "black": "#000000", "white": "#ffffff", "red": "#ff0000", "green": "#008000", "blue": "#0000ff",
    "yellow": "#ffff00", "orange": "#ffa500", "purple": "#800080", "gray": "#808080", "grey": "#808080",
    "teal": "#008080", "navy": "#000080", "silver": "#c0c0c0", "maroon": "#800000", "lime": "#00ff00",
    "aqua": "#00ffff", "fuchsia": "#ff00ff", "olive": "#808000", "pink": "#ffc0cb", "brown": "#a52a2a",
}  # fmt: skip
_HEX6 = re.compile(r"^#([0-9a-fA-F]{6})$")
_HEX3 = re.compile(r"^#([0-9a-fA-F]{3})$")


def _rgb(value: str) -> tuple[int, int, int] | None:
    value = _NAMED.get(value.strip().lower(), value.strip())
    if m := _HEX6.match(value):
        h = m.group(1)
        return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    if m := _HEX3.match(value):
        h = m.group(1)
        return int(h[0] * 2, 16), int(h[1] * 2, 16), int(h[2] * 2, 16)
    return None


def _mix(a: str, b: str, share: float) -> str | None:
    """`a` with `share` of `b` mixed in, as hex; None when either is not a plain hex colour."""
    ra, rb = _rgb(a), _rgb(b)
    if ra is None or rb is None:
        return None
    return "#" + "".join(f"{round(x + (y - x) * share):02x}" for x, y in zip(ra, rb, strict=True))


def _saturation(value: str) -> float:
    rgb = _rgb(value)
    if rgb is None:
        return 0.0
    hi, lo = max(rgb), min(rgb)
    return 0.0 if hi == 0 else (hi - lo) / hi * (hi / 255)


def _clean(value: str) -> str:
    return value.strip().rstrip(";").strip()


def _role_of(name: str) -> str:
    lowered = name.lower()
    for role, words in _ROLE_WORDS:
        for word in words:
            if lowered == word or lowered.startswith(word + "-") or lowered.endswith("-" + word) or word in lowered.split("-"):
                return role
    return ""


def _resolve(value: str, vars_: dict[str, str], depth: int = 0) -> str:
    match = re.fullmatch(r"var\(\s*--([A-Za-z0-9_-]+)\s*(?:,[^)]*)?\)", value.strip())
    if match and depth < 4 and match.group(1) in vars_:
        return _resolve(vars_[match.group(1)], vars_, depth + 1)
    return _NAMED.get(value.strip().lower(), value.strip())


def _blocks(css: str) -> dict[str, dict[str, str]]:
    """Declarations by selector kind: 'light' (root), 'dark' (a dark scheme or data-theme=dark), and the rest."""
    blocks: dict[str, dict[str, str]] = {"light": {}, "dark": {}}
    for media in re.finditer(r"@media[^{]*prefers-color-scheme\s*:\s*dark[^{]*\{(.*?)\}\s*\}", css, flags=re.S):
        for _sel, body in _RULE.findall(media.group(1) + "}"):
            blocks["dark"].update({k: _clean(v) for k, v in _DECL.findall(body)})
    stripped = re.sub(r"@media[^{]*\{(?:[^{}]*\{[^{}]*\})*[^{}]*\}", "", css, flags=re.S)
    for selector, body in _RULE.findall(stripped):
        sel = selector.strip()
        decls = {k: _clean(v) for k, v in _DECL.findall(body)}
        if not decls:
            continue
        if re.search(r"data-theme\s*=\s*[\"']?dark|\.dark\b", sel) and ":not" not in sel:
            blocks["dark"].update(decls)
        elif re.fullmatch(r":root|html|body|:root\s*,\s*html", sel):
            blocks["light"].update(decls)
        elif ":root" in sel and "dark" not in sel and ":not" not in sel:
            blocks["light"].update(decls)
    return blocks


def _rule_props(css: str, selectors: tuple[str, ...]) -> dict[str, str]:
    found: dict[str, str] = {}
    for selector, body in _RULE.findall(css):
        names = [s.strip() for s in selector.split(",")]
        if any(n in selectors for n in names):
            for prop, value in re.findall(r"([a-z-]+)\s*:\s*([^;}]+)", body):
                found.setdefault(prop, _clean(value))
    return found


def _tokens_from(vars_: dict[str, str]) -> tuple[dict[str, str], dict[str, str]]:
    """Map a mockup's custom properties to look tokens by name; return (tokens, {variable: why it was not used})."""
    tokens: dict[str, str] = {}
    skipped: dict[str, str] = {}
    for name, raw in vars_.items():
        value = _resolve(raw, vars_)
        if not is_colour(value):
            continue
        role = _role_of(name)
        if not role:
            continue
        if role in tokens:
            skipped[name] = f"{role} is already set by an earlier variable"
            continue
        tokens[role] = value
    return tokens, skipped


def _classify_shadow(value: str) -> str:
    if not value or value.strip() == "none":
        return "none"
    blur = [float(n) for n in re.findall(r"(\d+(?:\.\d+)?)px", value)]
    biggest = max(blur) if blur else 0
    if "inset" in value or biggest < 3:
        return "soft" if biggest else "none"
    if re.search(r"(?:#[0-9a-f]{3,8}|rgba?\([^)]*\))", value, flags=re.I) and biggest >= 28:
        return "lift"
    return "soft"


def _pick_face(text: str) -> str:
    lowered = text.lower()
    if "mono" in lowered or "consolas" in lowered or "menlo" in lowered:
        return "mono"
    if "serif" in lowered and "sans" not in lowered.split("serif")[0][-8:]:
        return "serif"
    if "nunito" in lowered or "rounded" in lowered or "quicksand" in lowered or "poppins" in lowered:
        return "rounded"
    if "condensed" in lowered or "narrow" in lowered:
        return "condensed"
    if any(w in lowered for w in ("segoe", "candara", "trebuchet", "optima", "source sans", "lato", "open sans", "manrope", "sora")):
        return "humanist"
    return "system"


def digest_html(text: str, name: str = "") -> tuple[dict[str, Any], dict[str, Any]]:
    """Read a mockup's design tokens into a look. Returns (look, report); the report says what was read, guessed, not found."""
    css = "\n".join(re.findall(r"<style[^>]*>(.*?)</style>", text, flags=re.S | re.I)) or text
    css = re.sub(r"/\*.*?\*/", "", css, flags=re.S)
    blocks = _blocks(css)
    light_tokens, skipped = _tokens_from(blocks["light"])
    dark_tokens, _ = _tokens_from({**blocks["light"], **blocks["dark"]}) if blocks["dark"] else ({}, {})
    # A page colour named only as the ground, or a card colour only as the page's, is still those tokens.
    for tokens in (light_tokens, dark_tokens):
        if tokens and "bg" not in tokens and "ground" in tokens:
            tokens["bg"] = tokens["ground"]
        if tokens and "surface" not in tokens and "bg" in tokens and "ground" in tokens:
            tokens["surface"] = tokens["bg"]
    notes: list[str] = []
    found_by_name = sorted(light_tokens)

    # A token the variables did not name: read it from the rules that use it.
    body_rule = _rule_props(css, ("body", "html"))
    card_rule = _rule_props(css, (".card", ".panel", ".tile", ".widget", ".kpi", ".kpi-card"))
    guessed: list[str] = []

    def take(token: str, candidate: str, where: str) -> None:
        value = _resolve(candidate, blocks["light"])
        if token not in light_tokens and is_colour(value):
            light_tokens[token] = value
            guessed.append(f"{token} (from {where})")

    take("bg", body_rule.get("background-color", body_rule.get("background", "")), "body background")
    take("text", body_rule.get("color", ""), "body colour")
    take("surface", card_rule.get("background-color", card_rule.get("background", "")), "the card rule")
    take("border", re.sub(r"^[\d.]+(?:px|rem)\s+\w+\s+", "", card_rule.get("border", "")), "the card border")
    if "bg" not in light_tokens and "ground" in light_tokens:
        light_tokens["bg"] = light_tokens["ground"]
        guessed.append("bg (from ground)")
    if "bg" in light_tokens and "ground" in light_tokens and "surface" not in light_tokens:
        light_tokens["surface"] = light_tokens["bg"]
        guessed.append("surface (from bg)")
    # A mockup that does not name a muted text colour has one: the text, part way to the page.
    for tokens in (light_tokens, dark_tokens):
        if tokens and "text-muted" not in tokens and "text" in tokens and "bg" in tokens:
            if derived := _mix(tokens["text"], tokens["bg"], 0.45):
                tokens["text-muted"] = derived
                guessed.append("text-muted (the text colour, 45% of the way to the page)")
    if "accent" not in light_tokens:
        everything = {c for c in re.findall(r"#[0-9a-fA-F]{6}\b|#[0-9a-fA-F]{3}\b", css)} - set(light_tokens.values())
        vivid = sorted(everything, key=_saturation, reverse=True)
        if vivid and _saturation(vivid[0]) > 0.35:
            light_tokens["accent"] = vivid[0]
            guessed.append("accent (the most saturated colour in the mockup)")
    missing = [t for t in REQUIRED_TOKENS if t not in light_tokens]
    if missing:
        raise LookError(
            "could not read these from the mockup: " + ", ".join(missing) + ". It should define them as CSS custom "
            "properties on :root (e.g. --bg, --card, --border, --ink, --muted, --accent) or style body and .card."
        )

    radius_value = blocks["light"].get("radius") or blocks["light"].get("r") or blocks["light"].get("radius-card")
    radius_source = "a --radius variable"
    if not radius_value:
        radius_value, radius_source = card_rule.get("border-radius", ""), "the card rule"
    radius = 12
    if radius_value:
        m = _RADIUS_PX.search(_resolve(radius_value, blocks["light"]))
        if m:
            px = float(m.group(1)) * (16 if m.group(2) in ("rem", "em") else 1)
            radius = max(0, min(40, round(px)))
        else:
            notes.append(f"radius {radius_value!r} is not in px or rem: 12 used.")
    else:
        notes.append("no card radius found: 12 used.")

    shadow_text = blocks["light"].get("shadow", "") or card_rule.get("box-shadow", "")
    shadow = _classify_shadow(_resolve(shadow_text, blocks["light"])) if shadow_text else "none"
    font_text = " ".join(v for k, v in blocks["light"].items() if "font" in k) or body_rule.get("font-family", "")
    display_text = " ".join(v for k, v in blocks["light"].items() if "font" in k and "display" in k) or font_text

    look: dict[str, Any] = {
        "label": (name or "Digested look")[:60],
        "about": "Read from an HTML mockup.",
        "mode": "device" if dark_tokens and all(t in dark_tokens for t in REQUIRED_TOKENS) else "light",
        "light": {k: v for k, v in light_tokens.items() if k in TOKENS},
        "font": _pick_face(font_text) if font_text else "system",
        "display": _pick_face(display_text) if display_text else "system",
        "radius": radius,
        "shadow": shadow,
        "density": "comfortable",
        "card": "raised" if shadow != "none" else "outlined",
        "frame": "rail" if "rail" in light_tokens else "plain",
        "header": "bar",
        "kpi": "plain",
    }
    dark_dark = bool(re.match(r"^#(?:0|1|2)", light_tokens["bg"])) and not dark_tokens
    if dark_tokens and look["mode"] == "device":
        look["dark"] = {k: v for k, v in dark_tokens.items() if k in TOKENS}
        for token in REQUIRED_TOKENS:
            look["dark"].setdefault(token, light_tokens[token])
    elif dark_dark:
        look["mode"] = "dark"
        look["dark"] = look.pop("light")
        notes.append("the mockup's colours are dark and it has no light set: the look is dark only.")
    palette = [v for k, v in blocks["light"].items() if is_colour(_resolve(v, blocks["light"])) and _role_of(k) in ("", "accent", "green", "red", "orange")]
    palette = [_resolve(c, blocks["light"]) for c in palette][:8]
    if len(palette) >= 3:
        look["palette"] = list(dict.fromkeys(palette))
    unread = sorted(k for k, v in blocks["light"].items() if is_colour(_resolve(v, blocks["light"])) and not _role_of(k))
    report = {
        "read_by_name": found_by_name,
        "read_from_rules": guessed,
        "radius": f"{radius}px from {radius_source}" if radius_value else "default 12px",
        "shadow": shadow,
        "dark_set": bool(look.get("dark")),
        "colours_not_mapped_to_a_token": unread[:12],
        "duplicate_roles": sorted(skipped)[:8],
        "notes": notes,
    }
    return validate_look(look), report
