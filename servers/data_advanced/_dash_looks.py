"""dashboard_looks: the looks a dashboard can wear, and new ones from a tweak or an HTML mockup."""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

_ROOT = str(Path(__file__).resolve().parents[2])
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from shared.dashboard_looks import (
    BUILTIN,
    GRAMMAR,
    TOKENS,
    LookError,
    digest_html,
    grammar,
    mode_tokens,
    resolve_look,
    validate_look,
)
from shared.file_utils import atomic_write_text, error_text, hint_for_error, resolve_path
from shared.look_maker import BRAND_KEYS, MOODS, look_from_brand
from shared.mockup_layout import digest_arrangement
from shared.progress import fail, info, ok
from shared.receipt import append_receipt
from shared.version_control import snapshot_if_exists

logger = logging.getLogger(__name__)

MAX_MOCKUP_BYTES = 5_000_000


def _summary(name: str, look: dict) -> dict:
    light = mode_tokens(look, "dark" if look["mode"] == "dark" else "light")
    return {
        "name": name,
        "label": look.get("label", name),
        "about": look.get("about", ""),
        "mode": look["mode"],
        "frame": look["frame"],
        "card": look["card"],
        "kpi": look["kpi"],
        "density": look["density"],
        "font": look["font"],
        "swatch": {k: light[k] for k in ("bg", "surface", "accent", "text")},
    }


def _refusal(error: str, hint: str, label: str = "Looks refused") -> dict:
    return {
        "success": False,
        "op": "dashboard_looks",
        "error": error,
        "hint": hint,
        "progress": [fail(label, error[:200])],
        "token_estimate": 40,
    }


def dashboard_looks(
    name: str = "", source: str = "", overrides: dict | None = None, output_path: str = "", brand: dict | None = None
) -> dict:
    progress: list[dict] = []
    try:
        if sum(bool(x) for x in (name, source, brand)) > 1:
            return _refusal(
                "Pass one of name, source or brand.",
                "name starts from a look; source reads an HTML mockup; brand makes a look from an accent and a mood.",
            )
        report: dict = {}
        arrangement: dict = {}
        if not name and not source and not overrides and not brand:
            listing = [_summary(n, validate_look(look)) for n, look in BUILTIN.items()]
            progress.append(ok("Looks listed", f"{len(listing)} built in"))
            return {
                "success": True,
                "op": "dashboard_looks",
                "looks": listing,
                "tokens": list(TOKENS),
                "grammar": grammar(),
                "moods": {k: v["about"] for k, v in MOODS.items()},
                "hint": (
                    "Use one with generate_dashboard(spec={'style': {'look': 'lagoon'}}). Define your own: "
                    "brand={'accent': '#7c3aed', 'mood': 'editorial'} makes a complete look from an accent and a "
                    "mood (contrast-checked, light and dark); name and overrides change one key by key "
                    "(`grammar` lists every key and what it takes, e.g. {'chart': {'bar_radius': 8}, 'tabs': "
                    "'underline'}); source reads an HTML mockup; output_path saves a .json look the spec can name."
                ),
                "progress": progress,
                "token_estimate": 700,
            }
        if source:
            path = resolve_path(source)
            if not path.is_file():
                return _refusal(f"Mockup not found: {path.name}", "Pass the absolute path of an .html file.")
            if path.stat().st_size > MAX_MOCKUP_BYTES:
                return _refusal(
                    f"{path.name} is over {MAX_MOCKUP_BYTES // 1_000_000} MB.", "Pass the mockup's HTML alone."
                )
            text = path.read_text(encoding="utf-8", errors="replace")
            look, report = digest_html(text, path.stem)
            arrangement = digest_arrangement(text)
            # How the mockup is framed and spaced is part of its look; how wide its cards are is the arrangement.
            look = validate_look(
                {
                    **look,
                    "frame": arrangement["frame"],
                    **({"density": arrangement["density"]} if arrangement["density"] else {}),
                }
            )
            report["frame"] = f"{arrangement['frame']} (from {arrangement['report']['frame_from']})"
            progress.append(ok(f"Read {path.name}", f"{len(report['read_by_name'])} token(s) by name"))
            label = path.stem
        elif brand:
            if not isinstance(brand, dict):
                return _refusal(
                    "brand is a dict: {'accent': '#7c3aed', 'mood': 'calm'}.", f"Keys: {', '.join(BRAND_KEYS)}."
                )
            unknown = sorted(str(k) for k in brand if k not in BRAND_KEYS)
            if unknown or "accent" not in brand:
                return _refusal(
                    "brand needs an accent"
                    + (f" and has unknown key(s): {', '.join(unknown)}" if unknown else "")
                    + ".",
                    f"brand takes {', '.join(BRAND_KEYS)}; moods are {', '.join(MOODS)}.",
                )
            look, report = look_from_brand(**brand)
            progress.append(ok("Look made", f"{brand.get('mood', 'calm')} in {brand['accent']}"))
            label = str(look.get("label", "brand"))
        elif name:
            look = resolve_look(name, resolve_path)
            label = name
        else:
            return _refusal(
                "overrides need a look to change.", "Pass name (a built-in or saved look) or source (a mockup)."
            )
        if overrides:
            if not isinstance(overrides, dict):
                return _refusal(
                    "overrides is a dict of look keys, e.g. {'frame': 'banner'}.", "See `looks` for the keys."
                )
            groups = ("light", "dark", *GRAMMAR)  # colour tokens and chart/type/space change key by key, not whole
            merged = {**look, **{k: v for k, v in overrides.items() if k not in groups}}
            for group in groups:
                if isinstance(overrides.get(group), dict):
                    merged[group] = {**look.get(group, {}), **overrides[group]}
                elif group in overrides:
                    merged[group] = overrides[group]  # not a dict: validation says so by name
            look = validate_look(merged)
            progress.append(info("Overrides applied", ", ".join(sorted(overrides))))
        result: dict = {"success": True, "op": "dashboard_looks", "look": look, "progress": progress}
        if report:
            result["report"] = report
        use: dict | str = look
        if output_path:
            out = resolve_path(output_path)
            if out.suffix.lower() != ".json":
                return _refusal("output_path is a .json file.", "A saved look is a .json document.")
            out.parent.mkdir(parents=True, exist_ok=True)
            backup = snapshot_if_exists(out)
            atomic_write_text(out, json.dumps(look, indent=2))
            append_receipt(
                str(out),
                tool="dashboard_looks",
                args={"name": name, "source": Path(source).name, "brand": bool(brand)},
                result=label,
                backup=backup,
            )
            result["output_path"] = str(out)
            if backup:
                result["backup"] = backup
            progress.append(ok("Look saved", out.name))
            use = str(out)
        result["use"] = {"style": {"look": use}}
        if arrangement and arrangement["spans"]:
            result["arrangement"] = {
                "rows": arrangement["rows"],
                "spans": arrangement["spans"],
                "report": arrangement["report"],
            }
            result["use"]["style"]["arrange"] = arrangement["spans"]
            progress.append(
                ok(
                    "Arrangement read",
                    f"{arrangement['report']['cards']} card(s): "
                    + ", ".join(f"{k} {v}" for k, v in arrangement["spans"].items()),
                )
            )
        result["hint"] = (
            "Pass `use` as part of generate_dashboard's spec (spec={'style': {'look': ...}}); a mockup's `arrange` "
            "also sets the widths of the generated cards (a KPI a quarter of the row, the trend chart two thirds). "
            "customize_dashboard(changes={'style': {'look': ...}}) re-dresses a page that exists."
        )
        result["token_estimate"] = len(str(result)) // 4
        return result
    except LookError as exc:
        return _refusal(str(exc), "A look is the name of a built-in, a saved .json look, or a dict; see `looks`.")
    except Exception as exc:
        logger.exception("dashboard_looks error")
        return {
            "success": False,
            "op": "dashboard_looks",
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check the paths are absolute and the files readable."),
            "progress": [fail("Unexpected error", str(exc)[:200])],
            "token_estimate": 20,
        }
