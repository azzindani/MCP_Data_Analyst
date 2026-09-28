"""A page a founder could put in front of an investor unedited: offline, legible, colour-blind safe, printable.

The quality bar the sweep's review set for a dashboard someone presents: it
opens with no network, its text passes contrast in light and dark, its series
colours are told apart by a colour-blind reader, it reflows on a phone, it
prints as clean cards, and a chart can be saved as a picture for a slide.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_advanced")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _adv_dashboard import generate_dashboard  # noqa: E402
from _dash_ext import EXT_CSS, EXT_JS  # noqa: E402

from shared.html_theme import _DARK_VARS, _LIGHT_VARS  # noqa: E402
from shared.story import SAFE_PALETTE  # noqa: E402

OKABE_ITO = ["#0072B2", "#E69F00", "#009E73", "#D55E00", "#56B4E9", "#CC79A7", "#F0E442", "#999999"]


@pytest.fixture
def page(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(3)
    n = 400
    pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=n).strftime("%Y-%m-%d"),
            "country": rng.choice(["France", "Japan", "Brazil"], n),
            "channel": rng.choice(["web", "app"], n),
            "spend": rng.gamma(2, 20, n).round(2),
            "clicks": rng.integers(0, 90, n),
            "impressions": rng.integers(500, 3000, n),
        }
    ).to_csv(tmp_path / "p.csv", index=False)
    r = generate_dashboard(str(tmp_path / "p.csv"), output_path=str(tmp_path / "p.html"), open_after=False)
    assert r["success"] is True, r.get("error")
    return r, Path(r["output_path"]).read_text(encoding="utf-8")


def _luminance(hex_colour: str) -> float:
    rgb = [int(hex_colour.lstrip("#")[i : i + 2], 16) / 255 for i in (0, 2, 4)]
    lin = [c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4 for c in rgb]
    return 0.2126 * lin[0] + 0.7152 * lin[1] + 0.0722 * lin[2]


def _contrast(a: str, b: str) -> float:
    hi, lo = sorted((_luminance(a), _luminance(b)), reverse=True)
    return (hi + 0.05) / (lo + 0.05)


def _tokens(css: str) -> dict[str, str]:
    return dict(re.findall(r"--([a-z-]+):(#[0-9a-fA-F]{6})", css))


def test_it_opens_with_no_network(page):
    _, html = page
    assert not re.search(r"<(script|link|img|iframe)\b[^>]*\b(src|href)=[\"']https?://", html, flags=re.I)
    assert "@import" not in html and "fonts.googleapis" not in html


@pytest.mark.parametrize("theme", [_LIGHT_VARS, _DARK_VARS], ids=["light", "dark"])
def test_its_text_passes_contrast(theme):
    t = _tokens(theme)
    for fg in ("text", "text-muted", "accent"):
        for bg in ("bg", "surface"):
            assert _contrast(t[fg], t[bg]) >= 4.5, f"--{fg} on --{bg}: {_contrast(t[fg], t[bg]):.2f}"


def test_a_storyline_draws_in_colours_a_colour_blind_reader_tells_apart(page):
    r, _ = page
    assert [c.upper() for c in SAFE_PALETTE] == OKABE_ITO
    assert r["spec"]["style"]["palette"] == SAFE_PALETTE


def test_it_reflows_on_a_phone(page):
    _, html = page
    assert 'name="viewport"' in html and "width=device-width" in html
    assert "@media(max-width:68.75rem){.cgrid.g12{grid-template-columns:minmax(0,1fr)}" in html


def test_it_prints_as_cards_without_its_controls():
    rules = EXT_CSS[EXT_CSS.index("@media print") :]
    assert "button" in rules and "display:none!important" in rules
    assert "break-inside:avoid" in rules


def test_a_chart_is_saved_as_a_picture(page):
    r, html = page
    plots = re.findall(r'<div id="([^"]+)" style="width:100%;height:100%"></div>', html)
    assert plots and all(f'data-png="{cid}"' in html for cid in plots)
    assert "Plotly.downloadImage(el,{format:'png'" in EXT_JS
