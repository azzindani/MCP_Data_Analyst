"""Native controls follow the page's theme, and a page that was told its theme wears it.

The report CSS never declared `color-scheme`, so a dark page kept light scrollbars, dropdown lists and date icons.
And once the studio look became the default, `look_css` followed only the look's own mode: `theme="dark"` drew the
charts dark over cards that were light under a light OS (and the reverse).
"""

from __future__ import annotations

import re

import numpy as np
import pandas as pd
import pytest

from servers.data_advanced._adv_dashboard import generate_dashboard
from shared import dashboard_looks as looks
from shared.html_theme import css_vars


def _first_scheme(css: str) -> str:
    return re.search(r":root\{[^}]*color-scheme:(light|dark)\}", css).group(1)


class TestNativeControlsFollowTheTheme:
    def test_dark_light_and_device(self):
        assert "color-scheme:dark" in css_vars("dark") and "color-scheme:light" not in css_vars("dark")
        assert "color-scheme:light" in css_vars("light") and "color-scheme:dark" not in css_vars("light")
        light, dark = css_vars("device").split("@media", 1)
        assert "color-scheme:light" in light and "color-scheme:dark" in dark


class TestALookWearsTheThemeItWasToldToWear:
    def test_a_device_look_told_dark_is_dark_whatever_the_os_says(self):
        css = looks.look_css(looks.resolve_look("studio"), "dark")
        assert _first_scheme(css) == "dark" and "prefers-color-scheme" not in css

    def test_told_light_it_is_light(self):
        css = looks.look_css(looks.resolve_look("studio"), "light")
        assert _first_scheme(css) == "light" and "prefers-color-scheme" not in css

    def test_left_to_the_device_it_follows_it(self):
        assert "prefers-color-scheme:dark" in looks.look_css(looks.resolve_look("studio"), "device")

    def test_a_dark_only_look_stays_dark_when_asked_for_light(self):
        assert _first_scheme(looks.look_css(looks.resolve_look("nocturne"), "light")) == "dark"


@pytest.mark.parametrize("theme", ["dark", "light"])
def test_a_generated_dashboard_wears_the_theme_it_was_asked_for(tmp_path, monkeypatch, theme):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(1)
    frame = pd.DataFrame({"region": rng.choice(["a", "b", "c"], 200), "units": rng.integers(1, 9, 200)})
    frame.to_csv(tmp_path / "d.csv", index=False)
    out = tmp_path / "p.html"
    assert generate_dashboard(str(tmp_path / "d.csv"), output_path=str(out), open_after=False, theme=theme)["success"]
    roots = re.findall(r":root\{[^}]*color-scheme:(light|dark)\}", out.read_text(encoding="utf-8"))
    assert roots and roots[-1] == theme
