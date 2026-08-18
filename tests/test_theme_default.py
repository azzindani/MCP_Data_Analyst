"""The default theme has to follow the viewer, not the machine that built it.

Every chart, report and dashboard here defaulted to `theme="dark"`, so every
artifact came out dark regardless of who opened it. These files get sent to
colleagues; a fixed palette is the build machine imposing its preference on
every reader. The default is now "device": the page ships the light palette
plus a `prefers-color-scheme: dark` override and a script that re-themes the
Plotly figures live when the viewer's setting changes.

Nothing covered the default before this — the flip from "dark" to "device"
broke no test in this repo, which is exactly why these exist.
"""

from __future__ import annotations

import inspect
from pathlib import Path

import pandas as pd
import pytest

from shared.html_theme import css_vars, get_theme, plotly_template

try:
    from servers.data_advanced.engine import generate_chart

    HAS_ADVANCED = True
except ImportError:
    HAS_ADVANCED = False


class TestThemeResolution:
    def test_default_is_device(self):
        assert get_theme() == get_theme("device")

    def test_unknown_name_falls_back_to_the_default_not_dark(self):
        """A typo should not silently pin a shared report to one palette."""
        assert get_theme("Device") == get_theme("device")
        assert plotly_template("nonsense") == plotly_template("device")

    def test_device_ships_light_with_a_dark_override(self):
        css = css_vars("device")
        assert "prefers-color-scheme:dark" in css
        assert "#ffffff" in css  # light palette is the base
        assert "#0d1117" in css  # dark palette inside the media query

    def test_explicit_choices_are_still_absolute(self):
        """Asking for one palette means one palette — no media query."""
        assert "prefers-color-scheme" not in css_vars("light")
        assert "prefers-color-scheme" not in css_vars("dark")
        assert "#0d1117" in css_vars("dark")
        assert "#ffffff" in css_vars("light")


class TestToolSignatures:
    """The default lives in ~39 tool signatures; a stray "dark" reintroduces the
    old behaviour for whichever tool it was left in."""

    def test_no_tool_still_defaults_to_dark(self):
        offenders = []
        for path in list(Path("servers").rglob("*.py")) + list(Path("shared").rglob("*.py")):
            text = path.read_text(encoding="utf-8")
            if 'theme: str = "dark"' in text:
                offenders.append(str(path))
        assert offenders == [], f"still defaulting to dark: {offenders}"

    @pytest.mark.skipif(not HAS_ADVANCED, reason="data_advanced deps unavailable")
    def test_generate_chart_signature_default(self):
        assert inspect.signature(generate_chart).parameters["theme"].default == "device"


@pytest.mark.skipif(not HAS_ADVANCED, reason="data_advanced deps unavailable")
class TestGeneratedPageFollowsTheViewer:
    @pytest.fixture()
    def csv(self, tmp_path: Path) -> Path:
        path = tmp_path / "d.csv"
        pd.DataFrame({"k": list("abcde"), "v": [3, 1, 4, 1, 5]}).to_csv(path, index=False)
        return path

    def test_a_chart_built_with_no_theme_argument_adapts(self, csv, tmp_path):
        """The end-to-end assertion: not just that the default string changed,
        but that the page it produces actually carries the switching machinery."""
        out = tmp_path / "c.html"
        result = generate_chart(
            str(csv),
            chart_type="bar",
            value_column="v",
            category_column="k",
            output_path=str(out),
            open_after=False,
        )
        assert result["success"] is True

        page = out.read_text(encoding="utf-8")
        assert "prefers-color-scheme" in page, "page cannot respond to the viewer's setting"
        assert "Plotly.relayout" in page, "figures would keep the light template on a dark system"
        assert "plotly_dark" in page and "plotly_white" in page, "both templates must be reachable"
