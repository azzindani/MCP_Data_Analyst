"""Every CSS variable a dashboard uses is one its theme defines; its tabs sit above their cards.

The paged table and the tab bar were styled with var(--fg), var(--fg2),
var(--bd), var(--ac) and var(--bg2) -- names no theme defines, so each
resolved to nothing: no border, no background, no accent, and on a light page
the selected tab was white text on a transparent button. Nothing failed; the
page just looked unfinished. And the tab bar was written after the grid,
under the cards it shows and hides.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pytest

from servers.data_advanced._adv_dashboard import generate_dashboard


@pytest.fixture
def csv(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    path = tmp_path / "s.csv"
    pd.DataFrame({"region": ["a", "b", "c"] * 20, "units": range(60), "revenue": [x * 1.5 for x in range(60)]}).to_csv(
        path, index=False
    )
    return path


def _page(csv: Path, theme: str) -> str:
    spec = {
        "layout": [{"chart": "bar"}, {"chart": "kpi"}, {"chart": "table"}],
        "tabs": [{"name": "Charts", "slots": [0]}, {"name": "Numbers", "slots": [1, 2]}],
        "interactions": {"table": True},
    }
    out = csv.parent / f"{theme}.html"
    r = generate_dashboard(str(csv), output_path=str(out), open_after=False, spec=spec, theme=theme)
    assert r["success"] is True, r
    return out.read_text(encoding="utf-8")


@pytest.mark.parametrize("theme", ["light", "dark", "device"])
def test_every_variable_used_is_defined(csv, theme):
    css = "\n".join(re.findall(r"<style>(.*?)</style>", _page(csv, theme), flags=re.S))
    defined = set(re.findall(r"(--[\w-]+)\s*:", css))
    used = set(re.findall(r"var\((--[\w-]+)", css))
    assert used <= defined, f"used but never defined: {sorted(used - defined)}"


def test_the_tab_bar_comes_before_the_cards_it_switches(csv):
    html = _page(csv, "light")
    assert html.index('<div class="tabs"') < html.index('<div class="cgrid')


def test_a_detected_page_puts_its_tabs_first_too(csv):
    out = csv.parent / "detected.html"
    r = generate_dashboard(
        str(csv), output_path=str(out), open_after=False, spec={"tabs": [{"name": "All", "slots": [0]}]}
    )
    assert r["success"] is True, r
    html = out.read_text(encoding="utf-8")
    assert html.index('<div class="tabs"') < html.index('<div class="cgrid')
