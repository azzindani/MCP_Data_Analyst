"""A dashboard you can scroll with the pointer over a chart, and an inset look you can scroll at all.

The Lagoon look could not be scrolled. It draws the page as a rounded card with `body{overflow:hidden}`, and a
browser hands a body's overflow to the viewport when `html` leaves its own `visible`: the whole page was locked,
on a desktop and on a phone. Under it sat a second trap, made by the toolbar round: every chart took the mouse
wheel (`scrollZoom`) and every one-finger drag (drag-to-zoom cancels the touch), so a page of charts could not be
scrolled with the pointer over one. No stub can see either; only the browser's own scrolling can, so the last
class runs one when this machine has one, and the rest pin the rules the browser was obeying.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from servers.data_advanced._adv_dashboard import generate_dashboard
from shared.dashboard_looks import BUILTIN
from shared.look_maker import look_from_brand
from tests.dashboard_page import NODE, drawn, run_js

needs_node = pytest.mark.skipif(NODE is None, reason="node is not installed")


@pytest.fixture
def sales(tmp_path, monkeypatch) -> Path:
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(7)
    n = 600
    path = tmp_path / "sales.csv"
    pd.DataFrame(
        {
            "day": pd.date_range("2024-01-01", periods=n).strftime("%Y-%m-%d"),
            "region": rng.choice(["East", "North", "South", "West"], n),
            "channel": rng.choice(["Web", "Store", "Phone"], n),
            "units": rng.integers(1, 90, n),
            "revenue": rng.normal(300, 40, n).round(2),
        }
    ).to_csv(path, index=False)
    return path


def _page(sales: Path, look) -> str:
    out = sales.parent / "page.html"
    r = generate_dashboard(str(sales), output_path=str(out), open_after=False, spec={"style": {"look": look}})
    assert r["success"] is True, r.get("error")
    return out.read_text(encoding="utf-8")


def _rules(html: str, selector: str) -> list[str]:
    """The bodies of every CSS rule whose selector is exactly `selector`."""
    return re.findall(
        r"(?:^|[};])\s*" + re.escape(selector) + r"\{([^}]*)\}",
        "\n".join(re.findall(r"<style[^>]*>(.*?)</style>", html, flags=re.S)),
    )


def _inset_look() -> dict:
    look, _ = look_from_brand("#0e7490", "calm", "light")
    return {**look, "inset": True}


class TestAnInsetPageIsNotLockedByItsOwnCorners:
    @pytest.mark.parametrize("look", sorted(BUILTIN))
    def test_no_look_hands_overflow_hidden_to_the_viewport(self, sales, look):
        """`body{overflow:hidden}` with html left `visible` is the viewport's overflow: the page cannot scroll."""
        html = _page(sales, look)
        assert not any("overflow:hidden" in body.replace(" ", "") for body in _rules(html, "body")), look

    def test_an_inset_page_lets_html_scroll_and_the_body_only_clip(self, sales):
        html = _page(sales, "lagoon")
        assert any("overflow-y:auto" in b.replace(" ", "") for b in _rules(html, "html"))
        assert any("overflow:clip" in b.replace(" ", "") for b in _rules(html, "body"))

    def test_a_look_defined_by_hand_is_inset_without_the_lock_too(self, sales):
        html = _page(sales, _inset_look())
        assert any("overflow-y:auto" in b.replace(" ", "") for b in _rules(html, "html"))
        assert not any("overflow:hidden" in b.replace(" ", "") for b in _rules(html, "body"))


@needs_node
class TestTheWheelScrollsThePageOverAChart:
    def test_no_chart_takes_every_wheel_turn(self, sales):
        figures = drawn(_page(sales, "studio"))["figures"]
        assert figures and all(f["config"]["scrollZoom"] is False for f in figures.values())

    def test_only_ctrl_or_cmd_and_the_wheel_zooms_a_chart(self, sales):
        probe = (
            "(function(){var gd={id:'p0',_fullLayout:{}},t={closest:function(){return gd;}},o={};"
            "function turn(c,m,id){gd.id=id||'p0';__dlis.wheel.forEach(function(f){f({target:t,ctrlKey:c,metaKey:m});});"
            "return gd._fullLayout._enablescrollzoom;}"
            "o.plain=turn(false,false);o.ctrl=turn(true,false);o.cmd=turn(false,true);o.after=turn(false,false);"
            "o.expanded=turn(false,false,'mdiv');return o;})()"
        )
        got = run_js(_page(sales, "studio"), probe)
        assert got == {"plain": False, "ctrl": True, "cmd": True, "after": False, "expanded": True}

    def test_the_expanded_chart_drags_to_zoom_whatever_the_page_does(self, sales):
        assert "dragmode:'zoom'" in _page(sales, "studio")


@needs_node
class TestAFingerScrollsPastAChart:
    def test_on_a_touch_screen_no_chart_drags(self, sales):
        figures = drawn(_page(sales, "studio"), coarse=True)["figures"]
        assert figures and all(f["layout"].get("dragmode") is False for f in figures.values())

    def test_with_a_mouse_every_chart_still_drags_to_zoom(self, sales):
        figures = drawn(_page(sales, "studio"))["figures"]
        assert figures and not any(f["layout"].get("dragmode") is False for f in figures.values())


# A browser, when this machine has one: the python that runs the tests may not carry playwright, the system's may.
def _system_python() -> str | None:
    for candidate in ("/usr/bin/python3", "/usr/local/bin/python3", shutil.which("python3")):
        if not candidate or not Path(candidate).exists() or Path(candidate).resolve() == Path(sys.executable).resolve():
            continue
        done = subprocess.run(
            [candidate, "-c", "from playwright.sync_api import sync_playwright as s\nwith s() as p: p.chromium.launch().close()"],
            capture_output=True, timeout=120,
        )  # fmt: skip
        if done.returncode == 0:
            return candidate
    return None


SYSTEM_PYTHON = _system_python()


_SCROLL = r"""
import json, sys
from playwright.sync_api import sync_playwright
url, out = sys.argv[1], {}
SEL = ".cc-body .js-plotly-plot .nsewdrag"
def swipe(cdp, x, y, dy):
    cdp.send("Input.dispatchTouchEvent", {"type": "touchStart", "touchPoints": [{"x": x, "y": y}]})
    for i in range(1, 13):
        cdp.send("Input.dispatchTouchEvent", {"type": "touchMove", "touchPoints": [{"x": x, "y": y + dy * i / 12}]})
    cdp.send("Input.dispatchTouchEvent", {"type": "touchEnd", "touchPoints": []})
with sync_playwright() as p:
    b = p.chromium.launch()
    for label, size, phone in (("desktop", (1440, 900), False), ("phone", (390, 844), True)):
        ctx = b.new_context(viewport={"width": size[0], "height": size[1]}, is_mobile=phone, has_touch=phone)
        pg = ctx.new_page(); pg.goto(url, wait_until="load"); pg.wait_for_timeout(2000)
        pg.add_style_tag(content="html{scroll-behavior:auto!important}")
        cdp = ctx.new_cdp_session(pg) if phone else None
        room = pg.evaluate("document.documentElement.scrollHeight-innerHeight")
        pg.mouse.move(size[0] // 2, 40) if not phone else None
        if not phone:
            pg.mouse.wheel(0, 400); pg.wait_for_timeout(500)
        else:
            swipe(cdp, 200, 600, -300); pg.wait_for_timeout(800)
        out[label + "_page_moves"] = pg.evaluate("scrollY") > 0
        out[label + "_room"] = room
        moved = []
        n = pg.evaluate(f"document.querySelectorAll('{SEL}').length")
        for i in range(n):
            pg.evaluate("scrollTo(0,0)"); pg.wait_for_timeout(200)
            xy = pg.evaluate(f"(()=>{{const e=document.querySelectorAll('{SEL}')[{i}];e.scrollIntoView({{block:'center'}});const r=e.getBoundingClientRect();return [r.left+r.width/2,r.top+r.height/2]}})()")
            before = pg.evaluate("scrollY"); up = pg.evaluate("document.documentElement.scrollHeight-innerHeight-scrollY") < 300
            if phone: swipe(cdp, xy[0], xy[1], 250 if up else -250)
            else: pg.mouse.move(*xy); pg.mouse.wheel(0, -250 if up else 250)
            pg.wait_for_timeout(700); moved.append(pg.evaluate("scrollY") != before)
        out[label + "_charts"] = moved
        if not phone and n:
            sel = f"document.querySelectorAll('.cc-body .js-plotly-plot')[0]"
            xy = pg.evaluate(f"(()=>{{const e=document.querySelectorAll('{SEL}')[0];e.scrollIntoView({{block:'center'}});const r=e.getBoundingClientRect();return [r.left+r.width/2,r.top+r.height/2]}})()")
            r0 = pg.evaluate(f"JSON.stringify({sel}._fullLayout.xaxis.range)")
            pg.mouse.move(*xy); pg.keyboard.down("Control"); pg.mouse.wheel(0, -300); pg.wait_for_timeout(600); pg.keyboard.up("Control")
            out["ctrl_wheel_zooms"] = pg.evaluate(f"JSON.stringify({sel}._fullLayout.xaxis.range)") != r0
        ctx.close()
    b.close()
print(json.dumps(out))
"""


@pytest.mark.skipif(NODE is None or SYSTEM_PYTHON is None, reason="no browser with playwright on this machine")
class TestInARealBrowser:
    @pytest.fixture(scope="class", params=["lagoon", "studio"])
    def scrolled(self, request, tmp_path_factory) -> dict:
        home = tmp_path_factory.mktemp("browser")
        rng = np.random.default_rng(3)
        n = 500
        csv = home / "sales.csv"
        pd.DataFrame(
            {
                "day": pd.date_range("2024-01-01", periods=n).strftime("%Y-%m-%d"),
                "region": rng.choice(["East", "North", "South", "West"], n),
                "channel": rng.choice(["Web", "Store", "Phone"], n),
                "units": rng.integers(1, 90, n),
                "revenue": rng.normal(300, 40, n).round(2),
            }
        ).to_csv(csv, index=False)
        page = home / "page.html"
        r = generate_dashboard(
            str(csv), output_path=str(page), open_after=False, spec={"style": {"look": request.param}}
        )
        assert r["success"] is True, r.get("error")
        done = subprocess.run(
            [SYSTEM_PYTHON, "-c", _SCROLL, page.as_uri()], capture_output=True, encoding="utf-8", timeout=300
        )
        assert done.returncode == 0, done.stderr[-2000:]
        return json.loads(done.stdout)

    def test_the_page_scrolls_on_a_desktop_and_on_a_phone(self, scrolled):
        assert scrolled["desktop_room"] > 0 and scrolled["phone_room"] > 0
        assert scrolled["desktop_page_moves"] is True and scrolled["phone_page_moves"] is True

    def test_it_scrolls_with_the_pointer_over_any_chart(self, scrolled):
        assert scrolled["desktop_charts"] and all(scrolled["desktop_charts"]), scrolled["desktop_charts"]
        assert scrolled["phone_charts"] and all(scrolled["phone_charts"]), scrolled["phone_charts"]

    def test_ctrl_and_the_wheel_still_zooms_a_chart(self, scrolled):
        assert scrolled["ctrl_wheel_zooms"] is True
