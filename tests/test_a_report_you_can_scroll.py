"""An EDA or profile report you can scroll with the pointer over a chart.

Every report chart took the mouse wheel (`scrollZoom`) and, drag-to-zoom cancelling the touch, every one-finger
swipe, so a page of charts stopped scrolling whenever the pointer was on one. The wheel now scrolls the page and
Ctrl/Cmd + wheel zooms a chart; on a touch screen the charts do not drag. The dashboards have their own pins
(test_a_dashboard_you_can_scroll.py); this is every other page that carries Plotly. The last class runs a real
browser when this machine has one, because no stub can see a browser's scrolling.
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

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_advanced")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _adv_eda import run_eda  # noqa: E402
from _adv_profile import generate_auto_profile  # noqa: E402

from shared.chart_page import CHART_SCROLL_JS, chart_page_html  # noqa: E402
from shared.html_layout import PLOTLY_CFG_JS, plotly_config  # noqa: E402


@pytest.fixture(scope="module")
def reports(tmp_path_factory) -> dict[str, Path]:
    home = tmp_path_factory.mktemp("scroll")
    mp = pytest.MonkeyPatch()
    mp.setenv("MCP_OUTPUT_DIR", str(home))
    mp.setenv("MCP_DATA_ROOT", str(home))
    rng = np.random.default_rng(5)
    n = 600
    df = pd.DataFrame(
        {
            "a": rng.normal(size=n),
            "b": rng.normal(size=n),
            "c": rng.integers(0, 5, n),
            "kind": rng.choice(["x", "y", "z"], n),
        }
    )
    df["d"] = df["a"] * 2 + rng.normal(0, 0.5, n)
    csv = home / "data.csv"
    df.to_csv(csv, index=False)
    out = {"eda": home / "eda.html", "profile": home / "profile.html"}
    assert run_eda(str(csv), output_path=str(out["eda"]), open_after=False)["success"] is True
    assert generate_auto_profile(str(csv), output_path=str(out["profile"]), open_after=False)["success"] is True
    mp.undo()
    return out


class TestTheWheelScrollsThePageOverAChart:
    def test_the_shared_config_does_not_take_the_wheel(self):
        assert plotly_config()["scrollZoom"] is False
        assert json.loads(PLOTLY_CFG_JS)["scrollZoom"] is False

    @pytest.mark.parametrize("report", ["eda", "profile"])
    def test_no_chart_of_a_report_asks_for_scroll_zoom(self, reports, report):
        configs = re.findall(r'"scrollZoom":\s*(true|false)', reports[report].read_text(encoding="utf-8"))
        assert configs and set(configs) == {"false"}

    @pytest.mark.parametrize("report", ["eda", "profile"])
    def test_every_report_carries_the_script(self, reports, report):
        assert CHART_SCROLL_JS in reports[report].read_text(encoding="utf-8")

    def test_a_single_chart_page_carries_it_too(self):
        assert CHART_SCROLL_JS in chart_page_html("<div></div>", "t", ":root{}")

    def test_only_ctrl_or_cmd_and_the_wheel_zooms(self):
        assert "e.ctrlKey||e.metaKey" in CHART_SCROLL_JS and "_enablescrollzoom" in CHART_SCROLL_JS

    def test_on_a_touch_screen_the_charts_do_not_drag(self):
        assert "(pointer:coarse)" in CHART_SCROLL_JS and "dragmode:false" in CHART_SCROLL_JS


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
SEL = ".js-plotly-plot .nsewdrag"
def swipe(cdp, x, y, dy):
    cdp.send("Input.dispatchTouchEvent", {"type": "touchStart", "touchPoints": [{"x": x, "y": y}]})
    for i in range(1, 13):
        cdp.send("Input.dispatchTouchEvent", {"type": "touchMove", "touchPoints": [{"x": x, "y": y + dy * i / 12}]})
    cdp.send("Input.dispatchTouchEvent", {"type": "touchEnd", "touchPoints": []})
with sync_playwright() as p:
    b = p.chromium.launch()
    for label, size, phone in (("desktop", (1440, 900), False), ("phone", (390, 844), True)):
        ctx = b.new_context(viewport={"width": size[0], "height": size[1]}, is_mobile=phone, has_touch=phone)
        pg = ctx.new_page(); pg.goto(url, wait_until="load"); pg.wait_for_timeout(2500)
        pg.add_style_tag(content="html{scroll-behavior:auto!important}")
        cdp = ctx.new_cdp_session(pg) if phone else None
        moved = []
        n = min(pg.evaluate(f"document.querySelectorAll('{SEL}').length"), 5)
        for i in range(n):
            pg.evaluate("scrollTo(0,0)"); pg.wait_for_timeout(200)
            xy = pg.evaluate(f"(()=>{{const e=document.querySelectorAll('{SEL}')[{i}];e.scrollIntoView({{block:'center'}});const r=e.getBoundingClientRect();return [r.left+r.width/2,r.top+r.height/2]}})()")
            if not (0 < xy[1] < size[1]): continue
            before = pg.evaluate("scrollY"); room = pg.evaluate("document.documentElement.scrollHeight-innerHeight-scrollY"); up = room < 50
            if phone: swipe(cdp, xy[0], xy[1], 250 if up else -250)
            else: pg.mouse.move(*xy); pg.mouse.wheel(0, -250 if up else 250)
            pg.wait_for_timeout(700); moved.append(pg.evaluate("scrollY") != before)
        out[label + "_charts"] = moved
        if not phone and n:
            idx = pg.evaluate("[...document.querySelectorAll('.js-plotly-plot')].findIndex(g=>g.querySelector('.nsewdrag')&&g._fullLayout.xaxis&&g._fullLayout.xaxis.range&&typeof g._fullLayout.xaxis.range[0]==='number')")
            if idx >= 0:
                xy = pg.evaluate(f"(()=>{{const e=document.querySelectorAll('.js-plotly-plot')[{idx}].querySelector('.nsewdrag');e.scrollIntoView({{block:'center'}});const r=e.getBoundingClientRect();return [r.left+r.width/2,r.top+r.height/2]}})()")
                rng = lambda: pg.evaluate("JSON.stringify(document.querySelectorAll('.js-plotly-plot')[%d]._fullLayout.xaxis.range)" % idx)
                r0 = rng(); pg.mouse.move(*xy); pg.keyboard.down("Control"); pg.mouse.wheel(0, -300); pg.wait_for_timeout(600); pg.keyboard.up("Control")
                out["ctrl_wheel_zooms"] = rng() != r0
        ctx.close()
    b.close()
print(json.dumps(out))
"""


@pytest.mark.skipif(SYSTEM_PYTHON is None, reason="no browser with playwright on this machine")
class TestInARealBrowser:
    @pytest.fixture(scope="class", params=["eda", "profile"])
    def scrolled(self, request, reports) -> dict:
        done = subprocess.run(
            [SYSTEM_PYTHON, "-c", _SCROLL, reports[request.param].as_uri()],
            capture_output=True, encoding="utf-8", timeout=300,
        )  # fmt: skip
        assert done.returncode == 0, done.stderr[-2000:]
        return json.loads(done.stdout)

    def test_it_scrolls_with_the_wheel_over_any_chart(self, scrolled):
        assert len(scrolled["desktop_charts"]) >= 2 and all(scrolled["desktop_charts"]), scrolled["desktop_charts"]

    def test_it_scrolls_with_a_swipe_over_any_chart(self, scrolled):
        assert len(scrolled["phone_charts"]) >= 2 and all(scrolled["phone_charts"]), scrolled["phone_charts"]

    def test_ctrl_and_the_wheel_still_zooms_a_chart(self, scrolled):
        assert scrolled.get("ctrl_wheel_zooms", True) is True
