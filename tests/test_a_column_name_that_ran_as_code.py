"""A column name is data. It must never become markup or code in a dashboard.

Column names come straight from whatever CSV was loaded, and the dashboard
wrote them raw into two places that execute: JavaScript string literals in
every chart template (``r['{cc}']``, ``title:'{nc1}'``) and inline event
handlers in the filter bar (``onchange="numCh('{nc}')"``), plus one HTML label
(the numeric range's ``<div class="flbl">{nc}</div>``). A header holding ``'``
broke every chart on the page; a crafted one ran script in the browser of
whoever opened the dashboard -- which is the artifact people send to a
colleague.

The property is tested on the page as a whole, against a twin: the same data
under harmless names must produce the same set of tags and attribute names,
and every inline script must still parse.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
from html.parser import HTMLParser
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_advanced")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _adv_dashboard import generate_dashboard  # noqa: E402

from tests.dashboard_page import drawn  # noqa: E402

HOSTILE = {
    "region": "<img src=x onerror=alert(1)>",
    "channel": "re\"gion' </script>",
    "revenue": "rev'enue\\",
    "units": '</script><script>alert("u")</script>',
    "cost": 'x" onmouseover="alert(2)',
}


class _Shape(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tags: set[str] = set()
        self.attrs: set[str] = set()
        self.scripts: list[str] = []
        self._in_script = False
        self._buf: list[str] = []

    def handle_starttag(self, tag, attrs):
        self.tags.add(tag)
        self.attrs.update(name for name, _ in attrs)
        if tag == "script" and not dict(attrs).get("src"):
            self._in_script, self._buf = True, []

    def handle_endtag(self, tag):
        if tag == "script" and self._in_script:
            self.scripts.append("".join(self._buf))
            self._in_script = False

    def handle_data(self, data):
        if self._in_script:
            self._buf.append(data)


def _frame(names: dict[str, str]) -> pd.DataFrame:
    rng = np.random.default_rng(5)
    n = 60
    return pd.DataFrame(
        {
            names["region"]: rng.choice(["North", "South", "East"], n),
            names["channel"]: rng.choice(["web", "store"], n),
            names["revenue"]: rng.normal(1000, 200, n).round(2),
            names["units"]: rng.integers(1, 50, n),
            names["cost"]: rng.normal(300, 50, n).round(2),
        }
    )


def _build(tmp_path: Path, names: dict[str, str], tag: str, spec: dict | None) -> str:
    path = tmp_path / f"{tag}.csv"
    _frame(names).to_csv(path, index=False)
    r = generate_dashboard(str(path), output_path=str(tmp_path / f"{tag}.html"), open_after=False, spec=spec)
    assert r["success"] is True, r
    return Path(r["output_path"]).read_text(encoding="utf-8")


def _shape(html: str) -> _Shape:
    parser = _Shape()
    parser.feed(html)
    return parser


def _spec(names: dict[str, str]) -> dict:
    return {
        "layout": [
            {"chart": "bar", "cols": {"category": names["region"], "value": names["revenue"]}},
            {"chart": "pie", "cols": {"category": names["channel"], "value": names["units"]}},
            {"chart": "scatter", "cols": {"x": names["revenue"], "y": names["cost"]}},
            {"chart": "box", "cols": {"value": names["units"], "category": names["region"]}},
            {"chart": "histogram", "cols": {"value": names["cost"]}},
        ],
        "kpis": [names["revenue"], names["units"], names["cost"]],
        "filters": [names["region"], names["channel"], names["revenue"]],
        "interactions": {"table": True},
    }


@pytest.fixture(autouse=True)
def _output_dir(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))


@pytest.mark.parametrize("with_spec", [False, True], ids=["detected", "caller-layout"])
def test_hostile_names_add_no_markup(tmp_path, with_spec):
    plain = {k: k for k in HOSTILE}
    benign = _shape(_build(tmp_path, plain, "benign", _spec(plain) if with_spec else None))
    hostile_html = _build(tmp_path, HOSTILE, "hostile", _spec(HOSTILE) if with_spec else None)
    hostile = _shape(hostile_html)
    assert hostile.tags == benign.tags, f"new tags: {hostile.tags - benign.tags}"
    assert hostile.attrs == benign.attrs, f"new attributes: {hostile.attrs - benign.attrs}"
    assert len(hostile.scripts) == len(benign.scripts), "a name opened or closed a <script>"
    # Inside a script block the text is inert (the embedded spec carries the
    # names verbatim, with </ escaped); outside one it would be markup.
    markup = re.sub(r"<script\b.*?</script>", "", hostile_html, flags=re.S | re.I)
    assert "<img src=x onerror" not in markup


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
@pytest.mark.parametrize("with_spec", [False, True], ids=["detected", "caller-layout"])
def test_every_inline_script_still_parses(tmp_path, with_spec):
    html = _build(tmp_path, HOSTILE, "hostile", _spec(HOSTILE) if with_spec else None)
    scripts = [s for s in _shape(html).scripts if "_PANELS" in s or "numCh" in s or "ddChange" in s]
    assert scripts, "the page's own script was not found"
    for i, body in enumerate(scripts):
        src = tmp_path / f"script_{i}.js"
        src.write_text(body, encoding="utf-8")
        done = subprocess.run(["node", "--check", str(src)], capture_output=True, text=True)
        assert done.returncode == 0, done.stderr[-2000:]


def test_the_names_still_read_as_themselves(tmp_path):
    # Escaping must not mangle what a reader sees: the label is the column name.
    html = _build(tmp_path, HOSTILE, "hostile", _spec(HOSTILE))
    assert "rev&#x27;enue" in html or "rev'enue" in re.sub(r"<script.*?</script>", "", html, flags=re.S)


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
@pytest.mark.parametrize("with_spec", [False, True], ids=["detected", "caller-layout"])
def test_every_card_is_drawn_from_its_hostile_columns(tmp_path, with_spec):
    # Parsing is not drawing. A name is data in the page's _PANELS document,
    # so every card still reads its column by that exact name.
    out = drawn(_build(tmp_path, HOSTILE, "hostile", _spec(HOSTILE) if with_spec else None))
    assert out["warnings"] == []
    assert set(out["figures"]) == {p["id"] for p in out["panels"]}
    for p in out["panels"]:
        assert all(t for t in out["figures"][p["id"]]["data"]), p["id"]
