"""A missing value must not be counted as a zero in the browser.

The dashboard embeds its rows and recomputes every chart and KPI in the page,
reading each value with ``+r['col']``. A missing cell is embedded as ``null``,
and in JavaScript ``+null`` is ``0`` -- which passes the templates'
``isNaN`` guard. So a group holding [10, missing] averaged to 5, its minimum
became 0, and the KPI mean sank, while the Python-side first paint of the same
KPI (which drops missing values) showed the right number until the script ran.

These tests run the generated JavaScript itself, in node, on rows with missing
values, and check the numbers it produces.
"""

from __future__ import annotations

import json
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

from _adv_dashboard import _js_kpi_expr, generate_dashboard  # noqa: E402

pytestmark = pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")

ROWS = [
    {"g": "a", "v": 10},
    {"g": "a", "v": None},
    {"g": "b", "v": 4},
    {"g": "b", "v": 6},
]


def _function(html: str, name: str) -> str:
    """The source of `function name(...){...}` from the page, by brace matching."""
    start = html.index(f"function {name}(")
    depth, i = 0, html.index("{", start)
    while True:
        ch = html[i]
        depth += ch == "{"
        depth -= ch == "}"
        i += 1
        if depth == 0:
            return html[start:i]


def _node(program: str) -> object:
    done = subprocess.run(["node", "-e", program], capture_output=True, text=True, timeout=60)
    assert done.returncode == 0, done.stderr[-2000:]
    return json.loads(done.stdout.strip().splitlines()[-1])


def _bar_values(tmp_path: Path, agg: str) -> dict:
    tmp_path.mkdir(parents=True, exist_ok=True)
    csv = tmp_path / "g.csv"
    pd.DataFrame({"g": ["a", "a", "b", "b"] * 5, "v": [10, np.nan, 4, 6] * 5}).to_csv(csv, index=False)
    spec = {"layout": [{"chart": "bar", "cols": {"category": "g", "value": "v"}, "agg": agg}]}
    r = generate_dashboard(str(csv), output_path=str(tmp_path / "g.html"), open_after=False, spec=spec)
    assert r["success"] is True, r
    html = Path(r["output_path"]).read_text(encoding="utf-8")
    program = (
        _function(html, "_num")
        + "\n"
        + _function(html, "rf_p0_bar")
        + "\nvar out=null;var Plotly={react:function(id,t){out={};t[0].x.forEach(function(k,i){out[k]=t[0].y[i];});}};"
        + "function am(l){return l;}"
        + f"\nrf_p0_bar({json.dumps(ROWS)});console.log(JSON.stringify(out));"
    )
    return _node(program)  # type: ignore[return-value]


@pytest.fixture(autouse=True)
def _env(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))


@pytest.mark.parametrize(("agg", "a", "b"), [("mean", 10, 5), ("min", 10, 4), ("max", 10, 6), ("sum", 10, 10)])
def test_a_bar_ignores_missing_values(tmp_path, agg, a, b):
    got = _bar_values(tmp_path / agg, agg)
    assert got["a"] == pytest.approx(a), f"{agg} of [10, missing] must be {a}"
    assert got["b"] == pytest.approx(b)


@pytest.mark.parametrize(("agg", "want"), [("mean", 20 / 3), ("min", 4), ("max", 10), ("sum", 20)])
def test_a_kpi_ignores_missing_values(tmp_path, agg, want):
    csv = tmp_path / "k.csv"
    pd.DataFrame({"v": [1.0, 2.0]}).to_csv(csv, index=False)
    r = generate_dashboard(str(csv), output_path=str(tmp_path / "k.html"), open_after=False)
    html = Path(r["output_path"]).read_text(encoding="utf-8")
    expr = _js_kpi_expr("v", agg)
    program = _function(html, "_num") + f"\nvar d={json.dumps(ROWS)};console.log(JSON.stringify({expr}));"
    assert _node(program) == pytest.approx(want)
