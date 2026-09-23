"""The dashboard draws median, count and distinct count -- not only sum/mean/max/min.

A caller asking for the median of a skewed column, or for how many orders or
distinct customers fall in each group, had no way to say so: the aggregate had
four values, and until the override parser was fixed the request was dropped
and a sum was drawn in its place.

Each aggregator in the page (bar, KPI, time series, grouped bar, heatmap,
choropleth) now hands median / count / count_distinct to one reducer, `_agg`.
These tests run the page's own script in node and check the numbers in the
figures it draws, including that a missing value is never counted.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_advanced")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _adv_dashboard import generate_dashboard  # noqa: E402

from tests.dashboard_page import drawn, run_js  # noqa: E402

pytestmark = pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")

ROWS = [
    {"g": "a", "h": "x", "day": "2024-01-05", "v": 10},
    {"g": "a", "h": "x", "day": "2024-01-20", "v": None},
    {"g": "b", "h": "y", "day": "2024-02-03", "v": 4},
    {"g": "b", "h": "x", "day": "2024-02-09", "v": 6},
    {"g": "b", "h": "y", "day": "2024-02-11", "v": 6},
]


def _traces(html: str, card: str) -> list:
    """The traces the page hands Plotly for one card, drawn from ROWS."""
    return drawn(html, rows=ROWS)["figures"][card]["data"]


def _card(html: str, prefix: str) -> str:
    return next(cid for cid in drawn(html, rows=ROWS)["figures"] if cid.startswith(prefix))


def _page(tmp_path: Path, **kw) -> str:
    csv = tmp_path / "g.csv"
    pd.DataFrame(ROWS * 4).to_csv(csv, index=False)
    r = generate_dashboard(str(csv), output_path=str(tmp_path / "g.html"), open_after=False, **kw)
    assert r["success"] is True, r
    return Path(r["output_path"]).read_text(encoding="utf-8")


@pytest.fixture(autouse=True)
def _env(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))


def _bar(tmp_path, agg) -> dict:
    html = _page(tmp_path, spec={"layout": [{"chart": "bar", "cols": {"category": "g", "value": "v"}, "agg": agg}]})
    trace = _traces(html, "p0_bar")[0]
    return dict(zip(trace["x"], trace["y"], strict=True))


@pytest.mark.parametrize(
    ("agg", "a", "b"),
    [("median", 10, 6), ("count", 1, 3), ("count_distinct", 1, 2)],
)
def test_bar(tmp_path, agg, a, b):
    got = _bar(tmp_path, agg)
    assert got["a"] == pytest.approx(a) and got["b"] == pytest.approx(b)


@pytest.mark.parametrize(("agg", "want"), [("median", 6), ("count", 4), ("count_distinct", 3)])
def test_kpi(tmp_path, agg, want):
    html = _page(tmp_path)
    assert run_js(html, f"_kpi({json.dumps(ROWS)}, 'v', {json.dumps(agg)})") == pytest.approx(want)


def test_time_series_median_per_month(tmp_path):
    spec = {"layout": [{"chart": "line", "cols": {"date": "day", "value": "v"}, "agg": "median"}]}
    trace = _traces(_page(tmp_path, spec=spec), "p0_line")[0]
    assert dict(zip(trace["x"], trace["y"], strict=True)) == {"2024-01": 10, "2024-02": 6}


def test_grouped_bar_and_heatmap_take_an_override(tmp_path):
    html = _page(tmp_path, agg_overrides=["v:count"])
    counts = {t["name"]: dict(zip(t["x"], t["y"], strict=True)) for t in _traces(html, _card(html, "grp_"))}
    assert counts["x"]["a"] == 1, "the missing value in (a, x) is not counted"
    z = _traces(html, _card(html, "aghm_"))[0]["z"]
    assert sorted(v for row in z for v in row) == [0, 1, 1, 2]


def test_labels_and_first_paint(tmp_path):
    html = _page(tmp_path, agg_overrides=["v:median"])
    assert "Median v" in html
    # The KPI's value before any script runs is computed in Python, and has to
    # agree with what the script will compute: median of 10, 4, 6, 6 is 6.
    assert re.search(r'id="kv-v">6<', html)


def test_distinct_count_label(tmp_path):
    html = _page(tmp_path, agg_overrides=["v:count_distinct"])
    assert "Distinct v" in html
    assert re.search(r'id="kv-v">3<', html), "distinct values of 10, 4, 6, 6 are three"
