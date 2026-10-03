"""A large file's page embeds a cube, and every total, count, average, minimum and maximum on it is exact.

Every number on a dashboard is computed in the browser from the rows embedded
in it, so a large file made a page too big to open, and capping the rows made
every number an estimate. Above CUBE_ROWS the page now embeds the rows grouped
by what it can group and filter by, each measure as its sum, count, min and
max. These tests hold the page to pandas on the full frame -- unfiltered and
filtered -- and hold the panels that plot rows to the sample they say they use.
The thresholds are lowered so the frames stay small.
"""

from __future__ import annotations

import shutil
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

from shared import cube  # noqa: E402
from tests.dashboard_page import drawn, run_js  # noqa: E402

needs_node = pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
N = 6000


@pytest.fixture
def big(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setattr(cube, "CUBE_ROWS", 1000)
    monkeypatch.setattr(cube, "SAMPLE_ROWS", 400)
    rng = np.random.default_rng(1)
    df = pd.DataFrame(
        {
            "date": (pd.Timestamp("2024-01-01") + pd.to_timedelta(rng.integers(0, 90, N), unit="D")).strftime(
                "%Y-%m-%d"
            ),
            "platform": rng.choice(["Search", "Social", "Video"], N),
            "order_id": np.arange(N),
            "impressions": rng.integers(100, 2000, N),
            "clicks": rng.integers(0, 90, N),
            "spend": rng.gamma(2, 20, N).round(2),
        }
    )
    df.loc[rng.choice(N, 300, replace=False), "spend"] = np.nan
    df.to_csv(tmp_path / "big.csv", index=False)
    return tmp_path, df


def _make(folder, **spec):
    r = generate_dashboard(
        str(folder / "big.csv"), spec=spec or None, output_path=str(folder / "b.html"), open_after=False
    )
    assert r["success"] is True, r.get("error")
    return r, Path(r["output_path"]).read_text(encoding="utf-8")


KPIS = [{"chart": "kpi", "cols": {"value": "spend"}, "agg": a} for a in ("sum", "mean", "count", "min", "max")]


def test_the_page_embeds_cells_and_says_so(big):
    folder, df = big
    r, html = _make(folder)
    c = r["cube"]
    assert c["rows"] == N and c["keys"] == ["date", "platform"] and c["left_out"] == ["order_id"]
    assert c["cells"] == df.groupby(["date", "platform"]).ngroups == r["rows_embedded"]
    assert "<b>Aggregated on the server.</b> 6,000 rows in" in html
    assert "box panels draw a sample of 400 rows." in html


def test_a_sample_of_every_row_is_not_called_a_sample(big, monkeypatch):
    folder, _ = big
    monkeypatch.setattr(cube, "SAMPLE_ROWS", N)
    r, html = _make(folder)
    assert r["cube"]["sample_rows"] == N
    assert "box panels draw all 6,000 rows." in html and "draw a sample of" not in html


@needs_node
def test_every_exact_aggregate_is_the_whole_frames(big):
    folder, df = big
    _, html = _make(folder, layout=KPIS)
    spend = df["spend"]
    want = [spend.sum(), spend.mean(), spend.count(), spend.min(), spend.max()]
    values = run_js(
        html,
        "['sum','mean','count','min','max'].map(function(a){return _agg(_RAW.map(function(r){return _cell(r,'spend');}),a);})",
    )
    assert values == pytest.approx(want)
    kpis = drawn(html)["html"]
    assert f">{spend.count():,.0f}<" in kpis["p2_kpi"].replace(",", ",")


@needs_node
def test_a_filter_on_a_key_gives_the_filtered_frames_numbers(big):
    folder, df = big
    _, html = _make(folder, layout=[{"chart": "bar", "cols": {"category": "platform", "value": "CTR"}}])
    got = run_js(
        html,
        "(function(){var d=_RAW.filter(function(r){return r.date>='2024-02-01';});"
        "var o={};_rgroups(d,function(r){return r.platform;}).forEach(function(v,k){o[k]=_mval(_METRICS.CTR.tree,v);});return o;})()",
    )
    part = df[df["date"] >= "2024-02-01"]
    want = (part.groupby("platform")["clicks"].sum() / part.groupby("platform")["impressions"].sum()).to_dict()
    assert got == pytest.approx(want)


@needs_node
def test_the_row_count_is_the_rows_not_the_cells(big):
    folder, _ = big
    _, html = _make(folder)
    assert run_js(html, "_TOTAL") == N


@needs_node
def test_a_scatter_plots_the_sample(big):
    folder, _ = big
    _, html = _make(folder, layout=[{"chart": "scatter", "cols": {"x": "spend", "y": "clicks"}}])
    fig = drawn(html)["figures"]["p0_scatter"]
    points = sum(len(t["x"]) for t in fig["data"] if t.get("mode") == "markers")
    assert 0 < points <= 400


def test_a_median_keeps_every_row(big):
    folder, _ = big
    r, _ = _make(folder, layout=[{"chart": "kpi", "cols": {"value": "spend"}, "agg": "median"}])
    assert "cube" not in r and r["rows_embedded"] == N


def test_a_forced_cube_refuses_a_median_by_name(big):
    folder, _ = big
    r = generate_dashboard(
        str(folder / "big.csv"),
        spec={
            "layout": [{"chart": "kpi", "cols": {"value": "spend"}, "agg": "median"}],
            "interactions": {"cube": True},
        },
        open_after=False,
    )
    assert r["success"] is False and "takes median, which a cube" in r["error"]


def test_cube_false_or_embed_rows_keeps_the_rows(big):
    folder, _ = big
    r, _ = _make(folder, interactions={"cube": False})
    assert "cube" not in r and r["rows_embedded"] == N
    r, _ = _make(folder, interactions={"embed_rows": 2000})
    assert "cube" not in r and r["rows_embedded"] == 2000


def test_cube_is_auto_true_or_false(big):
    folder, _ = big
    r = generate_dashboard(str(folder / "big.csv"), spec={"interactions": {"cube": "yes"}}, open_after=False)
    assert r["success"] is False and "interactions.cube is 'auto', true or false" in r["error"]


def test_a_cube_no_smaller_than_the_rows_is_not_made(big, monkeypatch):
    folder, df = big
    df.assign(platform=[f"p{i}" for i in range(N)]).to_csv(folder / "big.csv", index=False)
    monkeypatch.setattr(cube, "MAX_KEY_LEVELS", N)
    r, _ = _make(folder)
    assert "cube" not in r


def test_the_detected_page_gives_up_range_filters_on_measures(big):
    folder, _ = big
    r, _ = _make(folder, story=False)
    assert "cube" in r
    assert "spend" not in r["filter_columns"] and "clicks" not in r["filter_columns"]
    assert any(p["message"] == "Range filters left out" for p in r["progress"])


def test_a_callers_range_filter_on_a_measure_keeps_every_row(big):
    folder, _ = big
    r, _ = _make(folder, filters=["platform", "spend"])
    assert "cube" not in r and "spend" in r["filter_columns"]
    forced = generate_dashboard(
        str(folder / "big.csv"), spec={"filters": ["spend"], "interactions": {"cube": True}}, open_after=False
    )
    assert forced["success"] is False and "filters spend narrow rows by a measure's own value" in forced["error"]
