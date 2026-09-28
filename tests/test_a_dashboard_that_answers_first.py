"""A dashboard asked for nothing opens on the answer, and every number it quotes is the page's own.

The sweep's zero-argument dashboard opened on a 16-row data-quality alert
wall, then drew a chart per column pair; nowhere on it was the finding a
director would act on. It now plans a storyline -- Summary, Drivers,
Segments, Risks, Appendix -- led by a headline insight. These tests hold the
storyline to three things: the insight's numbers are the metric's own ratio
of sums; a placeholder is never a segment; and a caller who asks for charts,
or for `story: false`, still gets the detected page.
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

from shared.metrics import auto_ratios, by_group  # noqa: E402
from shared.story import fmt, grain_for  # noqa: E402
from tests.dashboard_page import drawn  # noqa: E402

needs_node = pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
STORY = ["Summary", "Drivers", "Segments", "Risks", "Appendix"]


def _ads() -> pd.DataFrame:
    """Two platforms, one buying impressions far cheaper; a device column holding its own name."""
    rng = np.random.default_rng(7)
    rows = []
    for day in pd.date_range("2024-01-01", "2024-05-17", freq="D"):
        for platform, cpm, ctr in (("Social", 3.0, 0.004), ("Search", 40.0, 0.05)):
            for device in ("mobile", "desktop", "device"):
                impressions = int(rng.integers(800, 1200))
                rows.append(
                    {
                        "day": day.strftime("%Y-%m-%d"),
                        "platform": platform,
                        "device": device,
                        "impressions": impressions,
                        "clicks": int(impressions * ctr * rng.uniform(0.8, 1.2)),
                        "spend": round(impressions / 1000 * cpm * rng.uniform(0.9, 1.1), 2),
                    }
                )
    return pd.DataFrame(rows)


@pytest.fixture(scope="module")
def ads() -> pd.DataFrame:
    return _ads()


@pytest.fixture
def home(tmp_path, monkeypatch, ads):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    ads.to_csv(tmp_path / "ads.csv", index=False)
    return tmp_path


def _make(home, **kw):
    r = generate_dashboard(str(home / "ads.csv"), output_path=str(home / "d.html"), open_after=False, **kw)
    assert r["success"] is True, r.get("error")
    return r, Path(r["output_path"]).read_text(encoding="utf-8")


def test_a_call_with_nothing_is_a_storyline(home):
    r, html = _make(home)
    assert r["pages"] == STORY
    assert r["headline"] and r["headline"] in html
    for tab in STORY:
        assert f">{tab}</button>" in html


def test_the_efficiency_headline_quotes_each_platforms_ratio_of_sums(home, ads):
    r, _ = _make(home)
    cpm = {m.name: m for m in auto_ratios(ads, ["impressions", "clicks", "spend"])}["CPM"]
    per = by_group(cpm, ads, ["platform"])
    want = f"Social CPM {fmt(per['Social'], 'currency')} against {fmt(per['Search'], 'currency')} for Search"
    assert want in [i["headline"] for i in r["insights"]]


def test_a_placeholder_is_never_quoted_as_a_segment(home):
    r, _ = _make(home)
    for i in r["insights"]:
        if i["page"] == "Risks":
            continue
        assert not i["headline"].startswith("device ") and not i["headline"].endswith(" for device"), i
        assert "vs device" not in i["comparison"] and ": device vs" not in i["comparison"], i


def test_an_override_that_makes_a_measure_no_total_is_never_shared_out(home):
    # A median of spend is no total: "Search brings 90% of spend" would be a wrong number.
    r, _ = _make(home, agg_overrides=["spend:median"])
    assert not any("of spend" in i["headline"] for i in r["insights"]), r["insights"]
    assert not any(
        p["chart"] in ("pareto", "stacked_bar", "waterfall") and p["cols"].get("value") == "spend"
        for p in r["spec"]["layout"]
    )


def test_the_plan_says_what_it_found(home):
    r, _ = _make(home)
    assert r["plan"]["grain"]["frequency"] == "day"
    assert r["plan"]["dimensions"][0] == "platform"


def test_asking_for_charts_gets_the_detected_page(home):
    r, _ = _make(home, chart_types=["bar"])
    assert "pages" not in r


def test_story_false_gets_the_detected_page(home):
    r, _ = _make(home, spec={"story": False})
    assert "pages" not in r


def test_story_is_true_or_false(home):
    r = generate_dashboard(str(home / "ads.csv"), spec={"story": "yes"}, open_after=False)
    assert r["success"] is False and "story" in r["error"]


@needs_node
def test_the_page_draws_every_card_without_a_warning(home, ads):
    _, html = _make(home)
    out = drawn(html)
    assert out["warnings"] == []
    kpis = [body for cid, body in out["html"].items() if cid.endswith("_kpi")]
    assert len(kpis) == 4
    ctr = next(body for body in kpis if "%" in body.split("</div>")[0])
    total = float(ads["clicks"].sum() / ads["impressions"].sum())
    assert f">{total * 100:.2f}%<" in ctr


@pytest.mark.parametrize(
    ("start", "end", "grain", "complete"),
    [
        ("2024-01-01", "2024-05-17", "month", "2024-04"),
        ("2024-01-01", "2024-05-31", "month", "2024-05"),
        ("2024-01-01", "2024-01-31", "week", "2024-01-22"),
        ("2024-01-01", "2024-01-28", "week", "2024-01-22"),
        ("2024-01-01", "2024-01-10", "day", "2024-01-10"),
    ],
)
def test_the_last_period_compared_is_the_last_complete_one(start, end, grain, complete):
    got = grain_for({"grain": {"date": "day", "start": start, "end": end}})
    assert got == {"date": "day", "grain": grain, "complete": complete}
