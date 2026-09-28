"""Several files, one page: each triaged, how they relate found, their numbers blended on one grain.

`sources=[...]` gave each extra file a tab of its own -- totals and a table,
nothing joined -- so spend from the ads export and revenue from the sales
export were never on one chart, and ROAS was on no page. `spec.datasets`
names the files; the page draws one row set blended from them on the grain
they share, and says which file every number came from.
"""

from __future__ import annotations

import json
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

from _adv_dashboard import customize_dashboard, generate_dashboard  # noqa: E402

from tests.dashboard_page import run_js  # noqa: E402

DAYS = pd.date_range("2024-01-01", "2024-04-30", freq="D")


@pytest.fixture
def folder(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
    rng = np.random.default_rng(3)
    pd.DataFrame(
        [
            {"date": d.strftime("%Y-%m-%d"), "region": r, "spend": round(rng.uniform(50, 150), 2), "clicks": 40 + i % 3}
            for i, d in enumerate(DAYS)
            for r in ("North", "South", "East")
        ]
    ).to_csv(tmp_path / "ads.csv", index=False)
    # Weekly, with its region column called area, a region ads never ran in, and clicks of its own.
    pd.DataFrame(
        [
            {"week": d.strftime("%Y-%m-%d"), "area": r, "revenue": round(rng.uniform(300, 900), 2), "clicks": 250 + i % 2}
            for i, d in enumerate(DAYS[::7])
            for r in ("North", "South", "East", "West")
        ]
    ).to_csv(tmp_path / "sales.csv", index=False)
    return tmp_path


def _make(folder, **spec):
    r = generate_dashboard(str(folder / "ads.csv"), spec=spec, output_path=str(folder / "d.html"), open_after=False)
    assert r["success"] is True, r.get("error")
    return r


def test_each_file_is_triaged(folder):
    r = _make(folder, datasets={"sales": "sales.csv"})
    ads, sales = r["datasets"]["ads"], r["datasets"]["sales"]
    assert (ads["rows"], ads["grain"]["frequency"]) == (363, "day")
    assert (sales["rows"], sales["grain"]["frequency"]) == (72, "week")
    assert ads["one_row_per_period_and_segment"] is True


def test_a_key_named_differently_is_found_by_its_values(folder):
    (rel,) = _make(folder, datasets={"sales": "sales.csv"})["relationships"]
    assert rel["on"] == ["region", "area"] and rel["found_by"] == "values"
    assert (rel["match_left"], rel["match_right"]) == (1.0, 0.75)
    assert rel["orphans_right"] == ["West"] and rel["orphans_left_count"] == 0


def test_the_blend_sums_each_file_to_the_coarser_grain(folder):
    r = _make(folder, datasets={"sales": "sales.csv"})
    b = r["blend"]
    assert (b["grain"], b["on"], b["date"]) == ("week", ["region"], "date")
    assert b["columns"]["spend"]["dataset"] == "ads" and b["columns"]["revenue"]["dataset"] == "sales"
    assert b["columns"]["clicks (ads)"]["dataset"] == "ads" and b["columns"]["clicks (sales)"]["dataset"] == "sales"


def test_a_ratio_across_files_is_a_ratio_of_their_sums(folder):
    r = _make(folder, datasets={"sales": "sales.csv"})
    assert r["metrics"]["ROAS"]["formula"] == "sum(revenue) / sum(spend)"
    assert "from ads, sales" in r["metrics"]["ROAS"]["description"]
    # CPC takes both parts from ads, not the clicks sales counts.
    assert r["metrics"]["CPC"]["formula"] == "sum(spend) / sum(`clicks (ads)`)"


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
def test_the_blend_keeps_every_files_totals(folder):
    _make(folder, datasets={"sales": "sales.csv"})
    rows = run_js((folder / "d.html").read_text(encoding="utf-8"), "_RAW")
    ads, sales = pd.read_csv(folder / "ads.csv"), pd.read_csv(folder / "sales.csv")
    assert sum(x["spend"] for x in rows) == pytest.approx(ads["spend"].sum())
    assert sum(x["revenue"] for x in rows) == pytest.approx(sales["revenue"].sum())


def test_the_same_measure_in_two_files_is_reconciled(folder):
    (rec,) = _make(folder, datasets={"sales": "sales.csv"})["reconciliation"]
    assert rec["measure"] == "clicks"
    ads, sales = pd.read_csv(folder / "ads.csv"), pd.read_csv(folder / "sales.csv")
    assert rec["totals"] == {"ads": float(ads["clicks"].sum()), "sales": float(sales["clicks"].sum())}
    assert rec["largest_gaps"][0]["region"] == "West", "West has sales clicks and no ads"


def test_the_storyline_says_where_its_rows_came_from(folder):
    r = _make(folder, datasets={"sales": "sales.csv"})
    sources = next(p for p in r["spec"]["layout"] if p.get("title") == "Sources")
    assert "ads and sales share **region = area** (many-to-many)" in sources["text"]
    risks = next(t for t in r["spec"]["tabs"] if t["name"] == "Risks")
    titles = [r["spec"]["layout"][i].get("title") for i in risks["slots"]]
    assert "clicks: ads and sales differ" in titles and "Nothing flagged" not in titles


def test_a_filter_on_the_shared_key_narrows_both_files(folder):
    r = _make(folder, datasets={"sales": "sales.csv"})
    assert any((f if isinstance(f, str) else f.get("column")) == "region" for f in r["spec"]["filters"])


def test_same_schema_files_are_stacked(folder):
    for month in ("01", "02"):
        pd.DataFrame({"date": [f"2024-{month}-0{d}" for d in range(1, 8)], "region": "North", "spend": 1.0}).to_csv(
            folder / f"m{month}.csv", index=False
        )
    r = _make(folder, datasets={"monthly": {"paths": ["m01.csv", "m02.csv"]}})
    assert r["datasets"]["monthly"]["rows"] == 14 and r["datasets"]["monthly"]["origin"] == "2 files stacked"


def test_files_that_do_not_stack_are_named(folder):
    pd.DataFrame({"date": ["2024-01-01"], "spend": [1.0]}).to_csv(folder / "a.csv", index=False)
    pd.DataFrame({"date": ["2024-02-01"], "cost": [1.0]}).to_csv(folder / "b.csv", index=False)
    r = generate_dashboard(
        str(folder / "ads.csv"), spec={"datasets": {"m": {"paths": ["a.csv", "b.csv"]}}}, open_after=False
    )
    assert r["success"] is False
    assert "b.csv does not have a.csv's columns; it lacks spend; it adds cost" in r["error"]


def test_a_saved_chain_is_a_dataset_and_writes_nothing(folder):
    steps = [
        {"id": "raw", "load": "sales.csv"},
        {"id": "big", "ops": [{"op": "filter", "where": "revenue > 0"}]},
        {"id": "out", "write": "never.csv"},
    ]
    (folder / "clean.chain.json").write_text(json.dumps({"steps": steps}), encoding="utf-8")
    r = _make(folder, datasets={"sales": {"chain": "clean.chain.json"}})
    assert r["datasets"]["sales"]["origin"] == "the chain clean.chain.json"
    assert not (folder / "never.csv").exists()


def test_a_workbook_is_a_dataset(folder):
    pd.read_csv(folder / "sales.csv").to_excel(folder / "sales.xlsx", index=False)
    r = _make(folder, datasets={"sales": "sales.xlsx"})
    assert r["datasets"]["sales"]["rows"] == 72


@pytest.mark.parametrize(
    ("spec", "says"),
    [
        ({"datasets": {"sales": "nope.csv"}}, "datasets.sales: nope.csv was not found"),
        ({"datasets": {"2x": "sales.csv"}}, "datasets name '2x'"),
        ({"datasets": {"ads": "sales.csv"}}, "is the file's own name"),
        ({"datasets": {"sales": {"file": "sales.csv"}}}, "takes one of path, paths or chain; unknown key(s): file"),
        ({"datasets": {"sales": "sales.csv"}, "blend": {"on": ["nope"]}}, "blend.on names 'nope'"),
        ({"datasets": {"sales": "sales.csv"}, "blend": {"grain": "day"}}, "finer than a dataset kept by week"),
    ],
)
def test_what_cannot_be_blended_is_refused_by_name(folder, spec, says):
    r = generate_dashboard(str(folder / "ads.csv"), spec=spec, open_after=False)
    assert r["success"] is False and says in r["error"], r.get("error")


def test_customizing_a_blended_page_blends_again(folder):
    first = _make(folder, datasets={"sales": "sales.csv"})
    again = customize_dashboard(first["output_path"], {"title": "Ads and sales"}, open_after=False)
    assert again["success"] is True, again
    assert again["blend"] == first["blend"] and again["reconciliation"] == first["reconciliation"]
