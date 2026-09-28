"""A scatter over groups that each follow their own line is drawn a line per group.

The sweep's dashboard drew spends against impressions with one trend line
(r=0.74) through two visibly separate populations -- the two ad platforms,
each on a line of its own. A scatter panel now takes a `group` column and
draws each group in its colour with its own line, and the detected page picks
the group when separate lines fit markedly better than one (a Chow test).
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_advanced")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from servers.data_advanced._adv_dashboard import _scatter_group, generate_dashboard  # noqa: E402


def _two_platforms(n: int = 300, seed: int = 4) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    platform = np.where(np.arange(n) % 2 == 0, "Facebook", "Google")
    spends = rng.uniform(10, 500, n)
    impressions = spends * np.where(platform == "Facebook", 400.0, 15.0) * rng.uniform(0.9, 1.1, n)
    return pd.DataFrame(
        {"platform": platform, "spends": spends, "impressions": impressions, "noise": rng.choice(["a", "b"], n)}
    )


def test_the_group_that_splits_the_line_is_found():
    df = _two_platforms()
    assert _scatter_group(df, "spends", "impressions", ["noise", "platform"]) == "platform"


def test_one_population_is_not_split():
    rng = np.random.default_rng(9)
    x = rng.uniform(0, 10, 300)
    df = pd.DataFrame({"x": x, "y": 3 * x + rng.normal(0, 1, 300), "label": rng.choice(["p", "q", "r"], 300)})
    assert _scatter_group(df, "x", "y", ["label"]) == ""


def test_the_storyline_splits_its_scatter_too(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    _two_platforms().to_csv(tmp_path / "ads.csv", index=False)
    r = generate_dashboard(str(tmp_path / "ads.csv"), output_path=str(tmp_path / "d.html"), open_after=False)
    assert r["success"] is True, r.get("error")
    scatter = next(p for p in r["spec"]["layout"] if p["chart"] == "scatter")
    assert "group" not in scatter["cols"], "left to the page, the groups are the page's choice"
    page = Path(r["output_path"]).read_text(encoding="utf-8")
    assert '"group": "platform"' in page or '"group":"platform"' in page


def test_the_detected_page_draws_a_line_per_platform(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    _two_platforms().to_csv(tmp_path / "ads.csv", index=False)
    r = generate_dashboard(
        str(tmp_path / "ads.csv"), output_path=str(tmp_path / "d.html"), open_after=False, spec={"story": False}
    )
    assert r["success"] is True, r.get("error")
    page = Path(r["output_path"]).read_text(encoding="utf-8")
    assert "spends vs impressions by platform" in page
    assert '"group": "platform"' in page or '"group":"platform"' in page


def test_a_spec_panel_names_its_group(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    _two_platforms().to_csv(tmp_path / "ads.csv", index=False)
    spec = {"layout": [{"chart": "scatter", "cols": {"x": "spends", "y": "impressions", "group": "noise"}}]}
    r = generate_dashboard(str(tmp_path / "ads.csv"), spec=spec, output_path=str(tmp_path / "d.html"), open_after=False)
    assert r["success"] is True, r.get("error")
    assert "spends vs impressions by noise" in Path(r["output_path"]).read_text(encoding="utf-8")


@pytest.mark.parametrize("cols", [{"x": "spends", "y": "impressions", "group": "nope"}])
def test_a_group_that_is_not_a_column_is_refused(tmp_path, monkeypatch, cols):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    _two_platforms().to_csv(tmp_path / "ads.csv", index=False)
    r = generate_dashboard(
        str(tmp_path / "ads.csv"), spec={"layout": [{"chart": "scatter", "cols": cols}]}, open_after=False
    )
    assert r["success"] is False and "nope" in r["error"]
