"""A chart given a color_column draws a trace per colour -- it used to fail every time.

generate_chart groups bar, line and scatter values by category_column before
drawing, and the grouping kept only that column and the value. So the colour
column was gone by the time Plotly looked for it, and every one of those
charts given a color_column failed with "Value of 'color' is not the name of a
column in 'data_frame'" -- an argument the tool advertised that could only
ever break the call.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from servers.data_visual.engine import generate_chart
from shared.plotly_payload import decode_array, split_newplot


@pytest.fixture
def csv(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(43)
    n = 180
    path = tmp_path / "sales.csv"
    pd.DataFrame(
        {
            "region": rng.choice(["North", "South", "East"], n),
            "channel": rng.choice(["web", "store"], n),
            "revenue": rng.normal(1000, 150, n).round(2),
        }
    ).to_csv(path, index=False)
    return path


def _traces(path: str) -> list[dict]:
    _, _, data, _, _, _ = split_newplot(Path(path).read_text(encoding="utf-8"))
    return json.loads(data)


@pytest.mark.parametrize("kind", ["bar", "line", "scatter"])
def test_each_colour_is_a_trace_of_its_own_totals(csv, kind):
    r = generate_chart(str(csv), kind, "revenue", category_column="region", color_column="channel", open_after=False)
    assert r["success"] is True, r.get("error")
    want = pd.read_csv(csv).groupby(["channel", "region"])["revenue"].sum()
    traces = _traces(r["output_path"])
    assert sorted(t["name"] for t in traces) == ["store", "web"]
    for t in traces:
        got = dict(zip(t["x"], decode_array(t["y"]), strict=True))
        assert got == pytest.approx(want[t["name"]].to_dict()), t["name"]


def test_the_bars_are_ranked_by_their_category_total(csv):
    r = generate_chart(str(csv), "bar", "revenue", category_column="region", color_column="channel", open_after=False)
    order = list(pd.read_csv(csv).groupby("region")["revenue"].sum().sort_values(ascending=False).index)
    seen: list[str] = []
    for t in _traces(r["output_path"]):
        seen += [x for x in t["x"] if x not in seen]
    assert seen == order


def test_a_colour_column_that_is_not_there_is_refused_by_name(csv):
    r = generate_chart(str(csv), "bar", "revenue", category_column="region", color_column="chanel", open_after=False)
    assert r["success"] is False and "color_column 'chanel' is not a column. Columns: region, channel" in r["error"]
