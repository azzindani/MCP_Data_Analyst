"""A finding names its segment, a duration is averaged, and a KPI tile draws its line.

The hotel-bookings file read "1 brings 52% of lead_time" -- the 1 is `is_canceled`, a column the sentence
did not name -- over a "Total lead_time" KPI, summing days of lead time across 119,390 bookings. A code in
a sentence is the column and the value ("is_canceled = 1"), a duration or a rate is a mean, and the KPI
tile in a layout carries the measure by period as a sparkline, as the default KPI row always has.
"""

from __future__ import annotations

import re

import numpy as np
import pandas as pd
import pytest

from servers.data_advanced._adv_dashboard import generate_dashboard
from shared.analysis_plan import plan
from shared.story import _segment


@pytest.fixture(autouse=True)
def _home(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    return tmp_path


@pytest.fixture
def bookings(_home) -> str:
    rng = np.random.default_rng(5)
    n = 900
    frame = pd.DataFrame(
        {
            "arrival": pd.date_range("2023-01-01", periods=n).strftime("%Y-%m-%d"),
            "is_canceled": rng.choice([0, 1], n, p=[0.35, 0.65]),
            "channel": rng.choice(["web", "agent", "phone"], n),
            "revenue": rng.integers(80, 600, n),
            "lead_time": rng.integers(0, 300, n),
            "adr": rng.uniform(60, 220, n).round(2),
            "latency_ms": rng.integers(5, 900, n),
        }
    )
    path = _home / "bookings.csv"
    frame.to_csv(path, index=False)
    return str(path)


class TestACodeIsNamedByItsColumn:
    @pytest.mark.parametrize("value", ["0", "1", "12", "-3", "4.5", "True", "false", "Y", "n", "yes"])
    def test_a_bare_code_is_the_column_and_the_value(self, value):
        assert _segment("is_canceled", value) == f"is_canceled = {value}"

    @pytest.mark.parametrize("value", ["Portugal", "City Hotel", "web", "2017-08", "Q3", "A-12"])
    def test_a_name_is_left_as_it_is(self, value):
        assert _segment("channel", value) == value

    def test_the_story_says_which_column_the_code_is_of(self, bookings, _home):
        # A flag is an outcome, so a story with a real dimension to split by splits by that; with only the
        # flag left to split by, the code is what it has, and it still names its column.
        only_the_code = _home / "only_the_code.csv"
        frame = pd.read_csv(bookings).drop(columns=["channel"])
        # Cancelled bookings carry three times the revenue: a share that differs from the rows' is a finding.
        frame["revenue"] = frame["revenue"] * np.where(frame["is_canceled"] == 1, 3, 1)
        frame.to_csv(only_the_code, index=False)
        result = generate_dashboard(str(only_the_code), output_path=str(_home / "p.html"), open_after=False)
        assert result["success"] is True, result.get("error")
        words = " ".join(str(p.get(k, "")) for p in result["spec"]["layout"] for k in ("title", "text"))
        assert re.search(r"is_canceled = [01] (brings|is where)", words), words[:600]
        assert not re.search(r"(^|\. |### )[01] (brings|is where)", words), "a bare code opens a sentence"


class TestADurationIsAveragedNotSummed:
    @pytest.mark.parametrize("column", ["lead_time", "adr", "latency_ms"])
    def test_it_is_not_additive(self, bookings, column):
        found = plan(pd.read_csv(bookings))["columns"][column]
        assert found["role"] == "measure" and found["additive"] is False

    def test_a_count_of_money_still_adds_up(self, bookings):
        assert plan(pd.read_csv(bookings))["columns"]["revenue"]["additive"] is True

    def test_the_kpi_for_a_duration_is_its_mean(self, bookings, _home):
        result = generate_dashboard(bookings, output_path=str(_home / "k.html"), open_after=False)
        kpis = {p["cols"].get("value"): p for p in result["spec"]["layout"] if p["chart"] == "kpi"}
        for column in ("lead_time", "adr"):
            if column in kpis:
                assert kpis[column].get("agg") == "mean", column
        assert "revenue" in kpis and kpis["revenue"].get("agg", "sum") == "sum"


class TestAKpiTileDrawsItsLine:
    def test_the_page_has_the_sparkline_code_and_a_place_for_it(self, bookings, _home):
        result = generate_dashboard(bookings, output_path=str(_home / "s.html"), open_after=False)
        html = (_home / "s.html").read_text(encoding="utf-8")
        assert "HTMLP_AFTER.kpi=" in html and "HTMLP_AFTER[p.type](p,d)" in html
        assert 'class="kpi-spark"' in html or "kpi-spark" in html
        tiles = re.findall(r'"type":"kpi".{0,200}?"date":"arrival"', html)
        assert len(tiles) >= 2, "a tile knows its date column"
        assert result["success"] is True

    def test_it_is_drawn_by_plotly_in_the_pages_accent(self, bookings, _home):
        generate_dashboard(bookings, output_path=str(_home / "t.html"), open_after=False)
        html = (_home / "t.html").read_text(encoding="utf-8")
        block = html[html.index("HTMLP_AFTER.kpi=") :]
        block = block[: block.index("\n};") + 3]
        assert "Plotly.react" in block and "--accent" in block and "staticPlot:true" in block
        assert "<svg" not in block, "Plotly stays the chart engine, small ones too"
