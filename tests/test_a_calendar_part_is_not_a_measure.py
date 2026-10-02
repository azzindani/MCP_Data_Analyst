"""A piece of a date is not a quantity, a month name is not a date, and "Avg" means mean.

The 2026-10-02 sweep ran the hotel-bookings file (119,390 rows) through the dashboard and found
three defects that read as results:

* `arrival_date_year`, `arrival_date_week_number`, `arrival_date_day_of_month` and the agent and
  company codes were planned as measures: a "Total lead_time" KPI sat beside headlines like
  "1 brings 52% of lead_time" and "0 brings 63% of arrival_date_week_number".
* The KPI titled "Avg arrival_date_year" showed 240.7M. The title came from the metric (a mean),
  but the panel carried no `agg`, so the renderer drew its default, a sum.
* `arrival_date_month` ("July") was detected as a date column, offered `cast_column
  dtype=datetime`, and written as `1-07-01` -- year 1 -- with `failed: 0`; date_parts then gave
  every row `arrival_date_month_year = 1`.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_advanced"), str(ROOT / "servers" / "data_medium")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _adv_dashboard import generate_dashboard  # noqa: E402
from _med_inspect import auto_detect_schema  # noqa: E402

from servers.data_basic.engine import apply_patch  # noqa: E402
from servers.data_transform.engine import feature_engineering  # noqa: E402
from shared.analysis_plan import plan  # noqa: E402
from shared.column_utils import calendar_part, is_identifier, looks_like_dates, parse_date_column  # noqa: E402

MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October"]


def _hotel(n: int = 400, seed: int = 4) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    arrival = pd.to_datetime("2016-01-01") + pd.to_timedelta(rng.integers(0, 600, n), unit="D")
    return pd.DataFrame(
        {
            "hotel": rng.choice(["City Hotel", "Resort Hotel"], n),
            "is_canceled": rng.integers(0, 2, n),
            "lead_time": rng.integers(0, 400, n),
            "arrival_date_year": arrival.year,
            "arrival_date_month": arrival.strftime("%B"),
            "arrival_date_week_number": arrival.isocalendar().week.astype(int),
            "arrival_date_day_of_month": arrival.day,
            "stays_in_week_nights": rng.integers(0, 6, n),
            "agent": rng.choice([9.0, 14.0, 240.0, 7.0, 28.0], n),
            "adr": rng.normal(100, 30, n).round(2),
            "reservation_status_date": arrival.strftime("%Y-%m-%d"),
        }
    )


class TestACalendarPartIsNotAMeasure:
    @pytest.mark.parametrize(
        "name,values,part",
        [
            ("arrival_date_year", [2015, 2016, 2017], "year"),
            ("arrival_date_week_number", [1, 27, 53], "week"),
            ("arrival_date_day_of_month", [1, 15, 31], "day_of_month"),
            ("Month", [1, 6, 12], "month"),
            ("birth_year", [1985, 1990], "year"),
        ],
    )
    def test_a_piece_of_a_date_is_named_one(self, name, values, part):
        assert calendar_part(name, pd.Series(values)) == part

    @pytest.mark.parametrize(
        "name,values",
        [
            ("stays_in_week_nights", [0, 3, 50]),  # ends in "nights": a quantity of them
            ("days_in_waiting_list", [0, 5, 300]),
            ("lead_time", [0, 100, 700]),
            ("tenure_years", [1, 2, 3]),
            ("week_number", [1.5, 2.5]),  # not whole numbers
            ("revenue_month", [100, 200, 300]),  # a month's revenue, outside 1-12
        ],
    )
    def test_a_quantity_with_a_calendar_word_in_its_name_is_not(self, name, values):
        assert calendar_part(name, pd.Series(values)) == ""

    def test_an_integer_code_named_for_an_entity_is_an_identifier(self):
        assert is_identifier("agent", pd.Series([9.0, 14.0, 240.0]))
        assert is_identifier("company", pd.Series([40.0, 223.0]))
        assert not is_identifier("agent_fee", pd.Series([9.5, 14.2]))
        assert not is_identifier("store_count", pd.Series([3, 5, 9]))

    def test_the_plan_keeps_them_out_of_the_measures(self):
        planned = plan(_hotel())
        assert planned["columns"]["arrival_date_year"]["role"] == "calendar"
        assert planned["columns"]["arrival_date_week_number"]["role"] == "calendar"
        assert planned["columns"]["agent"]["role"] == "id"
        assert {"lead_time", "stays_in_week_nights", "adr"} <= set(planned["measures"])
        assert not {"arrival_date_year", "arrival_date_week_number", "arrival_date_day_of_month", "agent"} & set(
            planned["measures"]
        )
        assert any("part of a date" in n or "parts of a date" in n for n in planned["notes"]), planned["notes"]


class TestAnAvgKpiDrawsAMean:
    @pytest.fixture
    def page(self, tmp_path, monkeypatch):
        monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
        monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
        rng = np.random.default_rng(1)
        n = 240
        pd.DataFrame(
            {
                "day": pd.date_range("2024-01-01", periods=n).strftime("%Y-%m-%d"),
                "region": rng.choice(["North", "South", "East"], n),
                "spend": rng.integers(50, 500, n),
                "rating": rng.uniform(1, 5, n).round(2),
            }
        ).to_csv(tmp_path / "shop.csv", index=False)
        result = generate_dashboard(str(tmp_path / "shop.csv"), output_path=str(tmp_path / "d.html"), open_after=False)
        assert result["success"] is True, result.get("error")
        return result

    def test_a_title_that_says_avg_comes_with_the_mean(self, page):
        kpis = [p for p in page["spec"]["layout"] if p["chart"] == "kpi"]
        titled = [p for p in kpis if str(p["title"]).startswith("Avg ")]
        assert titled, [p["title"] for p in kpis]
        for panel in titled:
            assert panel.get("agg") == "mean", panel

    def test_every_panel_that_names_a_column_metric_carries_its_aggregate(self, page):
        for panel in page["spec"]["layout"]:
            title = str(panel.get("title", ""))
            if panel["chart"] in ("kpi", "table", "bar", "time_series") and title.startswith(("Avg ", "Total ")):
                assert panel.get("agg") == ("mean" if title.startswith("Avg ") else "sum"), panel


class TestAMonthNameIsNotADate:
    def _names(self) -> pd.Series:
        return pd.Series((MONTHS * 30)[:300])

    def test_neither_detector_reads_names_as_dates(self):
        assert parse_date_column(self._names()) is None
        assert looks_like_dates(self._names())[0] is False

    def test_a_date_with_a_year_still_is_one(self):
        assert parse_date_column(pd.Series(["Jul 4, 2017", "Aug 9, 2016"] * 20)) is not None
        assert looks_like_dates(pd.Series(["2017-07-01", "2017-08-02"] * 20))[0] is True

    def test_auto_detect_schema_does_not_suggest_a_datetime_cast(self, tmp_path):
        path = tmp_path / "h.csv"
        _hotel().to_csv(path, index=False)
        columns = auto_detect_schema(str(path))["columns"]
        assert columns["arrival_date_month"]["inferred_type"] != "datetime"
        assert "cast_column" not in str(columns["arrival_date_month"].get("suggestion"))

    def test_the_explicit_cast_is_refused_with_the_reason(self, tmp_path):
        path = tmp_path / "h.csv"
        _hotel().to_csv(path, index=False)
        result = apply_patch(str(path), [{"op": "cast_column", "column": "arrival_date_month", "dtype": "datetime"}])
        assert result["success"] is False
        reason = result["op_errors"][0]["error"]
        assert "year 1" in reason and "replace_values" in reason
        assert pd.read_csv(path)["arrival_date_month"].iloc[0] in MONTHS  # nothing was written

    def test_date_parts_makes_none_from_a_month_name(self, tmp_path):
        path = tmp_path / "h.csv"
        _hotel().to_csv(path, index=False)
        result = feature_engineering(
            str(path), features=["date_parts"], output_path=str(tmp_path / "out.csv"), open_after=False
        )
        assert result["success"] is True, result.get("error")
        assert not any(c.startswith("arrival_date_month_") for c in result["new_columns"]), result["new_columns"]
        assert "reservation_status_date_year" in result["new_columns"]


class TestAnExplicitLayoutsKpiRow:
    """A page laid out by the caller, with no `kpis`, opened on `numeric_cols[:7]`: every number, so the
    hotel file's row led with "Total arrival_date_year 240.7M" and a total of week numbers."""

    def test_the_default_kpis_are_the_measures(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
        _hotel().to_csv(tmp_path / "h.csv", index=False)
        result = generate_dashboard(
            str(tmp_path / "h.csv"),
            output_path=str(tmp_path / "h.html"),
            open_after=False,
            spec={"layout": [{"chart": "bar", "cols": {"category": "hotel", "value": "adr"}, "agg": "mean"}]},
        )
        assert result["success"] is True, result.get("error")
        shown = set(result["spec"]["kpis"])
        assert {"lead_time", "adr"} <= shown
        assert not shown & {"arrival_date_year", "arrival_date_week_number", "arrival_date_day_of_month", "agent"}
