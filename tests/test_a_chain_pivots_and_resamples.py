"""A chain pivots a table and resamples it by period, and says the same thing as pandas.

pivot_table and resample_timeseries were tools of their own, so a chain that
cleaned and joined had to write a file and hand it to another call. Two step
kinds bring them in:

- `pivot`: {"pivot": "channel", "rows": ["region"], "value": "sum(net)"} --
  a column per value of `channel`, a row per region, each cell the formula;
- `resample`: {"resample": "day", "every": "month", "by": ["channel"],
  "agg": {"revenue": "sum(net)"}} -- a row per period (and group), every
  period from a group's first to its last, weeks starting on Monday.

One rule covers the cells a reshape invents: a pivot cell or a period that no
row falls in holds the formula over no rows -- a sum or count of nothing is
0, a mean of nothing is empty. The pandas export writes the same files.
"""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd
import pytest

from servers.data_transform import engine

# The module the engine runs -- imported by its own name, so patch that one.
CHAIN = sys.modules[engine.run_chain.__module__]


@pytest.fixture
def data(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
    rng = np.random.default_rng(53)
    n = 90
    days = pd.date_range("2024-01-03", periods=n, freq="2D")
    # March is left empty on purpose: a period no row falls in.
    days = days[(days.month != 3)]
    pd.DataFrame(
        {
            "day": days.strftime("%Y-%m-%d"),
            "region": rng.choice(["North", "South", "East", None], len(days)),
            "channel": rng.choice(["web", "store", None], len(days), p=[0.45, 0.45, 0.1]),
            "net": rng.integers(1, 100, len(days)).astype(float),
        }
    ).to_csv(tmp_path / "orders.csv", index=False)
    return tmp_path


def _run(steps: list[dict], **kw) -> dict:
    return engine.run_chain([{"id": "orders", "load": "orders.csv"}, *steps], **kw)


def _orders(data) -> pd.DataFrame:
    return pd.read_csv(data / "orders.csv")


class TestPivot:
    def test_every_cell_is_the_formula_of_its_rows(self, data):
        r = _run([{"pivot": "channel", "rows": "region", "value": "sum(net)"}, {"write": "wide.csv"}])
        assert r["success"] is True, r.get("error")
        wide = pd.read_csv(data / "wide.csv", keep_default_na=False, na_values=[""])
        df = _orders(data)
        sums = df.dropna(subset=["channel"]).groupby(["region", "channel"], dropna=False)["net"].sum()
        want = {(r if isinstance(r, str) else None, c): v for (r, c), v in sums.items()}
        assert list(wide.columns) == ["region", "store", "web"] and len(wide) == 4, "a missing region is a row too"
        for _, row in wide.iterrows():
            for channel in ("store", "web"):
                key = (row["region"] if isinstance(row["region"], str) else None, channel)
                assert row[channel] == pytest.approx(want.get(key, 0.0)), key

    def test_an_empty_cell_is_the_formula_over_no_rows(self, data):
        steps = [
            {"ops": [{"op": "filter", "where": "not (region == 'East' and channel == 'web')"}]},
            {"pivot": "channel", "rows": ["region"], "value": "count()"},
            {"write": "count.csv"},
        ]
        r = _run(steps)
        assert r["success"] is True, r.get("error")
        count = pd.read_csv(data / "count.csv").set_index("region")
        assert count.loc["East", "web"] == 0 and count["web"].dtype.kind == "i"
        steps[1]["value"] = "mean(net)"
        steps[2]["write"] = "mean.csv"
        assert _run(steps)["success"] is True
        assert pd.isna(pd.read_csv(data / "mean.csv").set_index("region").loc["East", "web"])

    def test_rows_with_no_pivot_value_are_counted_out(self, data):
        r = _run([{"id": "wide", "pivot": "channel", "rows": ["region"], "value": "sum(net)"}])
        missing = int(_orders(data)["channel"].isna().sum())
        step = next(s for s in r["steps"] if s["id"] == "wide")
        assert step["note"] == f"{missing} rows with no 'channel' are in no column" and step["columns_made"] == 2

    @pytest.mark.parametrize(
        ("step", "says"),
        [
            ({"pivot": "channel", "value": "sum(net)"}, "rows must name the column(s) that stay rows"),
            ({"pivot": "channel", "rows": ["channel"], "value": "sum(net)"}, "is both the pivot and a rows column"),
            ({"pivot": "channel", "rows": ["region"], "value": "sum($nope)"}, "uses $nope, which no scalar or param"),
            ({"pivot": "chanel", "rows": ["region"], "value": "sum(net)"}, "'chanel' is not a column of 'orders'"),
        ],
    )
    def test_refused_by_name(self, data, step, says):
        r = _run([step])
        assert r["success"] is False and says in r["error"], r.get("error")

    def test_too_many_columns(self, data, monkeypatch):
        monkeypatch.setattr(CHAIN, "MAX_PIVOT_COLUMNS", 5)
        r = _run([{"pivot": "net", "rows": ["region"], "value": "count()"}])
        assert r["success"] is False and "distinct values; a pivot makes at most 5 columns" in r["error"]

    def test_a_value_that_is_also_a_rows_column(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
        pd.DataFrame({"k": ["a", "b"], "c": ["k", "x"], "v": [1, 2]}).to_csv(tmp_path / "t.csv", index=False)
        r = engine.run_chain([{"load": "t.csv"}, {"pivot": "c", "rows": ["k"], "value": "sum(v)"}])
        assert r["success"] is False and "'c' holds 'k', which is also a rows column" in r["error"]


class TestResample:
    def test_a_month_is_pandas_resample(self, data):
        steps = [
            {"resample": "day", "every": "month", "by": "channel", "agg": {"revenue": "sum(net)", "n": "count()"}},
            {"write": "monthly.csv"},
        ]
        r = _run(steps)
        assert r["success"] is True, r.get("error")
        got = pd.read_csv(data / "monthly.csv", keep_default_na=False, na_values=[""])
        df = _orders(data).assign(day=lambda d: pd.to_datetime(d["day"])).dropna(subset=["channel"])
        for channel, group in df.groupby("channel"):
            want = group.set_index("day")["net"].resample("MS").agg(["sum", "count"])
            mine = got[got["channel"] == channel]
            assert mine["day"].tolist() == [d.strftime("%Y-%m-%d") for d in want.index]
            assert mine["revenue"].tolist() == pytest.approx(want["sum"].tolist())
            assert mine["n"].tolist() == want["count"].tolist()

    def test_the_empty_month_is_there(self, data):
        r = _run(
            [{"resample": "day", "every": "month", "agg": {"n": "count()", "avg": "mean(net)"}}, {"write": "m.csv"}]
        )
        assert r["success"] is True, r.get("error")
        march = pd.read_csv(data / "m.csv").set_index("day").loc["2024-03-01"]
        assert march["n"] == 0 and pd.isna(march["avg"])

    def test_a_week_starts_on_monday(self, data):
        r = _run([{"resample": "day", "every": "week", "agg": {"n": "count()"}}, {"write": "w.csv"}])
        weeks = pd.to_datetime(pd.read_csv(data / "w.csv")["day"])
        assert (weeks.dt.dayofweek == 0).all() and weeks.iloc[0] == pd.Timestamp("2024-01-01")
        assert r["steps"][-2]["periods"] == len(weeks)

    def test_day_first_dates_are_read_day_first(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
        monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
        pd.DataFrame({"d": ["13/01/2024", "02/02/2024", "25/02/2024"], "v": [1, 2, 3]}).to_csv(
            tmp_path / "t.csv", index=False
        )
        r = engine.run_chain(
            [{"load": "t.csv"}, {"resample": "d", "every": "month", "agg": {"v": "sum(v)"}}, {"write": "o.csv"}]
        )
        assert r["success"] is True, r.get("error")
        assert pd.read_csv(tmp_path / "o.csv").to_dict("list") == {"d": ["2024-01-01", "2024-02-01"], "v": [1, 5]}

    def test_a_date_that_does_not_read_is_counted_out(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
        pd.DataFrame({"d": ["2024-01-05", "soon", None], "v": [1, 2, 3]}).to_csv(tmp_path / "t.csv", index=False)
        r = engine.run_chain(
            [{"load": "t.csv"}, {"id": "m", "resample": "d", "every": "month", "agg": {"v": "sum(v)"}}]
        )
        assert r["success"] is True, r.get("error")
        assert r["steps"][-1]["notes"] == ["2 rows with no readable 'd' are in no period"]

    @pytest.mark.parametrize(
        ("step", "says"),
        [
            (
                {"resample": "day", "every": "fortnight", "agg": {"n": "count()"}},
                "every 'fortnight' -- use one of day, week, month, quarter, year",
            ),
            (
                {"resample": "day", "every": "month", "by": ["day"], "agg": {"n": "count()"}},
                "is both the date and a by column",
            ),
            ({"resample": "day", "every": "month", "agg": {"day": "count()"}}, "agg 'day' is also a by or date column"),
            ({"resample": "day", "every": "month"}, "agg must map each new column to an aggregate"),
            ({"resample": "region", "every": "month", "agg": {"n": "count()"}}, "no value of 'region' reads as a date"),
        ],
    )
    def test_refused_by_name(self, data, step, says):
        r = _run([step])
        assert r["success"] is False and says in r["error"], r.get("error")

    def test_too_many_periods(self, data, monkeypatch):
        monkeypatch.setattr(CHAIN, "MAX_RESHAPE_ROWS", 10)
        r = _run([{"resample": "day", "every": "day", "agg": {"n": "count()"}}])
        assert r["success"] is False and "over the limit of 10; use a longer period" in r["error"]
