"""A three-day effect looks like no effect at all through a contemporaneous correlation.

`correlation_analysis` compares row i against row i. Spend does not produce
clicks the same instant it is booked -- it produces them over the days that
follow -- so a real effect delayed by a few periods reaches that tool as noise.
On a series built so that y[t] = 2*x[t-3] exactly:

    contemporaneous (lag 0)    r = -0.014     "no relationship"
    lag +3                     r =  0.999

Both numbers are correct. The first is just the answer to a different question.

`lag_correlation` sweeps the lags and reports the curve. The parts already
existed -- apply_patch has lag and lead ops and correlation_analysis runs the
correlation -- so a caller could do this by hand, one lag per pass, reading the
peak off by eye.

What the tests below are mostly guarding is the ways a lag sweep quietly lies:

  the sign        +k has to mean x leads y, consistently, or every reading of
                  the result is backwards
  the grid        shifting transaction-level rows counts records, not time
  the overlap     each lag is its own paired sample; the far ends are thinnest,
                  and that is exactly where a spurious peak turns up
  the gaps        resample().sum() reports 0 for a period with no rows, and a
                  run of invented zeros bends the correlation on its own
  the p-value     the peak is the smallest of 2n+1 draws and is optimistic by
                  construction
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_statistics")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from servers.data_statistics import engine  # noqa: E402


def write(frame: pd.DataFrame, tmp_path: Path, name: str = "s.csv") -> str:
    p = tmp_path / name
    frame.to_csv(p, index=False)
    return str(p)


@pytest.fixture
def lagged(tmp_path) -> str:
    """y follows x by exactly three days."""
    rng = np.random.default_rng(0)
    n = 200
    x = rng.normal(10, 3, n)
    y = np.full(n, np.nan)
    y[3:] = x[:-3] * 2 + rng.normal(0, 0.2, n - 3)
    frame = pd.DataFrame(
        {"Date": pd.date_range("2024-01-01", periods=n, freq="D").astype(str), "x": x, "y": y}
    ).dropna()
    return write(frame, tmp_path)


class TestTheLagIsRecovered:
    def test_the_peak_sits_at_the_real_lag(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=8)
        assert r["success"] is True, r.get("error")
        assert r["peak_lag"] == 3, r["peak_lag"]

    def test_the_peak_is_strong(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=8)
        assert r["peak_r"] > 0.99, r["peak_r"]

    def test_the_contemporaneous_correlation_misses_it(self, lagged):
        # The reason the tool exists: lag 0 is what correlation_analysis reports.
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=8)
        assert abs(r["contemporaneous_r"]) < 0.1, r["contemporaneous_r"]
        assert r["gain_over_lag_0"] > 0.9, r["gain_over_lag_0"]

    def test_the_reading_names_which_leads(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=8)
        assert "x leads y" in r["reading"], r["reading"]

    def test_the_sign_convention_is_stated(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=8)
        assert "leads" in r["sign_convention"] and "corr(x[t], y[t+k])" in r["sign_convention"]


class TestTheSignIsNotBackwards:
    def test_swapping_the_columns_flips_the_sign(self, lagged):
        # The one mistake that makes every reading of this tool wrong.
        a = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=8)
        b = engine.lag_correlation(lagged, date_column="Date", x_column="y", y_column="x", max_lag=8)
        assert a["peak_lag"] == 3, a["peak_lag"]
        assert b["peak_lag"] == -3, b["peak_lag"]

    def test_the_reading_flips_too(self, lagged):
        b = engine.lag_correlation(lagged, date_column="Date", x_column="y", y_column="x", max_lag=8)
        assert "x leads y" in b["reading"], b["reading"]

    def test_a_simultaneous_series_peaks_at_zero(self, tmp_path):
        rng = np.random.default_rng(1)
        n = 120
        x = rng.normal(0, 1, n)
        frame = pd.DataFrame(
            {
                "Date": pd.date_range("2024-01-01", periods=n, freq="D").astype(str),
                "x": x,
                "y": x * 3 + rng.normal(0, 0.01, n),
            }
        )
        r = engine.lag_correlation(write(frame, tmp_path), date_column="Date", x_column="x", y_column="y", max_lag=6)
        assert r["peak_lag"] == 0, r["peak_lag"]
        assert "no lead or lag" in r["reading"]


class TestTheGridIsTimeNotRows:
    def test_transaction_rows_are_collapsed_per_period(self, tmp_path):
        # Many rows per day, which is the shape of the reference dataset. A shift
        # over raw rows would be a shift of "however many records happened to be
        # logged", not of days.
        days = pd.date_range("2024-01-01", periods=60, freq="D")
        rows = []
        for i, d in enumerate(days):
            for _ in range(7):
                rows.append({"Date": str(d.date()), "x": float(i), "y": 0.0})
        frame = pd.DataFrame(rows)
        # y[t] = x[t-2], applied per day
        per_day = {str(d.date()): float(i) for i, d in enumerate(days)}
        keys = sorted(per_day)
        for j, k in enumerate(keys):
            if j >= 2:
                frame.loc[frame["Date"] == k, "y"] = per_day[keys[j - 2]]
        r = engine.lag_correlation(write(frame, tmp_path), date_column="Date", x_column="x", y_column="y", max_lag=5)
        assert r["success"] is True, r.get("error")
        assert r["periods"] == 60, r["periods"]
        assert r["peak_lag"] == 2, r["peak_lag"]

    def test_the_period_unit_is_reported_back(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", period_unit="W", max_lag=3)
        assert r["period_unit"] == "W"

    def test_a_period_alias_is_accepted(self, lagged):
        # period_comparison takes MoM/WoW/YoY as well as the bare letters, and a
        # caller who learned the vocabulary there should not meet a refusal here.
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", period_unit="WoW", max_lag=2)
        assert r["success"] is True, r.get("error")
        assert r["period_unit"] == "W"


class TestAGapIsNotAZero:
    def test_missing_periods_do_not_become_observed_zeros(self, tmp_path):
        # resample().sum() reports 0 for a period with no rows in it. Those are
        # not observations, and a run of them at one end drags the correlation.
        days = list(pd.date_range("2024-03-01", periods=40, freq="D"))
        keep = [d for i, d in enumerate(days) if not (10 <= i < 25)]  # a 15-day hole
        rng = np.random.default_rng(3)
        vals = rng.normal(50, 5, len(keep))
        frame = pd.DataFrame({"Date": [str(d.date()) for d in keep], "x": vals, "y": vals * 2})
        r = engine.lag_correlation(write(frame, tmp_path), date_column="Date", x_column="x", y_column="y", max_lag=4)
        assert r["success"] is True, r.get("error")
        # 40 periods on the grid, but only the 25 that carry rows are usable.
        assert r["periods"] == 40, r["periods"]
        assert r["periods_with_both"] == 25, r["periods_with_both"]

    def test_the_hole_does_not_invent_a_correlation(self, tmp_path):
        days = list(pd.date_range("2024-03-01", periods=40, freq="D"))
        keep = [d for i, d in enumerate(days) if not (10 <= i < 25)]
        rng = np.random.default_rng(4)
        frame = pd.DataFrame(
            {
                "Date": [str(d.date()) for d in keep],
                "x": rng.normal(50, 5, len(keep)),
                "y": rng.normal(50, 5, len(keep)),
            }
        )
        r = engine.lag_correlation(write(frame, tmp_path), date_column="Date", x_column="x", y_column="y", max_lag=4)
        # Two independent series: nothing should come back strong.
        assert abs(r["peak_r"]) < 0.8, r["peak_r"]


class TestEachLagIsItsOwnPairedSample:
    def test_n_is_reported_per_lag(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=8)
        assert all("n" in c for c in r["correlations"])

    def test_the_overlap_shrinks_as_the_lag_widens(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=8)
        by_lag = {c["lag"]: c["n"] for c in r["correlations"]}
        assert by_lag[0] > by_lag[8], by_lag
        assert by_lag[0] > by_lag[-8], by_lag

    def test_nulls_are_dropped_pairwise_not_column_by_column(self, tmp_path):
        # The round-11 defect, one level down: dropping each column's nulls
        # separately and truncating to the shorter length offsets every pair
        # after the first null.
        n = 150
        rng = np.random.default_rng(5)
        x = rng.normal(20, 4, n)
        y = np.full(n, np.nan)
        y[2:] = x[:-2] * 1.5
        frame = pd.DataFrame({"Date": pd.date_range("2024-01-01", periods=n, freq="D").astype(str), "x": x, "y": y})
        frame.loc[40:60, "y"] = np.nan  # a block of nulls in the middle
        r = engine.lag_correlation(write(frame, tmp_path), date_column="Date", x_column="x", y_column="y", max_lag=5)
        assert r["peak_lag"] == 2, r["peak_lag"]
        assert r["peak_r"] > 0.99, r["peak_r"]

    def test_a_lag_too_thin_to_report_is_skipped_not_guessed(self, tmp_path):
        rng = np.random.default_rng(6)
        n = 14
        frame = pd.DataFrame(
            {
                "Date": pd.date_range("2024-01-01", periods=n, freq="D").astype(str),
                "x": rng.normal(0, 1, n),
                "y": rng.normal(0, 1, n),
            }
        )
        r = engine.lag_correlation(
            write(frame, tmp_path), date_column="Date", x_column="x", y_column="y", max_lag=10, min_overlap=8
        )
        assert r["success"] is True, r.get("error")
        assert r["lags_skipped"], "wide lags on 14 points should not all be reportable"
        assert all(c["n"] >= 8 for c in r["correlations"])


class TestThePeakIsNotOversold:
    def test_the_adjusted_p_value_is_bonferroni_over_the_lags(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=8)
        expected = min(1.0, r["peak_p_value"] * r["lags_tested"])
        assert r["peak_p_value_adjusted"] == pytest.approx(expected)

    def test_it_is_clamped_at_one(self, tmp_path):
        rng = np.random.default_rng(7)
        n = 60
        frame = pd.DataFrame(
            {
                "Date": pd.date_range("2024-01-01", periods=n, freq="D").astype(str),
                "x": rng.normal(0, 1, n),
                "y": rng.normal(0, 1, n),
            }
        )
        r = engine.lag_correlation(write(frame, tmp_path), date_column="Date", x_column="x", y_column="y", max_lag=6)
        assert 0.0 <= r["peak_p_value_adjusted"] <= 1.0

    def test_the_note_says_where_the_adjustment_came_from(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=8)
        assert "Bonferroni" in r["note"]

    def test_the_raw_p_value_is_still_there(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=8)
        assert r["peak_p_value"] <= r["peak_p_value_adjusted"]


class TestTheArgumentsAreHonoured:
    def test_the_aggregation_is_reported(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=4)
        assert set(r["aggregation"]) == {"x", "y"}

    def test_an_explicit_aggregation_is_used(self, lagged):
        r = engine.lag_correlation(
            lagged, date_column="Date", x_column="x", y_column="y", max_lag=4, x_agg="mean", y_agg="max"
        )
        assert r["aggregation"] == {"x": "mean", "y": "max"}

    def test_the_method_is_used(self, lagged):
        a = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=4)
        b = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=4, method="spearman")
        assert b["method"] == "spearman"
        assert a["peak_r"] != b["peak_r"]

    def test_date_col_is_accepted_as_an_alias(self, lagged):
        r = engine.lag_correlation(lagged, date_col="Date", x_column="x", y_column="y", max_lag=4)
        assert r["success"] is True, r.get("error")
        assert r["peak_lag"] == 3

    def test_max_lag_bounds_the_curve(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=4)
        assert max(abs(c["lag"]) for c in r["correlations"]) <= 4


class TestItRefusesRatherThanGuesses:
    def test_no_date_column(self, lagged):
        r = engine.lag_correlation(lagged, x_column="x", y_column="y")
        assert r["success"] is False
        assert "date_col" in r["hint"]

    def test_no_value_columns(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date")
        assert r["success"] is False
        assert "x_column" in r["hint"]

    def test_a_column_that_is_not_there(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="nope", y_column="y")
        assert r["success"] is False
        assert "Available" in r["hint"]

    def test_a_non_numeric_column(self, tmp_path):
        frame = pd.DataFrame(
            {
                "Date": pd.date_range("2024-01-01", periods=30, freq="D").astype(str),
                "x": ["a"] * 30,
                "y": range(30),
            }
        )
        r = engine.lag_correlation(write(frame, tmp_path), date_column="Date", x_column="x", y_column="y")
        assert r["success"] is False
        assert "read_column_stats" in r["hint"]

    def test_an_unparseable_date_column(self, tmp_path):
        frame = pd.DataFrame({"Date": ["not a date"] * 30, "x": range(30), "y": range(30)})
        r = engine.lag_correlation(write(frame, tmp_path), date_column="Date", x_column="x", y_column="y")
        assert r["success"] is False
        assert "inspect_dataset" in r["hint"]

    def test_a_bad_method(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", method="cosine")
        assert r["success"] is False
        assert "pearson" in r["hint"]

    def test_a_bad_period_unit(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", period_unit="fortnight")
        assert r["success"] is False
        assert "quarter" in r["hint"]

    def test_a_bad_aggregation(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", x_agg="median")
        assert r["success"] is False
        assert "sum" in r["hint"]

    def test_max_lag_of_zero_points_at_the_other_tool(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=0)
        assert r["success"] is False
        assert "correlation_analysis" in r["hint"]

    def test_min_overlap_below_the_floor(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", min_overlap=2)
        assert r["success"] is False
        assert "not defined" in r["hint"]

    def test_a_series_too_short_to_correlate(self, tmp_path):
        frame = pd.DataFrame(
            {"Date": pd.date_range("2024-01-01", periods=4, freq="D").astype(str), "x": [1, 2, 3, 4], "y": [4, 3, 2, 1]}
        )
        r = engine.lag_correlation(write(frame, tmp_path), date_column="Date", x_column="x", y_column="y")
        assert r["success"] is False
        assert "period_unit" in r["hint"]

    def test_a_missing_file(self, tmp_path):
        r = engine.lag_correlation(str(tmp_path / "nope.csv"), date_column="Date", x_column="x", y_column="y")
        assert r["success"] is False
        assert "absolute" in r["hint"]


class TestTheResponseContract:
    def test_token_estimate_is_present(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=4)
        assert isinstance(r["token_estimate"], int) and r["token_estimate"] > 0

    def test_failures_carry_it_too(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", method="cosine")
        assert isinstance(r["token_estimate"], int)

    def test_op_is_named_on_both_paths(self, lagged):
        good = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=4)
        bad = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", method="cosine")
        assert good["op"] == bad["op"] == "lag_correlation"

    def test_progress_is_a_list(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=4)
        assert isinstance(r["progress"], list) and r["progress"]

    def test_nothing_is_a_raw_numpy_scalar(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=4)
        for c in r["correlations"]:
            assert type(c["lag"]) is int
            assert type(c["n"]) is int
            assert type(c["r"]) is float


class TestConstrainedMode:
    def test_a_wide_sweep_is_capped(self, lagged, monkeypatch):
        monkeypatch.setenv("MCP_CONSTRAINED_MODE", "1")
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=40)
        assert r["success"] is True, r.get("error")
        assert r["max_lag_capped_to"] == 10
        assert max(abs(c["lag"]) for c in r["correlations"]) <= 10

    def test_the_cap_is_announced(self, lagged, monkeypatch):
        monkeypatch.setenv("MCP_CONSTRAINED_MODE", "1")
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=40)
        assert any("capped" in str(p).lower() for p in r["progress"]), r["progress"]

    def test_an_uncapped_sweep_says_nothing_about_capping(self, lagged):
        r = engine.lag_correlation(lagged, date_column="Date", x_column="x", y_column="y", max_lag=5)
        assert "max_lag_capped_to" not in r


class TestTheWrapperMatchesTheEngine:
    def test_every_declared_parameter_reaches_the_engine(self):
        import inspect

        from servers.data_statistics import server

        fn = getattr(server, "lag_correlation")
        fn = getattr(fn, "fn", fn)
        declared = list(inspect.signature(fn).parameters)
        engine_params = list(inspect.signature(engine.lag_correlation).parameters)
        assert declared == engine_params, (declared, engine_params)

    def test_the_docstring_fits_the_schema_budget(self):
        from servers.data_statistics import server

        fn = getattr(server, "lag_correlation")
        fn = getattr(fn, "fn", fn)
        assert len(fn.__doc__.strip()) <= 80, fn.__doc__
