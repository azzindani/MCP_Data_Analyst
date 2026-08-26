"""An empty "stl" and a computed seasonal_strength of 0.0 looked identical.

    time_series_analysis(Ad_Data.csv, period="W", value_columns=[...])
      -> stl: {}
         acf: {spends: {...}, clicks: {...}}     <- populated
         adf: {spends: {...}, clicks: {...}}     <- populated
         hint: "HTML chart saved -- open output_path for ..."

The tool's docstring is "Auto-detect dates, compute trend seasonality rolling
stats", so a caller asks it for seasonality. It returned an empty decomposition
with nothing anywhere saying why -- no progress line, no field, and a hint that
talks about the chart. ACF and ADF sat right beside it fully populated, which
makes the empty stl read as a *result*: statsmodels clearly ran, so an empty
seasonal decomposition must mean there is no seasonal component.

It means nothing of the kind. STL needs two whole cycles, and a cycle at
period="W" is 52 points, so it wants 104 weeks; the fixture resamples to 39.
The decomposition was never attempted. Round 16 caught it at "W"; it is worse
than the sweep saw, because "M" wants 24 months and the fixture has 10, so on
this dataset STL silently declined at every period a caller would reach for.

The proof that the two cases are genuinely different is period="D": there STL
does run, and it reports seasonal_strength 0.0 -- a real measurement that the
series has no weekly seasonality. Identical-looking answer, opposite meaning.
One was measured and one was skipped.

So the fix is disclosure plus a route out. stl_skipped names the reason and the
arithmetic (39 available, 104 needed, cycle 52), and the hint names a period
that actually works. That advice is counterintuitive -- a *finer* period, not a
coarser one -- because finer sampling clears the two-cycle bar from both
directions at once, and no caller guesses that. A hint naming a route that then
also fails would be worse than no hint, so the test below re-runs the period
the hint suggests and requires a decomposition to come back.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from servers.data_statistics import engine as ds

COLS = ["spends", "clicks"]


def _run(path: Path, period: str) -> dict:
    r = ds.time_series_analysis(
        str(path), date_column="Date", value_columns=COLS, period=period, output_path="", open_after=False
    )
    assert r["success"] is True, r.get("error")
    return r


@pytest.fixture()
def weekly(ad_data_full_csv: Path) -> dict:
    return _run(ad_data_full_csv, "W")


class TestTheSkipIsDeclared:
    def test_the_fixture_is_still_too_short_to_decompose_weekly(self, weekly: dict) -> None:
        """If the data grew past 104 weeks, everything below would pass for free."""
        assert weekly["total_periods"] < 104, weekly["total_periods"]
        assert weekly["stl"] == {}, weekly["stl"]

    def test_every_column_says_why(self, weekly: dict) -> None:
        assert set(weekly["stl_skipped"]) == set(COLS), weekly["stl_skipped"]

    def test_the_reason_carries_the_arithmetic(self, weekly: dict) -> None:
        skip = weekly["stl_skipped"]["spends"]
        assert skip["periods_available"] == weekly["total_periods"]
        assert skip["periods_needed"] == 104, skip
        assert skip["seasonal_cycle"] == 52, skip
        assert "104" in skip["reason"] and "52" in skip["reason"], skip

    def test_the_assumed_cycle_is_reported(self, weekly: dict) -> None:
        assert weekly["stl_seasonal_cycle"] == 52, weekly

    def test_the_hint_denies_the_wrong_reading(self, weekly: dict) -> None:
        """The exact misreading the silence invited."""
        hint = weekly["hint"]
        assert "does NOT" in hint and "no seasonality" in hint, hint
        assert "stl_skipped" in hint, hint

    def test_a_progress_line_carries_it_too(self, weekly: dict) -> None:
        msgs = " | ".join(str(p.get("message", "")) for p in weekly["progress"])
        assert "No STL decomposition" in msgs, msgs

    def test_acf_and_adf_still_populate(self, weekly: dict) -> None:
        """Their being full beside an empty stl is what made it misread."""
        assert set(weekly["acf"]) == set(COLS)
        assert set(weekly["adf"]) == set(COLS)


class TestMonthlyIsSilentlySkippedToo:
    """The sweep only checked "W"; the default period="M" fails the same way."""

    def test_the_default_period_also_declines(self, ad_data_full_csv: Path) -> None:
        r = _run(ad_data_full_csv, "M")
        assert r["stl"] == {}
        assert r["stl_skipped"]["spends"]["periods_needed"] == 24
        assert r["stl_skipped"]["spends"]["seasonal_cycle"] == 12


class TestTheSuggestedPeriodReallyWorks:
    """A hint naming a route that also fails would be worse than none."""

    def test_the_hint_names_a_period(self, weekly: dict) -> None:
        assert "Re-run with period=" in weekly["hint"], weekly["hint"]

    def test_that_period_returns_a_decomposition(self, weekly: dict, ad_data_full_csv: Path) -> None:
        hint = weekly["hint"]
        suggested = hint.split("Re-run with period=")[1][1]
        assert suggested in {"D", "W", "M", "Q"}, suggested

        r = _run(ad_data_full_csv, suggested)
        assert r["stl"], f"the hint sent the caller to period={suggested!r} and it decomposed nothing"
        assert set(r["stl"]) == set(COLS), r["stl"]
        assert r["stl_skipped"] == {}, r["stl_skipped"]

    def test_the_working_period_measures_zero_seasonality(self, ad_data_full_csv: Path) -> None:
        """The whole point: 0.0 measured is not {} skipped.

        Both look like "no seasonality" to a reader. Only one of them is.
        """
        r = _run(ad_data_full_csv, "D")
        assert r["stl"]["spends"]["seasonal_strength"] == 0.0
        assert r["stl"]["spends"]["trend_strength"] > 0.5, r["stl"]

    def test_a_successful_run_carries_no_stl_preamble(self, ad_data_full_csv: Path) -> None:
        r = _run(ad_data_full_csv, "D")
        assert "does NOT" not in r["hint"], r["hint"]
        msgs = " | ".join(str(p.get("message", "")) for p in r["progress"])
        assert "No STL decomposition" not in msgs, msgs
