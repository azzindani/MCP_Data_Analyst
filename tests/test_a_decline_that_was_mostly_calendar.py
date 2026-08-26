"""Seven days held against thirty, reported as a 78.84% decline.

    period_comparison(Ad_Data.csv, period_unit="M", compare_to="previous")
      -> current_period: "2020-07"
         reference_period: "2020-06"
         comparisons: [{metric: spends, pct_change: -78.84, direction: "down"}]

The fixture ends on 2020-07-07. So the current period is seven days of July and
the reference is all thirty days of June, and the tool said "down 78.84%" with
nothing anywhere in the response to say the two periods are not the same length.

The arithmetic is correct. Read as what a percentage change normally means --
that the business fell by four fifths -- it is wrong by roughly a factor of
four, and it is wrong in the direction that makes someone act. That is the
whole shape of this round's axis: a true number whose meaning does not survive
being read the ordinary way.

Nothing in the module mentioned partial periods before this; there was no
detection to be wrong, it simply was not considered. Both ends are reported
now, not just the current one, because a dataset that begins mid-month produces
the same distortion with its sign flipped -- an invented improvement rather than
an invented collapse.

The hint offers the two real answers: compare complete periods, or divide by
days_with_data and compare daily rates.

Found by round 16, which checked where the fixture ends rather than only
whether the percentage was arithmetically right.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from servers.data_statistics import engine as ds

METRICS = ["spends", "clicks"]


@pytest.fixture()
def july(ad_data_full_csv: Path) -> dict:
    r = ds.period_comparison(
        str(ad_data_full_csv),
        date_col="Date",
        metrics=METRICS,
        period_unit="M",
        compare_to="previous",
        open_after=False,
    )
    assert r["success"] is True, r.get("error")
    return r


class TestThePartialPeriodIsDeclared:
    def test_the_fixture_still_ends_mid_month(self, ad_data_full_csv: Path) -> None:
        """If the data changed, everything below would pass for free."""
        end = pd.to_datetime(pd.read_csv(ad_data_full_csv)["Date"], format="mixed").max()
        assert (end.year, end.month, end.day) == (2020, 7, 7), end

    def test_it_flags_the_comparison_as_partial(self, july: dict) -> None:
        assert july["partial_period"] is True, july

    def test_it_counts_the_days_it_actually_has(self, july: dict) -> None:
        cov = july["current_period_coverage"]
        assert cov["period"] == "2020-07"
        assert cov["days_with_data"] == 7, cov
        assert cov["days_in_period"] == 31, cov
        assert cov["complete"] is False

    def test_the_reference_month_is_whole(self, july: dict) -> None:
        ref = july["reference_period_coverage"]
        assert ref["period"] == "2020-06"
        assert ref["days_with_data"] == ref["days_in_period"] == 30, ref
        assert ref["complete"] is True

    def test_the_hint_says_the_change_is_partly_calendar(self, july: dict) -> None:
        hint = july["hint"]
        assert "7 of 31 days" in hint, hint
        assert "calendar" in hint, hint

    def test_the_hint_offers_both_ways_out(self, july: dict) -> None:
        hint = july["hint"]
        assert "all_periods_available" in hint, hint
        assert "days_with_data" in hint, hint

    def test_a_progress_line_carries_it_too(self, july: dict) -> None:
        msgs = " | ".join(str(p.get("message", "")) for p in july["progress"])
        assert "Partial period" in msgs, msgs

    def test_the_percentage_itself_is_untouched(self, july: dict) -> None:
        """Disclosure, not silent correction -- the number stays what it is.

        One dict per group, with each metric's block under its own name; there
        is no "metric" key to filter on.
        """
        blocks = july["comparisons"]
        assert blocks, july
        spends = blocks[0]["spends"]
        assert spends["pct_change"] < -50, spends
        assert spends["direction"] == "down"


class TestTwoWholeMonthsAreNotFlagged:
    """The flag must fire on truncation, not on every comparison."""

    @pytest.fixture()
    def whole(self, ad_data_full_csv: Path) -> dict:
        r = ds.period_comparison(
            str(ad_data_full_csv),
            date_col="Date",
            metrics=METRICS,
            period_unit="M",
            current_period="2020-06",
            compare_to="previous",
            open_after=False,
        )
        assert r["success"] is True, r.get("error")
        return r

    def test_not_partial(self, whole: dict) -> None:
        assert whole["partial_period"] is False, whole

    def test_both_sides_complete(self, whole: dict) -> None:
        assert whole["current_period_coverage"]["complete"] is True
        assert whole["reference_period_coverage"]["complete"] is True

    def test_no_calendar_hint(self, whole: dict) -> None:
        assert "calendar" not in (whole.get("hint") or "")


class TestAShortFirstPeriodIsFlaggedToo:
    """A run that begins mid-month invents an improvement the same way."""

    def test_a_truncated_reference_is_declared(self, tmp_path: Path) -> None:
        rows = ["Date,spends"]
        for day in range(20, 32):  # January starts on the 20th
            rows.append(f"2024-01-{day:02d},100")
        for day in range(1, 30):
            rows.append(f"2024-02-{day:02d},100")
        src = tmp_path / "late_start.csv"
        src.write_text("\n".join(rows) + "\n", encoding="utf-8")

        r = ds.period_comparison(
            str(src),
            date_col="Date",
            metrics=["spends"],
            period_unit="M",
            current_period="2024-02",
            compare_to="previous",
            open_after=False,
        )
        assert r["success"] is True, r.get("error")
        assert r["partial_period"] is True, r
        assert r["reference_period_coverage"]["days_with_data"] == 12, r["reference_period_coverage"]
        assert r["reference_period_coverage"]["days_in_period"] == 31
        assert "2024-01 has 12 of 31 days" in r["hint"], r["hint"]
