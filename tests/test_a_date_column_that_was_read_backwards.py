"""A day-first date column was not refused. It was transposed.

Every date parse in this repo hardcoded `dayfirst=False` -- pandas' default,
and right for US-style files. Handed a day-first column it does not fail; it
swaps the two fields and carries on.

The SFO air-cargo file is monthly, so every value is the first of a month::

    01-07-1999  ->  1999-01-07   (read as 7 January)
    truth       ->  1999-07-01   (1 July)

Every row parsed. `errors="coerce"` dropped nothing. 291 distinct months
collapsed into 25 Januaries with the real month hiding in the day field.
Yearly totals stayed right, which is what made it survive: seasonality,
`period_comparison`, `resample_timeseries` and `lag_correlation` were all
silently wrong under `success: true`.

The fix reads the orientation off the column. A value above 12 in either field
settles it outright. Where nothing is decisive -- days 1-12 only, one year --
the answer stays month-first so no existing behaviour moves, and the response
says the column was ambiguous instead of guessing quietly.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (
    str(ROOT),
    str(ROOT / "servers" / "data_medium"),
    str(ROOT / "servers" / "data_statistics"),
):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from shared.column_utils import detect_dayfirst, parse_dates  # noqa: E402


@pytest.fixture
def monthly_dayfirst_csv(tmp_path):
    """Three years of month-start rows written DD-MM-YYYY, as SFO ships them."""
    rows = [(f"01-{month:02d}-{year}", month * year) for year in (2020, 2021, 2022) for month in range(1, 13)]
    body = "\n".join(f"{d},{v}" for d, v in rows)
    f = tmp_path / "monthly.csv"
    f.write_text(f"period,value\n{body}\n", encoding="utf-8")
    return f


# --- detection --------------------------------------------------------------


def test_a_field_above_twelve_settles_it_outright():
    dayfirst, reason, ambiguous = detect_dayfirst(pd.Series(["25-12-2020", "04-07-2020"]))
    assert dayfirst is True
    assert ambiguous is False
    assert "exceeds 12" in reason

    dayfirst, _, ambiguous = detect_dayfirst(pd.Series(["07-04-2020", "12-25-2020"]))
    assert dayfirst is False
    assert ambiguous is False


def test_a_constant_first_field_across_years_is_a_day(monthly_dayfirst_csv):
    """The case that bit: no value above 12 anywhere in the column.

    A series spanning three years cannot sit inside one calendar month, so a
    first field that never changes while the second cycles is the day.
    """
    column = pd.read_csv(monthly_dayfirst_csv)["period"]
    dayfirst, reason, ambiguous = detect_dayfirst(column)
    assert dayfirst is True
    assert ambiguous is False
    assert "constant" in reason


def test_the_us_shape_of_the_same_file_is_not_flipped():
    """Month-first month-start data must keep reading month-first."""
    values = pd.Series([f"{month:02d}-01-{year}" for year in (2020, 2021) for month in range(1, 13)])
    dayfirst, _, ambiguous = detect_dayfirst(values)
    assert dayfirst is False
    assert ambiguous is False


def test_iso_dates_are_never_ambiguous():
    dayfirst, reason, ambiguous = detect_dayfirst(pd.Series(["2019-10-16", "2020-01-05"]))
    assert dayfirst is False
    assert ambiguous is False
    assert "ISO" in reason


def test_a_genuinely_ambiguous_column_says_so_instead_of_guessing():
    """Days 1-12 inside one year: no rule can settle it, and none pretends to."""
    dayfirst, _, ambiguous = detect_dayfirst(pd.Series(["01-05-2020", "01-06-2020", "01-07-2020"]))
    assert ambiguous is True
    # Unchanged from the old behaviour, so nothing that worked moves.
    assert dayfirst is False


# --- parsing ----------------------------------------------------------------


def test_the_months_survive_the_round_trip(monthly_dayfirst_csv):
    column = pd.read_csv(monthly_dayfirst_csv)["period"]
    parsed, info = parse_dates(column)
    assert info["dayfirst"] is True
    assert parsed.dt.month.nunique() == 12, "all twelve months must survive"
    assert parsed.dt.day.unique().tolist() == [1]

    # What the old code did, kept here so the defect stays visible.
    old = pd.to_datetime(column, format="mixed", dayfirst=False, errors="coerce")
    assert old.notna().all(), "the bug never raised -- that is the whole problem"
    assert old.dt.month.nunique() == 1


def test_the_caller_can_overrule_the_detector(monthly_dayfirst_csv):
    column = pd.read_csv(monthly_dayfirst_csv)["period"]
    parsed, info = parse_dates(column, dayfirst="false")
    assert info["dayfirst"] is False
    assert "caller passed" in info["reason"]
    assert parsed.dt.month.nunique() == 1


# --- the tools themselves ---------------------------------------------------


def test_time_series_analysis_counts_every_month(monthly_dayfirst_csv, tmp_path):
    from _med_analysis import time_series_analysis

    r = time_series_analysis(
        str(monthly_dayfirst_csv),
        date_column="period",
        value_columns=["value"],
        period="M",
        output_path=str(tmp_path / "ts.html"),
        open_after=False,
    )
    assert r["success"] is True
    assert r["date_range"]["start"].startswith("2020-01-01")
    assert r["date_range"]["end"].startswith("2022-12-01")
    assert any("day-first" in str(p.get("message", "")) for p in r["progress"])


def test_period_comparison_takes_the_same_override(monthly_dayfirst_csv):
    from _stats_comparative import period_comparison

    r = period_comparison(
        str(monthly_dayfirst_csv),
        date_col="period",
        metrics=["value"],
        period_unit="Y",
        open_after=False,
    )
    assert r["success"] is True
    assert any("day-first" in str(p.get("message", "")) for p in r["progress"])
