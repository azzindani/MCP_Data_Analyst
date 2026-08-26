"""It read a thousand rows and reported the column had no nulls. It has 546.

    load_dataset(".../Ad_Data.csv", max_rows=1000)
      -> rows: 1000
         null_counts: {..., "link_clicks": 0}

The first null in link_clicks is at row 2,011, so a thousand-row read sees a
gapless column and says so. Every number under `null_counts`, `unique_counts`
and `dtypes` is a fact about the rows that were read, presented in the same
flat shape as a fact about the file -- and `rows: 1000` is the only clue, which
reads as a small file rather than a small look at a large one.

A zero that means "not looked at" is indistinguishable from "there are none".
That is the third time this fleet has produced that shape: fs_index and
fs_query were both fixed for it, and so was list_models over in the ML repo
this same round.

The sibling on this very server already has the fix. auto_detect_schema samples
1,000 rows *by default* and was corrected in an earlier round after the same
column caught it out -- it reports `rows_sampled`, `total_rows` and
`inferred_from_sample`, and its hint names inspect_dataset() as the way to get
a whole-column answer. Its comment even names row 2,011. load_dataset is the
sibling that was missed, which is the half-fix this file closes.

The counts are not made whole -- reading the file entire is exactly what the
caller asked not to do. The response is made honest about what it counted.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from servers.data_basic import engine as db

# Where the fixture's first missing link_clicks value actually sits.
FIRST_NULL_ROW = 2011
TOTAL_ROWS = 16834
TRUE_NULLS = 546


@pytest.fixture()
def ad(ad_data_full_csv: Path) -> Path:
    return ad_data_full_csv


def load(path: Path, **kw) -> dict:
    r = db.load_dataset(str(path), **kw)
    assert r["success"] is True, r.get("error")
    return r


class TestTheSampleIsDeclared:
    def test_a_sampled_read_says_how_big_the_file_is(self, ad: Path) -> None:
        r = load(ad, max_rows=1000)
        assert r["rows"] == 1000
        assert r["total_rows"] == TOTAL_ROWS, r.get("total_rows")

    def test_it_says_the_counts_came_from_a_sample(self, ad: Path) -> None:
        assert load(ad, max_rows=1000)["counted_from_sample"] is True

    def test_the_hint_names_the_tool_that_reads_the_whole_column(self, ad: Path) -> None:
        hint = load(ad, max_rows=1000)["hint"]
        assert "inspect_dataset" in hint, hint
        assert "1,000" in hint and "16,834" in hint, hint

    def test_the_zero_is_still_reported_but_no_longer_alone(self, ad: Path) -> None:
        """The count is right about the sample; the response must frame it."""
        r = load(ad, max_rows=1000)
        assert r["null_counts"]["link_clicks"] == 0
        assert r["counted_from_sample"] is True
        assert r["total_rows"] > r["rows"]


class TestAFullReadIsNotMarkedAsSampled:
    def test_reading_everything_sets_the_flag_false(self, ad: Path) -> None:
        r = load(ad)
        assert r["counted_from_sample"] is False, r
        assert r["rows"] == r["total_rows"] == TOTAL_ROWS

    def test_a_full_read_finds_every_null(self, ad: Path) -> None:
        assert load(ad)["null_counts"]["link_clicks"] == TRUE_NULLS

    def test_a_max_rows_larger_than_the_file_is_not_a_sample(self, ad: Path) -> None:
        r = load(ad, max_rows=TOTAL_ROWS + 500)
        assert r["counted_from_sample"] is False, r
        assert r["null_counts"]["link_clicks"] == TRUE_NULLS

    def test_the_ordinary_hint_survives(self, ad: Path) -> None:
        hint = load(ad)["hint"]
        assert "search_columns" in hint or "inspect_dataset" in hint, hint


class TestTheFixtureStillHasTheShapeThisIsAbout:
    """If the data changed, the tests above would pass without proving anything."""

    def test_the_first_null_is_below_the_default_sample(self, ad: Path) -> None:
        import pandas as pd

        col = pd.read_csv(ad)["link_clicks"]
        assert int(col.isna().sum()) == TRUE_NULLS
        assert int(col.isna().to_numpy().nonzero()[0][0]) == FIRST_NULL_ROW
        assert FIRST_NULL_ROW > 1000, "a 1,000-row sample must miss it, or this file tests nothing"


class TestItMatchesTheSiblingThatWasAlreadyFixed:
    def test_auto_detect_schema_reports_the_same_total(self, ad: Path) -> None:
        from servers.data_statistics import engine as ds

        a = ds.auto_detect_schema(str(ad), max_rows=1000)
        b = load(ad, max_rows=1000)
        assert a["total_rows"] == b["total_rows"] == TOTAL_ROWS

    def test_both_flag_the_sample(self, ad: Path) -> None:
        from servers.data_statistics import engine as ds

        assert ds.auto_detect_schema(str(ad), max_rows=1000)["inferred_from_sample"] is True
        assert load(ad, max_rows=1000)["counted_from_sample"] is True
