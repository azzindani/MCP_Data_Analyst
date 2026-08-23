"""A dtype read off 1,000 rows was reported as the column's dtype.

auto_detect_schema reads `max_rows` rows and infers from those. That is the
right design -- schema detection on a large file should not parse all of it --
but `current_dtype` is a fact about the file, and the response gave it without
saying how much of the file it had seen.

On the reference dataset the first null in link_clicks is at row 2,011, so the
default 1,000-row sample sees a gapless integer column:

    auto_detect_schema(Ad_Data.csv)     current_dtype int64, category_encoded
    pandas.read_csv(Ad_Data.csv)        float64, 546 nulls in 16,834 rows

Both readings are right about what they read. The problem is that the same
server's inspect_dataset and validate_dataset report the second, so a caller
comparing them finds two tools on one server contradicting each other about
one column with nothing to explain it. The low cardinality of those first rows
also made a numeric count column "category_encoded", with a suggestion to
consider its label meanings.

`rows_sampled: 1000` was in the response already and could not settle it:
1,000 sampled reads the same whether the file has 1,000 rows or 16,834.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_medium")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from servers.data_medium import engine  # noqa: E402
from shared.file_utils import count_data_rows  # noqa: E402


@pytest.fixture
def late_nulls(tmp_path) -> Path:
    """Integers for the first 1,200 rows, then nulls -- the shape that misleads."""
    # Two columns, so a null in `n` is an empty field rather than a blank line
    # -- pandas skips blank lines entirely, so a one-column file cannot express
    # the null this fixture is about.
    rows = [f"{i % 7},x" for i in range(1200)]
    rows += [",x"] * 300
    rows += [f"{i % 7},x" for i in range(500)]
    p = tmp_path / "late.csv"
    p.write_text("n,k\n" + "\n".join(rows) + "\n", encoding="utf-8")
    return p


class TestTheSampleIsDeclared:
    def test_the_total_is_reported_next_to_the_sample(self, late_nulls):
        r = engine.auto_detect_schema(str(late_nulls), max_rows=1000)
        assert r["success"] is True, r.get("error")
        assert r["rows_sampled"] == 1000
        assert r["total_rows"] == 2000, r

    def test_a_partial_read_is_flagged(self, late_nulls):
        r = engine.auto_detect_schema(str(late_nulls), max_rows=1000)
        assert r["inferred_from_sample"] is True

    def test_the_hint_names_both_numbers_and_where_to_get_the_truth(self, late_nulls):
        r = engine.auto_detect_schema(str(late_nulls), max_rows=1000)
        hint = r["hint"]
        assert "1,000" in hint and "2,000" in hint, hint
        assert "inspect_dataset" in hint, hint

    def test_a_full_read_is_not_flagged(self, late_nulls):
        r = engine.auto_detect_schema(str(late_nulls), max_rows=100_000)
        assert r["inferred_from_sample"] is False
        assert r["rows_sampled"] == r["total_rows"] == 2000
        assert "inspect_dataset" not in r["hint"]


class TestTheSampleReallyDoesDisagree:
    """The fixture has to reproduce the disagreement, or the flag proves nothing."""

    def test_the_sample_sees_an_integer_column(self, late_nulls):
        r = engine.auto_detect_schema(str(late_nulls), max_rows=1000)
        assert r["columns"]["n"]["current_dtype"] == "int64"

    def test_the_whole_column_is_a_float(self, late_nulls):
        r = engine.auto_detect_schema(str(late_nulls), max_rows=100_000)
        assert r["columns"]["n"]["current_dtype"] == "float64"
        assert str(pd.read_csv(late_nulls)["n"].dtype) == "float64"


class TestTheRowCountIsRight:
    def test_it_excludes_the_header(self, tmp_path):
        p = tmp_path / "a.csv"
        p.write_text("h\n1\n2\n3\n", encoding="utf-8")
        assert count_data_rows(p) == 3

    def test_a_missing_trailing_newline_still_counts(self, tmp_path):
        p = tmp_path / "b.csv"
        p.write_text("h\n1\n2\n3", encoding="utf-8")
        assert count_data_rows(p) == 3

    def test_a_header_only_file_has_no_data_rows(self, tmp_path):
        p = tmp_path / "c.csv"
        p.write_text("h\n", encoding="utf-8")
        assert count_data_rows(p) == 0

    def test_an_empty_file_is_zero(self, tmp_path):
        p = tmp_path / "d.csv"
        p.write_bytes(b"")
        assert count_data_rows(p) == 0

    def test_a_missing_file_is_zero_not_an_error(self, tmp_path):
        assert count_data_rows(tmp_path / "nope.csv") == 0

    def test_it_agrees_with_pandas(self, late_nulls):
        assert count_data_rows(late_nulls) == len(pd.read_csv(late_nulls))
