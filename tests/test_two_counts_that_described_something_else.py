"""Two ingest tools wrote the right file and reported the wrong number.

Both were found in one sweep phase, in the notes column of rows marked PASS,
and both are the same shape: the artifact is correct, so nothing that checks
the artifact can see it, and the number is what the caller reads.

promote_header
--------------

    file:   Quarterly report        <- row 0
            region,value            <- row 1, promoted
            North,1

    promote_header(row_index=1) -> rows_dropped_above: 2

One row sat above the header. The count was `row_index + 1`, which is the width
of the slice `df.iloc[row_index + 1:]` -- and one of the rows that slice removes
is the promoted row itself, which is not dropped at all: it becomes the columns.
What the caller loses is exactly `row_index`.

trim_empty
----------

    file:   <blank>
            <blank>
            region,value
            North,1
            South,2
            <blank>

    trim_empty(...) -> rows_before: 2, rows_dropped: 0

Three blank lines went in and none came out, and the tool whose entire job is
removing empty rows said it had removed none. pandas' read_csv drops blank
lines before the frame exists (`skip_blank_lines=True`), so they were never
among the rows counted "before".

Reading with skip_blank_lines=False is not the fix, and this is the part worth
keeping: a leading blank line then becomes the header row and pandas raises
"No columns to parse from file", so the tool fails outright on the exact shape
it exists to repair. Trading a wrong number for a refusal is a worse trade.
They are counted from the file instead, with csv.reader so that a blank line
inside a quoted field stays part of its row.

Found in a round-15 sweep report.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from servers.data_ingest import engine as ing


@pytest.fixture()
def padded(tmp_path: Path) -> Path:
    f = tmp_path / "padded.csv"
    f.write_text("\n\nregion,value\nNorth,1\nSouth,2\n\n", encoding="utf-8")
    return f


@pytest.fixture()
def titled(tmp_path: Path) -> Path:
    f = tmp_path / "titled.csv"
    f.write_text("Quarterly report\nregion,value\nNorth,1\nSouth,2\n", encoding="utf-8")
    return f


class TestPromoteHeaderCountsWhatItDropped:
    def test_one_row_above_is_one(self, titled: Path, tmp_path: Path) -> None:
        r = ing.promote_header(str(titled), row_index=1, output_path=str(tmp_path / "out.csv"))
        assert r["success"] is True, r.get("error")
        assert r["rows_dropped_above"] == 1, r

    def test_the_header_row_itself_is_not_dropped(self, titled: Path, tmp_path: Path) -> None:
        """It becomes the columns, so counting it as lost is what went wrong."""
        out = tmp_path / "out.csv"
        r = ing.promote_header(str(titled), row_index=1, output_path=str(out))
        assert r["new_headers"] == ["region", "value"]
        assert out.read_text(encoding="utf-8").splitlines()[0] == "region,value"

    def test_no_rows_above_is_zero(self, tmp_path: Path) -> None:
        f = tmp_path / "plain.csv"
        f.write_text("region,value\nNorth,1\n", encoding="utf-8")
        r = ing.promote_header(str(f), row_index=0, output_path=str(tmp_path / "o.csv"))
        assert r["rows_dropped_above"] == 0, r

    def test_the_count_matches_the_rows_that_vanished(self, tmp_path: Path) -> None:
        """Derived from the files, so it cannot drift from the arithmetic."""
        f = tmp_path / "banners.csv"
        f.write_text("one\ntwo\nthree\nregion,value\nNorth,1\n", encoding="utf-8")
        before = len(f.read_text(encoding="utf-8").splitlines())
        out = tmp_path / "o.csv"
        r = ing.promote_header(str(f), row_index=3, output_path=str(out))
        after = len(out.read_text(encoding="utf-8").splitlines())
        assert r["rows_dropped_above"] == before - after, (r, before, after)

    def test_the_dry_run_agrees_with_the_real_one(self, titled: Path, tmp_path: Path) -> None:
        dry = ing.promote_header(str(titled), row_index=1, dry_run=True)
        wet = ing.promote_header(str(titled), row_index=1, output_path=str(tmp_path / "o.csv"))
        assert dry["would_change"]["rows_dropped_above"] == wet["rows_dropped_above"]


class TestTrimEmptyCountsTheBlankLines:
    def test_it_reports_the_ones_it_removed(self, padded: Path, tmp_path: Path) -> None:
        r = ing.trim_empty(str(padded), output_path=str(tmp_path / "out.csv"))
        assert r["success"] is True, r.get("error")
        assert r["rows_dropped"] == 3, r

    def test_rows_before_describes_the_file_as_written(self, padded: Path, tmp_path: Path) -> None:
        r = ing.trim_empty(str(padded), output_path=str(tmp_path / "out.csv"))
        assert r["rows_before"] == 5, r  # two data rows + three blank lines

    def test_the_output_is_what_it_always_was(self, padded: Path, tmp_path: Path) -> None:
        """The artifact was never wrong; only the report was."""
        out = tmp_path / "out.csv"
        ing.trim_empty(str(padded), output_path=str(out))
        assert out.read_text(encoding="utf-8") == "region,value\nNorth,1\nSouth,2\n"

    def test_before_minus_after_is_dropped(self, padded: Path, tmp_path: Path) -> None:
        r = ing.trim_empty(str(padded), output_path=str(tmp_path / "out.csv"))
        assert r["rows_before"] - r["rows_after"] == r["rows_dropped"], r

    def test_a_file_with_nothing_to_trim_says_nothing_was(self, tmp_path: Path) -> None:
        f = tmp_path / "clean.csv"
        f.write_text("region,value\nNorth,1\nSouth,2\n", encoding="utf-8")
        r = ing.trim_empty(str(f), output_path=str(tmp_path / "out.csv"))
        assert r["rows_dropped"] == 0, r
        assert r["rows_before"] == 2, r

    def test_a_leading_blank_line_does_not_break_the_read(self, tmp_path: Path) -> None:
        """The regression the obvious fix would have caused.

        Re-reading with skip_blank_lines=False makes the blank first line the
        header and pandas raises "No columns to parse from file" -- the tool
        refusing the exact input it exists to repair.
        """
        f = tmp_path / "leading.csv"
        f.write_text("\nregion,value\nNorth,1\n", encoding="utf-8")
        r = ing.trim_empty(str(f), output_path=str(tmp_path / "out.csv"))
        assert r["success"] is True, r.get("error")
        assert r["rows_dropped"] == 1, r


class TestCountingTheBlankLines:
    def test_it_finds_them(self, tmp_path: Path) -> None:
        f = tmp_path / "a.csv"
        f.write_text("a,b\n\n1,2\n\n", encoding="utf-8")
        assert ing.blank_line_count(f) == 2

    def test_a_row_of_commas_is_not_a_blank_line(self, tmp_path: Path) -> None:
        """It reaches the frame, so the frame already counts it."""
        f = tmp_path / "b.csv"
        f.write_text("a,b\n,\n1,2\n", encoding="utf-8")
        assert ing.blank_line_count(f) == 0

    def test_a_blank_line_inside_a_quoted_field_is_part_of_its_row(self, tmp_path: Path) -> None:
        f = tmp_path / "c.csv"
        f.write_text('a,b\n"line one\n\nline three",2\n', encoding="utf-8")
        assert ing.blank_line_count(f) == 0

    def test_a_file_that_cannot_be_read_counts_nothing(self, tmp_path: Path) -> None:
        assert ing.blank_line_count(tmp_path / "missing.csv") == 0
