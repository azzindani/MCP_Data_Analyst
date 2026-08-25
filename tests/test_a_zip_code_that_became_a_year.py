"""Adding one column rewrote two the caller never named.

    apply_patch(ops=[{"op": "column_math",
                      "target_column": "doubled", "formula": "amount * 2"}])

    before   0007,01970,12345678901234567890,keep,1
    after    7,1970,12345678901234567890,keep,1,2.0

`employee_id` lost its padding and `zip` 01970 -- Salem, Massachusetts --
came back as 1970, which is not a ZIP code. Neither column was mentioned by
the op, the tool reported success, and the receipt says "applied 1 ops".

pandas reads a column of digits as int64 whether or not those digits were a
number, and apply_patch edits the caller's file **in place**: it reads, applies
the ops, and writes the whole frame back with `df.to_csv()`. So every column
makes a round trip through pandas' type inference, and a zero-padded identifier
does not survive one. Nothing in the tool ever looked at those columns.

The loss happens at read, before any op runs, so no amount of care at write can
undo it -- by then the zeros are gone. And the row set is not stable either:
ten of the filter ops `reset_index(drop=True)`, so the original text cannot be
spliced back in afterwards by position.

So the read is where it is fixed, and only for the tools that write the frame
back over the caller's own file. `read_csv_preserving_ids` re-reads the columns
pandas made numeric, as text, and pins the ones with a leading zero to `str`.
Ordinary reads keep using `read_csv`: the second pass is worth paying to avoid
rewriting someone's file and is not worth paying to compute a mean.

Two things deliberately NOT claimed here. `0` -> `0.0` in a genuinely numeric
column with missing values still happens -- the value is identical and only its
rendering changed. And a column of digits with no padding is still a number,
because that is what it looks like and what every op expects.

Found in a round-15 sweep report, in a note on a row marked PASS:
"Values reformatted ("0"->"0.0") -- content-preserving."
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from servers.data_basic import engine as db
from shared.file_utils import padded_id_columns, read_csv, read_csv_preserving_ids

HEADER = "employee_id,zip,account,note,amount\n"
ROWS = [
    "0007,01970,12345678901234567890,keep,1",
    "0042,02138,98765432109876543210,keep,2",
    "0113,60614,11111111111111111111,keep,3",
]


@pytest.fixture()
def ids_csv(tmp_path: Path) -> Path:
    f = tmp_path / "ids.csv"
    f.write_text(HEADER + "\n".join(ROWS) + "\n", encoding="utf-8")
    return f


def untouched_part(line: str) -> str:
    """The five original fields, ignoring any column an op appended."""
    return ",".join(line.split(",")[:5])


class TestTheIdentifiersSurviveAPatch:
    def test_adding_a_column_leaves_the_others_alone(self, ids_csv: Path) -> None:
        r = db.apply_patch(
            str(ids_csv),
            ops=[{"op": "column_math", "target_column": "doubled", "formula": "amount * 2"}],
        )
        assert r["success"] is True, r.get("error")
        after = ids_csv.read_text(encoding="utf-8").splitlines()[1:]
        assert [untouched_part(line) for line in after] == ROWS

    def test_the_zip_is_still_a_zip(self, ids_csv: Path) -> None:
        db.apply_patch(
            str(ids_csv),
            ops=[{"op": "column_math", "target_column": "doubled", "formula": "amount * 2"}],
        )
        text = ids_csv.read_text(encoding="utf-8")
        assert "01970" in text, text
        assert ",1970," not in text, text

    def test_it_survives_an_op_that_drops_rows(self, ids_csv: Path) -> None:
        """The filters reset the index, which is why this is fixed at read."""
        r = db.apply_patch(
            str(ids_csv),
            ops=[{"op": "filter_between", "column": "amount", "min": 2, "max": 3}],
        )
        assert r["success"] is True, r.get("error")
        after = ids_csv.read_text(encoding="utf-8").splitlines()[1:]
        assert [untouched_part(line) for line in after] == ROWS[1:]

    def test_the_op_that_was_asked_for_still_happened(self, ids_csv: Path) -> None:
        db.apply_patch(
            str(ids_csv),
            ops=[{"op": "column_math", "target_column": "doubled", "formula": "amount * 2"}],
        )
        out = pd.read_csv(ids_csv)
        assert list(out["doubled"]) == [2, 4, 6]


class TestWhatCountsAsPadded:
    def test_a_leading_zero_before_a_digit(self, tmp_path: Path) -> None:
        f = tmp_path / "a.csv"
        f.write_text("zip,n\n01970,1\n02138,2\n", encoding="utf-8")
        assert padded_id_columns(str(f), read_csv(str(f))) == ["zip"]

    def test_a_decimal_is_a_number(self, tmp_path: Path) -> None:
        f = tmp_path / "b.csv"
        f.write_text("rate,n\n0.5,1\n0.25,2\n", encoding="utf-8")
        assert padded_id_columns(str(f), read_csv(str(f))) == []

    def test_a_plain_zero_is_a_number(self, tmp_path: Path) -> None:
        f = tmp_path / "c.csv"
        f.write_text("count,n\n0,1\n7,2\n", encoding="utf-8")
        assert padded_id_columns(str(f), read_csv(str(f))) == []

    def test_one_padded_row_among_many_is_enough(self, tmp_path: Path) -> None:
        """Sampling the first rows would miss this, which is the same bug again."""
        f = tmp_path / "d.csv"
        rows = "\n".join(f"{i},{i}" for i in range(1, 500))
        f.write_text("code,n\n" + rows + "\n0042,500\n", encoding="utf-8")
        assert padded_id_columns(str(f), read_csv(str(f))) == ["code"]

    def test_a_text_column_is_left_out_of_it(self, tmp_path: Path) -> None:
        """Already text, nothing to lose -- and not worth re-reading."""
        f = tmp_path / "e.csv"
        f.write_text("name,n\nalpha,1\nbravo,2\n", encoding="utf-8")
        assert padded_id_columns(str(f), read_csv(str(f))) == []


class TestTheReaderItself:
    def test_it_keeps_the_padded_column_as_text(self, ids_csv: Path) -> None:
        df = read_csv_preserving_ids(str(ids_csv))
        assert list(df["zip"]) == ["01970", "02138", "60614"]
        assert list(df["employee_id"]) == ["0007", "0042", "0113"]

    def test_it_leaves_real_numbers_numeric(self, ids_csv: Path) -> None:
        df = read_csv_preserving_ids(str(ids_csv))
        assert pd.api.types.is_numeric_dtype(df["amount"])
        assert list(df["amount"]) == [1, 2, 3]

    def test_a_file_with_nothing_padded_reads_identically(self, tmp_path: Path) -> None:
        f = tmp_path / "plain.csv"
        f.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
        plain, preserving = read_csv(str(f)), read_csv_preserving_ids(str(f))
        assert plain.equals(preserving)
        assert dict(plain.dtypes.astype(str)) == dict(preserving.dtypes.astype(str))

    def test_an_unreadable_file_does_not_break_the_caller(self, tmp_path: Path) -> None:
        """Detection is an improvement, not a precondition: it must never raise."""
        f = tmp_path / "gone.csv"
        f.write_text("a,b\n1,2\n", encoding="utf-8")
        df = read_csv(str(f))
        f.unlink()
        assert padded_id_columns(str(f), df) == []
