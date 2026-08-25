"""The ingest cleaners refused the exact file shape they exist to clean.

A spreadsheet exported with a title or a "generated on ..." banner above the
real table produces a CSV whose rows disagree about how many fields they have:

    Quarterly Export
    generated 2026-01-01
    Date,product,spends
    2019-10-16,P1,0

pandas fixes the column count from the first row it reads and raises the moment
a later row is wider:

    ParserError: Error tokenizing data. C error: Expected 1 fields in line 3, saw 3

promote_header() — whose entire job is "make row N the header and drop the junk
above it" — could not read that file. Neither could trim_empty(),
normalize_headers() or convert_file(). All four returned success:false quoting
a pandas internal, so the one server that exists to repair messy exports
rejected the messiest thing an export does.

read_csv_ragged() tries a plain read first, so a well-formed file takes exactly
the path it always did; only after pandas has refused is the file re-scanned
for its true width and the short rows padded, which is what a spreadsheet shows
for the same cells.

The sweep report that surfaced this also claimed promote_header's row_index was
off by one against the physical file. It is not — the read passes header=None,
so row_index counts raw lines, and TestRowIndexCountsPhysicalLines pins that
down so the claim is not re-investigated.
"""

from __future__ import annotations

import pandas as pd
import pytest

from servers.data_ingest import engine

BANNER = "Quarterly Export\ngenerated 2026-01-01\nDate,product,spends\n2019-10-16,P1,0\n2019-10-17,P2,5\n"


@pytest.fixture
def ragged(tmp_path):
    p = tmp_path / "export.csv"
    p.write_text(BANNER, encoding="utf-8")
    return p


@pytest.fixture
def tidy(tmp_path):
    p = tmp_path / "tidy.csv"
    p.write_text("Date,product,spends\n2019-10-16,P1,0\n2019-10-17,P2,5\n", encoding="utf-8")
    return p


class TestTheRaggedFileIsAccepted:
    def test_pandas_alone_still_refuses_it(self, ragged):
        # If this ever stops raising, the fix below is dead weight rather than
        # silently wrong -- so assert the premise, not just the cure.
        with pytest.raises(pd.errors.ParserError):
            pd.read_csv(str(ragged))

    def test_promote_header_reads_it(self, ragged, tmp_path):
        out = tmp_path / "promoted.csv"
        r = engine.promote_header(str(ragged), row_index=2, output_path=str(out))
        assert r["success"] is True, r.get("error")
        assert r["new_headers"] == ["Date", "product", "spends"]
        assert out.read_text(encoding="utf-8") == "Date,product,spends\n2019-10-16,P1,0\n2019-10-17,P2,5\n"

    @pytest.mark.parametrize("tool", ["trim_empty", "normalize_headers"])
    def test_the_other_cleaners_read_it(self, ragged, tmp_path, tool):
        out = tmp_path / f"{tool}.csv"
        r = getattr(engine, tool)(str(ragged), output_path=str(out))
        assert r["success"] is True, r.get("error")
        assert out.exists() and out.stat().st_size > 0

    def test_convert_file_reads_it(self, ragged, tmp_path):
        out = tmp_path / "converted.json"
        r = engine.convert_file(str(ragged), output_format="json", output_path=str(out))
        assert r["success"] is True, r.get("error")

    def test_short_rows_are_padded_not_dropped(self, ragged):
        df = engine.read_csv_ragged(ragged, header=None)
        assert len(df) == 5, "every physical line must survive"
        assert df.shape[1] == 3, "width comes from the widest row"
        assert pd.isna(df.iloc[0, 1]), "the banner's missing cells are empty, not shifted"


class TestTheWellFormedPathIsUnchanged:
    def test_it_matches_a_plain_pandas_read(self, tidy):
        assert engine.read_csv_ragged(tidy).equals(pd.read_csv(str(tidy)))

    def test_dtypes_are_not_flattened_to_object(self, tidy):
        assert list(engine.read_csv_ragged(tidy).dtypes) == list(pd.read_csv(str(tidy)).dtypes)


class TestRowIndexCountsPhysicalLines:
    @pytest.mark.parametrize(
        "row_index,expected",
        [
            (0, ["Quarterly Export", "Unnamed: 1", "Unnamed: 2"]),
            (2, ["Date", "product", "spends"]),
        ],
    )
    def test_the_row_promoted_is_the_line_counted_from_zero(self, ragged, tmp_path, row_index, expected):
        out = tmp_path / "p.csv"
        r = engine.promote_header(str(ragged), row_index=row_index, output_path=str(out))
        assert r["success"] is True, r.get("error")
        got = [h if not h.startswith("nan") else "Unnamed" for h in r["new_headers"]]
        assert got[0] == expected[0]
        assert r["rows_dropped_above"] == row_index + 1


class TestTheHintNamesTheFix:
    def test_a_parser_error_points_at_the_ingest_tools(self):
        from shared.file_utils import hint_for_error

        hint = hint_for_error(pd.errors.ParserError("Expected 1 fields in line 3, saw 16"), "domain fallback")
        assert "promote_header" in hint and "trim_empty" in hint
        assert "domain fallback" not in hint
