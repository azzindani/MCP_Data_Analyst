"""The exported .xlsx has to be a workbook, not a CSV wearing an .xlsx suffix.

The user review's verdict, in full:

    `Credit_Risk_chargedoff.xlsx` (735 KB, Sheet1 5,334 x 24) -- GOOD BUT THIN
    AGI: add `README` sheet (filter, counts, date), frozen header, autofilter,
    formatted amounts, validation on `loan_status`. If >10k rows,
    `top_1k + full_csv_link`.

Nothing about the old output was wrong, which is why nothing caught it: `list_sheets`
round-tripped, the row count was exact, every value was intact. It was simply
unusable -- scroll past row 40 and the header is gone, no way to filter to one
grade, and nothing in the file saying which 5,333 of 38,576 rows these are.

The last clause is the one this file is most careful about. `top_1k +
full_csv_link` is a good idea and a terrible default: an export tool that
silently drops nine tenths of its rows is the exact defect the rest of this
fleet has spent its time closing. So the lever exists, it is off unless asked
for, and asking for it writes the full CSV alongside -- because a preview whose
full version does not exist is a truncation with a friendlier name.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from servers.data_visual import engine as dv
from shared.workbook import DATA_SHEET, LARGE_ROWS, README_SHEET, VALIDATION_MAX_CHARS, write_workbook

openpyxl = pytest.importorskip("openpyxl")


@pytest.fixture()
def loans() -> pd.DataFrame:
    rows = 300
    return pd.DataFrame(
        {
            "id": range(rows),
            "loan_amount": [2500.0 + i for i in range(rows)],
            "grade": ["A", "B", "C"] * (rows // 3),
            "loan_status": ["Fully Paid", "Charged Off"] * (rows // 2),
            "emp_title": [f"job {i}" for i in range(rows)],
        }
    )


@pytest.fixture()
def workbook(loans: pd.DataFrame, tmp_path: Path):
    out = tmp_path / "chargedoff.xlsx"
    report = write_workbook(loans, out, source="Credit_Risk.csv", op="export_data", params={"filter": "Charged Off"})
    return openpyxl.load_workbook(out), report, out


class TestTheFileSaysWhatItIs:
    def test_the_readme_is_the_sheet_that_opens(self, workbook):
        book, _report, _out = workbook
        assert book.sheetnames[0] == README_SHEET, (
            "a workbook that travels arrives with no conversation attached; the first tab is the one that answers"
        )
        assert DATA_SHEET in book.sheetnames

    def test_it_names_the_source_and_both_row_counts(self, workbook):
        book, _report, _out = workbook
        text = {str(row[0]): str(row[1]) for row in book[README_SHEET].iter_rows(min_row=2, values_only=True) if row[0]}
        assert "Credit_Risk.csv" in text["Source file"]
        assert "300" in text["Rows in source"]
        assert text["Parameter: filter"] == "Charged Off"

    def test_it_is_dated(self, workbook):
        book, _report, _out = workbook
        fields = {str(row[0]) for row in book[README_SHEET].iter_rows(min_row=2, values_only=True) if row[0]}
        assert "Generated" in fields


class TestTheDataSheetCanBeWorkedIn:
    def test_the_header_stays_put(self, workbook):
        book, report, _out = workbook
        assert book[DATA_SHEET].freeze_panes == "A2"
        assert report["frozen_header"] is True

    def test_the_columns_can_be_filtered(self, workbook, loans):
        book, report, _out = workbook
        ref = book[DATA_SHEET].auto_filter.ref
        assert ref == f"A1:E{len(loans) + 1}", ref
        assert report["autofilter"] is True

    def test_amounts_are_formatted(self, workbook):
        book, report, _out = workbook
        assert "loan_amount" in report["formatted_columns"]
        # Row 2 is the first data row; the header keeps its own style.
        assert book[DATA_SHEET]["B2"].number_format == "#,##0.00"

    def test_the_low_cardinality_columns_get_a_dropdown(self, workbook):
        book, report, _out = workbook
        assert "loan_status" in report["validated_columns"], report["validated_columns"]
        assert "grade" in report["validated_columns"]
        formulas = [dv_.formula1 for dv_ in book[DATA_SHEET].data_validations.dataValidation]
        assert any("Charged Off" in f for f in formulas), formulas

    def test_an_identifier_column_gets_no_dropdown(self, workbook):
        """300 distinct job titles is a scroll bar, not a choice."""
        _book, report, _out = workbook
        assert "emp_title" not in report["validated_columns"]
        assert "id" not in report["validated_columns"]

    def test_a_validation_list_stays_inside_excels_own_limit(self, workbook):
        book, _report, _out = workbook
        for dv_ in book[DATA_SHEET].data_validations.dataValidation:
            assert len(dv_.formula1) <= VALIDATION_MAX_CHARS, (
                "a longer list makes the workbook unopenable, not just unvalidated"
            )


class TestValuesSurviveTheEnrichment:
    def test_every_row_and_column_is_there(self, workbook, loans):
        book, report, _out = workbook
        sheet = book[DATA_SHEET]
        assert sheet.max_row == len(loans) + 1
        assert sheet.max_column == len(loans.columns)
        assert report["is_preview"] is False

    def test_numbers_are_still_numbers(self, workbook):
        """Formatting must not turn an amount into a string that cannot be summed."""
        book, _report, _out = workbook
        assert isinstance(book[DATA_SHEET]["B2"].value, int | float)

    def test_it_round_trips_through_pandas(self, workbook, loans):
        _book, _report, out = workbook
        back = pd.read_excel(out, sheet_name=DATA_SHEET)
        pd.testing.assert_frame_equal(back, loans, check_dtype=False)


class TestNothingIsDroppedWithoutBeingAsked:
    def test_the_default_writes_every_row(self, loans, tmp_path):
        report = write_workbook(loans, tmp_path / "all.xlsx")
        assert report["rows_written"] == report["rows_total"] == len(loans)
        assert report["full_csv"] == ""

    def test_a_preview_writes_the_full_csv_beside_it(self, loans, tmp_path):
        report = write_workbook(loans, tmp_path / "preview.xlsx", preview_rows=50)
        assert report["rows_written"] == 50
        assert report["rows_total"] == len(loans)
        assert report["is_preview"] is True
        companion = Path(report["full_csv"])
        assert companion.is_file(), "a preview whose full version does not exist is a truncation"
        assert len(pd.read_csv(companion)) == len(loans)

    def test_the_readme_names_the_full_file(self, loans, tmp_path):
        out = tmp_path / "preview.xlsx"
        report = write_workbook(loans, out, preview_rows=50)
        book = openpyxl.load_workbook(out)
        text = {str(row[0]): str(row[1]) for row in book[README_SHEET].iter_rows(min_row=2, values_only=True) if row[0]}
        assert "50 of 300" in text["Rows in this workbook"]
        assert Path(text["Full data"]).name == Path(report["full_csv"]).name

    def test_a_preview_larger_than_the_table_is_not_a_preview(self, loans, tmp_path):
        report = write_workbook(loans, tmp_path / "big.xlsx", preview_rows=10_000)
        assert report["is_preview"] is False
        assert report["rows_written"] == len(loans)


class TestExportDataShipsIt:
    """The enrichment has to reach the tool, not just the module."""

    @pytest.fixture()
    def exported(self, loans, tmp_path):
        csv = tmp_path / "Credit_Risk.csv"
        loans.to_csv(csv, index=False)
        out = tmp_path / "out.xlsx"
        result = dv.export_data(str(csv), output_path=str(out), format="excel", open_after=False)
        assert result["success"] is True, result.get("error")
        return result, out

    def test_the_response_reports_what_was_done(self, exported):
        result, _out = exported
        assert result["workbook"]["sheets"] == [README_SHEET, DATA_SHEET]
        assert result["workbook"]["frozen_header"] is True
        assert "loan_status" in result["workbook"]["validated_columns"]

    def test_the_file_on_disk_has_both_sheets(self, exported):
        _result, out = exported
        assert openpyxl.load_workbook(out).sheetnames == [README_SHEET, DATA_SHEET]

    def test_csv_export_is_untouched(self, loans, tmp_path):
        csv = tmp_path / "in.csv"
        loans.to_csv(csv, index=False)
        out = tmp_path / "out.csv"
        result = dv.export_data(str(csv), output_path=str(out), format="csv", open_after=False)
        assert result["success"] is True
        assert "workbook" not in result
        assert len(pd.read_csv(out)) == len(loans)

    def test_a_large_table_is_told_about_the_lever_not_trimmed_by_it(self, tmp_path):
        big = pd.DataFrame({"n": range(LARGE_ROWS + 5)})
        csv = tmp_path / "big.csv"
        big.to_csv(csv, index=False)
        out = tmp_path / "big.xlsx"
        result = dv.export_data(str(csv), output_path=str(out), format="excel", open_after=False)
        assert result["rows"] == len(big), "nothing is dropped without being asked"
        assert "preview_rows" in result["hint"]
