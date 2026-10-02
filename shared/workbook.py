"""An .xlsx someone can actually work in, rather than a CSV with a new suffix.

The user review's verdict on the exported workbook was the whole of this
module's brief:

    `Credit_Risk_chargedoff.xlsx` (735 KB, Sheet1 5,334 x 24) -- GOOD BUT THIN
    `list_sheets`: 1 sheet, header + 5,333 rows, 24 cols. Round-trips.
    AGI: add `README` sheet (filter, counts, date), frozen header, autofilter,
    formatted amounts, validation on `loan_status`. If >10k rows,
    `top_1k + full_csv_link`.

`df.to_excel(out, index=False)` produces a file that is correct and unusable:
scroll past row 40 and the header is gone, there is no way to filter to one
grade, every amount reads `2500` instead of `2,500.00`, and nothing in the file
says which of 38,576 rows these 5,333 are.

**The README sheet is the tab that opens; the data is the first sheet.** A
workbook that travels -- and this one is a deliverable, so it travels -- arrives
with no conversation attached, and the README answers what a recipient asks
first: what is this, where did it come from, how many rows were there before.
It opens because it is the ACTIVE sheet, not because it comes first. It used to
come first, and every program that reads "the first sheet" read the README:
pd.read_excel(file) got 6 rows of metadata, and this server's own convert_file
turned an export back into a 6-row CSV and ignored the data.

**Every row is written unless the caller asks otherwise.** The review's
`top_1k + full_csv_link` is offered as `preview_rows`, not imposed: an export
tool that silently drops nine tenths of its rows is the exact defect class the
rest of this fleet has been closing. When `preview_rows` is used, the full CSV
is written alongside automatically and the README names it, because a preview
whose full version does not exist is just a truncation.

**Formatting is applied per column, not per cell, wherever Excel allows it.**
38,576 rows x 24 columns is 925,824 cells, and styling each one individually
costs more time than writing the file did.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

README_SHEET = "README"
DATA_SHEET = "Data"

# Above this many rows the review asked for a lighter workbook. We do not
# truncate on its behalf -- we say the lever exists.
LARGE_ROWS = 10_000

# A column with at most this many distinct values is a category someone will
# want a dropdown for. Above it, the dropdown is a scroll bar.
VALIDATION_MAX_CHOICES = 25

# Excel's own ceiling on an inline validation list, quoted rather than
# discovered: a longer formula1 makes the workbook unopenable, not just
# unvalidated.
VALIDATION_MAX_CHARS = 255

# Wide enough for a date or a currency amount, narrow enough that 24 of them
# still fit on a screen.
MAX_COL_WIDTH = 46
MIN_COL_WIDTH = 9

# Rows sampled to size a column. Measuring all 38,576 to choose a width costs
# more than the width is worth.
_WIDTH_SAMPLE = 200

_INT_FORMAT = "#,##0"
_FLOAT_FORMAT = "#,##0.00"
_DATE_FORMAT = "yyyy-mm-dd"


def _column_letter(idx: int) -> str:
    from openpyxl.utils import get_column_letter

    return get_column_letter(idx)


def _number_format(series) -> str:
    import pandas as pd

    if pd.api.types.is_datetime64_any_dtype(series):
        return _DATE_FORMAT
    if pd.api.types.is_bool_dtype(series):
        return ""
    if pd.api.types.is_integer_dtype(series):
        return _INT_FORMAT
    if pd.api.types.is_float_dtype(series):
        return _FLOAT_FORMAT
    return ""


def _validation_choices(series) -> list[str] | None:
    """The dropdown for a column, or None when there should not be one."""
    import pandas as pd

    if pd.api.types.is_numeric_dtype(series) or pd.api.types.is_datetime64_any_dtype(series):
        return None
    values = series.dropna().unique()
    if len(values) < 2 or len(values) > VALIDATION_MAX_CHOICES:
        return None
    choices = [str(v) for v in values]
    # A comma inside a value would split it into two options and a quote would
    # end the formula early. Both produce a workbook Excel repairs on open.
    if any("," in c or '"' in c for c in choices):
        return None
    if len(",".join(choices)) > VALIDATION_MAX_CHARS - 2:
        return None
    return sorted(choices)


def _readme_rows(
    *,
    source: str,
    op: str,
    rows_total: int,
    rows_written: int,
    columns: int,
    params: dict[str, Any] | None,
    full_csv: str,
    source_fingerprint: str,
) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = [
        ("What this is", f"{DATA_SHEET} sheet: {rows_written:,} row(s) x {columns} column(s)."),
        ("Produced by", op or "export_data"),
        ("Source file", source or "(not recorded)"),
    ]
    if source_fingerprint:
        rows.append(("Source fingerprint", source_fingerprint))
    rows.append(("Rows in source", f"{rows_total:,}"))
    if rows_written != rows_total:
        rows.append(("Rows in this workbook", f"{rows_written:,} of {rows_total:,} — a preview, not the whole table"))
        rows.append(("Full data", full_csv or "(not written)"))
    rows.append(("Generated", datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%SZ")))
    for key, value in (params or {}).items():
        rows.append((f"Parameter: {key}", str(value)))
    return rows


def write_workbook(
    df,
    out_path: str | Path,
    *,
    source: str = "",
    source_fingerprint: str = "",
    op: str = "",
    params: dict[str, Any] | None = None,
    preview_rows: int = 0,
    full_csv: str = "",
) -> dict[str, Any]:
    """Write `df` as a workbook a person can use. Returns what was done to it.

    The return value is a report rather than a bool because every enrichment
    here can legitimately not happen -- a frame with no low-cardinality text
    column gets no dropdown, a frame with no numbers gets no number formats --
    and a caller that says "formatted amounts" in its response had better be
    able to name which columns got them.
    """
    out = Path(out_path)
    rows_total = len(df)
    body = df.head(preview_rows) if preview_rows and preview_rows < rows_total else df
    rows_written = len(body)

    report: dict[str, Any] = {
        "sheets": [DATA_SHEET, README_SHEET],
        "opens_on": README_SHEET,
        "rows_total": rows_total,
        "rows_written": rows_written,
        "is_preview": rows_written < rows_total,
        "frozen_header": False,
        "autofilter": False,
        "formatted_columns": [],
        "validated_columns": [],
        "full_csv": "",
    }

    if report["is_preview"]:
        # A preview whose full version does not exist is a truncation with a
        # friendlier name. Write the CSV before the workbook that points at it.
        csv_path = Path(full_csv) if full_csv else out.with_name(f"{out.stem}_full.csv")
        try:
            df.to_csv(csv_path, index=False)
            report["full_csv"] = str(csv_path)
        except OSError as exc:
            logger.warning("full CSV companion could not be written: %s", exc)

    readme = _readme_rows(
        source=source,
        op=op,
        rows_total=rows_total,
        rows_written=rows_written,
        columns=len(df.columns),
        params=params,
        full_csv=report["full_csv"],
        source_fingerprint=source_fingerprint,
    )

    report.update(_write_streaming(body, readme, out))
    return report


# Excel's own ceilings: a sheet past either is a file Excel repairs on open, not one it shows.
EXCEL_MAX_ROWS = 1_048_576
EXCEL_MAX_COLS = 16_384

# Rows turned into Python values at a time. The workbook is written as a stream: a 119,390 x 32
# export (3.8 million cells) was 3 GB through `pd.ExcelWriter`, which holds every cell as an
# object, and was killed at the server's 1 GB limit -- and the server with it.
_CHUNK_ROWS = 5_000


def _plain(value: Any) -> Any:
    """A value openpyxl will write: what pandas' own writer wrote, text for anything it cannot."""
    from datetime import date, time, timedelta
    from decimal import Decimal

    if isinstance(value, float) and value in (float("inf"), float("-inf")):
        return "inf" if value > 0 else "-inf"  # Excel has no infinity; pandas' own writer wrote this text
    if value is None or isinstance(value, (str, bool, int, float, datetime, date, time, timedelta, Decimal)):
        return value
    return str(value)


def _write_streaming(body, readme: list[tuple[str, str]], out: Path) -> dict[str, Any]:
    """Write the Data sheet row by row, then README, and return the styling report.

    Everything sheet-wide (frozen header, filter, widths, dropdowns) is declared before the first
    row, which is the order a streaming worksheet needs; number formats go on the cells of the
    columns that have one.
    """
    from openpyxl import Workbook
    from openpyxl.cell import WriteOnlyCell
    from openpyxl.styles import Alignment, Font
    from openpyxl.worksheet.datavalidation import DataValidation

    n_rows, n_cols = len(body), len(body.columns)
    _check_excel_limits(n_rows, n_cols)

    book = Workbook(write_only=True)
    data = book.create_sheet(DATA_SHEET)
    notes = book.create_sheet(README_SHEET)

    formatted: list[str] = []
    validated: list[str] = []
    formats: list[str] = []
    data.freeze_panes = "A2"
    last_col = _column_letter(n_cols) if n_cols else "A"
    if n_rows and n_cols:
        data.auto_filter.ref = f"A1:{last_col}{n_rows + 1}"
    for i, name in enumerate(body.columns, start=1):
        letter = _column_letter(i)
        series = body[name]
        sample = series.head(_WIDTH_SAMPLE).astype(str)
        widest = int(sample.str.len().max()) if len(sample) else 0
        data.column_dimensions[letter].width = max(MIN_COL_WIDTH, min(MAX_COL_WIDTH, max(widest, len(str(name))) + 2))
        fmt = _number_format(series) if n_rows else ""
        formats.append(fmt)
        if fmt:
            formatted.append(str(name))
        choices = _validation_choices(series) if n_rows else None
        if choices:
            dv = DataValidation(type="list", formula1='"' + ",".join(choices) + '"', allow_blank=True)
            dv.error = f"Not one of the {len(choices)} value(s) in this column."
            dv.errorTitle = f"{name}: unexpected value"
            data.data_validations.append(dv)
            dv.add(f"{letter}2:{letter}{n_rows + 1}")
            validated.append(str(name))

    bold = Font(bold=True)
    header = []
    for name in body.columns:
        cell = WriteOnlyCell(data, value=str(name))
        cell.font = bold
        header.append(cell)
    data.append(header)

    _append_rows(data, body, formats)

    notes.column_dimensions["A"].width = 26
    notes.column_dimensions["B"].width = 78
    notes.append([_bold_cell(notes, "Field", bold), _bold_cell(notes, "Value", bold)])
    for field, value in readme:
        cell = WriteOnlyCell(notes, value=str(value))
        cell.alignment = Alignment(wrap_text=True, vertical="top")
        notes.append([field, cell])

    book.active = book.sheetnames.index(README_SHEET)
    for ws in (data, notes):
        ws.sheet_view.tabSelected = ws.title == README_SHEET
    book.save(out)
    return {
        "frozen_header": True,
        "autofilter": bool(n_rows and n_cols),
        "formatted_columns": formatted,
        "validated_columns": validated,
    }


def _check_excel_limits(n_rows: int, n_cols: int) -> None:
    if n_rows + 1 > EXCEL_MAX_ROWS or n_cols > EXCEL_MAX_COLS:
        raise ValueError(
            f"This sheet is too large for Excel ({n_rows + 1:,} rows x {n_cols:,} columns; the limit is "
            f"{EXCEL_MAX_ROWS:,} x {EXCEL_MAX_COLS:,}). Use preview_rows, or export as csv or parquet."
        )


def _append_rows(sheet, body, formats: list[str]) -> None:
    """Append `body` to a write-only sheet a chunk at a time; a column with a number format gets it per cell."""
    from openpyxl.cell import WriteOnlyCell

    styled = [i for i, fmt in enumerate(formats) if fmt]
    for start in range(0, len(body), _CHUNK_ROWS):
        chunk = body.iloc[start : start + _CHUNK_ROWS]
        values = chunk.astype(object).where(chunk.notna(), None)
        for row in values.itertuples(index=False, name=None):
            line = [_plain(v) for v in row]
            for i in styled:
                if line[i] is not None:
                    cell = WriteOnlyCell(sheet, value=line[i])
                    cell.number_format = formats[i]
                    line[i] = cell
            sheet.append(line)


def write_plain_workbook(df, out_path: str | Path, sheet_name: str = "Sheet1") -> None:
    """`df` as a one-sheet workbook with nothing added: what `df.to_excel` wrote, without holding it all.

    For a conversion, where the caller asked for the same table in another format; `write_workbook`
    is the one that adds a README and formatting. Written to a temporary file beside the target and
    moved into place, so a failure leaves no half-written workbook under the real name.
    """
    import os
    import shutil
    import tempfile

    from openpyxl import Workbook

    from shared.file_utils import apply_default_mode

    _check_excel_limits(len(df), len(df.columns))
    book = Workbook(write_only=True)
    sheet = book.create_sheet(sheet_name)
    sheet.append([str(c) for c in df.columns])
    _append_rows(sheet, df, [""] * len(df.columns))
    target = Path(out_path)
    fd, tmp = tempfile.mkstemp(dir=target.parent, suffix=".xlsx")
    os.close(fd)
    try:
        book.save(tmp)
        apply_default_mode(tmp)
        shutil.move(tmp, str(target))
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def _bold_cell(ws, value: str, font) -> Any:
    from openpyxl.cell import WriteOnlyCell

    cell = WriteOnlyCell(ws, value=value)
    cell.font = font
    return cell
