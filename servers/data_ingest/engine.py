"""data_ingest engine — spreadsheet ingestion logic. Zero MCP imports."""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import re
import sys
import tomllib
from pathlib import Path
from typing import Any

_ROOT = str(Path(__file__).resolve().parents[2])
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import pandas as pd

from shared.counts import counted
from shared.file_utils import (
    atomic_write,
    atomic_write_text,
    embed_content,
    error_text,
    get_default_output_dir,
    hint_for_error,
    normalise_export_format,
    resolve_path,
)
from shared.platform_utils import get_max_results
from shared.progress import fail, info, ok, warn
from shared.receipt import append_receipt
from shared.version_control import drop_snapshot_if_unwritten, snapshot

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stderr, level=logging.WARNING)

_XLSX_EXTS = {".xlsx", ".ods"}
_ALL_INPUT_EXTS = {".xlsx", ".ods", ".csv", ".json", ".parquet", ".toml", ".xml"}
_OUTPUT_FMTS = {"csv", "json", "parquet", "excel"}
_FMT_EXT = {"csv": ".csv", "json": ".json", "parquet": ".parquet", "excel": ".xlsx"}

# The alias table lives in shared, not here. It was here first, and export_data
# on the data_advanced server -- same vocabulary, same "excel" for a file called
# .xlsx -- went on refusing `xlsx` for another half a day because the fix had a
# copy rather than a home.


def _token_estimate(obj: object) -> int:
    return len(str(obj)) // 4


def _needs_a_workbook(op: str, ext: str) -> dict:
    """The refusal every sheet tool here gives a .csv, written once.

    It was written five times, with five different pieces of advice, and three
    of them sent the caller to a tool that refuses for exactly the same reason:
    extract_sheet said "use list_sheets() first" (list_sheets rejects a .csv
    too), and detect_tables and extract_table both said "use extract_sheet() to
    get a CSV first" -- to a caller who is holding a CSV, about a tool that
    needs a workbook. Following any of the three arrives back here.

    Two of the five were right, and both named convert_file(). That is the tool
    that turns a CSV into a workbook, so it is the one all five name now, with
    the argument spelled out: the format is called "excel", which is not the
    word these hints used to use.
    """
    return {
        "success": False,
        "error": f"Expected .xlsx or .ods, got {ext!r}",
        "hint": (
            'Use convert_file(file_path, output_format="excel") to make a workbook from a '
            f"CSV, JSON or Parquet file, then call {op}() on that."
        ),
        "progress": [fail("Wrong file type", ext)],
        "token_estimate": 30,
    }


def _widest_row(path: Path) -> int:
    """Field count of the widest row in a CSV, honouring quoting."""
    width = 0
    with open(path, newline="", encoding="utf-8", errors="replace") as fh:
        for row in csv.reader(fh):
            width = max(width, len(row))
    return width


def _first_row(path: Path) -> list[str]:
    """The first row's fields, honouring quoting."""
    with open(path, newline="", encoding="utf-8", errors="replace") as fh:
        for row in csv.reader(fh):
            return row
    return []


def blank_line_count(path: Path) -> int:
    """Physically blank lines in a CSV -- the ones pandas never shows anyone.

    `read_csv` drops them before the frame exists (skip_blank_lines defaults to
    True), so a tool that removes empty rows cannot count the ones it removed:
    trim_empty reported rows_dropped 0 on a file with two blank lines above the
    table and one below, having correctly written all three out of the result.

    Reading with skip_blank_lines=False is not the fix. A leading blank line
    then becomes the header row and pandas raises "No columns to parse from
    file" -- so the tool would fail outright on the exact shape it exists to
    repair, which is worse than a wrong count.

    csv.reader rather than a line scan, so a blank line inside a quoted field
    stays part of its row instead of being counted as an empty one.
    """
    try:
        with open(path, newline="", encoding="utf-8", errors="replace") as fh:
            return sum(1 for row in csv.reader(fh) if not row)
    except OSError:
        return 0


def read_csv_ragged(path: Path, header: int | None = 0) -> pd.DataFrame:
    """Read a CSV whose rows disagree about how many fields they have.

    pandas fixes the column count from the first row it reads and raises
    ParserError the moment a later row is wider:

        Error tokenizing data. C error: Expected 1 fields in line 3, saw 16

    That is the ordinary shape of the files this server exists to repair -- a
    title line above a table, a sheet exported with a short header row, a
    "generated on ..." banner. So promote_header(), trim_empty() and
    normalize_headers() each refused the exact input they were written for,
    and the error named a pandas internal rather than anything the caller
    could act on.

    The well-formed path is unchanged and pays nothing: a plain read is tried
    first, and the file is only re-scanned for its true width after pandas has
    already refused it. Short rows are then padded with NaN, which is what a
    spreadsheet shows for the same cells.

    """
    try:
        return pd.read_csv(str(path), header=header)
    except pd.errors.ParserError:
        # Only a width the whole file agrees on can rescue this; if the file
        # has no rows at all, the original ParserError is the honest answer.
        width = _widest_row(path)
        if width == 0:
            raise

    if header is None:
        return pd.read_csv(str(path), names=range(width))

    # Take the header off with the csv module and let pandas infer dtypes from
    # the data rows alone -- leaving the header in place would make every
    # column object-typed.
    names = _first_row(path)
    columns = [
        str(names[i]).strip() if i < len(names) and str(names[i]).strip() else f"Unnamed: {i}" for i in range(width)
    ]
    frame = pd.read_csv(str(path), names=range(width), skiprows=1)
    frame.columns = columns
    return frame


def _resolve_sheet(wb, sheet: str):
    """Return (ws, sheet_name) from an openpyxl workbook. sheet may be name or int-as-str."""
    names = wb.sheetnames
    if not sheet:
        ws = wb.active
        return ws, ws.title
    if sheet.lstrip("-").isdigit():
        idx = int(sheet)
        if idx < 0 or idx >= len(names):
            return None, None
        return wb[names[idx]], names[idx]
    if sheet not in names:
        return None, None
    return wb[sheet], sheet


def _sheet_to_df(path: Path, sheet_name: str, header_row: int = 0) -> pd.DataFrame:
    """Read a single sheet into a DataFrame regardless of file format."""
    ext = path.suffix.lower()
    if ext == ".ods":
        return pd.read_excel(str(path), sheet_name=sheet_name, header=header_row, engine="odf")
    return pd.read_excel(str(path), sheet_name=sheet_name, header=header_row, engine="openpyxl")


def _find_tables(ws, min_rows: int, min_cols: int) -> list[dict]:
    """Detect bounding boxes of separate tables in a worksheet."""
    max_row = ws.max_row or 0
    max_col = ws.max_column or 0
    if max_row == 0 or max_col == 0:
        return []

    # Sequential iter_rows(), not random ws.cell(row, col) access — read_only
    # worksheets stream lazily and random cell access can stall or crawl.
    occupied = []
    for row in ws.iter_rows(min_row=1, max_row=max_row, min_col=1, max_col=max_col, values_only=True):
        occupied.append([val is not None and str(val).strip() != "" for val in row])

    # Find contiguous non-empty row groups
    row_groups: list[tuple[int, int]] = []
    in_group = False
    group_start = 0
    for i, row in enumerate(occupied):
        if any(row):
            if not in_group:
                group_start = i
                in_group = True
        else:
            if in_group:
                row_groups.append((group_start, i - 1))
                in_group = False
    if in_group:
        row_groups.append((group_start, len(occupied) - 1))

    tables = []
    for rs, rend in row_groups:
        if (rend - rs + 1) < min_rows:
            continue
        col_occ = [False] * max_col
        for r in range(rs, rend + 1):
            for c in range(max_col):
                if occupied[r][c]:
                    col_occ[c] = True
        in_col = False
        cs = 0
        for c, occ in enumerate(col_occ):
            if occ:
                if not in_col:
                    cs = c
                    in_col = True
            else:
                if in_col:
                    if (c - cs) >= min_cols:
                        tables.append(
                            {
                                "row_start": rs,
                                "row_end": rend,
                                "col_start": cs,
                                "col_end": c - 1,
                                "rows": rend - rs + 1,
                                "cols": c - cs,
                            }
                        )
                    in_col = False
        if in_col and (max_col - cs) >= min_cols:
            tables.append(
                {
                    "row_start": rs,
                    "row_end": rend,
                    "col_start": cs,
                    "col_end": max_col - 1,
                    "rows": rend - rs + 1,
                    "cols": max_col - cs,
                }
            )
    for i, t in enumerate(tables):
        t["index"] = i
    return tables


# ---------------------------------------------------------------------------
# 1. list_sheets
# ---------------------------------------------------------------------------


def list_sheets(file_path: str) -> dict:
    progress = []
    try:
        path = resolve_path(file_path)
        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check that file_path is absolute and the file exists.",
                "progress": [fail("File not found", str(path))],
                "token_estimate": 20,
            }
        ext = path.suffix.lower()
        if ext not in _XLSX_EXTS:
            return _needs_a_workbook("list_sheets", ext)

        if ext == ".ods":
            xl = pd.ExcelFile(str(path), engine="odf")
            sheet_names = xl.sheet_names
            sheets = []
            for name in sheet_names:
                df = xl.parse(name, header=None)
                sheets.append({"name": name, "rows": len(df), "cols": len(df.columns)})
        else:
            import openpyxl

            wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
            sheet_names = wb.sheetnames
            sheets = []
            for name in sheet_names:
                ws = wb[name]
                sheets.append({"name": name, "rows": ws.max_row or 0, "cols": ws.max_column or 0})
            wb.close()

        max_r = get_max_results()
        total_sheets = len(sheets)
        if total_sheets > max_r:
            progress.append(warn("Truncated", f"Showing {max_r} of {total_sheets} sheets"))
            sheets = sheets[:max_r]

        progress.append(ok("Listed sheets", f"{len(sheet_names)} sheet(s) in {path.name}"))
        result = {
            "success": True,
            "op": "list_sheets",
            "file": path.name,
            "file_path": str(path),
            "sheet_count": total_sheets,
            "sheets": sheets,
            **counted(len(sheets), total_sheets),
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result
    except Exception as exc:
        logger.exception("list_sheets error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check that the file is a valid Excel or ODS file."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# 2. extract_sheet
# ---------------------------------------------------------------------------


def extract_sheet(
    file_path: str,
    sheet: str = "",
    output_path: str = "",
    header_row: int = 0,
    dry_run: bool = False,
    return_content: bool = False,
) -> dict:
    backup = None
    progress = []
    try:
        path = resolve_path(file_path)
        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check that file_path is absolute and the file exists.",
                "progress": [fail("File not found", str(path))],
                "token_estimate": 20,
            }
        ext = path.suffix.lower()
        if ext not in _XLSX_EXTS:
            return _needs_a_workbook("extract_sheet", ext)

        # Resolve sheet name
        if ext == ".ods":
            xl = pd.ExcelFile(str(path), engine="odf")
            available = xl.sheet_names
            sheet_name = (
                available[0] if not sheet else (available[int(sheet)] if sheet.lstrip("-").isdigit() else sheet)
            )
            if sheet_name not in available:
                return {
                    "success": False,
                    "error": f"Sheet {sheet!r} not found",
                    "hint": f"Available: {available}. Call list_sheets() to inspect.",
                    "progress": [fail("Sheet not found", sheet)],
                    "token_estimate": 20,
                }
        else:
            import openpyxl

            wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
            ws, sheet_name = _resolve_sheet(wb, sheet)
            wb.close()
            if ws is None:
                available = wb.sheetnames if hasattr(wb, "sheetnames") else []
                return {
                    "success": False,
                    "error": f"Sheet {sheet!r} not found",
                    "hint": f"Available: {available}. Call list_sheets() to inspect.",
                    "progress": [fail("Sheet not found", sheet)],
                    "token_estimate": 20,
                }

        df = _sheet_to_df(path, sheet_name, header_row)
        out_dir = resolve_path(output_path).parent if output_path else get_default_output_dir(str(path))
        stem = f"{path.stem}_{sheet_name}" if sheet_name != path.stem else path.stem
        out = resolve_path(output_path) if output_path else out_dir / f"{stem}.csv"

        if dry_run:
            progress.append(info("Dry run — no changes written", path.name))
            result = {
                "success": True,
                "dry_run": True,
                "op": "extract_sheet",
                "sheet": sheet_name,
                "would_change": {"output_path": str(out), "rows": len(df), "cols": len(df.columns)},
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        if out.exists():
            backup = snapshot(str(out))
            progress.append(info("Snapshot created", Path(backup).name))

        out.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_text(out, df.to_csv(index=False))
        append_receipt(
            str(path),
            tool="extract_sheet",
            args={"sheet": sheet_name, "header_row": header_row},
            result=f"extracted {len(df)} rows to {out.name}",
            backup=backup or "",
        )
        progress.append(ok("Extracted sheet", f"{len(df)} rows → {out.name}"))
        result = {
            "success": True,
            "op": "extract_sheet",
            "file": path.name,
            "sheet": sheet_name,
            "output_path": str(out),
            "rows": len(df),
            "cols": len(df.columns),
            "backup": backup,
            "progress": progress,
        }
        embed_content(result, out, return_content)
        result["token_estimate"] = _token_estimate(result)
        return result
    except Exception as exc:
        logger.exception("extract_sheet error")
        return {
            "success": False,
            "error": error_text(exc),
            "backup": drop_snapshot_if_unwritten(backup, out),
            "hint": hint_for_error(exc, "Use list_sheets() to verify sheet names."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# 3. extract_all_sheets
# ---------------------------------------------------------------------------


def extract_all_sheets(file_path: str, output_dir: str = "", dry_run: bool = False) -> dict:
    backup = None
    progress = []
    try:
        path = resolve_path(file_path)
        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check that file_path is absolute and the file exists.",
                "progress": [fail("File not found", str(path))],
                "token_estimate": 20,
            }
        ext = path.suffix.lower()
        if ext not in _XLSX_EXTS:
            return _needs_a_workbook("extract_all_sheets", ext)

        # Through the resolver like every other path: a bare Path(output_dir)
        # read a relative folder from the process cwd and took an absolute one
        # as given, so a confined server wrote its CSVs wherever it was pointed.
        out_dir = resolve_path(output_dir) if output_dir else get_default_output_dir(str(path))

        if ext == ".ods":
            xl = pd.ExcelFile(str(path), engine="odf")
            sheet_names = xl.sheet_names
        else:
            import openpyxl

            wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
            sheet_names = wb.sheetnames
            wb.close()

        if dry_run:
            would = [{"sheet": s, "output_path": str(out_dir / f"{path.stem}_{s}.csv")} for s in sheet_names]
            progress.append(info("Dry run — no changes written", path.name))
            result = {
                "success": True,
                "dry_run": True,
                "op": "extract_all_sheets",
                "would_change": would,
                "sheet_count": len(sheet_names),
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        out_dir.mkdir(parents=True, exist_ok=True)

        extracted = []
        for name in sheet_names:
            df = _sheet_to_df(path, name)
            out = out_dir / f"{path.stem}_{name}.csv"
            atomic_write_text(out, df.to_csv(index=False))
            extracted.append({"sheet": name, "output_path": str(out), "rows": len(df)})
            progress.append(ok(f"Extracted {name}", f"{len(df)} rows → {out.name}"))

        append_receipt(
            str(path),
            tool="extract_all_sheets",
            args={"output_dir": str(out_dir)},
            result=f"extracted {len(extracted)} sheets",
            backup=backup,
        )
        result = {
            "success": True,
            "op": "extract_all_sheets",
            "file": path.name,
            "output_dir": str(out_dir),
            "extracted": extracted,
            "sheet_count": len(extracted),
            "backup": backup,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result
    except Exception as exc:
        logger.exception("extract_all_sheets error")
        return {
            "success": False,
            "error": error_text(exc),
            "backup": backup,
            "hint": hint_for_error(exc, "Use restore_version() on the input file if a snapshot was taken."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# 4. detect_tables
# ---------------------------------------------------------------------------


def detect_tables(file_path: str, sheet: str = "", min_rows: int = 2, min_cols: int = 2) -> dict:
    progress = []
    try:
        path = resolve_path(file_path)
        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check that file_path is absolute and the file exists.",
                "progress": [fail("File not found", str(path))],
                "token_estimate": 20,
            }
        ext = path.suffix.lower()
        if ext not in _XLSX_EXTS:
            return _needs_a_workbook("detect_tables", ext)

        import openpyxl

        wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
        ws, sheet_name = _resolve_sheet(wb, sheet)
        if ws is None:
            available = wb.sheetnames
            wb.close()
            return {
                "success": False,
                "error": f"Sheet {sheet!r} not found",
                "hint": f"Available: {available}. Call list_sheets() to inspect.",
                "progress": [fail("Sheet not found", sheet)],
                "token_estimate": 20,
            }

        tables = _find_tables(ws, min_rows, min_cols)
        wb.close()

        max_r = get_max_results()
        # `table_count` used to be computed after the slice below, so a sheet
        # with 25 tables reported `table_count: 20, truncated: true` and named
        # its real total nowhere. Take the total before cutting, and let
        # `counted()` derive the flag from the two numbers.
        total_tables = len(tables)
        if total_tables > max_r:
            progress.append(warn("Truncated", f"Showing {max_r} of {total_tables} tables"))
            tables = tables[:max_r]

        progress.append(ok("Detected tables", f"{len(tables)} table(s) in sheet {sheet_name!r}"))
        result = {
            "success": True,
            "op": "detect_tables",
            "file": path.name,
            "sheet": sheet_name,
            "table_count": total_tables,
            "tables": tables,
            **counted(len(tables), total_tables),
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result
    except Exception as exc:
        logger.exception("detect_tables error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check the file is a valid Excel file."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# 5. extract_table
# ---------------------------------------------------------------------------


def extract_table(
    file_path: str,
    table_index: int = 0,
    sheet: str = "",
    output_path: str = "",
    min_rows: int = 2,
    min_cols: int = 2,
    dry_run: bool = False,
    return_content: bool = False,
) -> dict:
    backup = None
    progress = []
    try:
        path = resolve_path(file_path)
        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check that file_path is absolute and the file exists.",
                "progress": [fail("File not found", str(path))],
                "token_estimate": 20,
            }
        ext = path.suffix.lower()
        if ext not in _XLSX_EXTS:
            return _needs_a_workbook("extract_table", ext)

        import openpyxl

        wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
        ws, sheet_name = _resolve_sheet(wb, sheet)
        if ws is None:
            available = wb.sheetnames
            wb.close()
            return {
                "success": False,
                "error": f"Sheet {sheet!r} not found",
                "hint": f"Available: {available}. Call list_sheets() to inspect.",
                "progress": [fail("Sheet not found", sheet)],
                "token_estimate": 20,
            }

        tables = _find_tables(ws, min_rows=min_rows, min_cols=min_cols)
        wb.close()

        if table_index < 0 or table_index >= len(tables):
            return {
                "success": False,
                "error": f"table_index {table_index} out of range (found {len(tables)} tables)",
                "hint": "Call detect_tables() with the same min_rows/min_cols first to see available table indices.",
                "progress": [fail("Table index out of range", str(table_index))],
                "token_estimate": 20,
            }

        t = tables[table_index]
        bbox = {
            "row_start": t["row_start"],
            "row_end": t["row_end"],
            "col_start": t["col_start"],
            "col_end": t["col_end"],
        }

        # Read the whole sheet then slice
        df_full = _sheet_to_df(path, sheet_name, header_row=t["row_start"])
        col_slice = slice(t["col_start"], t["col_end"] + 1)
        # header_row consumed the first row; remaining rows start after header
        data_rows = t["row_end"] - t["row_start"]
        df = df_full.iloc[:data_rows, col_slice].reset_index(drop=True)

        out_dir = get_default_output_dir(str(path))
        out = resolve_path(output_path) if output_path else out_dir / f"{path.stem}_{sheet_name}_table{table_index}.csv"

        if dry_run:
            progress.append(info("Dry run — no changes written", path.name))
            result = {
                "success": True,
                "dry_run": True,
                "op": "extract_table",
                "table_index": table_index,
                "sheet": sheet_name,
                "bounding_box": bbox,
                "would_change": {"output_path": str(out), "rows": len(df), "cols": len(df.columns)},
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        if out.exists():
            backup = snapshot(str(out))
            progress.append(info("Snapshot created", Path(backup).name))

        out.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_text(out, df.to_csv(index=False))
        append_receipt(
            str(path),
            tool="extract_table",
            args={"table_index": table_index, "sheet": sheet_name},
            result=f"extracted {len(df)} rows to {out.name}",
            backup=backup or "",
        )
        progress.append(ok("Extracted table", f"{len(df)} rows × {len(df.columns)} cols → {out.name}"))
        result = {
            "success": True,
            "op": "extract_table",
            "file": path.name,
            "table_index": table_index,
            "sheet": sheet_name,
            "bounding_box": bbox,
            "output_path": str(out),
            "rows": len(df),
            "cols": len(df.columns),
            "backup": backup,
            "progress": progress,
        }
        embed_content(result, out, return_content)
        result["token_estimate"] = _token_estimate(result)
        return result
    except Exception as exc:
        logger.exception("extract_table error")
        return {
            "success": False,
            "error": error_text(exc),
            "backup": drop_snapshot_if_unwritten(backup, out),
            "hint": hint_for_error(exc, "Call detect_tables() to verify table indices."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# 6. normalize_headers
# ---------------------------------------------------------------------------


def normalize_headers(
    file_path: str,
    lowercase: bool = True,
    replace_spaces: bool = True,
    output_path: str = "",
    dry_run: bool = False,
) -> dict:
    import re

    backup = None
    progress = []
    try:
        path = resolve_path(file_path)
        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check that file_path is absolute and the file exists.",
                "progress": [fail("File not found", str(path))],
                "token_estimate": 20,
            }
        if path.suffix.lower() != ".csv":
            return {
                "success": False,
                "error": f"Expected .csv, got {path.suffix!r}",
                "hint": "Use extract_sheet() or convert_file() to produce a CSV first.",
                "progress": [fail("Wrong file type", path.suffix)],
                "token_estimate": 20,
            }

        df = read_csv_ragged(path)
        old_cols = list(df.columns)
        new_cols: list[str] = []
        assigned: set[str] = set()

        for col in old_cols:
            new = col.strip()
            if lowercase:
                new = new.lower()
            if replace_spaces:
                new = new.replace(" ", "_")
            new = re.sub(r"_+", "_", new).strip("_") or "col"
            candidate = new
            counter = 2
            while candidate in assigned:
                candidate = f"{new}_{counter}"
                counter += 1
            assigned.add(candidate)
            new_cols.append(candidate)

        changes = {old: nw for old, nw in zip(old_cols, new_cols) if old != nw}
        out = resolve_path(output_path) if output_path else path

        if dry_run:
            progress.append(info("Dry run — no changes written", path.name))
            result = {
                "success": True,
                "dry_run": True,
                "op": "normalize_headers",
                "would_change": changes,
                "would_write": str(out),
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        # Only the source needs saving from itself; a named destination leaves
        # the caller's file untouched, so there is nothing to snapshot.
        if out == path:
            backup = snapshot(str(path))
            progress.append(info("Snapshot created", Path(backup).name))
        df.columns = new_cols
        out.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_text(out, df.to_csv(index=False))
        append_receipt(
            str(path),
            tool="normalize_headers",
            args={"lowercase": lowercase, "replace_spaces": replace_spaces},
            result=f"renamed {len(changes)} headers → {out.name}",
            backup=backup or "",
        )
        progress.append(ok("Normalized headers", f"{len(changes)} renamed → {out.name}"))
        result = {
            "success": True,
            "op": "normalize_headers",
            "file": path.name,
            "output_path": str(out),
            "changes": changes,
            "renamed_count": len(changes),
            "deduped_count": sum(
                1 for o, n in zip(old_cols, new_cols) if o != n and n != o.strip().lower().replace(" ", "_")
            ),
            "backup": backup or "",
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result
    except Exception as exc:
        logger.exception("normalize_headers error")
        return {
            "success": False,
            "error": error_text(exc),
            "backup": drop_snapshot_if_unwritten(backup, path),
            "hint": hint_for_error(exc, "Use inspect_dataset() to verify column names first."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# 7. trim_empty
# ---------------------------------------------------------------------------


def trim_empty(file_path: str, output_path: str = "", dry_run: bool = False) -> dict:
    backup = None
    progress = []
    try:
        path = resolve_path(file_path)
        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check that file_path is absolute and the file exists.",
                "progress": [fail("File not found", str(path))],
                "token_estimate": 20,
            }
        if path.suffix.lower() != ".csv":
            return {
                "success": False,
                "error": f"Expected .csv, got {path.suffix!r}",
                "hint": "Use extract_sheet() or convert_file() to produce a CSV first.",
                "progress": [fail("Wrong file type", path.suffix)],
                "token_estimate": 20,
            }

        df = read_csv_ragged(path)
        # Blank lines are the thing this tool exists to remove, and the reader
        # drops them before the frame exists -- so counting the frame counted
        # everything except them. A file with two blank lines above the table
        # and one below came back "rows_before: 16834, rows_dropped: 0" while
        # the output correctly had all three gone. They are counted from the
        # file instead; see blank_line_count for why not from the parse.
        blank_lines = blank_line_count(path)
        rows_before = len(df) + blank_lines
        cols_before = len(df.columns)

        df = df.replace(r"^\s*$", pd.NA, regex=True)
        df = df.dropna(axis=1, how="all")
        df = df.dropna(axis=0, how="all")
        df = df.reset_index(drop=True)
        df = df.fillna("")

        rows_after = len(df)
        cols_after = len(df.columns)
        rows_dropped = rows_before - rows_after
        cols_dropped = cols_before - cols_after

        out = resolve_path(output_path) if output_path else path

        if dry_run:
            progress.append(info("Dry run — no changes written", path.name))
            result = {
                "success": True,
                "dry_run": True,
                "op": "trim_empty",
                "would_change": {"rows_to_drop": rows_dropped, "cols_to_drop": cols_dropped},
                "would_write": str(out),
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        if out == path:
            backup = snapshot(str(path))
            progress.append(info("Snapshot created", Path(backup).name))
        out.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_text(out, df.to_csv(index=False))
        append_receipt(
            str(path),
            tool="trim_empty",
            args={},
            result=f"dropped {rows_dropped} rows, {cols_dropped} cols → {out.name}",
            backup=backup or "",
        )
        progress.append(ok("Trimmed empty", f"-{rows_dropped} rows, -{cols_dropped} cols → {out.name}"))
        result = {
            "success": True,
            "op": "trim_empty",
            "file": path.name,
            "output_path": str(out),
            "rows_before": rows_before,
            "rows_after": rows_after,
            "cols_before": cols_before,
            "cols_after": cols_after,
            "rows_dropped": rows_dropped,
            "cols_dropped": cols_dropped,
            "backup": backup or "",
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result
    except Exception as exc:
        logger.exception("trim_empty error")
        return {
            "success": False,
            "error": error_text(exc),
            "backup": drop_snapshot_if_unwritten(backup, path),
            "hint": hint_for_error(exc, "Use inspect_dataset() to verify the file structure first."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# 8. promote_header
# ---------------------------------------------------------------------------


def promote_header(file_path: str, row_index: int = 0, output_path: str = "", dry_run: bool = False) -> dict:
    backup = None
    progress = []
    try:
        path = resolve_path(file_path)
        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check that file_path is absolute and the file exists.",
                "progress": [fail("File not found", str(path))],
                "token_estimate": 20,
            }
        if path.suffix.lower() != ".csv":
            return {
                "success": False,
                "error": f"Expected .csv, got {path.suffix!r}",
                "hint": "Use extract_sheet() or convert_file() to produce a CSV first.",
                "progress": [fail("Wrong file type", path.suffix)],
                "token_estimate": 20,
            }

        df = read_csv_ragged(path, header=None)
        if row_index < 0 or row_index >= len(df):
            return {
                "success": False,
                "error": f"row_index {row_index} out of range (file has {len(df)} rows)",
                "hint": f"Valid range: 0 to {len(df) - 1}.",
                "progress": [fail("Row index out of range", str(row_index))],
                "token_estimate": 20,
            }

        new_headers = [str(v) if pd.notna(v) else f"col_{i}" for i, v in enumerate(df.iloc[row_index])]
        # The slice starts after the promoted row, so it removes row_index + 1
        # rows from the frame -- but one of those is the header row itself, and
        # it is not dropped: it becomes the columns. What the caller actually
        # loses is whatever sat above it, which is exactly row_index. Reporting
        # the slice width under the name `rows_dropped_above` said 2 for a file
        # with one title line over the header.
        rows_above = row_index
        df = df.iloc[row_index + 1 :].copy()
        df.columns = new_headers
        df = df.reset_index(drop=True)

        out = resolve_path(output_path) if output_path else path

        if dry_run:
            progress.append(info("Dry run — no changes written", path.name))
            result = {
                "success": True,
                "dry_run": True,
                "op": "promote_header",
                "would_change": {"new_headers": new_headers, "rows_dropped_above": rows_above},
                "would_write": str(out),
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        if out == path:
            backup = snapshot(str(path))
            progress.append(info("Snapshot created", Path(backup).name))
        out.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_text(out, df.to_csv(index=False))
        append_receipt(
            str(path),
            tool="promote_header",
            args={"row_index": row_index},
            result=f"promoted row {row_index} as header → {out.name}",
            backup=backup or "",
        )
        progress.append(ok("Promoted header", f"row {row_index} → columns; {rows_above} row(s) above dropped"))
        result = {
            "success": True,
            "op": "promote_header",
            "file": path.name,
            "output_path": str(out),
            "promoted_row_index": row_index,
            "new_headers": new_headers,
            "rows_dropped_above": rows_above,
            "backup": backup or "",
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result
    except Exception as exc:
        logger.exception("promote_header error")
        return {
            "success": False,
            "error": error_text(exc),
            "backup": drop_snapshot_if_unwritten(backup, path),
            "hint": hint_for_error(exc, "Use inspect_dataset() to verify row structure first."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# 9. flatten_merged_cells
# ---------------------------------------------------------------------------


def flatten_merged_cells(
    file_path: str,
    sheet: str = "",
    output_path: str = "",
    dry_run: bool = False,
    return_content: bool = False,
) -> dict:
    backup = None
    progress = []
    try:
        path = resolve_path(file_path)
        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check that file_path is absolute and the file exists.",
                "progress": [fail("File not found", str(path))],
                "token_estimate": 20,
            }
        # ODS does not have merged cells in the same sense; xlsx only
        if path.suffix.lower() != ".xlsx":
            return {
                "success": False,
                "error": f"Expected .xlsx, got {path.suffix!r}",
                "hint": "flatten_merged_cells only works on .xlsx files.",
                "progress": [fail("Wrong file type", path.suffix)],
                "token_estimate": 20,
            }

        import openpyxl

        # Must NOT use read_only=True — merged_cells requires full load
        wb = openpyxl.load_workbook(str(path), data_only=True)
        ws, sheet_name = _resolve_sheet(wb, sheet)
        if ws is None:
            available = wb.sheetnames
            wb.close()
            return {
                "success": False,
                "error": f"Sheet {sheet!r} not found",
                "hint": f"Available: {available}. Call list_sheets() to inspect.",
                "progress": [fail("Sheet not found", sheet)],
                "token_estimate": 20,
            }

        merge_map: dict[tuple[int, int], object] = {}
        merged_count = len(list(ws.merged_cells.ranges))
        for merged_range in list(ws.merged_cells.ranges):
            top_val = ws.cell(merged_range.min_row, merged_range.min_col).value
            for r in range(merged_range.min_row, merged_range.max_row + 1):
                for c in range(merged_range.min_col, merged_range.max_col + 1):
                    merge_map[(r, c)] = top_val

        max_row = ws.max_row or 0
        max_col = ws.max_column or 0
        rows_data = []
        for r in range(1, max_row + 1):
            row_vals = []
            for c in range(1, max_col + 1):
                row_vals.append(merge_map.get((r, c), ws.cell(r, c).value))
            rows_data.append(row_vals)
        wb.close()

        if not rows_data:
            df = pd.DataFrame()
        else:
            headers = [str(v) if v is not None else f"col_{i}" for i, v in enumerate(rows_data[0])]
            df = pd.DataFrame(rows_data[1:], columns=headers)

        out_dir = get_default_output_dir(str(path))
        out = resolve_path(output_path) if output_path else out_dir / f"{path.stem}_{sheet_name}_flat.csv"

        if dry_run:
            progress.append(info("Dry run — no changes written", path.name))
            result = {
                "success": True,
                "dry_run": True,
                "op": "flatten_merged_cells",
                "sheet": sheet_name,
                "would_change": {
                    "merged_regions": merged_count,
                    "output_path": str(out),
                    "rows": len(df),
                    "cols": len(df.columns),
                },
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        if out.exists():
            backup = snapshot(str(out))
            progress.append(info("Snapshot created", Path(backup).name))

        out.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_text(out, df.to_csv(index=False))
        append_receipt(
            str(path),
            tool="flatten_merged_cells",
            args={"sheet": sheet_name},
            result=f"flattened {merged_count} merged regions → {out.name}",
            backup=backup or "",
        )
        progress.append(ok("Flattened merged cells", f"{merged_count} region(s) → {out.name}"))
        result = {
            "success": True,
            "op": "flatten_merged_cells",
            "file": path.name,
            "sheet": sheet_name,
            "output_path": str(out),
            "merged_regions_found": merged_count,
            "rows": len(df),
            "cols": len(df.columns),
            "backup": backup,
            "progress": progress,
        }
        embed_content(result, out, return_content)
        result["token_estimate"] = _token_estimate(result)
        return result
    except Exception as exc:
        logger.exception("flatten_merged_cells error")
        return {
            "success": False,
            "error": error_text(exc),
            "backup": drop_snapshot_if_unwritten(backup, out),
            "hint": hint_for_error(exc, "Check the file is a valid .xlsx with merged cells."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# 10. convert_file
# ---------------------------------------------------------------------------


def _toml_rows(path: Path) -> tuple[list, str]:
    """A TOML file as table rows: its one array of tables, or the document as one row.

    Several arrays of tables are several tables, and picking one would be a
    guess -- so that is refused, naming them, with the query that reads each.
    """
    doc = tomllib.loads(path.read_text(encoding="utf-8"))
    tables = [k for k, v in doc.items() if isinstance(v, list) and v and all(isinstance(x, dict) for x in v)]
    if len(tables) > 1:
        raise ValueError(
            f"{path.name} holds {len(tables)} arrays of tables ({', '.join(tables)}); convert_file reads one "
            f"table. query_json(path='$.{tables[0]}') reads each."
        )
    if tables:
        return doc[tables[0]], f"$.{tables[0]}"
    return [doc], "$"


def convert_file(
    file_path: str,
    output_format: str = "csv",
    output_path: str = "",
    sheet: str = "",
    dry_run: bool = False,
    return_content: bool = False,
) -> dict:
    from io import BytesIO

    backup = None
    out: Path | None = None  # unset until the input has been read
    progress = []
    try:
        path = resolve_path(file_path)
        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check that file_path is absolute and the file exists.",
                "progress": [fail("File not found", str(path))],
                "token_estimate": 20,
            }
        ext = path.suffix.lower()
        if ext not in _ALL_INPUT_EXTS:
            return {
                "success": False,
                "error": f"Unsupported input format {ext!r}",
                "hint": f"Supported inputs: {sorted(_ALL_INPUT_EXTS)}",
                "progress": [fail("Unsupported input", ext)],
                "token_estimate": 20,
            }
        output_format = normalise_export_format(output_format)
        if output_format not in _OUTPUT_FMTS:
            return {
                "success": False,
                "error": f"Unknown output_format {output_format!r}",
                "hint": f"Valid formats: {', '.join(sorted(_OUTPUT_FMTS))} (xlsx is accepted for excel).",
                "progress": [fail("Unknown output format", output_format)],
                "token_estimate": 20,
            }

        target_ext = _FMT_EXT[output_format]
        if ext == target_ext or (ext in {".xlsx", ".xls"} and output_format == "excel"):
            return {
                "success": False,
                "error": "File is already in the target format.",
                "hint": "Use normalize_headers() or trim_empty() to clean the file instead.",
                "progress": [fail("Same format", f"{ext} → {output_format}")],
                "token_estimate": 20,
            }

        # Read
        sheet_used = None
        other_sheets: list[str] = []
        if ext in {".xlsx", ".ods"}:
            pd_engine = "odf" if ext == ".ods" else "openpyxl"
            xl = pd.ExcelFile(str(path), engine=pd_engine)
            names = xl.sheet_names
            if sheet and sheet.lstrip("-").isdigit():
                idx = int(sheet)
                if idx < 0 or idx >= len(names):
                    return {
                        "success": False,
                        "error": f"Sheet index {sheet!r} out of range ({len(names)} sheet(s))",
                        "hint": f"Available: {names}. Call list_sheets() to inspect.",
                        "progress": [fail("Sheet not found", sheet)],
                        "token_estimate": 20,
                    }
                sheet_used = names[idx]
            elif sheet:
                if sheet not in names:
                    return {
                        "success": False,
                        "error": f"Sheet {sheet!r} not found",
                        "hint": f"Available: {names}. Call list_sheets() to inspect.",
                        "progress": [fail("Sheet not found", sheet)],
                        "token_estimate": 20,
                    }
                sheet_used = sheet
            else:
                sheet_used = names[0]
            other_sheets = [n for n in names if n != sheet_used]
            df = xl.parse(sheet_used)
        elif ext == ".csv":
            df = read_csv_ragged(path)
        elif ext == ".json":
            df = pd.read_json(str(path))
        elif ext == ".parquet":
            df = pd.read_parquet(str(path), engine="pyarrow")
        elif ext == ".toml":
            rows, where = _toml_rows(path)
            df = pd.json_normalize(rows)
            progress.append(info("Rows from", where))
        elif ext == ".xml":
            # The standard library's parser: the rows are the root's children,
            # their attributes and child elements the columns.
            df = pd.read_xml(str(path), parser="etree")
        else:
            df = pd.DataFrame()

        out = resolve_path(output_path) if output_path else path.parent / (path.stem + target_ext)

        if other_sheets:
            progress.append(
                warn(
                    "Other sheets ignored",
                    f"Converted only {sheet_used!r}; {len(other_sheets)} more sheet(s) not included: {other_sheets}",
                )
            )

        if dry_run:
            progress.append(info("Dry run — no changes written", path.name))
            result = {
                "success": True,
                "dry_run": True,
                "op": "convert_file",
                "would_change": {"input": path.name, "output_format": output_format, "output_path": str(out)},
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        if out.exists():
            backup = snapshot(str(out))
            progress.append(info("Snapshot created", Path(backup).name))

        out.parent.mkdir(parents=True, exist_ok=True)

        # Write
        if output_format == "csv":
            atomic_write_text(out, df.to_csv(index=False))
        elif output_format == "json":
            atomic_write_text(out, df.to_json(orient="records", indent=2))
        elif output_format == "parquet":
            buf = BytesIO()
            df.to_parquet(buf, engine="pyarrow", index=False)
            atomic_write(out, buf.getvalue())
        elif output_format == "excel":
            buf = BytesIO()
            df.to_excel(buf, index=False, engine="openpyxl")
            atomic_write(out, buf.getvalue())

        append_receipt(
            str(path),
            tool="convert_file",
            args={"output_format": output_format, "sheet": sheet_used or ""},
            result=f"converted {ext} → {target_ext} ({len(df)} rows)",
            backup=backup or "",
        )
        progress.append(ok("Converted file", f"{ext} → {target_ext}: {out.name}"))
        result = {
            "success": True,
            "op": "convert_file",
            "file": path.name,
            "input_format": ext.lstrip("."),
            "output_format": output_format,
            "output_path": str(out),
            "sheet": sheet_used,
            "other_sheets_ignored": other_sheets,
            "rows": len(df),
            "cols": len(df.columns),
            "backup": backup,
            "progress": progress,
        }
        embed_content(result, out, return_content)
        result["token_estimate"] = _token_estimate(result)
        return result
    except Exception as exc:
        logger.exception("convert_file error")
        return {
            "success": False,
            "error": error_text(exc),
            "backup": drop_snapshot_if_unwritten(backup, out) if backup and out else backup,
            "hint": f"Valid output formats: {', '.join(sorted(_OUTPUT_FMTS))}",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# query_json -- a JSONPath subset over JSON or TOML
# ---------------------------------------------------------------------------

JSONPATH_SYNTAX = "$, .key, ['key'], [n], [*] or .*, ..key"
_JSONPATH_TOKEN = re.compile(
    r"""\.\.(?P<deep>[A-Za-z_][\w-]*)|\.(?P<key>[A-Za-z_][\w-]*)|\.(?P<star>\*)"""
    r"""|\[(?P<index>-?\d+)\]|\[(?P<bstar>\*)\]|\[(?P<q>['"])(?P<bkey>.*?)(?P=q)\]"""
)


def _jsonpath_steps(path: str) -> list[tuple[str, Any]]:
    text = path.strip()
    if not text.startswith("$"):
        raise ValueError(f"path {path!r} must start at the root, $. Supported: {JSONPATH_SYNTAX}.")
    steps: list[tuple[str, Any]] = []
    at = 1
    while at < len(text):
        m = _JSONPATH_TOKEN.match(text, at)
        if not m:
            raise ValueError(
                f"path {path!r}: {text[at:]!r} is not supported (filters and slices are not). "
                f"Supported: {JSONPATH_SYNTAX}."
            )
        if m["deep"] is not None:
            steps.append(("deep", m["deep"]))
        elif m["key"] is not None:
            steps.append(("key", m["key"]))
        elif m["bkey"] is not None:
            steps.append(("key", m["bkey"]))
        elif m["index"] is not None:
            steps.append(("index", int(m["index"])))
        else:
            steps.append(("star", None))
        at = m.end()
    return steps


def _key_step(key: str) -> str:
    """A key as a path step that reads back: .key where it can be, ['key'] where it cannot."""
    if re.fullmatch(r"[A-Za-z_][\w-]*", key):
        return f".{key}"
    return f'["{key}"]' if "'" in key else f"['{key}']"


def _descend(node: Any, where: str) -> list[tuple[str, Any]]:
    """The node and everything under it, each with its path."""
    out = [(where, node)]
    if isinstance(node, dict):
        for k, v in node.items():
            out += _descend(v, where + _key_step(k))
    elif isinstance(node, list):
        for i, v in enumerate(node):
            out += _descend(v, f"{where}[{i}]")
    return out


def _jsonpath(doc: Any, steps: list[tuple[str, Any]]) -> list[tuple[str, Any]]:
    found: list[tuple[str, Any]] = [("$", doc)]
    for kind, arg in steps:
        nxt: list[tuple[str, Any]] = []
        for where, node in found:
            if kind == "key" and isinstance(node, dict) and arg in node:
                nxt.append((where + _key_step(arg), node[arg]))
            elif kind == "index" and isinstance(node, list) and -len(node) <= arg < len(node):
                nxt.append((f"{where}[{arg % len(node)}]", node[arg]))
            elif kind == "star" and isinstance(node, dict):
                nxt += [(where + _key_step(k), v) for k, v in node.items()]
            elif kind == "star" and isinstance(node, list):
                nxt += [(f"{where}[{i}]", v) for i, v in enumerate(node)]
            elif kind == "deep":
                nxt += [
                    (p + _key_step(arg), n[arg]) for p, n in _descend(node, where) if isinstance(n, dict) and arg in n
                ]
        found = nxt
    return found


def query_json(file_path: str, path: str = "$") -> dict:
    progress: list[dict] = []
    try:
        src = resolve_path(file_path)
        if not src.exists():
            return {
                "success": False,
                "error": f"File not found: {src.name}",
                "hint": "Check that file_path is absolute and the file exists.",
                "progress": [fail("File not found", str(src))],
                "token_estimate": 20,
            }
        ext = src.suffix.lower()
        if ext not in {".json", ".geojson", ".toml"}:
            return {
                "success": False,
                "error": f"query_json reads .json, .geojson or .toml, not {ext or 'a file with no extension'!r}",
                "hint": "convert_file() turns a table into .json first.",
                "progress": [fail("Unsupported input", ext)],
                "token_estimate": 20,
            }
        text = src.read_text(encoding="utf-8")
        doc = tomllib.loads(text) if ext == ".toml" else json.loads(text)
        matches = _jsonpath(doc, _jsonpath_steps(path))
        cap = get_max_results()
        shown = [{"path": p, "value": v} for p, v in matches[:cap]]
        progress.append(ok(f"Queried {src.name}", f"{len(matches)} match(es) for {path}"))
        return {
            "success": True,
            "op": "query_json",
            "file": src.name,
            "path": path,
            "matches": shown,
            **counted(len(shown), len(matches)),
            "hint": (
                f"Supported: {JSONPATH_SYNTAX}. Nothing matched; '$.*' lists the top level."
                if not matches
                else f"{len(matches)} match(es); narrow the path to see fewer."
                if len(matches) > len(shown)
                else "Each match carries its own path, to query deeper from."
            ),
            "progress": progress,
            "token_estimate": 0,
        }
    except (ValueError, tomllib.TOMLDecodeError) as exc:
        return {
            "success": False,
            "error": error_text(exc),
            "hint": f"Supported: {JSONPATH_SYNTAX}.",
            "progress": [fail("query_json refused", str(exc)[:200])],
            "token_estimate": 20,
        }
    except Exception as exc:
        logger.exception("query_json error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check that file_path is absolute and readable."),
            "progress": [fail("Unexpected error", str(exc)[:200])],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# hash_file -- a checksum, to prove a file is the one expected
# ---------------------------------------------------------------------------

HASH_ALGORITHMS: tuple[str, ...] = ("sha256", "md5", "sha1")


def hash_file(file_path: str, algorithm: str = "sha256") -> dict:
    try:
        src = resolve_path(file_path)
        if not src.is_file():
            return {
                "success": False,
                "error": f"File not found: {src.name}",
                "hint": "Check that file_path is absolute and the file exists.",
                "progress": [fail("File not found", str(src))],
                "token_estimate": 20,
            }
        if algorithm not in HASH_ALGORITHMS:
            return {
                "success": False,
                "error": f"Unknown algorithm {algorithm!r}",
                "hint": f"Valid: {', '.join(HASH_ALGORITHMS)}.",
                "progress": [fail("Unknown algorithm", algorithm)],
                "token_estimate": 20,
            }
        digest = hashlib.new(algorithm)
        size = 0
        with src.open("rb") as fh:
            while chunk := fh.read(1 << 20):
                digest.update(chunk)
                size += len(chunk)
        return {
            "success": True,
            "op": "hash_file",
            "file": src.name,
            "algorithm": algorithm,
            "digest": digest.hexdigest(),
            "bytes": size,
            "hint": "Hash the file again later and compare: the same digest means the same bytes.",
            "progress": [ok(f"Hashed {src.name}", f"{algorithm} over {size:,} bytes")],
            "token_estimate": 0,
        }
    except Exception as exc:
        logger.exception("hash_file error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check that file_path is absolute and readable."),
            "progress": [fail("Unexpected error", str(exc)[:200])],
            "token_estimate": 20,
        }
