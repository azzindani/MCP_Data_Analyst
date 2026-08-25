"""Three of five refusals sent the caller to a tool that refuses the same way.

Every sheet tool on this server needs a workbook and rejects a .csv. The
refusal was written five times, with five different pieces of advice:

    list_sheets        "Use convert_file() to convert CSV/JSON/Parquet to xlsx first."
    extract_all_sheets "Use convert_file() to convert to xlsx first."
    extract_sheet      "Use list_sheets() first to inspect available sheets."
    detect_tables      "Use extract_sheet() to get a CSV first, then call detect_tables."
    extract_table      "Use extract_sheet() first to get a CSV."

The last three are circles. `list_sheets` rejects a .csv for exactly the same
reason `extract_sheet` just did. And `extract_sheet` needs a workbook, so
telling someone holding a CSV to use it "to get a CSV first" is backwards twice
over -- they already have the CSV, and the tool named cannot help them get one.
Following any of the three arrives back at the same message.

The two that were right both named `convert_file()`, which is the tool that
actually turns a CSV into a workbook. That is now the one all five name, from a
single refusal, so a sixth sheet tool cannot invent a sixth piece of advice.

And the word: `convert_file` accepts output_format="excel" and had never
accepted "xlsx" -- the word two of these hints used, and the extension the
format produces. A caller following the hint was told "Unknown output_format
'xlsx'". Now it is an alias, along with xls/ods/spreadsheet; "excel" still
works, because every existing caller passes it.

Found in a round-15 sweep report: "Its hint is self-contradictory ... but
detect_tables then rejects that CSV", and separately "output_format:'xlsx' was
rejected; valid value is `excel`".
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from servers.data_ingest import engine as ing

# Every tool here that needs a workbook, with a call that supplies only the
# arguments the refusal is about.
SHEET_TOOLS = {
    "list_sheets": lambda p: ing.list_sheets(str(p)),
    "extract_sheet": lambda p: ing.extract_sheet(str(p)),
    "extract_all_sheets": lambda p: ing.extract_all_sheets(str(p)),
    "detect_tables": lambda p: ing.detect_tables(str(p)),
    "extract_table": lambda p: ing.extract_table(str(p)),
}


@pytest.fixture()
def csv_file(tmp_path: Path) -> Path:
    f = tmp_path / "plain.csv"
    f.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
    return f


@pytest.mark.parametrize("tool", sorted(SHEET_TOOLS))
class TestTheRefusalPointsSomewhereThatWorks:
    def test_it_still_refuses_a_csv(self, tool: str, csv_file: Path) -> None:
        r = SHEET_TOOLS[tool](csv_file)
        assert r["success"] is False
        assert ".xlsx" in r["error"], r["error"]

    def test_it_names_convert_file(self, tool: str, csv_file: Path) -> None:
        assert "convert_file" in SHEET_TOOLS[tool](csv_file)["hint"]

    def test_it_names_the_format_argument_that_works(self, tool: str, csv_file: Path) -> None:
        """ "Convert it first" is only actionable with the spelling attached."""
        hint = SHEET_TOOLS[tool](csv_file)["hint"]
        assert 'output_format="excel"' in hint, hint

    def test_it_does_not_send_the_caller_to_another_tool_that_refuses(self, tool: str, csv_file: Path) -> None:
        """The defect itself: a hint naming a *different* tool with the same gate.

        Naming the calling tool is not circular -- "then call detect_tables() on
        that" is telling the caller to come back here with a workbook, which is
        the whole point. Sending them to list_sheets() or extract_sheet(), which
        refuse a .csv for the identical reason, is.
        """
        hint = SHEET_TOOLS[tool](csv_file)["hint"]
        named = set(re.findall(r"\b(\w+)\(", hint))
        circular = (named & set(SHEET_TOOLS)) - {tool}
        assert not circular, f"{tool} sends the caller to {circular}, which refuses a .csv too"


class TestTheRefusalIsWrittenOnce:
    def test_every_tool_gives_the_same_advice(self, csv_file: Path) -> None:
        """Identical but for the caller's own name -- one source, not five."""
        shapes = {
            tool: call(csv_file)["hint"].replace(f"{tool}()", "<this tool>()") for tool, call in SHEET_TOOLS.items()
        }
        assert len(set(shapes.values())) == 1, shapes

    def test_it_names_the_tool_the_caller_was_using(self, csv_file: Path) -> None:
        """Shared wording, still specific about where to come back to."""
        for tool, call in SHEET_TOOLS.items():
            assert f"{tool}()" in call(csv_file)["hint"]


class TestTheWordTheHintsUse:
    @pytest.mark.parametrize("spelling", ["xlsx", "xls", "ods", "spreadsheet", "excel", "XLSX"])
    def test_a_workbook_can_be_asked_for_by_name(self, spelling: str, csv_file: Path, tmp_path: Path) -> None:
        out = tmp_path / f"out_{spelling.lower()}.xlsx"
        r = ing.convert_file(str(csv_file), output_format=spelling, output_path=str(out))
        assert r["success"] is True, r.get("error")
        assert out.exists()

    def test_the_hint_a_sheet_tool_gives_actually_works(self, csv_file: Path, tmp_path: Path) -> None:
        """Walk the advice end to end, which is what nobody had done."""
        refusal = ing.detect_tables(str(csv_file))
        assert refusal["success"] is False
        book = tmp_path / "converted.xlsx"
        made = ing.convert_file(str(csv_file), output_format="excel", output_path=str(book))
        assert made["success"] is True, made.get("error")
        second = ing.detect_tables(str(book))
        assert second["success"] is True, second.get("error")

    def test_a_real_typo_is_still_refused(self, csv_file: Path) -> None:
        r = ing.convert_file(str(csv_file), output_format="exel")
        assert r["success"] is False
        assert "exel" in r["error"]

    def test_the_refusal_lists_what_works(self, csv_file: Path) -> None:
        hint = ing.convert_file(str(csv_file), output_format="exel")["hint"]
        for fmt in ("csv", "json", "parquet", "excel"):
            assert fmt in hint, hint
        assert "xlsx" in hint, hint
