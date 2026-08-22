"""Three ingest tools took only CSV and were the only three not to say so.

A sweep working through data-ingest reached normalize_headers holding the .xlsx
every earlier tool in the server had accepted, and got:

    error: "Expected .csv, got '.xlsx'"
    hint : "Use extract_sheet() or convert_file() to produce a CSV first."

The hint is good and the sweep recovered in one call. The docstring is what
sent it there. In a server where every other tool names its formats --
list_sheets "in xlsx/ods", flatten_merged_cells "in xlsx sheet",
convert_file "xlsx/ods/csv/json/parquet" -- these three named none:

    "Strip whitespace, lowercase, dedup headers. output_path: write elsewhere."
    "Drop empty leading/trailing rows and cols. output_path: write elsewhere."
    "Make row N the header; drop rows above. output_path: write elsewhere."

and they are exactly the three that raise "Expected .csv". The live schemas
carry no parameter descriptions and no enums, so the 80-character docstring is
the whole contract a caller sees. Silence read as "any format the rest take".
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

SERVER = Path(__file__).parent.parent / "servers" / "data_ingest" / "server.py"
ENGINE = Path(__file__).parent.parent / "servers" / "data_ingest" / "engine.py"

CSV_ONLY = ["normalize_headers", "trim_empty", "promote_header"]


def tool_docstrings() -> dict[str, str]:
    """Docstring of every @mcp.tool() in the ingest server, read from the AST."""
    tree = ast.parse(SERVER.read_text(encoding="utf-8"))
    out: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and any(
            isinstance(d, ast.Call)
            and getattr(getattr(d.func, "value", None), "id", "") == "mcp"
            or getattr(d, "attr", "") == "tool"
            for d in node.decorator_list
        ):
            doc = ast.get_docstring(node)
            if doc:
                out[node.name] = doc
    return out


class TestTheThreeCsvOnlyToolsSaySo:
    @pytest.mark.parametrize("name", CSV_ONLY)
    def test_the_docstring_names_csv(self, name: str):
        doc = tool_docstrings()[name]
        assert "CSV" in doc or "csv" in doc, doc

    @pytest.mark.parametrize("name", CSV_ONLY)
    def test_it_is_still_within_the_eighty_char_limit(self, name: str):
        assert len(tool_docstrings()[name]) <= 80, tool_docstrings()[name]

    @pytest.mark.parametrize("name", CSV_ONLY)
    def test_the_output_path_note_survived(self, name: str):
        assert "output_path" in tool_docstrings()[name]


class TestTheRestStillNameTheirs:
    @pytest.mark.parametrize(
        ("name", "word"),
        [
            ("list_sheets", "xlsx"),
            ("flatten_merged_cells", "xlsx"),
            ("convert_file", "xlsx"),
        ],
    )
    def test_the_format_is_named(self, name: str, word: str):
        assert word in tool_docstrings()[name].lower(), tool_docstrings()[name]


class TestTheSetStaysInSync:
    def test_every_tool_that_rejects_non_csv_is_covered(self):
        """If a fourth tool starts raising "Expected .csv", it must say so too."""
        source = ENGINE.read_text(encoding="utf-8")
        tree = ast.parse(source)
        rejecting = {
            node.name
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and "Expected .csv" in ast.unparse(node)
        }
        assert rejecting == set(CSV_ONLY), rejecting

    def test_none_of_the_others_reject_csv_silently(self):
        docs = tool_docstrings()
        for name in CSV_ONLY:
            assert name in docs, name
