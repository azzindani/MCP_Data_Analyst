"""Four servers write one file. They have to agree what is in it.

Every repo in this fleet writes `{filename}.mcp_receipt.json` beside the file
it touched, and they touch the same files: the shared output directory means a
CSV written by MCP_Data_Analyst gets trained on by MCP_Machine_Learning and
renamed by MCP_File_System, all three appending to one log.

An earlier fix made them agree on the *name*. It did not make them agree on the
contents, and the divergence was worse than the naming had been because now
each one found the other's file and could not read it:

    MCP_Data_Analyst        JSON array
    MCP_Machine_Learning    JSON array
    MCP_File_System         JSON array
    MCP_Microsoft_Office    JSON object: {"file": ..., "entries": [...]}
    MCP_Web_Browser         JSON array, and no reader at all

Then Data_Analyst added a scope header at index 0 -- the fix for a user review
that read two entries after twenty calls and concluded eighteen operations had
vanished -- and the arrays stopped agreeing with each other too. Reproduced
before any of this was repaired, DA writing and ML reading:

    ML reads 2 entries:
      {'ts': '...', 'tool': 'filter_dataset'}
      {'_scope': 'mutations only: ...', '_format': 2}

One real operation, read as two, the second with no `tool`. An agent auditing
that log sees an operation that never happened.

So `_split_header` now accepts all three shapes everywhere, and everything
writes the headed array. The tests below check this repo's reader against each
shape -- which runs on a GitHub runner -- and then, on the box where the repos
actually live, that a receipt written by one is read the same way by all of
them.
"""

from __future__ import annotations

import json
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.receipt import RECEIPT_SCOPE, append_receipt, read_receipt  # noqa: E402

SIBLINGS = {
    "MCP_Machine_Learning": "shared/receipt.py",
    "MCP_File_System": "shared/receipt.py",
    "MCP_Microsoft_Office": "shared/shared/receipt.py",
    "MCP_Web_Browser": "shared/receipt.py",
}


# ---------------------------------------------------------------------------
# the three shapes, read here -- runs anywhere
# ---------------------------------------------------------------------------


@pytest.fixture
def target(tmp_path):
    f = tmp_path / "data.csv"
    f.write_text("a,b\n1,2\n", encoding="utf-8")
    return f


def _receipt_for(p: pathlib.Path) -> pathlib.Path:
    return p.parent / (p.name + ".mcp_receipt.json")


def test_a_headed_array_reads(target):
    """The current format."""
    append_receipt(str(target), "filter_dataset", {"op": "=="}, "wrote 1 row")
    entries, scope = read_receipt(str(target))
    assert [e["tool"] for e in entries] == ["filter_dataset"]
    assert scope == RECEIPT_SCOPE


def test_a_bare_array_reads(target):
    """What every repo wrote before the header existed."""
    _receipt_for(target).write_text(
        json.dumps([{"ts": "2026-01-01T00-00-00Z", "tool": "train_classifier", "result": "ok"}]),
        encoding="utf-8",
    )
    entries, scope = read_receipt(str(target))
    assert [e["tool"] for e in entries] == ["train_classifier"]
    assert scope == RECEIPT_SCOPE, "a v1 file has no header, so the default scope applies"


def test_the_office_object_reads(target):
    """What MCP_Microsoft_Office wrote. Every reader returned [] for it."""
    _receipt_for(target).write_text(
        json.dumps({"file": "data.csv", "entries": [{"ts": "x", "tool": "set_font", "result": "ok"}]}),
        encoding="utf-8",
    )
    entries, _ = read_receipt(str(target))
    assert [e["tool"] for e in entries] == ["set_font"]


def test_appending_to_a_v1_file_keeps_what_was_there(target):
    """Upgrading the format must not cost the history it was written to keep."""
    _receipt_for(target).write_text(
        json.dumps([{"ts": "2026-01-01T00-00-00Z", "tool": "old_tool", "result": "ok"}]),
        encoding="utf-8",
    )
    append_receipt(str(target), "filter_dataset", {}, "ok")
    entries, _ = read_receipt(str(target))
    # This repo reads newest-first.
    assert [e["tool"] for e in entries] == ["filter_dataset", "old_tool"]


def test_the_header_is_never_returned_as_an_entry(target):
    """The exact defect: a header counted as an operation that never happened."""
    append_receipt(str(target), "filter_dataset", {}, "ok")
    entries, _ = read_receipt(str(target))
    assert all("tool" in e for e in entries), entries
    assert not any("_scope" in e for e in entries), entries


# ---------------------------------------------------------------------------
# the fleet, on the box where it lives
# ---------------------------------------------------------------------------


def _present() -> dict[str, pathlib.Path]:
    out = {}
    for name, rel in SIBLINGS.items():
        p = pathlib.Path("/root") / name / rel
        if p.exists():
            out[name] = p
    return out


def test_every_sibling_understands_the_header():
    """Static, because importing five sibling packages here is not worth it."""
    present = _present()
    if not present:
        pytest.skip("sibling repos not present in this checkout")
    missing = [n for n, p in present.items() if "_split_header" not in p.read_text(encoding="utf-8")]
    assert not missing, (
        f"these still parse the receipt as a bare list: {missing}. "
        "They will report the scope header as an operation that never ran."
    )


def test_every_sibling_agrees_on_the_scope_sentence():
    """A caller must not be able to tell which server wrote the scope."""
    present = _present()
    if not present:
        pytest.skip("sibling repos not present in this checkout")
    wrong = []
    for name, p in present.items():
        if "mutations only: operations that wrote to this file." not in p.read_text(encoding="utf-8"):
            wrong.append(name)
    assert not wrong, f"these declare a different scope sentence: {wrong}"


def test_every_sibling_ships_a_reader():
    """MCP_Web_Browser wrote receipts and offered no supported way to read one."""
    present = _present()
    if not present:
        pytest.skip("sibling repos not present in this checkout")
    missing = [n for n, p in present.items() if "def read_receipt" not in p.read_text(encoding="utf-8")]
    assert not missing, f"these write a log nothing can read back: {missing}"
