"""Twenty operations, two receipt entries, and nothing said why.

A user review drove roughly twenty calls against one dataset -- loads,
inspections, correlations, aggregations, a filter, a model comparison -- then
read the receipt and found two entries. The log was not broken:
`append_receipt` is called by tools that WRITE, and reads write nothing.

The defect was that a file named `.mcp_receipt.json` invites exactly one
reading, and an agent trusting it as an audit log concludes eighteen operations
vanished. The review's verdict -- "agent cannot trust receipt as audit log
today" -- is the correct conclusion from a log that silently holds a subset.

Two things changed. The scope is declared, in the file and in the response, so
the count can be understood instead of doubted. And an entry now carries what
makes it lineage rather than a note: a hash of the arguments and a fingerprint
of the file it produced.

Version 1 receipts are bare lists. They are still read exactly as written --
an existing receipt must not become unreadable because the format grew a
header.
"""

from __future__ import annotations

import json

import pytest

from shared.receipt import RECEIPT_SCOPE, append_receipt, fingerprint, read_receipt, read_receipt_log


@pytest.fixture()
def dataset(tmp_path):
    f = tmp_path / "Credit_Risk.csv"
    f.write_text("loan_status,amount\nCharged Off,2500\nFully Paid,1000\n")
    return f


# --------------------------------------------------------------------------
# the scope, which is the actual finding
# --------------------------------------------------------------------------


def test_the_scope_is_stated_even_when_there_is_no_receipt_yet(dataset):
    entries, scope = read_receipt(str(dataset))
    assert entries == []
    assert "mutations only" in scope


def test_the_scope_says_what_is_missing_not_just_what_is_there(dataset):
    append_receipt(str(dataset), tool="filter_dataset", args={"op": "equals"}, result="1 row")
    _entries, scope = read_receipt(str(dataset))
    for word in ("Reads", "correlations", "not recorded"):
        assert word in scope


def test_the_scope_is_written_into_the_file_itself(dataset):
    """A reader who never calls the tool still learns what the log holds."""
    append_receipt(str(dataset), tool="filter_dataset", args={}, result="ok")
    written = json.loads((dataset.parent / (dataset.name + ".mcp_receipt.json")).read_text())
    assert written[0]["_scope"] == RECEIPT_SCOPE


# --------------------------------------------------------------------------
# lineage
# --------------------------------------------------------------------------


def test_an_entry_identifies_what_it_produced(dataset):
    append_receipt(str(dataset), tool="filter_dataset", args={"op": "equals"}, result="1 row")
    entry = read_receipt_log(str(dataset))[0]
    assert entry["output"].startswith("sha256:")
    assert entry["args_hash"].startswith("sha256:")


def test_two_different_calls_are_distinguishable(dataset):
    append_receipt(str(dataset), tool="filter_dataset", args={"op": "equals"}, result="1 row")
    append_receipt(str(dataset), tool="filter_dataset", args={"op": "gt"}, result="2 rows")
    a, b = read_receipt_log(str(dataset))
    assert a["args_hash"] != b["args_hash"]


def test_the_before_and_after_are_both_recordable(dataset):
    before = fingerprint(dataset)
    dataset.write_text("loan_status,amount\nCharged Off,2500\n")
    append_receipt(str(dataset), tool="filter_dataset", args={}, result="1 row", input_fingerprint=before)
    entry = read_receipt_log(str(dataset))[0]
    assert entry["input"] == before
    assert entry["output"] != before


def test_duration_is_kept_when_given(dataset):
    append_receipt(str(dataset), tool="filter_dataset", args={}, result="ok", duration_ms=12.34)
    assert read_receipt_log(str(dataset))[0]["duration_ms"] == 12.3


def test_a_fingerprint_says_which_kind_it_is(tmp_path, monkeypatch):
    """A cheap stand-in must never be mistaken for a content hash."""
    import shared.receipt as receipt_mod

    big = tmp_path / "big.csv"
    big.write_text("x" * 5000)
    monkeypatch.setattr(receipt_mod, "_MAX_HASH_BYTES", 100)
    assert fingerprint(big).startswith("size-mtime:")

    monkeypatch.setattr(receipt_mod, "_MAX_HASH_BYTES", 10_000)
    assert fingerprint(big).startswith("sha256:")


def test_a_missing_file_fingerprints_to_nothing_rather_than_raising(tmp_path):
    assert fingerprint(tmp_path / "gone.csv") == ""


# --------------------------------------------------------------------------
# the old format
# --------------------------------------------------------------------------


def test_a_version_1_receipt_is_still_readable(dataset):
    """A bare list, as written before there was a header."""
    legacy = [
        {"ts": "2026-09-01T00-00-00Z", "tool": "filter_rows", "args": {}, "result": "old", "backup": ""},
        {"ts": "2026-09-02T00-00-00Z", "tool": "apply_patch", "args": {}, "result": "older", "backup": ""},
    ]
    (dataset.parent / (dataset.name + ".mcp_receipt.json")).write_text(json.dumps(legacy))

    entries, scope = read_receipt(str(dataset))
    assert [e["tool"] for e in entries] == ["apply_patch", "filter_rows"]  # newest first
    assert "mutations only" in scope


def test_appending_to_a_version_1_receipt_keeps_its_entries(dataset):
    legacy = [{"ts": "2026-09-01T00-00-00Z", "tool": "filter_rows", "args": {}, "result": "old", "backup": ""}]
    (dataset.parent / (dataset.name + ".mcp_receipt.json")).write_text(json.dumps(legacy))

    append_receipt(str(dataset), tool="apply_patch", args={}, result="new")
    entries, _ = read_receipt(str(dataset))
    assert [e["tool"] for e in entries] == ["apply_patch", "filter_rows"]


def test_a_corrupt_receipt_does_not_take_the_tool_down_with_it(dataset):
    (dataset.parent / (dataset.name + ".mcp_receipt.json")).write_text("{not json")
    assert read_receipt(str(dataset)) == ([], RECEIPT_SCOPE)
    append_receipt(str(dataset), tool="apply_patch", args={}, result="ok")
    assert len(read_receipt_log(str(dataset))) == 1


def test_the_tool_response_carries_the_scope(dataset):
    """The count is only readable next to what it counts."""
    from servers.data_basic.engine import read_receipt as read_receipt_tool

    append_receipt(str(dataset), tool="filter_dataset", args={}, result="ok")
    result = read_receipt_tool(str(dataset))
    assert result["success"] is True
    assert "mutations only" in result["scope"]
    assert result["total_entries"] == 1
