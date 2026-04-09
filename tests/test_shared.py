"""Tests for shared/ utilities — 100% coverage required."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from shared.progress import ok, fail, info, warn, undo
from shared.platform_utils import get_max_rows, get_max_columns, get_max_results
from shared.file_utils import resolve_path, atomic_write_text
from shared.version_control import snapshot, restore, list_versions
from shared.patch_validator import validate_ops
from shared.receipt import append_receipt, read_receipt_log


# ---------------------------------------------------------------------------
# progress helpers
# ---------------------------------------------------------------------------


def test_ok_no_detail():
    r = ok("done")
    assert r == {"status": "ok", "message": "done"}


def test_ok_with_detail():
    r = ok("done", "5 rows")
    assert r["detail"] == "5 rows"


def test_fail():
    r = fail("error", "hint")
    assert r["status"] == "fail"
    assert r["detail"] == "hint"


def test_info():
    r = info("note")
    assert r["status"] == "info"


def test_warn():
    r = warn("caution")
    assert r["status"] == "warn"


def test_undo():
    r = undo("reverted")
    assert r["status"] == "undo"


# ---------------------------------------------------------------------------
# platform_utils
# ---------------------------------------------------------------------------


def test_get_max_rows_normal(monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    import importlib
    import shared.platform_utils as pu

    importlib.reload(pu)
    assert pu.get_max_rows() in (20, 100)  # depends on env; just check it's an int


def test_get_max_rows_returns_int():
    assert isinstance(get_max_rows(), int)


def test_get_max_columns_returns_int():
    assert isinstance(get_max_columns(), int)


def test_get_max_results_returns_int():
    assert isinstance(get_max_results(), int)


# ---------------------------------------------------------------------------
# file_utils
# ---------------------------------------------------------------------------


def test_resolve_path_absolute(tmp_path):
    p = tmp_path / "test.csv"
    p.write_text("a,b\n1,2")
    resolved = resolve_path(str(p))
    assert resolved == p


def test_resolve_path_relative(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    p = tmp_path / "rel.csv"
    p.write_text("a")
    resolved = resolve_path("rel.csv")
    assert resolved.is_absolute()


def test_atomic_write_text(tmp_path):
    target = tmp_path / "out.txt"
    atomic_write_text(target, "hello world")
    assert target.read_text() == "hello world"


def test_atomic_write_text_overwrites(tmp_path):
    target = tmp_path / "out.txt"
    target.write_text("old")
    atomic_write_text(target, "new")
    assert target.read_text() == "new"


# ---------------------------------------------------------------------------
# version_control
# ---------------------------------------------------------------------------


def test_snapshot_creates_backup(tmp_path):
    f = tmp_path / "data.csv"
    f.write_text("a,b\n1,2")
    backup = snapshot(str(f))
    assert Path(backup).exists()
    assert ".mcp_versions" in backup


def test_snapshot_content_matches(tmp_path):
    f = tmp_path / "data.csv"
    f.write_text("original content")
    backup = snapshot(str(f))
    assert Path(backup).read_text() == "original content"


def test_restore_overwrites_file(tmp_path):
    f = tmp_path / "data.csv"
    f.write_text("original")
    backup = snapshot(str(f))
    f.write_text("modified")
    restore(str(f), backup)
    assert f.read_text() == "original"


def test_list_versions_empty(tmp_path):
    f = tmp_path / "data.csv"
    f.write_text("x")
    assert list_versions(str(f)) == []


def test_list_versions_after_snapshot(tmp_path):
    f = tmp_path / "data.csv"
    f.write_text("x")
    snapshot(str(f))
    snapshot(str(f))
    versions = list_versions(str(f))
    assert len(versions) == 2


def test_list_versions_newest_first(tmp_path):
    f = tmp_path / "data.csv"
    f.write_text("x")
    _ = snapshot(str(f))
    _ = snapshot(str(f))
    versions = list_versions(str(f))
    assert len(versions) == 2
    # Sorted newest first (lexicographic ISO timestamps with microseconds)
    assert versions[0] >= versions[1]


# ---------------------------------------------------------------------------
# patch_validator
# ---------------------------------------------------------------------------


def test_validate_ops_empty():
    errors = validate_ops([])
    assert errors


def test_validate_ops_unknown_op():
    errors = validate_ops([{"op": "explode_everything"}])
    assert any("unknown op" in e for e in errors)


def test_validate_ops_missing_op_key():
    errors = validate_ops([{"not_op": "fill_nulls"}])
    assert errors


def test_validate_ops_fill_nulls_missing_strategy():
    errors = validate_ops([{"op": "fill_nulls", "column": "x", "strategy": "invalid"}])
    assert errors


def test_validate_ops_valid_fill_nulls():
    errors = validate_ops([{"op": "fill_nulls", "column": "x", "strategy": "median"}])
    assert not errors


def test_validate_ops_valid_drop_column():
    errors = validate_ops([{"op": "drop_column", "columns": ["col1"]}])
    assert not errors


def test_validate_ops_drop_column_missing_columns():
    errors = validate_ops([{"op": "drop_column"}])
    assert errors


def test_validate_ops_cast_column_invalid_dtype():
    errors = validate_ops([{"op": "cast_column", "column": "x", "dtype": "blob"}])
    assert errors


def test_validate_ops_add_column_math_missing_expr():
    errors = validate_ops([{"op": "add_column", "name": "new", "mode": "math"}])
    assert errors


def test_validate_ops_multiple_errors():
    errors = validate_ops(
        [
            {"op": "fill_nulls", "column": "x", "strategy": "bad"},
            {"op": "unknown_op"},
        ]
    )
    assert len(errors) == 2


def test_validate_ops_valid_drop_duplicates():
    errors = validate_ops([{"op": "drop_duplicates"}])
    assert not errors


# ---------------------------------------------------------------------------
# receipt
# ---------------------------------------------------------------------------


def test_append_and_read_receipt(tmp_path):
    f = tmp_path / "data.csv"
    f.write_text("a,b\n1,2")
    append_receipt(str(f), tool="apply_patch", args={"ops": []}, result="ok")
    entries = read_receipt_log(str(f), last_n=10)
    assert len(entries) == 1
    assert entries[0]["tool"] == "apply_patch"


def test_receipt_descending_order(tmp_path):
    f = tmp_path / "data.csv"
    f.write_text("a")
    append_receipt(str(f), "tool_a", {}, "first")
    append_receipt(str(f), "tool_b", {}, "second")
    entries = read_receipt_log(str(f), last_n=10)
    assert entries[0]["tool"] == "tool_b"
    assert entries[1]["tool"] == "tool_a"


def test_receipt_last_n(tmp_path):
    f = tmp_path / "data.csv"
    f.write_text("a")
    for i in range(5):
        append_receipt(str(f), f"tool_{i}", {}, str(i))
    entries = read_receipt_log(str(f), last_n=3)
    assert len(entries) == 3


def test_receipt_no_file(tmp_path):
    f = tmp_path / "no_receipt.csv"
    f.write_text("a")
    entries = read_receipt_log(str(f))
    assert entries == []


def test_append_receipt_never_raises(tmp_path):
    # Even with a bad path, should not raise
    append_receipt("/nonexistent/path/data.csv", "tool", {}, "result")
