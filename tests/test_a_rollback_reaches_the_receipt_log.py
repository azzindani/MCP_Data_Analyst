"""A restore replaces the whole dataset, so it belongs in the audit trail.

read_receipt reads one per-file log. apply_patch wrote to it; restore_version
did not — even though it takes a counter-snapshot first, so it plainly knows it
is making a change worth being able to undo. A reader asking read_receipt "what
happened to this file?" saw every patch and no sign that any of them had since
been rolled back: the log described a dataset that no longer existed.

Found by asking which functions take a snapshot but never write a receipt —
39 of 93 across the four repos, of which the ones worth fixing are the ones a
receipt *reader* exists to expose.
"""

from __future__ import annotations

import pytest

from servers.data_basic import engine


def logged(path) -> list[str]:
    # Through read_receipt_log(), which is what the read_receipt tool calls:
    # what matters is not that a line reached a file but that the reader shows it.
    from shared.receipt import read_receipt_log

    return [e.get("tool") for e in read_receipt_log(str(path), 50)]


@pytest.fixture
def dataset(tmp_path):
    p = tmp_path / "d.csv"
    p.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
    r = engine.apply_patch(str(p), [{"op": "round_values", "column": "b", "decimals": 0}])
    assert r["success"] is True, r.get("error")
    assert logged(p) == ["apply_patch"], "fixture needs a patch in the log to roll back"
    return p


class TestARestoreIsLogged:
    # Two things this tool does that are easy to get wrong from the outside:
    # an empty timestamp restores the most recent snapshot rather than listing
    # (ML's namesake lists), and read_receipt_log returns newest first.

    def test_rolling_back_leaves_a_trace(self, dataset):
        r = engine.restore_version(str(dataset), "")
        assert r["success"] is True, r.get("error")

        entries = logged(dataset)
        assert entries[0] == "restore_version", entries
        # A restore adds to the history, it does not replace it.
        assert "apply_patch" in entries, entries

    def test_restoring_a_named_snapshot_is_logged_too(self, dataset):
        listed = engine.restore_version(str(dataset), "")["available_versions"]
        assert listed, "fixture needs a snapshot to name"
        r = engine.restore_version(str(dataset), listed[0])
        assert r["success"] is True, r.get("error")
        assert logged(dataset).count("restore_version") == 2

    def test_the_counter_snapshot_is_recorded_as_the_backup(self, dataset):
        from shared.receipt import read_receipt_log

        engine.restore_version(str(dataset), "")
        entry = next(e for e in read_receipt_log(str(dataset), 50) if e.get("tool") == "restore_version")
        # Without this the restore is the one write whose own undo point
        # cannot be recovered from the log.
        assert entry.get("backup"), entry
