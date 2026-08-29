"""A failed op kept a snapshot of a file it had not touched, then offered it.

    apply_patch(ops=[{"op": "log_transform", "column": "name"}])   # name is text
      -> "applied":  0
         "error":    "1 op(s) failed"
         "backup":   ".mcp_versions/d_2026-08-29T11-33-22-109829Z.csv.bak"
         "hint":     "Fix failing ops and retry. Call restore_version() if you
                      want to reset to the snapshot."

The code three lines above that return says, in a comment, "Do NOT write the
modified df -- leave the original intact", and it is right: `applied` is 0 and
the CSV is byte-identical afterwards. So the snapshot is a copy of a file that
never changed, and the hint invites the caller to roll a file back to undo a
write that did not happen -- the advice round 18 spent a whole round removing
from the Office fleet, still live here.

`discard_snapshot_if_unchanged` has been in this repo since round 11 and is
already wired into the paths that SUCCEED without changing anything. The paths
that FAIL never got it. That is the same shape as the Office defect found in
round 19b, where the equivalent helper had no production caller at all: a fix
that stops at one sibling is half a fix.

The content check is what makes this safe to apply from any failure branch. It
compares the backup byte-for-byte against the file as it stands now, so a write
that got half way through leaves the two different, keeps its snapshot, and the
snapshot is then the only good copy of the original.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_basic")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from servers.data_basic.engine import apply_patch  # noqa: E402
from shared.version_control import drop_snapshot_if_unwritten, snapshot  # noqa: E402


@pytest.fixture
def csv(tmp_path) -> Path:
    p = tmp_path / "d.csv"
    p.write_text("name,qty\na,1\nb,2\n", encoding="utf-8")
    return p


def baks(csv: Path) -> list[Path]:
    d = csv.parent / ".mcp_versions"
    return sorted(d.glob("*.bak")) if d.exists() else []


# An op that passes schema validation and then fails while running: a log
# transform of a text column. The pre-write validation branches never reach
# the snapshot, so this is the one that actually exercises it.
BAD_OP = [{"op": "log_transform", "column": "name", "method": "log10"}]
GOOD_OP = [{"op": "round_values", "column": "qty", "decimals": 0}]


class TestAFailedOpLeavesNothingBehind:
    def test_it_still_fails(self, csv: Path):
        r = apply_patch(str(csv), BAD_OP)
        assert r["success"] is False
        assert r["applied"] == 0

    def test_the_file_is_untouched(self, csv: Path):
        before = csv.read_bytes()
        apply_patch(str(csv), BAD_OP)
        assert csv.read_bytes() == before

    def test_no_snapshot_is_left_on_disk(self, csv: Path):
        apply_patch(str(csv), BAD_OP)
        assert baks(csv) == []

    def test_the_response_does_not_advertise_one(self, csv: Path):
        assert not apply_patch(str(csv), BAD_OP)["backup"]

    def test_the_hint_does_not_offer_to_restore(self, csv: Path):
        assert "restore_version" not in apply_patch(str(csv), BAD_OP)["hint"]

    def test_the_hint_says_nothing_was_written(self, csv: Path):
        assert "Nothing was written" in apply_patch(str(csv), BAD_OP)["hint"]

    def test_the_progress_log_stops_claiming_a_snapshot(self, csv: Path):
        msgs = [str(e.get("message", "")) for e in apply_patch(str(csv), BAD_OP)["progress"]]
        assert not any(m == "Snapshot created" for m in msgs), msgs

    def test_repeated_failures_do_not_accumulate(self, csv: Path):
        for _ in range(3):
            apply_patch(str(csv), BAD_OP)
        assert baks(csv) == []


class TestARealWriteKeepsItsSnapshot:
    """The must-not-overreach direction. Discarding too eagerly loses the
    only copy of the original, which is far worse than keeping a spare."""

    def test_a_successful_change_still_snapshots(self, csv: Path):
        csv.write_text("name,qty\na,1.4\nb,2.6\n", encoding="utf-8")
        r = apply_patch(str(csv), GOOD_OP)
        assert r["success"] is True, r.get("error")
        assert csv.read_text() != "name,qty\na,1.4\nb,2.6\n", "nothing actually changed"
        assert len(baks(csv)) == 1

    def test_the_snapshot_holds_the_original(self, csv: Path):
        original = "name,qty\na,1.4\nb,2.6\n"
        csv.write_text(original, encoding="utf-8")
        apply_patch(str(csv), GOOD_OP)
        assert baks(csv)[0].read_text() == original


class TestTheHelperItself:
    def test_it_drops_a_snapshot_of_an_unchanged_file(self, csv: Path):
        bak = snapshot(str(csv))
        assert drop_snapshot_if_unwritten(bak, csv) == ""
        assert not Path(bak).exists()

    def test_it_keeps_a_snapshot_whose_file_moved_on(self, csv: Path):
        bak = snapshot(str(csv))
        csv.write_text("name,qty\nchanged,9\n", encoding="utf-8")
        assert drop_snapshot_if_unwritten(bak, csv) == bak
        assert Path(bak).exists()

    def test_it_corrects_the_progress_entry_it_invalidates(self, csv: Path):
        bak = snapshot(str(csv))
        progress = [{"status": "info", "message": "Snapshot created", "detail": Path(bak).name}]
        drop_snapshot_if_unwritten(bak, csv, progress)
        assert progress[0]["message"] != "Snapshot created"
        assert "discarded" in progress[0]["message"].lower()

    def test_it_leaves_the_log_alone_when_the_snapshot_stays(self, csv: Path):
        bak = snapshot(str(csv))
        csv.write_text("name,qty\nchanged,9\n", encoding="utf-8")
        progress = [{"status": "info", "message": "Snapshot created", "detail": "x"}]
        drop_snapshot_if_unwritten(bak, csv, progress)
        assert progress[0]["message"] == "Snapshot created"

    def test_an_empty_backup_is_a_no_op(self, csv: Path):
        assert drop_snapshot_if_unwritten("", csv) == ""
