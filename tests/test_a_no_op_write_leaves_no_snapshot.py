"""A call that changed nothing kept a full copy of the file anyway.

The snapshot has to be taken before the write -- nothing knows yet whether the
write will change anything. What was missing was the other half: throwing it
away when the answer turns out to be "nothing".

Round 11 called every tool twice with identical arguments and measured the
disk. On a 1.9 MB CSV:

    apply_patch  regex_replace Desktop -> D3SKTOP   changed 6,318 rows
    apply_patch  same call again        changed 0 rows, 1,938,840 B .bak
    restore_version to a timestamp the file already equals
                                        another full-size .bak

Four calls, ~7.5 MB in .mcp_versions, two of the four snapshots byte-identical
to the live file. A client retrying on timeout grows that without limit, and no
repo in the fleet prunes .mcp_versions.

Comparing after the write rather than guessing before it is what makes this
safe: a backup byte-identical to the file now on disk cannot restore anything
the file does not already hold.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_basic")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from servers.data_basic.engine import apply_patch, restore_version  # noqa: E402
from shared.version_control import discard_snapshot_if_unchanged  # noqa: E402


@pytest.fixture
def csv(tmp_path) -> Path:
    p = tmp_path / "d.csv"
    p.write_text("kind,v\nDesktop,1\nMobile,2\nDesktop,3\n", encoding="utf-8")
    return p


def snapshots(csv: Path) -> list[Path]:
    d = csv.parent / ".mcp_versions"
    return sorted(d.glob("*.bak")) if d.is_dir() else []


REPLACE = [{"op": "regex_replace", "column": "kind", "pattern": "Desktop", "replacement": "D3SKTOP"}]


class TestARealChangeStillSnapshots:
    def test_the_first_patch_keeps_its_backup(self, csv):
        r = apply_patch(str(csv), REPLACE)
        assert r["success"] is True, r.get("error")
        assert r["backup"], r
        assert len(snapshots(csv)) == 1

    def test_the_backup_holds_the_previous_content(self, csv):
        r = apply_patch(str(csv), REPLACE)
        assert "Desktop" in Path(r["backup"]).read_text(encoding="utf-8")

    def test_it_reports_that_the_file_changed(self, csv):
        r = apply_patch(str(csv), REPLACE)
        assert r["changed_file"] is True


class TestARetryThatChangesNothingKeepsNothing:
    def test_the_second_patch_leaves_one_snapshot(self, csv):
        apply_patch(str(csv), REPLACE)
        apply_patch(str(csv), REPLACE)
        assert len(snapshots(csv)) == 1, [p.name for p in snapshots(csv)]

    def test_the_second_patch_reports_no_backup(self, csv):
        apply_patch(str(csv), REPLACE)
        r = apply_patch(str(csv), REPLACE)
        assert r["success"] is True, r.get("error")
        assert not r["backup"], r["backup"]
        assert r["changed_file"] is False

    def test_the_first_backup_is_the_one_that_survives(self, csv):
        first = apply_patch(str(csv), REPLACE)["backup"]
        apply_patch(str(csv), REPLACE)
        assert [p.name for p in snapshots(csv)] == [Path(first).name]

    def test_the_data_is_still_correct(self, csv):
        apply_patch(str(csv), REPLACE)
        apply_patch(str(csv), REPLACE)
        text = csv.read_text(encoding="utf-8")
        assert text.count("D3SKTOP") == 2
        assert "Desktop" not in text


class TestARestoreToTheCurrentStateKeepsNothing:
    def test_a_redundant_restore_adds_no_snapshot(self, csv):
        # Both restores name the same timestamp, which is the shape the sweep
        # measured. With no timestamp, restore_version takes the *newest*
        # snapshot -- and after one restore that is the counter-snapshot the
        # restore itself just made, so a second call really does change the
        # file and really should keep its backup.
        backup = apply_patch(str(csv), REPLACE)["backup"]
        stamp = Path(backup).name.split("_", 1)[1].rsplit(".csv", 1)[0]

        first = restore_version(str(csv), timestamp=stamp)
        assert first["success"] is True, first.get("error")
        after_first = len(snapshots(csv))

        second = restore_version(str(csv), timestamp=stamp)
        assert second["success"] is True, second.get("error")
        assert len(snapshots(csv)) == after_first, "a redundant restore kept a counter-snapshot"

    def test_the_restore_still_works(self, csv):
        apply_patch(str(csv), REPLACE)
        r = restore_version(str(csv))
        assert r["success"] is True, r.get("error")
        assert "Desktop" in csv.read_text(encoding="utf-8")


class TestTheHelperIsExact:
    def test_it_keeps_a_backup_that_differs(self, tmp_path):
        live = tmp_path / "live.txt"
        back = tmp_path / "back.bak"
        live.write_bytes(b"new")
        back.write_bytes(b"old")
        assert discard_snapshot_if_unchanged(str(back), live) == str(back)
        assert back.exists()

    def test_it_drops_a_backup_that_matches(self, tmp_path):
        live = tmp_path / "live.txt"
        back = tmp_path / "back.bak"
        live.write_bytes(b"same")
        back.write_bytes(b"same")
        assert discard_snapshot_if_unchanged(str(back), live) == ""
        assert not back.exists()

    def test_a_same_size_difference_is_still_a_difference(self, tmp_path):
        live = tmp_path / "live.txt"
        back = tmp_path / "back.bak"
        live.write_bytes(b"abc")
        back.write_bytes(b"abd")
        assert discard_snapshot_if_unchanged(str(back), live) == str(back)
        assert back.exists()

    def test_an_empty_backup_path_is_passed_through(self, tmp_path):
        assert discard_snapshot_if_unchanged("", tmp_path / "nope.txt") == ""

    def test_a_missing_backup_is_not_an_error(self, tmp_path):
        live = tmp_path / "live.txt"
        live.write_bytes(b"x")
        gone = str(tmp_path / "gone.bak")
        assert discard_snapshot_if_unchanged(gone, live) == gone
