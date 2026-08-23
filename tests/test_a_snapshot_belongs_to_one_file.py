"""One file's snapshots must not be offered as another file's history.

Snapshots were named `{stem}_{timestamp}.bak`, with the extension dropped, and
looked up with `glob(f"{stem}_*.bak")`. Two consequences, both reachable from
the deployed endpoints with ordinary filenames:

  * `report.csv` and `report.docx` in one directory share a history. Calling
    restore_version on the CSV with no timestamp restored the newest snapshot
    under that stem -- the Word document -- and answered success: true with
    "Call inspect_dataset() to confirm the restored state". A 12-byte CSV came
    back as 37,117 bytes of .docx.
  * `Ad_Data_test.csv`'s snapshots answered a query about `Ad_Data.csv`,
    because `Ad_Data_*` matches both.

File_System already wrote `{stem}_{ts}{ext}.bak` and had recorded the
divergence from its three siblings in a comment; this is the sibling half of
that fix. Reading stays deliberately more forgiving than writing -- a snapshot
taken before this change is still listed and still restorable -- but only where
the old name cannot be ambiguous.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.version_control import list_versions, snapshot  # noqa: E402


class TestTheExtensionIsPartOfTheName:
    def test_two_namesakes_do_not_share_a_history(self, tmp_path):
        csv = tmp_path / "report.csv"
        csv.write_text("a,b\n1,2\n", encoding="utf-8")
        docx = tmp_path / "report.docx"
        docx.write_bytes(b"PK\x03\x04" + b"x" * 400)

        snapshot(str(csv))
        snapshot(str(docx))

        csv_versions = list_versions(str(csv))
        docx_versions = list_versions(str(docx))
        assert len(csv_versions) == 1, csv_versions
        assert len(docx_versions) == 1, docx_versions
        assert not set(csv_versions) & set(docx_versions)

    def test_the_newest_snapshot_of_a_csv_is_a_csv(self, tmp_path):
        # The failure that mattered: restore_version with no timestamp takes
        # versions[0], and the newest snapshot under the shared stem was the
        # other file's.
        csv = tmp_path / "report.csv"
        csv.write_text("a,b\n1,2\n", encoding="utf-8")
        snapshot(str(csv))
        time.sleep(0.01)
        docx = tmp_path / "report.docx"
        docx.write_bytes(b"PK\x03\x04" + b"x" * 400)
        snapshot(str(docx))

        newest = list_versions(str(csv))[0]
        restored = (tmp_path / ".mcp_versions" / newest).read_bytes()
        assert not restored.startswith(b"PK"), f"{newest} is the .docx"
        assert restored == csv.read_bytes()

    def test_a_longer_name_is_not_a_version_of_a_shorter_one(self, tmp_path):
        base = tmp_path / "Ad_Data.csv"
        base.write_text("a\n1\n", encoding="utf-8")
        other = tmp_path / "Ad_Data_test.csv"
        other.write_text("a\n2\n", encoding="utf-8")
        snapshot(str(other))
        assert list_versions(str(base)) == []
        assert len(list_versions(str(other))) == 1


class TestOlderSnapshotsAreStillReachable:
    def test_a_legacy_name_is_listed_when_nothing_shares_the_stem(self, tmp_path):
        csv = tmp_path / "solo.csv"
        csv.write_text("a\n1\n", encoding="utf-8")
        versions = tmp_path / ".mcp_versions"
        versions.mkdir()
        legacy = versions / "solo_2026-08-01T00-00-00-000000Z.bak"
        legacy.write_text("a\n0\n", encoding="utf-8")
        assert list_versions(str(csv)) == [legacy.name]

    def test_a_legacy_name_is_withheld_when_a_namesake_exists(self, tmp_path):
        csv = tmp_path / "shared.csv"
        csv.write_text("a\n1\n", encoding="utf-8")
        (tmp_path / "shared.docx").write_bytes(b"PK\x03\x04")
        versions = tmp_path / ".mcp_versions"
        versions.mkdir()
        (versions / "shared_2026-08-01T00-00-00-000000Z.bak").write_text("?", encoding="utf-8")
        # Ambiguous: it could be either file's. Better to show nothing than to
        # restore a Word document over a dataset.
        assert list_versions(str(csv)) == []

    def test_a_new_snapshot_is_still_found_beside_a_legacy_one(self, tmp_path):
        csv = tmp_path / "solo.csv"
        csv.write_text("a\n1\n", encoding="utf-8")
        versions = tmp_path / ".mcp_versions"
        versions.mkdir()
        (versions / "solo_2026-08-01T00-00-00-000000Z.bak").write_text("old", encoding="utf-8")
        snapshot(str(csv))
        assert len(list_versions(str(csv))) == 2


class TestSnapshotsStillWork:
    def test_a_snapshot_round_trips(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("original\n", encoding="utf-8")
        backup = snapshot(str(f))
        assert Path(backup).read_text(encoding="utf-8") == "original\n"
        assert Path(backup).name.endswith(".csv.bak")

    def test_two_snapshots_in_the_same_microsecond_do_not_collide(self, tmp_path, monkeypatch):
        f = tmp_path / "data.csv"
        f.write_text("x\n", encoding="utf-8")
        import shared.version_control as vc

        class FrozenClock:
            @staticmethod
            def now(tz=None):
                import datetime as _dt

                return _dt.datetime(2026, 8, 1, tzinfo=_dt.UTC)

        monkeypatch.setattr(vc, "datetime", FrozenClock)
        first = snapshot(str(f))
        second = snapshot(str(f))
        assert first != second
        assert Path(second).name.endswith(".csv.bak")

    def test_a_file_with_no_extension_still_snapshots(self, tmp_path):
        f = tmp_path / "README"
        f.write_text("x\n", encoding="utf-8")
        backup = snapshot(str(f))
        assert Path(backup).exists()
        assert list_versions(str(f)) == [Path(backup).name]
