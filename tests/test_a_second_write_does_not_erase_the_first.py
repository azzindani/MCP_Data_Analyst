"""Writing to an output_path must leave whatever was there recoverable.

Every tool that edits a dataset in place snapshots first. The tools that write
to an `output_path` did not, and an output path is not guaranteed to be a fresh
file -- it is wherever the caller points it. Against the live endpoints:

    export_data(file_path="d.csv", output_path="precious.csv")
    -> success: true

    precious.csv          "keep,this / 1,2"  ->  d.csv's rows
    .mcp_versions/        empty

Nothing to recover from and no indication anything was lost. The same shape
covers the retry case for the report and chart generators: regenerating over a
file, or re-sending a call whose first attempt timed out, replaced it outright.

The fix keeps the overwrite -- writing a report over its own path is the normal
way to refresh one -- and adds back the safety net the in-place tools already
had.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.html_layout import get_output_path  # noqa: E402
from shared.version_control import list_versions, snapshot_if_exists  # noqa: E402


class TestSnapshotIfExists:
    def test_an_existing_file_is_snapshotted(self, tmp_path):
        f = tmp_path / "precious.csv"
        f.write_text("keep,this\n1,2\n", encoding="utf-8")
        backup = snapshot_if_exists(f)
        assert backup and Path(backup).read_text(encoding="utf-8") == "keep,this\n1,2\n"

    def test_a_path_with_nothing_there_is_a_no_op(self, tmp_path):
        assert snapshot_if_exists(tmp_path / "fresh.csv") == ""
        assert not (tmp_path / ".mcp_versions").exists()

    def test_a_directory_is_not_snapshotted(self, tmp_path):
        d = tmp_path / "somewhere"
        d.mkdir()
        assert snapshot_if_exists(d) == ""

    def test_it_never_raises(self, tmp_path, monkeypatch):
        f = tmp_path / "x.csv"
        f.write_text("a\n", encoding="utf-8")
        import shared.version_control as vc

        monkeypatch.setattr(vc, "snapshot", lambda _p: (_ for _ in ()).throw(OSError("disk full")))
        assert vc.snapshot_if_exists(f) == ""


class TestTheReportPathSnapshotsWhatItReplaces:
    def test_an_existing_report_is_kept(self, tmp_path):
        out = tmp_path / "report.html"
        out.write_text("<html>the edited one</html>", encoding="utf-8")
        get_output_path(str(out), None, "report", "html")
        versions = list_versions(str(out))
        assert len(versions) == 1, versions
        assert "the edited one" in (tmp_path / ".mcp_versions" / versions[0]).read_text(encoding="utf-8")

    def test_a_fresh_path_snapshots_nothing(self, tmp_path):
        out = tmp_path / "report.html"
        get_output_path(str(out), None, "report", "html")
        assert list_versions(str(out)) == []

    def test_the_resolved_path_is_unchanged(self, tmp_path):
        out = tmp_path / "report.html"
        out.write_text("x", encoding="utf-8")
        assert get_output_path(str(out), None, "report", "html") == out.resolve()


class TestExportDataKeepsWhatItOverwrites:
    def test_exporting_over_a_dataset_leaves_a_way_back(self, tmp_path):
        sys.path.insert(0, str(ROOT / "servers" / "data_advanced"))
        from servers.data_advanced import _adv_charts

        source = tmp_path / "d.csv"
        source.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
        precious = tmp_path / "precious.csv"
        precious.write_text("keep,this\n1,2\n", encoding="utf-8")

        r = _adv_charts.export_data(str(source), output_path=str(precious), format="csv", open_after=False)
        assert r["success"] is True, r.get("error")

        versions = list_versions(str(precious))
        assert versions, "precious.csv was overwritten with no snapshot"
        recovered = (tmp_path / ".mcp_versions" / versions[0]).read_text(encoding="utf-8")
        assert recovered == "keep,this\n1,2\n", recovered

    def test_a_fresh_export_path_snapshots_nothing(self, tmp_path):
        sys.path.insert(0, str(ROOT / "servers" / "data_advanced"))
        from servers.data_advanced import _adv_charts

        source = tmp_path / "d.csv"
        source.write_text("a,b\n1,2\n", encoding="utf-8")
        out = tmp_path / "new_export.csv"
        r = _adv_charts.export_data(str(source), output_path=str(out), format="csv", open_after=False)
        assert r["success"] is True, r.get("error")
        assert list_versions(str(out)) == []
