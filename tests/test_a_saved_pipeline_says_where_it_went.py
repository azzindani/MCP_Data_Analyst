"""It wrote two files and named neither.

    save_workspace_pipeline(workspace_name="proj", pipeline_name="prep", ops=[...])
      -> success: true, op_count: 3, created: "...", artifacts: []

It had written `<workspace>/pipelines/prep.json` and updated
`<workspace>/workspace.json`. No path in the response, no artifact in the
context, nothing in progress beyond "3 ops". A caller was told the pipeline was
saved and had nothing to open, diff, copy or check.

Its two file-writing siblings on the same server both report what they wrote --
`register_workspace_file` names the file it registered, `run_workspace_pipeline`
returns `output_path` -- so this was the odd one out of three, which is where
these keep turning up.

Squarely the round-15 question: the artifact was correct and the reply did not
admit it existed. Nothing that checks artifacts can catch that, because there
was no path to check.

Found in a round-15 sweep report. Two other findings in the same phase did not
survive a direct check: `register_workspace_file` was said to ignore `base_dir`
and to hardcode a container path, and running the whole sequence with `base_dir`
on every call puts every file exactly where it is asked to -- the calls that
escaped had omitted the argument.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from servers.data_workspace import engine as ws

OPS = [{"op": "filter_between", "column": "spends", "min": 1, "max": 100}]


@pytest.fixture()
def workspace(tmp_path: Path) -> tuple[str, Path]:
    r = ws.create_workspace("proj", base_dir=str(tmp_path))
    assert r["success"] is True, r.get("error")
    return "proj", tmp_path


class TestItSaysWhereThePipelineWent:
    def test_the_response_carries_the_path(self, workspace: tuple[str, Path]) -> None:
        name, base = workspace
        r = ws.save_workspace_pipeline(name, "prep", ops=OPS, base_dir=str(base))
        assert r["success"] is True, r.get("error")
        assert r.get("output_path"), r

    def test_the_path_is_a_file_that_exists(self, workspace: tuple[str, Path]) -> None:
        name, base = workspace
        r = ws.save_workspace_pipeline(name, "prep", ops=OPS, base_dir=str(base))
        assert Path(r["output_path"]).is_file(), r["output_path"]

    def test_the_file_holds_the_ops_that_were_saved(self, workspace: tuple[str, Path]) -> None:
        """The path is only useful if what is at the end of it is right."""
        name, base = workspace
        r = ws.save_workspace_pipeline(name, "prep", ops=OPS, base_dir=str(base))
        saved = json.loads(Path(r["output_path"]).read_text(encoding="utf-8"))
        assert saved["ops"] == OPS
        assert saved["op_count"] == len(OPS)

    def test_the_context_lists_it_as_an_artifact(self, workspace: tuple[str, Path]) -> None:
        name, base = workspace
        r = ws.save_workspace_pipeline(name, "prep", ops=OPS, base_dir=str(base))
        artifacts = (r.get("context") or {}).get("artifacts") or []
        assert [a["path"] for a in artifacts] == [r["output_path"]], artifacts

    def test_the_path_is_under_the_base_dir_it_was_given(self, workspace: tuple[str, Path]) -> None:
        name, base = workspace
        r = ws.save_workspace_pipeline(name, "prep", ops=OPS, base_dir=str(base))
        assert Path(r["output_path"]).is_relative_to(base), (r["output_path"], base)


class TestEveryWritingToolHereNamesItsFile:
    """The asymmetry that made this findable: two of three already did."""

    def test_register_names_what_it_registered(self, workspace: tuple[str, Path], tmp_path: Path) -> None:
        name, base = workspace
        src = tmp_path / "data.csv"
        src.write_text("platform,spends\nGoogle Ads,10\nFacebook Ads,5\n", encoding="utf-8")
        r = ws.register_workspace_file(name, str(src), "ad_data", base_dir=str(base))
        assert r["success"] is True, r.get("error")
        paths = [a["path"] for a in (r.get("context") or {}).get("artifacts") or []]
        assert paths, r.get("context")

    def test_run_returns_its_output_path(self, workspace: tuple[str, Path], tmp_path: Path) -> None:
        name, base = workspace
        src = tmp_path / "data.csv"
        src.write_text("platform,spends\nGoogle Ads,10\nFacebook Ads,5\nGoogle Ads,0\n", encoding="utf-8")
        ws.register_workspace_file(name, str(src), "ad_data", base_dir=str(base))
        ws.save_workspace_pipeline(name, "prep", ops=OPS, base_dir=str(base))
        r = ws.run_workspace_pipeline(name, "prep", "ad_data", "cleaned", base_dir=str(base))
        assert r["success"] is True, r.get("error")
        assert Path(r["output_path"]).is_file()


class TestBaseDirIsHonouredThroughout:
    """The claim that did not survive: nothing escapes when it is passed."""

    def test_the_whole_sequence_stays_under_base_dir(self, tmp_path: Path) -> None:
        base = tmp_path / "shared"
        base.mkdir()
        src = base / "data.csv"
        src.write_text("platform,spends\nGoogle Ads,10\nFacebook Ads,5\nGoogle Ads,0\n", encoding="utf-8")

        assert ws.create_workspace("proj", base_dir=str(base))["success"]
        assert ws.register_workspace_file("proj", str(src), "ad_data", base_dir=str(base))["success"]
        assert ws.save_workspace_pipeline("proj", "prep", ops=OPS, base_dir=str(base))["success"]
        run = ws.run_workspace_pipeline("proj", "prep", "ad_data", "cleaned", base_dir=str(base))
        assert run["success"] is True, run.get("error")

        written = [p for p in base.rglob("*") if p.is_file()]
        assert written, "nothing was written under base_dir"
        for path in written:
            assert path.is_relative_to(base), path

    def test_the_alias_is_visible_to_the_next_call(self, tmp_path: Path) -> None:
        """The cascade that was reported: 'Alias not found ... Available: []'."""
        base = tmp_path / "shared"
        base.mkdir()
        src = base / "data.csv"
        src.write_text("platform,spends\nGoogle Ads,10\n", encoding="utf-8")
        ws.create_workspace("proj", base_dir=str(base))
        ws.register_workspace_file("proj", str(src), "ad_data", base_dir=str(base))
        listed = ws.list_workspace_files("proj", base_dir=str(base))
        assert [f["alias"] for f in listed["files"]] == ["ad_data"], listed
