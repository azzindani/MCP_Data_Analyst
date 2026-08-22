"""register_workspace_file blamed file_path when the workspace was missing.

Two different things raise FileNotFoundError inside this tool: the data file
being registered, and the workspace manifest it is being registered into. Only
the first had a hint, so registering into a workspace that does not exist
answered

    error: Workspace 'probe_ws' not found. Expected manifest at: .../workspace.json
    hint:  Check that file_path is an absolute path to an existing file.

sending the caller to inspect the one argument that was already correct. A
container restart is enough to produce this, because the manifests live outside
the mounted data directory. open_workspace and list_workspace_files, in the same
file, already tell the caller to run create_workspace.
"""

from __future__ import annotations

import csv as csvmod
from pathlib import Path

import pytest

from servers.data_workspace.engine import create_workspace, register_workspace_file


@pytest.fixture()
def base_dir(tmp_path: Path) -> str:
    return str(tmp_path / "workspaces")


@pytest.fixture()
def data_file(tmp_path: Path) -> str:
    path = tmp_path / "ad.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csvmod.writer(fh)
        w.writerow(["campaign_platform", "spends"])
        w.writerow(["Google Ads", 1939000])
    return str(path)


class TestAMissingWorkspace:
    def test_it_fails(self, data_file: str, base_dir: str):
        r = register_workspace_file("no_such_ws", data_file, "ad", base_dir=base_dir)
        assert r["success"] is False

    def test_the_error_names_the_workspace(self, data_file: str, base_dir: str):
        r = register_workspace_file("no_such_ws", data_file, "ad", base_dir=base_dir)
        assert "no_such_ws" in r["error"], r["error"]

    def test_the_hint_no_longer_blames_file_path(self, data_file: str, base_dir: str):
        r = register_workspace_file("no_such_ws", data_file, "ad", base_dir=base_dir)
        assert "file_path" not in r["hint"], r["hint"]

    def test_the_hint_says_to_create_the_workspace(self, data_file: str, base_dir: str):
        r = register_workspace_file("no_such_ws", data_file, "ad", base_dir=base_dir)
        assert "create_workspace" in r["hint"] and "no_such_ws" in r["hint"], r["hint"]


class TestAMissingFileStillBlamesTheFile:
    def test_the_hint_is_unchanged(self, tmp_path: Path, base_dir: str):
        create_workspace("ws", base_dir=base_dir)
        r = register_workspace_file("ws", str(tmp_path / "ghost.csv"), "ad", base_dir=base_dir)
        assert r["success"] is False
        assert "file_path" in r["hint"], r["hint"]


class TestTheGoodPathIsUnchanged:
    def test_registering_into_a_real_workspace_works(self, data_file: str, base_dir: str):
        assert create_workspace("ws", base_dir=base_dir)["success"] is True
        r = register_workspace_file("ws", data_file, "ad", base_dir=base_dir)
        assert r["success"] is True, r.get("error")
        assert r["alias"] == "ad"

    @pytest.mark.parametrize("stage", ["raw", "working", "trial", "output"])
    def test_every_stage_the_docstring_names_is_accepted(self, stage: str, data_file: str, base_dir: str):
        create_workspace("ws", base_dir=base_dir)
        r = register_workspace_file("ws", data_file, f"ad_{stage}", stage=stage, base_dir=base_dir)
        assert r["success"] is True, f"{stage}: {r.get('error')}"
        assert r["stage"] == stage
