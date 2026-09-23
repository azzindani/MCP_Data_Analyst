"""A remote server reads and writes only inside the folders it serves.

Every tool resolved its path with `Path(file_path).resolve()`, so any
authenticated caller of the deployed server could read any file the container
could. `inspect_dataset("/etc/hostname")` succeeded against the live endpoint,
and `/proc/self/environ` -- which holds the API keys -- was the same call. A
workspace `base_dir` and a workspace *name* ("../../etc") reached anywhere too,
and a relative path was read from the container's working directory rather than
from the data folder the caller can actually see.

With MCP_CONFINE_PATHS on (the default for every HTTP deployment) a path must
lie inside MCP_OUTPUT_DIR, the workspace root, or MCP_ALLOWED_ROOTS, judged
after symlinks are resolved. A local stdio install is the caller's own machine
and is unchanged -- except that `~` now expands, and a workspace name is always
a name.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_advanced")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _adv_dashboard import generate_dashboard  # noqa: E402

from servers.data_basic.engine import apply_patch, inspect_dataset  # noqa: E402
from shared.file_utils import PathOutsideRootError, resolve_path  # noqa: E402
from shared.workspace_utils import get_workspace_dir, get_workspace_root  # noqa: E402

OUTSIDE = "/etc/hostname" if Path("/etc/hostname").exists() else str(Path(__file__).resolve())


@pytest.fixture
def served(tmp_path, monkeypatch):
    data = tmp_path / "data"
    ws = tmp_path / "ws"
    data.mkdir()
    ws.mkdir()
    monkeypatch.setenv("MCP_CONFINE_PATHS", "1")
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(data))
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(ws))
    monkeypatch.delenv("MCP_DATA_ROOT", raising=False)
    monkeypatch.delenv("MCP_ALLOWED_ROOTS", raising=False)
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    pd.DataFrame({"region": ["N", "S"] * 5, "revenue": range(10)}).to_csv(data / "sales.csv", index=False)
    return data


class TestConfined:
    def test_a_file_outside_is_refused(self, served):
        with pytest.raises(PathOutsideRootError, match="outside the folders"):
            resolve_path(OUTSIDE)

    def test_a_relative_path_is_read_from_the_data_folder(self, served):
        assert resolve_path("sales.csv") == (served / "sales.csv").resolve()

    def test_climbing_out_of_the_data_folder_is_refused(self, served):
        with pytest.raises(PathOutsideRootError):
            resolve_path("../outside.csv")

    def test_a_symlink_is_judged_by_where_it_leads(self, served):
        link = served / "innocent.csv"
        try:
            link.symlink_to(OUTSIDE)
        except OSError, NotImplementedError:
            pytest.skip("cannot create a symlink here")
        with pytest.raises(PathOutsideRootError):
            resolve_path(str(link))

    def test_an_extra_root_can_be_served(self, served, tmp_path, monkeypatch):
        extra = tmp_path / "extra"
        extra.mkdir()
        monkeypatch.setenv("MCP_ALLOWED_ROOTS", str(extra))
        assert resolve_path(str(extra / "x.csv")) == (extra / "x.csv").resolve()

    def test_a_null_byte_is_refused(self, served):
        with pytest.raises(ValueError, match="null byte"):
            resolve_path("sales\x00.csv")

    def test_a_workspace_base_dir_outside_is_refused(self, served):
        with pytest.raises(PathOutsideRootError, match="base_dir"):
            get_workspace_root(os.path.dirname(OUTSIDE))

    def test_the_tool_refuses_and_says_why(self, served):
        r = inspect_dataset(OUTSIDE)
        assert r["success"] is False
        assert "outside the folders" in r["error"]
        assert "data folder" in r["hint"] and "permissions" not in r["hint"]

    def test_the_tool_reads_a_relative_path(self, served):
        r = inspect_dataset("sales.csv")
        assert r["success"] is True, r

    def test_concat_file_cannot_read_outside(self, served):
        r = apply_patch("sales.csv", [{"op": "concat_file", "file_path": OUTSIDE}])
        assert r["success"] is False
        assert "outside the folders" in str(r)

    def test_a_dashboard_cannot_write_outside(self, served, tmp_path):
        target = tmp_path / "elsewhere" / "dash.html"
        r = generate_dashboard("sales.csv", output_path=str(target), open_after=False)
        assert r["success"] is False
        assert not target.exists()

    def test_a_dashboard_source_cannot_be_read_from_outside(self, served):
        r = generate_dashboard("sales.csv", sources=[OUTSIDE], open_after=False)
        assert r["success"] is False


class TestLocal:
    def test_a_local_install_is_not_confined(self, tmp_path, monkeypatch):
        monkeypatch.delenv("MCP_CONFINE_PATHS", raising=False)
        assert resolve_path(OUTSIDE) == Path(OUTSIDE).resolve()

    def test_a_relative_path_is_still_read_from_the_working_directory(self, tmp_path, monkeypatch):
        monkeypatch.delenv("MCP_CONFINE_PATHS", raising=False)
        monkeypatch.delenv("MCP_DATA_ROOT", raising=False)
        monkeypatch.chdir(tmp_path)
        assert resolve_path("x.csv") == (tmp_path / "x.csv").resolve()

    def test_a_data_root_can_be_set_locally(self, tmp_path, monkeypatch):
        monkeypatch.delenv("MCP_CONFINE_PATHS", raising=False)
        monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
        assert resolve_path("x.csv") == (tmp_path / "x.csv").resolve()

    def test_home_is_expanded(self, monkeypatch):
        monkeypatch.delenv("MCP_CONFINE_PATHS", raising=False)
        assert resolve_path("~/x.csv") == (Path.home() / "x.csv").resolve()

    @pytest.mark.parametrize("name", ["../../etc", "..", "a/../../b"])
    def test_a_workspace_name_is_always_a_name(self, tmp_path, monkeypatch, name):
        monkeypatch.delenv("MCP_CONFINE_PATHS", raising=False)
        monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
        with pytest.raises(ValueError, match="not a plain name"):
            get_workspace_dir(name)


class TestOutputFolderConfined:
    """extract_all_sheets built its output folder with a bare Path(output_dir).

    Every other output here goes through get_output_path, which resolves and
    confines; this one did not, so a relative folder landed beside the process
    and an absolute one was used as given. Found by reading the output paths
    during a direct sweep of the deployed fleet.
    """

    @pytest.fixture
    def workbook(self, served):
        pd.DataFrame({"a": [1, 2], "b": [3, 4]}).to_excel(served / "book.xlsx", index=False, sheet_name="S1")
        return "book.xlsx"

    def test_an_outside_output_dir_is_refused_and_nothing_written(self, served, workbook, tmp_path):
        from servers.data_ingest.engine import extract_all_sheets

        target = tmp_path / "elsewhere"
        r = extract_all_sheets(workbook, output_dir=str(target))
        assert r["success"] is False, r
        assert "outside the folders" in r["error"]
        assert not target.exists()

    def test_a_relative_output_dir_is_the_data_folder(self, served, workbook):
        from servers.data_ingest.engine import extract_all_sheets

        r = extract_all_sheets(workbook, output_dir="sheets")
        assert r["success"] is True, r
        assert (served / "sheets" / "book_S1.csv").exists()


class TestARefusedPathIsAnAnswer:
    """A refusal raised anywhere inside a tool comes back in the failure shape.

    On the ML server a resolver outside a tool's own `try` let the refusal
    escape: nothing was written, but the caller got "Error executing tool ...:
    Path ... is outside" with no success, op or hint. The per-tool wrapper
    answers it here too, whichever resolver raised it.
    """

    def test_a_refusal_raised_inside_a_tool_is_answered(self):
        from types import SimpleNamespace

        from shared.file_utils import PathOutsideRootError  # type: ignore[reportMissingImports]
        from shared.missing_file import suggest_missing_files  # type: ignore[reportMissingImports]

        def save_it(output_path: str = "") -> dict:
            raise PathOutsideRootError(f"Path '{output_path}' is outside the folders this server can use.")

        registered = SimpleNamespace(name="save_it", fn=save_it)
        mcp = SimpleNamespace(_tool_manager=SimpleNamespace(_tools={"save_it": registered}))
        suggest_missing_files(mcp)
        r = registered.fn(output_path="/elsewhere/out.csv")
        assert r["success"] is False, r
        assert r["op"] == "save_it"
        assert "outside the folders" in r["error"]
        assert "data folder" in r["hint"]

    def test_any_other_error_still_propagates(self):
        from types import SimpleNamespace

        from shared.missing_file import suggest_missing_files  # type: ignore[reportMissingImports]

        def broken() -> dict:
            raise RuntimeError("a real bug")

        registered = SimpleNamespace(name="broken", fn=broken)
        suggest_missing_files(SimpleNamespace(_tool_manager=SimpleNamespace(_tools={"broken": registered})))
        with pytest.raises(RuntimeError, match="a real bug"):
            registered.fn()


class TestAPathOnTheCallersSideIsNamedAsOne:
    """A claude.ai upload path is refused for what it is, with the way in.

    `/mnt/user-data/uploads/Ad_Data.csv` is the only path a chat's model holds
    for an attached file. "Outside the folders this server can use" named the
    rule and sent it guessing folders on a server that cannot see the file.
    """

    def test_the_refusal_says_the_file_is_on_the_callers_side(self, served, monkeypatch):
        monkeypatch.setenv("MCP_FETCH_URLS", "1")
        with pytest.raises(PathOutsideRootError) as caught:
            resolve_path("/mnt/user-data/uploads/Ad_Data.csv")
        message = str(caught.value)
        assert "caller's side" in message
        assert "cannot see it" in message
        assert "link" in message

    def test_any_other_outside_path_keeps_the_plain_refusal(self, served):
        with pytest.raises(PathOutsideRootError, match="outside the folders"):
            resolve_path("/etc/hostname")


class TestAFileSentInlineIsRead:
    """A CSV sent as data:...;base64 is read by a registered tool like any file."""

    def test_inspect_reads_an_inline_csv(self, served):
        import base64
        import importlib

        uri = "data:text/csv;name=sent.csv;base64," + base64.b64encode(b"x,y\n1,2\n3,4\n5,6\n").decode()
        tools = importlib.import_module("servers.data_basic.server").mcp._tool_manager._tools
        r = tools["inspect_dataset"].fn(file_path=uri)
        assert r["success"] is True, r
        assert "base64" not in str(r)
        assert (served / "inbox" / "sent.csv").exists()
        assert "3" in str(r.get("rows", r.get("row_count", "")))
