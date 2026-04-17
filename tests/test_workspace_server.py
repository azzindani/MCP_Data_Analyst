"""Tests for servers/data_workspace/server.py — context + handover verification."""

from __future__ import annotations

from pathlib import Path

import pytest

from servers.data_workspace.server import (
    create_workspace,
    list_workspace_files,
    open_workspace,
    register_workspace_file,
    run_workspace_pipeline,
    save_workspace_pipeline,
)


@pytest.fixture()
def ws_base(tmp_path) -> Path:
    base = tmp_path / "mcp_workspace"
    base.mkdir()
    return base


@pytest.fixture()
def sample_csv(tmp_path) -> Path:
    f = tmp_path / "sales.csv"
    f.write_text("Region,Revenue\nWest,5000\nEast,7500\nSouth,2100\n")
    return f


# ---------------------------------------------------------------------------
# create_workspace
# ---------------------------------------------------------------------------


class TestCreateWorkspace:
    def test_success(self, ws_base):
        r = create_workspace("ws1", description="test", base_dir=str(ws_base))
        assert r["success"] is True
        assert r["name"] == "ws1"

    def test_context_field_present(self, ws_base):
        r = create_workspace("ws_ctx", base_dir=str(ws_base))
        assert r["success"] is True
        ctx = r["context"]
        assert ctx["op"] == "create_workspace"
        assert "ws_ctx" in ctx["summary"]
        assert "timestamp" in ctx

    def test_handover_field_present(self, ws_base):
        r = create_workspace("ws_ho", base_dir=str(ws_base))
        assert r["success"] is True
        ho = r["handover"]
        assert ho["workflow_step"] == "COLLECT"
        assert isinstance(ho["suggested_next"], list)
        assert len(ho["suggested_next"]) > 0
        # carry_forward must include workspace_name
        assert ho["carry_forward"]["workspace_name"] == "ws_ho"

    def test_handover_suggested_next_structure(self, ws_base):
        r = create_workspace("ws_sn", base_dir=str(ws_base))
        ho = r["handover"]
        for item in ho["suggested_next"]:
            assert "tool" in item
            assert "server" in item
            assert "domain" in item
            assert "reason" in item

    def test_duplicate_fails(self, ws_base):
        create_workspace("dup_ws", base_dir=str(ws_base))
        r2 = create_workspace("dup_ws", base_dir=str(ws_base))
        assert r2["success"] is False
        assert "hint" in r2

    def test_creates_manifest_file(self, ws_base):
        r = create_workspace("mf_ws", base_dir=str(ws_base))
        assert r["success"] is True
        manifest = Path(r["project_dir"]) / "workspace.json"
        assert manifest.exists()

    def test_no_context_on_failure(self, ws_base):
        create_workspace("fail_ws", base_dir=str(ws_base))
        r = create_workspace("fail_ws", base_dir=str(ws_base))
        assert r["success"] is False
        assert "context" not in r
        assert "handover" not in r


# ---------------------------------------------------------------------------
# open_workspace
# ---------------------------------------------------------------------------


class TestOpenWorkspace:
    def test_success(self, ws_base):
        create_workspace("open_ws", base_dir=str(ws_base))
        r = open_workspace("open_ws", base_dir=str(ws_base))
        assert r["success"] is True
        assert r["name"] == "open_ws"

    def test_context_on_success(self, ws_base):
        create_workspace("open_ctx", base_dir=str(ws_base))
        r = open_workspace("open_ctx", base_dir=str(ws_base))
        ctx = r["context"]
        assert ctx["op"] == "open_workspace"
        assert "open_ctx" in ctx["summary"]

    def test_handover_on_success(self, ws_base):
        create_workspace("open_ho", base_dir=str(ws_base))
        r = open_workspace("open_ho", base_dir=str(ws_base))
        ho = r["handover"]
        assert "workflow_step" in ho
        assert "suggested_next" in ho
        assert ho["carry_forward"]["workspace_name"] == "open_ho"

    def test_not_found(self, ws_base):
        r = open_workspace("ghost", base_dir=str(ws_base))
        assert r["success"] is False
        assert "hint" in r


# ---------------------------------------------------------------------------
# register_workspace_file
# ---------------------------------------------------------------------------


class TestRegisterWorkspaceFile:
    def test_success(self, ws_base, sample_csv):
        create_workspace("reg_ws", base_dir=str(ws_base))
        r = register_workspace_file("reg_ws", str(sample_csv), alias="raw_data", base_dir=str(ws_base))
        assert r["success"] is True
        assert r["alias"] == "raw_data"

    def test_context_field(self, ws_base, sample_csv):
        create_workspace("reg_ctx", base_dir=str(ws_base))
        r = register_workspace_file("reg_ctx", str(sample_csv), alias="sales", base_dir=str(ws_base))
        ctx = r["context"]
        assert ctx["op"] == "register_workspace_file"
        assert "sales" in ctx["summary"]
        artifacts = ctx["artifacts"]
        assert len(artifacts) == 1
        assert artifacts[0]["alias"] == "sales"
        assert artifacts[0]["role"] == "registered"

    def test_handover_carry_forward_file_path(self, ws_base, sample_csv):
        create_workspace("reg_ho", base_dir=str(ws_base))
        r = register_workspace_file("reg_ho", str(sample_csv), alias="myfile", base_dir=str(ws_base))
        ho = r["handover"]
        assert ho["carry_forward"]["file_path"] == "workspace:reg_ho/myfile"

    def test_missing_file_fails(self, ws_base):
        create_workspace("miss_ws", base_dir=str(ws_base))
        r = register_workspace_file("miss_ws", "/no/such/file.csv", alias="x", base_dir=str(ws_base))
        assert r["success"] is False


# ---------------------------------------------------------------------------
# list_workspace_files
# ---------------------------------------------------------------------------


class TestListWorkspaceFiles:
    def test_empty(self, ws_base):
        create_workspace("list_ws", base_dir=str(ws_base))
        r = list_workspace_files("list_ws", base_dir=str(ws_base))
        assert r["success"] is True
        assert r["count"] == 0

    def test_after_register(self, ws_base, sample_csv):
        create_workspace("list_ws2", base_dir=str(ws_base))
        register_workspace_file("list_ws2", str(sample_csv), alias="f1", stage="raw", base_dir=str(ws_base))
        r = list_workspace_files("list_ws2", base_dir=str(ws_base))
        assert r["count"] == 1

    def test_stage_filter(self, ws_base, sample_csv, tmp_path):
        f2 = tmp_path / "other.csv"
        f2.write_text("A,B\n1,2\n")
        create_workspace("filter_ws", base_dir=str(ws_base))
        register_workspace_file("filter_ws", str(sample_csv), alias="raw_f", stage="raw", base_dir=str(ws_base))
        register_workspace_file("filter_ws", str(f2), alias="work_f", stage="working", base_dir=str(ws_base))
        r = list_workspace_files("filter_ws", stage="raw", base_dir=str(ws_base))
        assert r["count"] == 1
        assert r["files"][0]["stage"] == "raw"


# ---------------------------------------------------------------------------
# save_workspace_pipeline
# ---------------------------------------------------------------------------


class TestSaveWorkspacePipeline:
    def test_success(self, ws_base):
        create_workspace("pipe_ws", base_dir=str(ws_base))
        ops = [{"op": "drop_nulls"}]
        r = save_workspace_pipeline("pipe_ws", "clean", ops, base_dir=str(ws_base))
        assert r["success"] is True
        assert r["op_count"] == 1

    def test_context_field(self, ws_base):
        create_workspace("pipe_ctx", base_dir=str(ws_base))
        ops = [{"op": "drop_nulls"}, {"op": "strip_whitespace", "column": "Region"}]
        r = save_workspace_pipeline("pipe_ctx", "my_pipe", ops, base_dir=str(ws_base))
        ctx = r["context"]
        assert ctx["op"] == "save_workspace_pipeline"
        assert "my_pipe" in ctx["summary"]

    def test_handover_suggests_run(self, ws_base):
        create_workspace("pipe_ho", base_dir=str(ws_base))
        ops = [{"op": "drop_nulls"}]
        r = save_workspace_pipeline("pipe_ho", "pipe_a", ops, base_dir=str(ws_base))
        ho = r["handover"]
        tools = [s["tool"] for s in ho["suggested_next"]]
        assert "run_workspace_pipeline" in tools
        assert ho["carry_forward"]["pipeline_name"] == "pipe_a"

    def test_pipeline_visible_after_open(self, ws_base):
        create_workspace("pipe_vis", base_dir=str(ws_base))
        ops = [{"op": "drop_nulls"}]
        save_workspace_pipeline("pipe_vis", "vis_pipe", ops, base_dir=str(ws_base))
        r = open_workspace("pipe_vis", base_dir=str(ws_base))
        assert "vis_pipe" in r["saved_pipelines"]


# ---------------------------------------------------------------------------
# run_workspace_pipeline
# ---------------------------------------------------------------------------


class TestRunWorkspacePipeline:
    def test_dry_run_success(self, ws_base, sample_csv):
        create_workspace("run_ws", base_dir=str(ws_base))
        register_workspace_file("run_ws", str(sample_csv), alias="raw", base_dir=str(ws_base))
        ops = [{"op": "drop_nulls"}]
        save_workspace_pipeline("run_ws", "pipe", ops, base_dir=str(ws_base))
        r = run_workspace_pipeline(
            "run_ws", "pipe",
            input_alias="raw", output_alias="clean",
            dry_run=True, base_dir=str(ws_base),
        )
        assert r["success"] is True
        assert r["dry_run"] is True
        assert r["would_apply"] == 1

    def test_dry_run_no_context_handover(self, ws_base, sample_csv):
        # context/handover only attached on non-dry-run success
        create_workspace("dry_ws", base_dir=str(ws_base))
        register_workspace_file("dry_ws", str(sample_csv), alias="raw", base_dir=str(ws_base))
        ops = [{"op": "drop_nulls"}]
        save_workspace_pipeline("dry_ws", "p", ops, base_dir=str(ws_base))
        r = run_workspace_pipeline(
            "dry_ws", "p",
            input_alias="raw", output_alias="out",
            dry_run=True, base_dir=str(ws_base),
        )
        # dry_run does NOT attach context+handover (server condition: not dry_run)
        assert "context" not in r
        assert "handover" not in r

    def test_missing_pipeline_fails(self, ws_base, sample_csv):
        create_workspace("np_ws", base_dir=str(ws_base))
        register_workspace_file("np_ws", str(sample_csv), alias="raw", base_dir=str(ws_base))
        r = run_workspace_pipeline(
            "np_ws", "ghost_pipe",
            input_alias="raw", output_alias="out",
            base_dir=str(ws_base),
        )
        assert r["success"] is False


# ---------------------------------------------------------------------------
# E2E: create -> register -> save -> dry-run
# ---------------------------------------------------------------------------


class TestE2EWorkspaceServer:
    def test_full_workflow(self, ws_base, sample_csv):
        r1 = create_workspace("e2e", description="end to end", base_dir=str(ws_base))
        assert r1["success"] is True
        assert "context" in r1
        assert "handover" in r1

        r2 = register_workspace_file("e2e", str(sample_csv), alias="raw_sales", base_dir=str(ws_base))
        assert r2["success"] is True
        # carry_forward path uses workspace: prefix
        assert r2["handover"]["carry_forward"]["file_path"] == "workspace:e2e/raw_sales"

        r3 = save_workspace_pipeline("e2e", "clean", [{"op": "drop_nulls"}], base_dir=str(ws_base))
        assert r3["success"] is True

        r4 = run_workspace_pipeline(
            "e2e", "clean",
            input_alias="raw_sales", output_alias="cleaned",
            dry_run=True, base_dir=str(ws_base),
        )
        assert r4["success"] is True
        assert r4["dry_run"] is True

        r5 = open_workspace("e2e", base_dir=str(ws_base))
        assert r5["success"] is True
        assert "raw_sales" in r5["aliases"]
        assert "clean" in r5["saved_pipelines"]
