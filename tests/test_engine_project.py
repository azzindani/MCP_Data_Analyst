"""Tests for servers/data_workspace/engine.py — workspace management e2e flows."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from servers.data_workspace.engine import (
    create_workspace,
    list_workspace_files,
    open_workspace,
    register_workspace_file,
    run_workspace_pipeline,
    save_workspace_pipeline,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sales_csv(tmp_path) -> Path:
    f = tmp_path / "sales.csv"
    f.write_text("Region,Revenue,Units\nWest,5000,10\nEast,7500,15\nSouth,2100,5\nNorth,4800,12\n")
    return f


@pytest.fixture()
def project_base(tmp_path) -> Path:
    """Isolated workspace directory for each test."""
    base = tmp_path / "mcp_projects"
    base.mkdir()
    return base


# ---------------------------------------------------------------------------
# create_workspace
# ---------------------------------------------------------------------------


class TestCreateWorkspace:
    def test_success(self, project_base):
        r = create_workspace("demo", description="Test workspace", base_dir=str(project_base))
        assert r["success"] is True
        assert r["name"] == "demo"
        assert "project_dir" in r
        assert "manifest" in r
        assert "token_estimate" in r

    def test_creates_standard_dirs(self, project_base):
        r = create_workspace("demo2", base_dir=str(project_base))
        assert r["success"] is True
        proj_dir = Path(r["project_dir"])
        assert (proj_dir / "data" / "raw").exists()
        assert (proj_dir / "data" / "working").exists()
        assert (proj_dir / "reports").exists()
        assert (proj_dir / "pipelines").exists()

    def test_creates_manifest(self, project_base):
        r = create_workspace("demo3", base_dir=str(project_base))
        assert r["success"] is True
        manifest_path = Path(r["project_dir"]) / "workspace.json"
        assert manifest_path.exists()

    def test_duplicate_workspace_fails(self, project_base):
        create_workspace("dup", base_dir=str(project_base))
        r2 = create_workspace("dup", base_dir=str(project_base))
        assert r2["success"] is False
        assert "hint" in r2
        assert "open_workspace" in r2["hint"]

    def test_progress_present(self, project_base):
        r = create_workspace("demo4", base_dir=str(project_base))
        assert "progress" in r
        assert any(p["status"] == "ok" for p in r["progress"])

    def test_context_and_handover_present(self, project_base):
        r = create_workspace("ctx_ws", base_dir=str(project_base))
        assert r["success"] is True
        assert "context" in r
        assert "handover" in r
        assert r["context"]["op"] == "create_workspace"
        assert r["handover"]["workflow_step"] == "COLLECT"


# ---------------------------------------------------------------------------
# open_workspace
# ---------------------------------------------------------------------------


class TestOpenWorkspace:
    def test_open_existing(self, project_base):
        create_workspace("myproj", base_dir=str(project_base))
        r = open_workspace("myproj", base_dir=str(project_base))
        assert r["success"] is True
        assert r["name"] == "myproj"
        assert "aliases" in r
        assert "saved_pipelines" in r
        assert "recent_history" in r

    def test_open_nonexistent_fails(self, project_base):
        r = open_workspace("ghost", base_dir=str(project_base))
        assert r["success"] is False
        assert "hint" in r
        assert "create_workspace" in r["hint"]

    def test_token_estimate(self, project_base):
        create_workspace("tok_proj", base_dir=str(project_base))
        r = open_workspace("tok_proj", base_dir=str(project_base))
        assert r["token_estimate"] > 0

    def test_context_and_handover_present(self, project_base):
        create_workspace("open_ctx", base_dir=str(project_base))
        r = open_workspace("open_ctx", base_dir=str(project_base))
        assert "context" in r
        assert "handover" in r
        assert r["context"]["op"] == "open_workspace"


# ---------------------------------------------------------------------------
# register_workspace_file
# ---------------------------------------------------------------------------


class TestRegisterWorkspaceFile:
    def test_register_success(self, project_base, sales_csv):
        create_workspace("reg_proj", base_dir=str(project_base))
        r = register_workspace_file("reg_proj", str(sales_csv), alias="raw_sales", base_dir=str(project_base))
        assert r["success"] is True
        assert r["alias"] == "raw_sales"
        assert r["stage"] == "raw"
        assert r["total_files"] == 1

    def test_register_sets_active(self, project_base, sales_csv):
        create_workspace("active_proj", base_dir=str(project_base))
        r = register_workspace_file(
            "active_proj", str(sales_csv), alias="main_file", set_active=True, base_dir=str(project_base)
        )
        assert r["success"] is True
        assert r["active_file"] == "main_file"

    def test_register_missing_file_fails(self, project_base):
        create_workspace("miss_proj", base_dir=str(project_base))
        r = register_workspace_file("miss_proj", "/nonexistent/path/data.csv", alias="x", base_dir=str(project_base))
        assert r["success"] is False
        assert "hint" in r

    def test_register_missing_workspace_fails(self, project_base, sales_csv):
        r = register_workspace_file("no_workspace", str(sales_csv), alias="x", base_dir=str(project_base))
        assert r["success"] is False

    def test_context_carry_forward(self, project_base, sales_csv):
        create_workspace("cf_proj", base_dir=str(project_base))
        r = register_workspace_file("cf_proj", str(sales_csv), alias="myfile", base_dir=str(project_base))
        assert r["handover"]["carry_forward"]["file_path"] == "workspace:cf_proj/myfile"


# ---------------------------------------------------------------------------
# list_workspace_files
# ---------------------------------------------------------------------------


class TestListWorkspaceFiles:
    def test_list_all(self, project_base, sales_csv):
        create_workspace("list_proj", base_dir=str(project_base))
        register_workspace_file("list_proj", str(sales_csv), alias="f1", stage="raw", base_dir=str(project_base))
        r = list_workspace_files("list_proj", base_dir=str(project_base))
        assert r["success"] is True
        assert r["count"] == 1
        assert r["files"][0]["alias"] == "f1"

    def test_filter_by_stage(self, project_base, sales_csv, tmp_path):
        create_workspace("stage_proj", base_dir=str(project_base))
        sales2 = tmp_path / "sales2.csv"
        sales2.write_text("A,B\n1,2\n")
        register_workspace_file("stage_proj", str(sales_csv), alias="raw_file", stage="raw", base_dir=str(project_base))
        register_workspace_file(
            "stage_proj", str(sales2), alias="work_file", stage="working", base_dir=str(project_base)
        )
        r = list_workspace_files("stage_proj", stage="raw", base_dir=str(project_base))
        assert r["success"] is True
        assert r["count"] == 1
        assert r["files"][0]["stage"] == "raw"

    def test_nonexistent_workspace(self, project_base):
        r = list_workspace_files("nope", base_dir=str(project_base))
        assert r["success"] is False
        assert "create_workspace" in r["hint"]


# ---------------------------------------------------------------------------
# save_workspace_pipeline
# ---------------------------------------------------------------------------


class TestSaveWorkspacePipeline:
    def test_save_success(self, project_base):
        create_workspace("pipe_proj", base_dir=str(project_base))
        ops = [{"op": "drop_nulls"}, {"op": "strip_whitespace", "column": "Region"}]
        r = save_workspace_pipeline(
            "pipe_proj", "clean_step", ops, description="Basic cleaning", base_dir=str(project_base)
        )
        assert r["success"] is True
        assert r["op_count"] == 2
        assert r["pipeline_name"] == "clean_step"

    def test_pipeline_visible_after_open(self, project_base):
        create_workspace("pipe_vis", base_dir=str(project_base))
        ops = [{"op": "drop_nulls"}]
        save_workspace_pipeline("pipe_vis", "my_pipeline", ops, base_dir=str(project_base))
        r_open = open_workspace("pipe_vis", base_dir=str(project_base))
        assert "my_pipeline" in r_open["saved_pipelines"]

    def test_handover_suggests_run(self, project_base):
        create_workspace("pipe_ho", base_dir=str(project_base))
        ops = [{"op": "drop_nulls"}]
        r = save_workspace_pipeline("pipe_ho", "pipe_a", ops, base_dir=str(project_base))
        tools = [s["tool"] for s in r["handover"]["suggested_next"]]
        assert "run_workspace_pipeline" in tools


# ---------------------------------------------------------------------------
# run_workspace_pipeline
# ---------------------------------------------------------------------------


class TestRunWorkspacePipeline:
    def test_dry_run(self, project_base, sales_csv):
        create_workspace("run_proj", base_dir=str(project_base))
        register_workspace_file("run_proj", str(sales_csv), alias="raw_sales", base_dir=str(project_base))
        ops = [{"op": "drop_nulls"}]
        save_workspace_pipeline("run_proj", "clean_pipe", ops, base_dir=str(project_base))
        r = run_workspace_pipeline(
            "run_proj",
            "clean_pipe",
            input_alias="raw_sales",
            output_alias="clean_sales",
            dry_run=True,
            base_dir=str(project_base),
        )
        assert r["success"] is True
        assert r["dry_run"] is True
        assert r["would_apply"] == 1

    def test_pipeline_not_found_fails(self, project_base, sales_csv):
        create_workspace("nopipe_proj", base_dir=str(project_base))
        register_workspace_file("nopipe_proj", str(sales_csv), alias="raw_sales", base_dir=str(project_base))
        r = run_workspace_pipeline(
            "nopipe_proj",
            "ghost_pipeline",
            input_alias="raw_sales",
            output_alias="output",
            base_dir=str(project_base),
        )
        assert r["success"] is False


# ---------------------------------------------------------------------------
# E2E: full workspace workflow — create -> register -> save pipeline -> run -> verify
# ---------------------------------------------------------------------------


class TestE2EWorkspaceWorkflow:
    def test_full_workflow(self, project_base, tmp_path):
        """Create workspace, register file, save pipeline, dry-run it."""
        f = tmp_path / "data_raw.csv"
        f.write_text("Region,Revenue\nWest,5000\nEast,\nSouth,2100\nNorth,4800\n")

        r_create = create_workspace("analytics", base_dir=str(project_base))
        assert r_create["success"] is True
        assert "context" in r_create
        assert "handover" in r_create

        r_reg = register_workspace_file(
            "analytics", str(f), alias="raw_data", set_active=True, base_dir=str(project_base)
        )
        assert r_reg["success"] is True
        assert r_reg["active_file"] == "raw_data"
        assert r_reg["handover"]["carry_forward"]["file_path"] == "workspace:analytics/raw_data"

        ops = [{"op": "fill_nulls", "column": "Revenue", "strategy": "median"}]
        r_save = save_workspace_pipeline("analytics", "clean_revenue", ops, base_dir=str(project_base))
        assert r_save["success"] is True

        r_open = open_workspace("analytics", base_dir=str(project_base))
        assert r_open["success"] is True
        assert "raw_data" in r_open["aliases"]
        assert "clean_revenue" in r_open["saved_pipelines"]

        original_content = f.read_text()
        r_run = run_workspace_pipeline(
            "analytics",
            "clean_revenue",
            input_alias="raw_data",
            output_alias="clean_data",
            dry_run=True,
            base_dir=str(project_base),
        )
        assert r_run["success"] is True
        assert r_run["dry_run"] is True
        assert f.read_text() == original_content

    def test_list_files_after_registration(self, project_base, tmp_path):
        """Register two files, list by stage, confirm counts."""
        f_raw = tmp_path / "raw.csv"
        f_raw.write_text("A,B\n1,2\n3,4\n")
        f_work = tmp_path / "work.csv"
        f_work.write_text("A,B\n5,6\n7,8\n")

        create_workspace("list_test", base_dir=str(project_base))
        register_workspace_file("list_test", str(f_raw), alias="raw_f", stage="raw", base_dir=str(project_base))
        register_workspace_file("list_test", str(f_work), alias="work_f", stage="working", base_dir=str(project_base))

        r_all = list_workspace_files("list_test", base_dir=str(project_base))
        assert r_all["count"] == 2

        r_raw = list_workspace_files("list_test", stage="raw", base_dir=str(project_base))
        assert r_raw["count"] == 1

        r_work = list_workspace_files("list_test", stage="working", base_dir=str(project_base))
        assert r_work["count"] == 1
