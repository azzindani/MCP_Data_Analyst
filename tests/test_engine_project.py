"""Tests for servers/data_project/engine.py — project management e2e flows."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from servers.data_project.engine import (
    create_project,
    list_project_files,
    open_project,
    register_file,
    run_saved_pipeline,
    save_pipeline,
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
    """Isolated projects directory for each test."""
    base = tmp_path / "mcp_projects"
    base.mkdir()
    return base


# ---------------------------------------------------------------------------
# create_project
# ---------------------------------------------------------------------------


class TestCreateProject:
    def test_success(self, project_base):
        r = create_project("demo", description="Test project", base_dir=str(project_base))
        assert r["success"] is True
        assert r["name"] == "demo"
        assert "project_dir" in r
        assert "manifest" in r
        assert "token_estimate" in r

    def test_creates_standard_dirs(self, project_base):
        r = create_project("demo2", base_dir=str(project_base))
        assert r["success"] is True
        proj_dir = Path(r["project_dir"])
        assert (proj_dir / "data" / "raw").exists()
        assert (proj_dir / "data" / "working").exists()
        assert (proj_dir / "reports").exists()
        assert (proj_dir / "pipelines").exists()

    def test_creates_manifest(self, project_base):
        r = create_project("demo3", base_dir=str(project_base))
        assert r["success"] is True
        manifest_path = Path(r["project_dir"]) / "project.json"
        assert manifest_path.exists()

    def test_duplicate_project_fails(self, project_base):
        create_project("dup", base_dir=str(project_base))
        r2 = create_project("dup", base_dir=str(project_base))
        assert r2["success"] is False
        assert "hint" in r2
        assert "open_project" in r2["hint"]

    def test_progress_present(self, project_base):
        r = create_project("demo4", base_dir=str(project_base))
        assert "progress" in r
        assert any(p["status"] == "ok" for p in r["progress"])


# ---------------------------------------------------------------------------
# open_project
# ---------------------------------------------------------------------------


class TestOpenProject:
    def test_open_existing(self, project_base):
        create_project("myproj", base_dir=str(project_base))
        r = open_project("myproj", base_dir=str(project_base))
        assert r["success"] is True
        assert r["name"] == "myproj"
        assert "aliases" in r
        assert "saved_pipelines" in r
        assert "recent_history" in r

    def test_open_nonexistent_fails(self, project_base):
        r = open_project("ghost", base_dir=str(project_base))
        assert r["success"] is False
        assert "hint" in r
        assert "create_project" in r["hint"]

    def test_token_estimate(self, project_base):
        create_project("tok_proj", base_dir=str(project_base))
        r = open_project("tok_proj", base_dir=str(project_base))
        assert r["token_estimate"] > 0


# ---------------------------------------------------------------------------
# register_file
# ---------------------------------------------------------------------------


class TestRegisterFile:
    def test_register_success(self, project_base, sales_csv):
        create_project("reg_proj", base_dir=str(project_base))
        r = register_file("reg_proj", str(sales_csv), alias="raw_sales", base_dir=str(project_base))
        assert r["success"] is True
        assert r["alias"] == "raw_sales"
        assert r["stage"] == "raw"
        assert r["total_files"] == 1

    def test_register_sets_active(self, project_base, sales_csv):
        create_project("active_proj", base_dir=str(project_base))
        r = register_file("active_proj", str(sales_csv), alias="main_file", set_active=True, base_dir=str(project_base))
        assert r["success"] is True
        assert r["active_file"] == "main_file"

    def test_register_missing_file_fails(self, project_base):
        create_project("miss_proj", base_dir=str(project_base))
        r = register_file("miss_proj", "/nonexistent/path/data.csv", alias="x", base_dir=str(project_base))
        assert r["success"] is False
        assert "hint" in r

    def test_register_missing_project_fails(self, project_base, sales_csv):
        r = register_file("no_project", str(sales_csv), alias="x", base_dir=str(project_base))
        assert r["success"] is False


# ---------------------------------------------------------------------------
# list_project_files
# ---------------------------------------------------------------------------


class TestListProjectFiles:
    def test_list_all(self, project_base, sales_csv):
        create_project("list_proj", base_dir=str(project_base))
        register_file("list_proj", str(sales_csv), alias="f1", stage="raw", base_dir=str(project_base))
        r = list_project_files("list_proj", base_dir=str(project_base))
        assert r["success"] is True
        assert r["count"] == 1
        assert r["files"][0]["alias"] == "f1"

    def test_filter_by_stage(self, project_base, sales_csv, tmp_path):
        create_project("stage_proj", base_dir=str(project_base))
        sales2 = tmp_path / "sales2.csv"
        sales2.write_text("A,B\n1,2\n")
        register_file("stage_proj", str(sales_csv), alias="raw_file", stage="raw", base_dir=str(project_base))
        register_file("stage_proj", str(sales2), alias="work_file", stage="working", base_dir=str(project_base))
        r = list_project_files("stage_proj", stage="raw", base_dir=str(project_base))
        assert r["success"] is True
        assert r["count"] == 1
        assert r["files"][0]["stage"] == "raw"

    def test_nonexistent_project(self, project_base):
        r = list_project_files("nope", base_dir=str(project_base))
        assert r["success"] is False
        assert "create_project" in r["hint"]


# ---------------------------------------------------------------------------
# save_pipeline
# ---------------------------------------------------------------------------


class TestSavePipeline:
    def test_save_success(self, project_base):
        create_project("pipe_proj", base_dir=str(project_base))
        ops = [{"op": "drop_nulls"}, {"op": "strip_whitespace", "column": "Region"}]
        r = save_pipeline("pipe_proj", "clean_step", ops, description="Basic cleaning", base_dir=str(project_base))
        assert r["success"] is True
        assert r["op_count"] == 2
        assert r["pipeline_name"] == "clean_step"

    def test_pipeline_visible_after_open(self, project_base):
        create_project("pipe_vis", base_dir=str(project_base))
        ops = [{"op": "drop_nulls"}]
        save_pipeline("pipe_vis", "my_pipeline", ops, base_dir=str(project_base))
        r_open = open_project("pipe_vis", base_dir=str(project_base))
        assert "my_pipeline" in r_open["saved_pipelines"]


# ---------------------------------------------------------------------------
# run_saved_pipeline
# ---------------------------------------------------------------------------


class TestRunSavedPipeline:
    def test_dry_run(self, project_base, sales_csv):
        create_project("run_proj", base_dir=str(project_base))
        register_file("run_proj", str(sales_csv), alias="raw_sales", base_dir=str(project_base))
        ops = [{"op": "drop_nulls"}]
        save_pipeline("run_proj", "clean_pipe", ops, base_dir=str(project_base))
        r = run_saved_pipeline(
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
        create_project("nopipe_proj", base_dir=str(project_base))
        register_file("nopipe_proj", str(sales_csv), alias="raw_sales", base_dir=str(project_base))
        r = run_saved_pipeline(
            "nopipe_proj",
            "ghost_pipeline",
            input_alias="raw_sales",
            output_alias="output",
            base_dir=str(project_base),
        )
        assert r["success"] is False


# ---------------------------------------------------------------------------
# E2E: full project workflow — create → register → save pipeline → run → verify
# ---------------------------------------------------------------------------


class TestE2EProjectWorkflow:
    def test_full_workflow(self, project_base, tmp_path):
        """Create project, register file, save pipeline, dry-run it."""
        # Setup CSV with nulls
        f = tmp_path / "data_raw.csv"
        f.write_text("Region,Revenue\nWest,5000\nEast,\nSouth,2100\nNorth,4800\n")

        # 1. Create project
        r_create = create_project("analytics", base_dir=str(project_base))
        assert r_create["success"] is True

        # 2. Register file
        r_reg = register_file("analytics", str(f), alias="raw_data", set_active=True, base_dir=str(project_base))
        assert r_reg["success"] is True
        assert r_reg["active_file"] == "raw_data"

        # 3. Save cleaning pipeline
        ops = [{"op": "fill_nulls", "column": "Revenue", "strategy": "median"}]
        r_save = save_pipeline("analytics", "clean_revenue", ops, base_dir=str(project_base))
        assert r_save["success"] is True

        # 4. Open project and confirm everything is registered
        r_open = open_project("analytics", base_dir=str(project_base))
        assert r_open["success"] is True
        assert "raw_data" in r_open["aliases"]
        assert "clean_revenue" in r_open["saved_pipelines"]

        # 5. Dry-run pipeline — no data should change
        original_content = f.read_text()
        r_run = run_saved_pipeline(
            "analytics",
            "clean_revenue",
            input_alias="raw_data",
            output_alias="clean_data",
            dry_run=True,
            base_dir=str(project_base),
        )
        assert r_run["success"] is True
        assert r_run["dry_run"] is True
        # Verify file unchanged
        assert f.read_text() == original_content

    def test_list_files_after_registration(self, project_base, tmp_path):
        """Register two files, list by stage, confirm counts."""
        f_raw = tmp_path / "raw.csv"
        f_raw.write_text("A,B\n1,2\n3,4\n")
        f_work = tmp_path / "work.csv"
        f_work.write_text("A,B\n5,6\n7,8\n")

        create_project("list_test", base_dir=str(project_base))
        register_file("list_test", str(f_raw), alias="raw_f", stage="raw", base_dir=str(project_base))
        register_file("list_test", str(f_work), alias="work_f", stage="working", base_dir=str(project_base))

        r_all = list_project_files("list_test", base_dir=str(project_base))
        assert r_all["count"] == 2

        r_raw = list_project_files("list_test", stage="raw", base_dir=str(project_base))
        assert r_raw["count"] == 1

        r_work = list_project_files("list_test", stage="working", base_dir=str(project_base))
        assert r_work["count"] == 1
