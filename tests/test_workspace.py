"""Tests for shared/workspace_utils.py and shared/handover.py."""

import json
from pathlib import Path

import pytest

from shared.handover import DOMAIN_SERVERS, STEP_TOOLS, WORKFLOW_STEPS, make_context, make_handover
from shared.workspace_utils import (
    _ALIAS_PREFIX,
    _LEGACY_ALIAS_PREFIX,
    create_manifest,
    create_project_dirs,
    create_workspace_dirs,
    get_project_dir,
    get_projects_root,
    get_workspace_dir,
    get_workspace_root,
    is_alias,
    load_manifest,
    load_pipeline,
    log_pipeline_run,
    register_file,
    resolve_alias,
    save_manifest,
    save_pipeline,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_workspace(base: Path, name: str, alias: str, csv: Path) -> None:
    ws_dir = base / name
    ws_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "name": name,
        "files": {
            alias: {
                "path": str(csv),
                "stage": "working",
                "rows": 2,
                "size_bytes": csv.stat().st_size if csv.exists() else 0,
                "registered": "2026-01-01T00:00:00+00:00",
            }
        },
        "pipelines": {},
        "pipeline_history": [],
        "updated": "2026-01-01T00:00:00+00:00",
    }
    (ws_dir / "workspace.json").write_text(json.dumps(manifest), encoding="utf-8")


def _make_legacy_workspace(base: Path, name: str, alias: str, csv: Path) -> None:
    ws_dir = base / name
    ws_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "name": name,
        "files": {alias: {"path": str(csv), "stage": "raw", "rows": 2, "size_bytes": 0}},
        "pipelines": {},
        "pipeline_history": [],
    }
    (ws_dir / "project.json").write_text(json.dumps(manifest), encoding="utf-8")


def _empty_workspace(base: Path, name: str) -> None:
    ws_dir = base / name
    ws_dir.mkdir(parents=True, exist_ok=True)
    manifest = {"name": name, "files": {}, "pipelines": {}, "pipeline_history": [], "updated": ""}
    (ws_dir / "workspace.json").write_text(json.dumps(manifest), encoding="utf-8")


# ---------------------------------------------------------------------------
# Alias constants
# ---------------------------------------------------------------------------


def test_ws_alias_prefix_value():
    assert _ALIAS_PREFIX == "workspace:"


def test_ws_legacy_alias_prefix_value():
    assert _LEGACY_ALIAS_PREFIX == "project:"


# ---------------------------------------------------------------------------
# get_workspace_root
# ---------------------------------------------------------------------------


def test_ws_root_default():
    root = get_workspace_root()
    assert root == Path.home() / "mcp_workspace"


def test_ws_root_env_var(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    assert get_workspace_root() == tmp_path


def test_ws_root_legacy_env(monkeypatch, tmp_path):
    monkeypatch.delenv("MCP_WORKSPACE_DIR", raising=False)
    monkeypatch.setenv("MCP_PROJECTS_DIR", str(tmp_path))
    assert get_workspace_root() == tmp_path


def test_ws_root_base_dir_arg(tmp_path):
    assert get_workspace_root(str(tmp_path)) == tmp_path


def test_ws_root_workspace_env_takes_priority(monkeypatch, tmp_path):
    ws_dir = tmp_path / "ws"
    proj_dir = tmp_path / "proj"
    ws_dir.mkdir()
    proj_dir.mkdir()
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(ws_dir))
    monkeypatch.setenv("MCP_PROJECTS_DIR", str(proj_dir))
    assert get_workspace_root() == ws_dir


def test_ws_root_base_dir_overrides_env(monkeypatch, tmp_path):
    env_dir = tmp_path / "env"
    base = tmp_path / "base"
    env_dir.mkdir()
    base.mkdir()
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(env_dir))
    assert get_workspace_root(str(base)) == base


def test_ws_backward_compat_get_root(tmp_path):
    assert get_projects_root(str(tmp_path)) == get_workspace_root(str(tmp_path))


def test_ws_backward_compat_get_dir(tmp_path):
    assert get_project_dir("foo", str(tmp_path)) == get_workspace_dir("foo", str(tmp_path))


# ---------------------------------------------------------------------------
# load_manifest
# ---------------------------------------------------------------------------


def test_ws_load_manifest_workspace_json(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    csv = tmp_path / "data.csv"
    csv.write_text("a,b\n1,2\n")
    _make_workspace(tmp_path, "ws1", "mydata", csv)
    m = load_manifest("ws1")
    assert m["name"] == "ws1"
    assert "mydata" in m["files"]


def test_ws_load_manifest_legacy_project_json(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    csv = tmp_path / "data.csv"
    csv.write_text("a,b\n1,2\n")
    _make_legacy_workspace(tmp_path, "legacyws", "raw_data", csv)
    m = load_manifest("legacyws")
    assert "raw_data" in m["files"]


def test_ws_load_manifest_not_found(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    with pytest.raises(FileNotFoundError, match="not found"):
        load_manifest("nonexistent")


# ---------------------------------------------------------------------------
# save_manifest / create_manifest
# ---------------------------------------------------------------------------


def test_ws_save_manifest_creates_workspace_json(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    (tmp_path / "newws").mkdir()
    save_manifest({"name": "newws", "files": {}}, "newws")
    assert (tmp_path / "newws" / "workspace.json").exists()


def test_ws_create_manifest_round_trip(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    (tmp_path / "fresh").mkdir()
    m = create_manifest("fresh", description="test ws")
    assert m["name"] == "fresh"
    assert m["description"] == "test ws"
    loaded = load_manifest("fresh")
    assert loaded["name"] == "fresh"


# ---------------------------------------------------------------------------
# register_file
# ---------------------------------------------------------------------------


def test_ws_register_file_success(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    csv = tmp_path / "out.csv"
    csv.write_text("a,b\n1,2\n3,4\n")
    _empty_workspace(tmp_path, "reg_ws")
    m = register_file("reg_ws", str(csv), "clean", stage="working")
    assert "clean" in m["files"]
    assert m["files"]["clean"]["stage"] == "working"


def test_ws_register_file_all_stages(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    for stage in ("raw", "working", "trial", "output"):
        csv = tmp_path / f"{stage}.csv"
        csv.write_text("a\n1\n")
        _empty_workspace(tmp_path, f"ws_{stage}")
        m = register_file(f"ws_{stage}", str(csv), "f", stage=stage)
        assert m["files"]["f"]["stage"] == stage


def test_ws_register_file_persisted(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    csv = tmp_path / "f.csv"
    csv.write_text("x\n1\n")
    _empty_workspace(tmp_path, "persist_ws")
    register_file("persist_ws", str(csv), "alias1")
    m = load_manifest("persist_ws")
    assert "alias1" in m["files"]


def test_ws_register_file_invalid_stage(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    _empty_workspace(tmp_path, "ws_bad")
    with pytest.raises(ValueError, match="Invalid stage"):
        register_file("ws_bad", str(tmp_path / "f.csv"), "a", stage="bad")


def test_ws_register_file_missing_file(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    _empty_workspace(tmp_path, "ws_miss")
    with pytest.raises(FileNotFoundError):
        register_file("ws_miss", str(tmp_path / "missing.csv"), "a")


# ---------------------------------------------------------------------------
# resolve_alias
# ---------------------------------------------------------------------------


def test_ws_resolve_alias_workspace_prefix(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    csv = tmp_path / "data.csv"
    csv.write_text("a,b\n1,2\n")
    _make_workspace(tmp_path, "resws", "mycsv", csv)
    assert resolve_alias("workspace:resws/mycsv") == csv.resolve()


def test_ws_resolve_alias_project_prefix(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    csv = tmp_path / "data.csv"
    csv.write_text("a,b\n1,2\n")
    _make_workspace(tmp_path, "legws", "mycsv", csv)
    assert resolve_alias("project:legws/mycsv") == csv.resolve()


def test_ws_resolve_alias_relative_path(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    ws_dir = tmp_path / "relws"
    ws_dir.mkdir()
    sub = ws_dir / "data" / "working"
    sub.mkdir(parents=True)
    csv = sub / "clean.csv"
    csv.write_text("a,b\n1,2\n")
    manifest = {
        "name": "relws",
        "files": {"clean": {"path": "data/working/clean.csv", "stage": "working"}},
        "pipelines": {},
        "pipeline_history": [],
    }
    (ws_dir / "workspace.json").write_text(json.dumps(manifest))
    result = resolve_alias("workspace:relws/clean")
    assert result == csv.resolve()


def test_ws_resolve_alias_no_prefix_passthrough(tmp_path):
    csv = tmp_path / "data.csv"
    csv.write_text("a,b\n1,2\n")
    assert resolve_alias(str(csv)) == csv.resolve()


def test_ws_resolve_alias_bad_format(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    with pytest.raises(ValueError, match="Invalid alias format"):
        resolve_alias("workspace:noslash")


def test_ws_resolve_alias_workspace_not_found(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    with pytest.raises(FileNotFoundError):
        resolve_alias("workspace:ghost/alias")


def test_ws_resolve_alias_alias_not_found(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    csv = tmp_path / "data.csv"
    csv.write_text("a,b\n1,2\n")
    _make_workspace(tmp_path, "ws_noa", "real_alias", csv)
    with pytest.raises(ValueError, match="not found"):
        resolve_alias("workspace:ws_noa/wrong_alias")


# ---------------------------------------------------------------------------
# is_alias
# ---------------------------------------------------------------------------


def test_ws_is_alias_workspace_prefix():
    assert is_alias("workspace:proj/file") is True


def test_ws_is_alias_project_prefix():
    assert is_alias("project:proj/file") is True


def test_ws_is_alias_absolute_path():
    assert is_alias("/home/user/data.csv") is False


def test_ws_is_alias_relative_path():
    assert is_alias("data/file.csv") is False


# ---------------------------------------------------------------------------
# save_pipeline / load_pipeline
# ---------------------------------------------------------------------------


def test_ws_save_and_load_pipeline(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    _empty_workspace(tmp_path, "pipe_ws")
    ops = [{"op": "drop_duplicates"}, {"op": "fill_nulls", "column": "x", "value": 0}]
    record = save_pipeline("pipe_ws", "clean_pipe", ops, description="My pipeline")
    assert record["name"] == "clean_pipe"
    assert record["op_count"] == 2
    loaded = load_pipeline("pipe_ws", "clean_pipe")
    assert loaded["ops"] == ops


def test_ws_load_pipeline_not_found(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    _empty_workspace(tmp_path, "nopipe_ws")
    with pytest.raises(FileNotFoundError, match="not found"):
        load_pipeline("nopipe_ws", "ghost_pipeline")


# ---------------------------------------------------------------------------
# create_workspace_dirs
# ---------------------------------------------------------------------------


def test_ws_create_workspace_dirs(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    dirs = create_workspace_dirs("newws")
    for key in ("root", "data_raw", "data_working", "data_trials", "reports", "pipelines", "versions"):
        assert Path(dirs[key]).exists(), f"Missing dir: {key}"


def test_ws_create_project_dirs_alias(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    dirs = create_project_dirs("alias_ws")
    assert Path(dirs["root"]).exists()


# ---------------------------------------------------------------------------
# log_pipeline_run
# ---------------------------------------------------------------------------


def test_ws_log_pipeline_run_appends(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    _empty_workspace(tmp_path, "log_ws")
    log_pipeline_run("log_ws", "filter_dataset", "raw", "clean")
    m = load_manifest("log_ws")
    assert len(m["pipeline_history"]) == 1
    assert m["pipeline_history"][0]["op"] == "filter_dataset"


def test_ws_log_pipeline_run_swallows_error(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path))
    log_pipeline_run("nonexistent_ws", "op", "in", "out")  # must not raise


# ---------------------------------------------------------------------------
# handover — make_context
# ---------------------------------------------------------------------------


def test_ws_make_context_fields():
    ctx = make_context("merge_datasets", "Merged A+B -> 500 rows")
    assert ctx["op"] == "merge_datasets"
    assert "500 rows" in ctx["summary"]
    assert ctx["artifacts"] == []
    assert "timestamp" in ctx


def test_ws_make_context_with_artifacts():
    arts = [{"type": "csv", "path": "/out/merged.csv", "role": "output"}]
    ctx = make_context("merge_datasets", "ok", artifacts=arts)
    assert ctx["artifacts"] == arts


# ---------------------------------------------------------------------------
# handover — make_handover
# ---------------------------------------------------------------------------


def test_ws_make_handover_step_normalization():
    h = make_handover("inspect", [])
    assert h["workflow_step"] == "INSPECT"
    assert h["workflow_next"] == "CLEAN"


def test_ws_make_handover_full_chain():
    for i, step in enumerate(WORKFLOW_STEPS[:-1]):
        h = make_handover(step, [])
        assert h["workflow_next"] == WORKFLOW_STEPS[i + 1]


def test_ws_make_handover_last_step_no_next():
    h = make_handover("REPORT", [])
    assert h["workflow_next"] == ""


def test_ws_make_handover_suggested_next_dict():
    suggestions = [
        {"tool": "check_data_quality", "server": "ml_medium", "domain": "ml", "reason": "verify ML readiness"}
    ]
    h = make_handover("PREPARE", suggestions, carry_forward={"file_path": "/out/data.csv"})
    assert h["suggested_next"][0]["tool"] == "check_data_quality"
    assert h["suggested_next"][0]["domain"] == "ml"
    assert h["carry_forward"]["file_path"] == "/out/data.csv"


def test_ws_make_handover_carry_forward_default_empty():
    h = make_handover("COLLECT", [])
    assert h["carry_forward"] == {}


def test_ws_workflow_steps_order():
    assert WORKFLOW_STEPS == ["COLLECT", "INSPECT", "CLEAN", "PREPARE", "TRAIN", "EVALUATE", "REPORT"]


def test_ws_domain_servers_content():
    assert DOMAIN_SERVERS["data"] == "MCP_Data_Analyst"
    assert DOMAIN_SERVERS["ml"] == "MCP_Machine_Learning"


def test_ws_step_tools_all_steps_covered():
    for step in WORKFLOW_STEPS:
        assert step in STEP_TOOLS
        assert len(STEP_TOOLS[step]) > 0
