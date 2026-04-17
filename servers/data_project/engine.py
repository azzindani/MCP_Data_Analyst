"""T0 data_project engine — all project management logic. Zero MCP imports."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
for _p in (str(_ROOT),):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from shared.progress import fail, info, ok, warn  # noqa: F401
from shared.project_utils import (
    create_manifest,
    create_project_dirs,
    get_project_dir,
    load_manifest,
    load_pipeline,
    log_pipeline_run,
    register_file as _register_file_util,
    resolve_alias,
    save_manifest,
    save_pipeline as _save_pipeline_util,
)

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stderr, level=logging.WARNING)


def _token_estimate(obj: object) -> int:
    return len(str(obj)) // 4


def _manifest_exists(project_dir: Path) -> bool:
    """Return True if any manifest file (workspace.json or project.json) exists."""
    return (project_dir / "workspace.json").exists() or (project_dir / "project.json").exists()


# ---------------------------------------------------------------------------
# create_project
# ---------------------------------------------------------------------------


def create_project(name: str, description: str = "", base_dir: str = "") -> dict:
    """Create project workspace with standard directories and manifest."""
    progress = []
    try:
        project_dir = get_project_dir(name, base_dir)
        if _manifest_exists(project_dir):
            return {
                "success": False,
                "error": f"Project '{name}' already exists at {project_dir}",
                "hint": "Use open_project() to load an existing project.",
                "progress": [fail("Project exists", name)],
                "token_estimate": 20,
            }
        dirs = create_project_dirs(name, base_dir)
        manifest = create_manifest(name, description, base_dir)
        progress.append(ok(f"Created project '{name}'", str(project_dir)))
        progress.append(info("Directories", "data/raw  data/working  data/trials  reports  pipelines"))
        result = {
            "success": True,
            "op": "create_project",
            "name": name,
            "project_dir": str(project_dir),
            "directories": dirs,
            "manifest": manifest,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result
    except Exception as exc:
        logger.exception("create_project error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check that base_dir is writable and name contains no special characters.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# open_project
# ---------------------------------------------------------------------------


def open_project(name: str, base_dir: str = "") -> dict:
    """Open project: returns aliases, pipeline history, active file."""
    progress = []
    try:
        manifest = load_manifest(name, base_dir)
        files = manifest.get("files", {})
        aliases = {
            alias: {
                "path": info_["path"],
                "stage": info_["stage"],
                "rows": info_.get("rows", -1),
            }
            for alias, info_ in files.items()
        }
        history = manifest.get("pipeline_history", [])[-10:]
        pipelines = list(manifest.get("pipelines", {}).keys())
        progress.append(ok(f"Opened project '{name}'", f"{len(aliases)} files registered"))
        result = {
            "success": True,
            "op": "open_project",
            "name": name,
            "description": manifest.get("description", ""),
            "created": manifest.get("created", ""),
            "active_file": manifest.get("active_file"),
            "file_count": len(aliases),
            "aliases": aliases,
            "saved_pipelines": pipelines,
            "recent_history": history,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result
    except FileNotFoundError as exc:
        return {
            "success": False,
            "error": str(exc),
            "hint": "Use create_project() to create a new project first.",
            "progress": [fail("Project not found", name)],
            "token_estimate": 20,
        }
    except Exception as exc:
        logger.exception("open_project error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check that base_dir matches the one used when creating the project.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# register_file
# ---------------------------------------------------------------------------


def register_file(
    project_name: str,
    file_path: str,
    alias: str,
    stage: str = "raw",
    set_active: bool = False,
    base_dir: str = "",
) -> dict:
    """Add file to project manifest with alias. stage: raw working trial output."""
    progress = []
    try:
        manifest = _register_file_util(project_name, file_path, alias, stage, base_dir)
        if set_active:
            manifest["active_file"] = alias
            save_manifest(manifest, project_name, base_dir)
        file_info = manifest["files"][alias]
        progress.append(ok(f"Registered '{alias}'", f"stage={stage}  rows={file_info.get('rows', '?')}"))
        result = {
            "success": True,
            "op": "register_file",
            "project": project_name,
            "alias": alias,
            "stage": stage,
            "file_info": file_info,
            "active_file": manifest.get("active_file"),
            "total_files": len(manifest.get("files", {})),
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result
    except FileNotFoundError as exc:
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check that file_path is an absolute path to an existing file.",
            "progress": [fail("File not found", file_path)],
            "token_estimate": 20,
        }
    except Exception as exc:
        logger.exception("register_file error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Valid stages: raw working trial output.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# list_project_files
# ---------------------------------------------------------------------------


def list_project_files(project_name: str, stage: str = "", base_dir: str = "") -> dict:
    """List all project files with alias, stage, size, row count."""
    progress = []
    try:
        manifest = load_manifest(project_name, base_dir)
        files = manifest.get("files", {})
        rows_list = []
        for alias, info_ in files.items():
            if stage and info_.get("stage") != stage:
                continue
            rows_list.append(
                {
                    "alias": alias,
                    "stage": info_.get("stage", "?"),
                    "path": info_.get("path", "?"),
                    "rows": info_.get("rows", -1),
                    "size_bytes": info_.get("size_bytes", -1),
                    "registered": info_.get("registered", ""),
                }
            )
        progress.append(ok(f"Listed files for '{project_name}'", f"{len(rows_list)} files"))
        result = {
            "success": True,
            "op": "list_project_files",
            "project": project_name,
            "filter_stage": stage or "all",
            "active_file": manifest.get("active_file"),
            "count": len(rows_list),
            "files": rows_list,
            "saved_pipelines": list(manifest.get("pipelines", {}).keys()),
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result
    except FileNotFoundError as exc:
        return {
            "success": False,
            "error": str(exc),
            "hint": "Use create_project() first, then register_file() to add files.",
            "progress": [fail("Project not found", project_name)],
            "token_estimate": 20,
        }
    except Exception as exc:
        logger.exception("list_project_files error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check project_name and base_dir.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# save_pipeline
# ---------------------------------------------------------------------------


def save_pipeline(
    project_name: str,
    pipeline_name: str,
    ops: list[dict],
    description: str = "",
    base_dir: str = "",
) -> dict:
    """Save named pipeline template (list of apply_patch op dicts)."""
    progress = []
    try:
        record = _save_pipeline_util(project_name, pipeline_name, ops, description, base_dir)
        progress.append(ok(f"Saved pipeline '{pipeline_name}'", f"{len(ops)} ops"))
        result = {
            "success": True,
            "op": "save_pipeline",
            "project": project_name,
            "pipeline_name": pipeline_name,
            "op_count": len(ops),
            "description": description,
            "created": record.get("created", ""),
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result
    except Exception as exc:
        logger.exception("save_pipeline error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "ops must be a list of apply_patch op dicts. Use list_patch_ops() for reference.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# run_saved_pipeline
# ---------------------------------------------------------------------------


def run_saved_pipeline(
    project_name: str,
    pipeline_name: str,
    input_alias: str,
    output_alias: str,
    output_stage: str = "working",
    base_dir: str = "",
    dry_run: bool = False,
) -> dict:
    """Execute saved pipeline on file alias. Produces new output alias."""
    progress = []
    try:
        record = load_pipeline(project_name, pipeline_name, base_dir)
        ops = record.get("ops", [])
        if not ops:
            return {
                "success": False,
                "error": f"Pipeline '{pipeline_name}' has no ops.",
                "hint": "Use save_pipeline() to define ops before running.",
                "progress": [fail("Empty pipeline", pipeline_name)],
                "token_estimate": 20,
            }

        # Resolve input alias — support both workspace: and project: prefix
        input_alias_str = f"workspace:{project_name}/{input_alias}"
        input_path = resolve_alias(input_alias_str, base_dir)
        if not input_path.exists():
            return {
                "success": False,
                "error": f"Input file for alias '{input_alias}' not found: {input_path}",
                "hint": "Check that the file exists and the alias is registered.",
                "progress": [fail("Input not found", str(input_alias))],
                "token_estimate": 20,
            }

        manifest = load_manifest(project_name, base_dir)
        project_dir = get_project_dir(project_name, base_dir)
        stage_dir = {
            "working": project_dir / "data" / "working",
            "trial": project_dir / "data" / "trials",
            "output": project_dir / "reports",
        }.get(output_stage, project_dir / "data" / "working")
        stage_dir.mkdir(parents=True, exist_ok=True)
        output_path = stage_dir / f"{output_alias}.csv"

        if dry_run:
            progress.append(info("Dry run", f"Would apply {len(ops)} ops from '{pipeline_name}'"))
            result = {
                "success": True,
                "dry_run": True,
                "op": "run_saved_pipeline",
                "pipeline_name": pipeline_name,
                "input_alias": input_alias,
                "output_alias": output_alias,
                "would_apply": len(ops),
                "ops_preview": ops,
                "output_path": str(output_path),
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        try:
            _basic_root = str(Path(__file__).resolve().parents[1] / "data_basic")
            if _basic_root not in sys.path:
                sys.path.insert(0, _basic_root)
            import importlib

            _basic_engine = importlib.import_module("engine")
            patch_result = _basic_engine.apply_patch(str(input_path), ops, dry_run=False)
        except ImportError:
            return {
                "success": False,
                "error": "data_basic server not available for pipeline execution.",
                "hint": "Ensure data_basic is installed in the same Python environment.",
                "progress": [fail("Import error", "data_basic.engine")],
                "token_estimate": 20,
            }

        if not patch_result.get("success"):
            return {
                "success": False,
                "error": f"Pipeline failed: {patch_result.get('error', 'unknown')}",
                "hint": patch_result.get("hint", "Check ops for errors."),
                "progress": [fail("Pipeline failed", pipeline_name)],
                "token_estimate": 20,
            }

        import shutil

        shutil.copy2(str(input_path), str(output_path))
        _basic_engine.apply_patch(str(output_path), ops, dry_run=False)

        _register_file_util(project_name, str(output_path), output_alias, output_stage, base_dir)
        log_pipeline_run(project_name, pipeline_name, input_alias, output_alias, base_dir)

        progress.append(ok(f"Ran pipeline '{pipeline_name}'", f"{len(ops)} ops applied"))
        progress.append(ok(f"Output registered as '{output_alias}'", Path(output_path).name))

        result = {
            "success": True,
            "op": "run_saved_pipeline",
            "pipeline_name": pipeline_name,
            "input_alias": input_alias,
            "output_alias": output_alias,
            "ops_applied": len(ops),
            "output_path": str(output_path),
            "backup": patch_result.get("backup", ""),
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except FileNotFoundError as exc:
        return {
            "success": False,
            "error": str(exc),
            "hint": "Use list_project_files() to check registered aliases.",
            "progress": [fail("Not found", str(exc))],
            "token_estimate": 20,
        }
    except Exception as exc:
        logger.exception("run_saved_pipeline error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check pipeline_name, input_alias, and project configuration.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


__all__ = [
    "create_project",
    "open_project",
    "register_file",
    "list_project_files",
    "save_pipeline",
    "run_saved_pipeline",
]
