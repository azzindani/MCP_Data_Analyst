"""Ring-2 shared utility — project manifest I/O and alias resolution.

Alias syntax: "project:project_name/alias"
  → resolves to absolute path via project.json manifest.

All I/O errors are raised; callers must catch them.
No MCP imports. No stdout writes.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PROJECTS_ROOT_ENV = "MCP_PROJECTS_DIR"
_DEFAULT_PROJECTS_DIR = Path.home() / "mcp_projects"
_MANIFEST_FILENAME = "project.json"
_ALIAS_PREFIX = "project:"


# ---------------------------------------------------------------------------
# Project root resolution
# ---------------------------------------------------------------------------


def get_projects_root(base_dir: str = "") -> Path:
    """Return root directory that holds all projects.

    Priority: base_dir arg → MCP_PROJECTS_DIR env → ~/mcp_projects
    """
    import os

    if base_dir:
        return Path(base_dir).expanduser().resolve()
    env_dir = os.environ.get(_PROJECTS_ROOT_ENV, "")
    if env_dir:
        return Path(env_dir).expanduser().resolve()
    return _DEFAULT_PROJECTS_DIR


def get_project_dir(name: str, base_dir: str = "") -> Path:
    """Return directory for a named project."""
    return get_projects_root(base_dir) / name


# ---------------------------------------------------------------------------
# Manifest read / write
# ---------------------------------------------------------------------------


def load_manifest(project_name: str, base_dir: str = "") -> dict:
    """Load project.json for project_name. Raises FileNotFoundError if absent."""
    manifest_path = get_project_dir(project_name, base_dir) / _MANIFEST_FILENAME
    if not manifest_path.exists():
        raise FileNotFoundError(f"Project '{project_name}' not found. Expected manifest at: {manifest_path}")
    with manifest_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_manifest(manifest: dict, project_name: str, base_dir: str = "") -> None:
    """Write manifest dict to project.json atomically."""
    import shutil
    import tempfile

    project_dir = get_project_dir(project_name, base_dir)
    manifest_path = project_dir / _MANIFEST_FILENAME
    tmp_fd, tmp_path = tempfile.mkstemp(dir=project_dir, suffix=".tmp")
    try:
        with open(tmp_fd, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
        shutil.move(tmp_path, manifest_path)
    except Exception:
        Path(tmp_path).unlink(missing_ok=True)
        raise


def create_manifest(
    name: str,
    description: str = "",
    base_dir: str = "",
) -> dict:
    """Create and save a fresh project manifest. Returns the manifest dict."""
    now = datetime.now(UTC).isoformat()
    manifest = {
        "name": name,
        "description": description,
        "created": now,
        "updated": now,
        "files": {},
        "active_file": None,
        "pipeline_history": [],
        "pipelines": {},
    }
    save_manifest(manifest, name, base_dir)
    return manifest


# ---------------------------------------------------------------------------
# File registration / alias management
# ---------------------------------------------------------------------------


def register_file(
    project_name: str,
    file_path: str,
    alias: str,
    stage: str = "raw",
    base_dir: str = "",
) -> dict:
    """Add a file alias to the project manifest. Returns updated manifest."""
    valid_stages = {"raw", "working", "trial", "output"}
    if stage not in valid_stages:
        raise ValueError(f"Invalid stage '{stage}'. Valid: {', '.join(sorted(valid_stages))}")

    path = Path(file_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    manifest = load_manifest(project_name, base_dir)
    project_dir = get_project_dir(project_name, base_dir)

    # Store relative path when inside the project dir, absolute otherwise
    try:
        rel = path.relative_to(project_dir)
        stored_path = str(rel)
    except ValueError:
        stored_path = str(path)

    # Count rows cheaply (count newlines minus header)
    try:
        row_count = sum(1 for _ in path.open("r", encoding="utf-8")) - 1
    except Exception:
        row_count = -1

    file_size = path.stat().st_size

    manifest["files"][alias] = {
        "path": stored_path,
        "stage": stage,
        "rows": row_count,
        "size_bytes": file_size,
        "registered": datetime.now(UTC).isoformat(),
    }
    manifest["updated"] = datetime.now(UTC).isoformat()
    save_manifest(manifest, project_name, base_dir)
    return manifest


# ---------------------------------------------------------------------------
# Alias resolution
# ---------------------------------------------------------------------------


def resolve_alias(alias_str: str, base_dir: str = "") -> Path:
    """Resolve 'project:project_name/alias' to an absolute Path.

    If alias_str does not start with 'project:', returns Path(alias_str).
    Raises ValueError / FileNotFoundError on lookup failures.
    """
    if not alias_str.startswith(_ALIAS_PREFIX):
        return Path(alias_str).expanduser().resolve()

    rest = alias_str[len(_ALIAS_PREFIX) :]
    if "/" not in rest:
        raise ValueError(f"Invalid alias format '{alias_str}'. Expected 'project:project_name/alias'.")
    project_name, file_alias = rest.split("/", 1)
    manifest = load_manifest(project_name, base_dir)
    files = manifest.get("files", {})
    if file_alias not in files:
        available = list(files.keys())
        raise ValueError(f"Alias '{file_alias}' not found in project '{project_name}'. Available: {available}")
    stored_path = files[file_alias]["path"]
    project_dir = get_project_dir(project_name, base_dir)
    # Stored path may be relative (within project) or absolute
    candidate = Path(stored_path)
    if not candidate.is_absolute():
        candidate = project_dir / candidate
    return candidate.resolve()


def is_alias(path_or_alias: str) -> bool:
    """Return True if string uses the project:name/alias syntax."""
    return path_or_alias.startswith(_ALIAS_PREFIX)


# ---------------------------------------------------------------------------
# Pipeline management
# ---------------------------------------------------------------------------


def save_pipeline(
    project_name: str,
    pipeline_name: str,
    ops: list[dict],
    description: str = "",
    base_dir: str = "",
) -> dict:
    """Save a named pipeline template to the project manifest."""
    manifest = load_manifest(project_name, base_dir)
    project_dir = get_project_dir(project_name, base_dir)
    pipelines_dir = project_dir / "pipelines"
    pipelines_dir.mkdir(parents=True, exist_ok=True)

    pipeline_record = {
        "name": pipeline_name,
        "description": description,
        "ops": ops,
        "created": datetime.now(UTC).isoformat(),
        "op_count": len(ops),
    }

    # Persist as standalone JSON file for easy inspection
    pipeline_path = pipelines_dir / f"{pipeline_name}.json"
    import shutil  # noqa: E401
    import tempfile

    tmp_fd, tmp_path = tempfile.mkstemp(dir=pipelines_dir, suffix=".tmp")
    try:
        with open(tmp_fd, "w", encoding="utf-8") as f:
            json.dump(pipeline_record, f, indent=2, ensure_ascii=False)
        shutil.move(tmp_path, pipeline_path)
    except Exception:
        Path(tmp_path).unlink(missing_ok=True)
        raise

    # Also index in manifest
    manifest.setdefault("pipelines", {})[pipeline_name] = {
        "file": str(pipeline_path.relative_to(project_dir)),
        "description": description,
        "op_count": len(ops),
        "created": pipeline_record["created"],
    }
    manifest["updated"] = datetime.now(UTC).isoformat()
    save_manifest(manifest, project_name, base_dir)
    return pipeline_record


def load_pipeline(
    project_name: str,
    pipeline_name: str,
    base_dir: str = "",
) -> dict:
    """Load a named pipeline template from the project directory."""
    project_dir = get_project_dir(project_name, base_dir)
    pipeline_path = project_dir / "pipelines" / f"{pipeline_name}.json"
    if not pipeline_path.exists():
        manifest = load_manifest(project_name, base_dir)
        available = list(manifest.get("pipelines", {}).keys())
        raise FileNotFoundError(
            f"Pipeline '{pipeline_name}' not found in project '{project_name}'. Available: {available}"
        )
    with pipeline_path.open("r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Project directory scaffolding
# ---------------------------------------------------------------------------


def create_project_dirs(project_name: str, base_dir: str = "") -> dict:
    """Create standard project directory structure. Returns paths dict."""
    project_dir = get_project_dir(project_name, base_dir)
    dirs = {
        "root": project_dir,
        "data_raw": project_dir / "data" / "raw",
        "data_working": project_dir / "data" / "working",
        "data_trials": project_dir / "data" / "trials",
        "reports": project_dir / "reports",
        "pipelines": project_dir / "pipelines",
        "versions": project_dir / ".versions",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return {k: str(v) for k, v in dirs.items()}


# ---------------------------------------------------------------------------
# Pipeline history logging
# ---------------------------------------------------------------------------


def log_pipeline_run(
    project_name: str,
    op: str,
    input_alias: str,
    output_alias: str,
    base_dir: str = "",
) -> None:
    """Append a pipeline run record to the project manifest history."""
    try:
        manifest = load_manifest(project_name, base_dir)
        manifest.setdefault("pipeline_history", []).append(
            {
                "ts": datetime.now(UTC).isoformat(),
                "op": op,
                "input": input_alias,
                "output": output_alias,
            }
        )
        manifest["updated"] = datetime.now(UTC).isoformat()
        save_manifest(manifest, project_name, base_dir)
    except Exception:
        pass  # Never raise from logging


__all__ = [
    "get_projects_root",
    "get_project_dir",
    "load_manifest",
    "save_manifest",
    "create_manifest",
    "register_file",
    "resolve_alias",
    "is_alias",
    "save_pipeline",
    "load_pipeline",
    "create_project_dirs",
    "log_pipeline_run",
]
