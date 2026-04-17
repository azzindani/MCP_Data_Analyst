"""Backward-compatible shim — re-exports everything from workspace_utils.

New code should import from shared.workspace_utils directly.
This module exists for compatibility with existing servers and tests.
"""

from __future__ import annotations

from shared.workspace_utils import (  # noqa: F401
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
