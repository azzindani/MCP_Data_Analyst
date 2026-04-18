"""data_workspace engine — project ops with context+handover. Zero MCP imports."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

logging.basicConfig(stream=sys.stderr, level=logging.WARNING)
logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from servers.data_project import engine as _project
from shared.handover import make_context, make_handover


def create_workspace(name: str, description: str = "", base_dir: str = "") -> dict:
    result = _project.create_project(name, description, base_dir)
    if result.get("success"):
        result["context"] = make_context(
            "create_workspace",
            f"Created workspace '{name}' with standard directory structure.",
        )
        result["handover"] = make_handover(
            "COLLECT",
            [
                {
                    "tool": "load_dataset",
                    "server": "data_basic",
                    "domain": "data",
                    "reason": "load a CSV into this workspace",
                },
                {
                    "tool": "register_workspace_file",
                    "server": "data_workspace",
                    "domain": "data",
                    "reason": "register an existing file",
                },
            ],
            carry_forward={"workspace_name": name, "base_dir": base_dir},
        )
    return result


def open_workspace(name: str, base_dir: str = "") -> dict:
    result = _project.open_project(name, base_dir)
    if result.get("success"):
        file_count = result.get("file_count", 0)
        result["context"] = make_context(
            "open_workspace",
            f"Opened workspace '{name}'. {file_count} registered file(s).",
        )
        result["handover"] = make_handover(
            "COLLECT",
            [
                {
                    "tool": "inspect_dataset",
                    "server": "data_basic",
                    "domain": "data",
                    "reason": "inspect a registered file",
                },
                {
                    "tool": "filter_dataset",
                    "server": "data_transform",
                    "domain": "data",
                    "reason": "filter a registered file",
                },
            ],
            carry_forward={"workspace_name": name},
        )
    return result


def register_workspace_file(
    workspace_name: str,
    file_path: str,
    alias: str,
    stage: str = "raw",
    set_active: bool = False,
    base_dir: str = "",
) -> dict:
    result = _project.register_file(workspace_name, file_path, alias, stage, set_active, base_dir)
    if result.get("success"):
        result["context"] = make_context(
            "register_workspace_file",
            f"Registered '{Path(file_path).name}' as '{alias}' (stage={stage}) in '{workspace_name}'.",
            artifacts=[{"type": "csv", "path": file_path, "alias": alias, "role": "registered"}],
        )
        result["handover"] = make_handover(
            "COLLECT",
            [
                {
                    "tool": "inspect_dataset",
                    "server": "data_basic",
                    "domain": "data",
                    "reason": "inspect the registered file",
                },
                {
                    "tool": "auto_detect_schema",
                    "server": "data_statistics",
                    "domain": "data",
                    "reason": "validate schema before processing",
                },
            ],
            carry_forward={"file_path": f"workspace:{workspace_name}/{alias}"},
        )
    return result


def list_workspace_files(workspace_name: str, stage: str = "", base_dir: str = "") -> dict:
    return _project.list_project_files(workspace_name, stage, base_dir)


def save_workspace_pipeline(
    workspace_name: str,
    pipeline_name: str,
    ops: list[dict],
    description: str = "",
    base_dir: str = "",
) -> dict:
    result = _project.save_pipeline(workspace_name, pipeline_name, ops, description, base_dir)
    if result.get("success"):
        result["context"] = make_context(
            "save_workspace_pipeline",
            f"Saved pipeline '{pipeline_name}' ({len(ops)} ops) in workspace '{workspace_name}'.",
        )
        result["handover"] = make_handover(
            "PREPARE",
            [
                {
                    "tool": "run_workspace_pipeline",
                    "server": "data_workspace",
                    "domain": "data",
                    "reason": "execute the saved pipeline",
                },
            ],
            carry_forward={"workspace_name": workspace_name, "pipeline_name": pipeline_name},
        )
    return result


def run_workspace_pipeline(
    workspace_name: str,
    pipeline_name: str,
    input_alias: str,
    output_alias: str,
    output_stage: str = "working",
    base_dir: str = "",
    dry_run: bool = False,
) -> dict:
    result = _project.run_saved_pipeline(
        workspace_name,
        pipeline_name,
        input_alias,
        output_alias,
        output_stage,
        base_dir,
        dry_run,
    )
    if result.get("success") and not dry_run:
        out_path = result.get("output_path", "")
        result["context"] = make_context(
            "run_workspace_pipeline",
            f"Pipeline '{pipeline_name}': '{input_alias}' -> '{output_alias}' in '{workspace_name}'.",
            artifacts=[{"type": "csv", "path": out_path, "alias": output_alias, "role": "output"}] if out_path else [],
        )
        result["handover"] = make_handover(
            "CLEAN",
            [
                {
                    "tool": "inspect_dataset",
                    "server": "data_basic",
                    "domain": "data",
                    "reason": "verify output quality",
                },
                {
                    "tool": "run_preprocessing",
                    "server": "ml_medium",
                    "domain": "ml",
                    "reason": "hand off to ML for preprocessing",
                },
            ],
            carry_forward={"file_path": f"workspace:{workspace_name}/{output_alias}"},
        )
    return result
