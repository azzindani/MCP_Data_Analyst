"""T0 data_project MCP server — thin wrapper only. Zero domain logic."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

logging.basicConfig(stream=sys.stderr, level=logging.WARNING)

from fastmcp import FastMCP

try:
    from . import engine
except ImportError:
    _root = str(Path(__file__).resolve().parents[2])
    if _root not in sys.path:
        sys.path.insert(0, _root)
    import engine

mcp = FastMCP("data_project")


@mcp.tool()
def create_workspace(name: str, description: str = "", base_dir: str = "") -> dict:
    """Create workspace with data/working/trial/report dirs."""
    return engine.create_workspace(name, description, base_dir)


@mcp.tool()
def open_workspace(name: str, base_dir: str = "") -> dict:
    """Open workspace. Returns file aliases, pipeline history, active file."""
    return engine.open_workspace(name, base_dir)


@mcp.tool()
def register_workspace_file(
    workspace_name: str,
    file_path: str,
    alias: str,
    stage: str = "raw",
    set_active: bool = False,
    base_dir: str = "",
) -> dict:
    """Add file to workspace with alias. stage: raw working trial output."""
    return engine.register_workspace_file(workspace_name, file_path, alias, stage, set_active, base_dir)


@mcp.tool()
def list_workspace_files(workspace_name: str, stage: str = "", base_dir: str = "") -> dict:
    """List all workspace files with alias, stage, size, row count."""
    return engine.list_workspace_files(workspace_name, stage, base_dir)


@mcp.tool()
def save_workspace_pipeline(
    workspace_name: str,
    pipeline_name: str,
    ops: list[dict],
    description: str = "",
    base_dir: str = "",
) -> dict:
    """Save named pipeline template. ops: list of apply_patch op dicts."""
    return engine.save_workspace_pipeline(workspace_name, pipeline_name, ops, description, base_dir)


@mcp.tool()
def run_workspace_pipeline(
    workspace_name: str,
    pipeline_name: str,
    input_alias: str,
    output_alias: str,
    output_stage: str = "working",
    base_dir: str = "",
    dry_run: bool = False,
) -> dict:
    """Execute saved pipeline on file alias. Creates new output alias."""
    return engine.run_workspace_pipeline(
        workspace_name,
        pipeline_name,
        input_alias,
        output_alias,
        output_stage,
        base_dir,
        dry_run,
    )


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
