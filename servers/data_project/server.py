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
def create_project(
    name: str,
    description: str = "",
    base_dir: str = "",
) -> dict:
    """Create project workspace. Sets up data/working/trial/report dirs."""
    return engine.create_project(name, description, base_dir)


@mcp.tool()
def open_project(
    name: str,
    base_dir: str = "",
) -> dict:
    """Open project. Returns file aliases, pipeline history, active file."""
    return engine.open_project(name, base_dir)


@mcp.tool()
def register_file(
    project_name: str,
    file_path: str,
    alias: str,
    stage: str = "raw",
    set_active: bool = False,
    base_dir: str = "",
) -> dict:
    """Add file to project with alias. stage: raw working trial output."""
    return engine.register_file(project_name, file_path, alias, stage, set_active, base_dir)


@mcp.tool()
def list_project_files(
    project_name: str,
    stage: str = "",
    base_dir: str = "",
) -> dict:
    """List all project files with alias, stage, size, row count."""
    return engine.list_project_files(project_name, stage, base_dir)


@mcp.tool()
def save_pipeline(
    project_name: str,
    pipeline_name: str,
    ops: list[dict],
    description: str = "",
    base_dir: str = "",
) -> dict:
    """Save named pipeline template. ops: list of apply_patch op dicts."""
    return engine.save_pipeline(project_name, pipeline_name, ops, description, base_dir)


@mcp.tool()
def run_saved_pipeline(
    project_name: str,
    pipeline_name: str,
    input_alias: str,
    output_alias: str,
    output_stage: str = "working",
    base_dir: str = "",
    dry_run: bool = False,
) -> dict:
    """Execute saved pipeline on file alias. Creates new output alias."""
    return engine.run_saved_pipeline(
        project_name,
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
