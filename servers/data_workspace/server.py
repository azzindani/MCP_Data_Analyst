"""T0 data_workspace MCP server — thin wrapper only. Zero domain logic."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(stream=sys.stderr, level=logging.WARNING)

_root = str(Path(__file__).resolve().parents[2])
if _root not in sys.path:
    sys.path.insert(0, _root)

from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from shared.arg_alias import missing, pick
from shared.deploy_auth import build_oauth_bridge, build_token_verifier
from shared.tool_annotations import CREATES, READS

try:
    from . import engine
except ImportError:
    import engine

_VERSION = "0.2.1"  # keep in sync with pyproject.toml [project].version

_oauth_bridge = build_oauth_bridge(
    "DA", state_dir=os.environ.get("DA_WORKSPACE_OAUTH_STATE_DIR", "/tmp/data-workspace-oauth-state")
)
_public_origin = os.environ.get("DA_PUBLIC_URL", "").rstrip("/")
_base_url = f"{_public_origin}/workspace" if _public_origin else None
mcp = FastMCP("data_workspace", auth=build_token_verifier("DA", _oauth_bridge, base_url=_base_url))
if _oauth_bridge is not None:
    _oauth_bridge.register_routes(mcp)


@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request) -> JSONResponse:
    """Liveness check. Unauthenticated."""
    return JSONResponse({"status": "ok", "version": _VERSION})


@mcp.custom_route("/version", methods=["GET"])
async def version(request: Request) -> JSONResponse:
    """Report running version. Unauthenticated."""
    return JSONResponse({"current": _VERSION})


# Four of this server's six tools name the workspace `workspace_name`; these two
# -- the first two anyone calls -- said `name`. A caller who reads
# list_workspace_files(workspace_name=...) and then writes the same spelling here
# is refused by pydantic before any server code runs, so the tool never gets to
# say what it wanted. Both spellings work; `name` keeps its position so no
# positional caller is rebound.
@mcp.tool(annotations=CREATES)
def create_workspace(name: str = "", description: str = "", base_dir: str = "", workspace_name: str = "") -> dict:
    """Create workspace with data/working/trial/report dirs."""
    chosen, _ = pick("create_workspace", "workspace_name", name, workspace_name)
    if not chosen:
        return missing("create_workspace", "workspace_name", "name")
    return engine.create_workspace(chosen, description, base_dir)


@mcp.tool(annotations=READS)
def open_workspace(name: str = "", base_dir: str = "", workspace_name: str = "") -> dict:
    """Open workspace. Returns file aliases, pipeline history, active file."""
    chosen, _ = pick("open_workspace", "workspace_name", name, workspace_name)
    if not chosen:
        return missing("open_workspace", "workspace_name", "name")
    return engine.open_workspace(chosen, base_dir)


@mcp.tool(annotations=CREATES)
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


@mcp.tool(annotations=READS)
def list_workspace_files(workspace_name: str, stage: str = "", base_dir: str = "") -> dict:
    """List all workspace files with alias, stage, size, row count."""
    return engine.list_workspace_files(workspace_name, stage, base_dir)


@mcp.tool(annotations=CREATES)
def save_workspace_pipeline(
    workspace_name: str,
    pipeline_name: str,
    ops: list[dict],
    description: str = "",
    base_dir: str = "",
) -> dict:
    """Save named pipeline template. ops: list of apply_patch op dicts."""
    return engine.save_workspace_pipeline(workspace_name, pipeline_name, ops, description, base_dir)


@mcp.tool(annotations=CREATES)
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
    parser = argparse.ArgumentParser(description="data_workspace MCP Server")
    parser.add_argument(
        "--transport", choices=["stdio", "http"], default=os.environ.get("DA_WORKSPACE_TRANSPORT", "stdio")
    )
    parser.add_argument("--host", default=os.environ.get("DA_WORKSPACE_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("DA_WORKSPACE_PORT", "8815")))
    args = parser.parse_args()

    if args.transport == "http":
        mcp.run(transport="http", host=args.host, port=args.port)
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
