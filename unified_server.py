"""Combined HTTP entry point — all 7 sub-servers in ONE process, ONE port.

Each sub-server keeps its own server.py for stdio / individual-HTTP use
(LM Studio "add one sub-server" installs, local dev). This file is
Docker/remote-deployment-only: it imports each sub-server's already-built
FastMCP instance and mounts its HTTP app at its own path prefix inside one
Starlette app, so pandas/numpy/scipy/matplotlib load ONCE instead of seven
times. Each sub-server's own /health, /version, and /mcp routes (added via
@mcp.custom_route in its own server.py) come along for free under the
mount prefix — nothing sub-server-specific is duplicated here.

data_advanced (retired stub, 0 tools) and data_project (a redirect alias to
data_workspace) are intentionally excluded — not distinct deployments, see
docker-compose.yml's own comment for the same exclusion.

Lifespans do NOT propagate through Starlette's Mount() automatically, so
each sub-server's session-manager lifespan is entered explicitly via
AsyncExitStack — verified live against real sub-servers before wiring this
up for real (see MCP_Machine_Learning's unified_server.py, prototyped and
proven first).
"""

from __future__ import annotations

import argparse
import os
from contextlib import AsyncExitStack, asynccontextmanager

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from servers.data_basic.server import mcp as basic_mcp
from servers.data_ingest.server import mcp as ingest_mcp
from servers.data_medium.server import mcp as medium_mcp
from servers.data_statistics.server import mcp as statistics_mcp
from servers.data_transform.server import mcp as transform_mcp
from servers.data_visual.server import mcp as visual_mcp
from servers.data_workspace.server import mcp as workspace_mcp

_VERSION = "0.2.0"

_SUB_SERVERS = {
    "basic": basic_mcp,
    "medium": medium_mcp,
    "statistics": statistics_mcp,
    "transform": transform_mcp,
    "visual": visual_mcp,
    "workspace": workspace_mcp,
    "ingest": ingest_mcp,
}
_sub_apps = {name: mcp.http_app(path="/mcp") for name, mcp in _SUB_SERVERS.items()}


@asynccontextmanager
async def _combined_lifespan(app):
    async with AsyncExitStack() as stack:
        for sub_app in _sub_apps.values():
            await stack.enter_async_context(sub_app.lifespan(sub_app))
        yield


async def _root_health(request: Request) -> JSONResponse:
    """Aggregate liveness check. Unauthenticated."""
    return JSONResponse({"status": "ok", "version": _VERSION, "sub_servers": list(_SUB_SERVERS)})


async def _root_version(request: Request) -> JSONResponse:
    """Report running version. Unauthenticated."""
    return JSONResponse({"current": _VERSION})


async def _root(request: Request) -> JSONResponse:
    return JSONResponse(
        {
            "server": "MCP_Data_Analyst",
            "sub_servers": {name: f"/{name}/mcp" for name in _SUB_SERVERS},
        }
    )


app = Starlette(
    routes=[
        Route("/health", _root_health),
        Route("/version", _root_version),
        Route("/", _root),
        *(Mount(f"/{name}", app=sub_app) for name, sub_app in _sub_apps.items()),
    ],
    lifespan=_combined_lifespan,
)


def main() -> None:
    import uvicorn

    parser = argparse.ArgumentParser(description="MCP_Data_Analyst unified server")
    parser.add_argument("--host", default=os.environ.get("DA_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("DA_PORT", "8810")))
    args = parser.parse_args()
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
