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

from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse
from starlette.routing import Mount, Route

from servers.data_basic.server import mcp as basic_mcp
from servers.data_domain.server import _oauth_bridge as _domain_bridge
from servers.data_domain.server import mcp as domain_mcp
from servers.data_ingest.server import mcp as ingest_mcp
from servers.data_medium.server import mcp as medium_mcp
from servers.data_statistics.server import mcp as statistics_mcp
from servers.data_transform.server import mcp as transform_mcp
from servers.data_visual.server import mcp as visual_mcp
from servers.data_workspace.server import mcp as workspace_mcp
from shared.exchange import upload_route

_VERSION = "0.3.0"

_SUB_SERVERS = {
    "basic": basic_mcp,
    "medium": medium_mcp,
    "statistics": statistics_mcp,
    "transform": transform_mcp,
    "visual": visual_mcp,
    "workspace": workspace_mcp,
    "ingest": ingest_mcp,
}
# streamable_http_app() takes no path argument -- the mount path comes from
# mcp.settings.streamable_http_path, which already defaults to "/mcp". And
# lifespans do NOT propagate through Starlette's Mount(), so each sub-server's
# session-manager lifespan is entered explicitly below. The official SDK
# returns a plain Starlette app (fastmcp 2.x returned its own subclass with a
# convenience `.lifespan`), so the lifespan is reached via
# `app.router.lifespan_context`. Same pattern as MCP_Microsoft_Office, which
# has been on the official SDK all along.
# Each sub-server's FastMCP defaults to host="127.0.0.1", which auto-enables
# DNS-rebinding Host-header validation restricted to 127.0.0.1/localhost. The
# unified server sits behind Caddy on a public hostname forwarded via
# `header_up Host {host}`, so that check rejects every real remote request with
# a 421 "Invalid Host header" -- healthy container, working /health, and every
# tool call refused. Caddy is already the trust boundary, so disable it for the
# mounted sub-apps. Same fix as MCP_Microsoft_Office, which hit this first.
for _sub_mcp in (*_SUB_SERVERS.values(), domain_mcp):
    _sub_mcp.settings.transport_security = TransportSecuritySettings(enable_dns_rebinding_protection=False)

_sub_apps = {name: mcp.streamable_http_app() for name, mcp in _SUB_SERVERS.items()}
# The eight domain tools, served at the root: /mcp. Every tier above keeps its
# own endpoint; see servers/data_domain/server.py.
_domain_app = domain_mcp.streamable_http_app()


@asynccontextmanager
async def _combined_lifespan(app):
    async with AsyncExitStack() as stack:
        for sub_app in (*_sub_apps.values(), _domain_app):
            await stack.enter_async_context(sub_app.router.lifespan_context(sub_app))
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
            "mcp": "/mcp",
            "sub_servers": {name: f"/{name}/mcp" for name in _SUB_SERVERS},
        }
    )


def _redirect(target: str):
    """308 redirect to a sub-server's real well-known route.

    RFC 8414/9728 clients build discovery URLs by inserting
    `/.well-known/...` between the origin and the resource/issuer path
    (e.g. `/.well-known/oauth-protected-resource/basic/mcp`), landing at the
    OUTER app's root. But Mount() nests each sub-server's real well-known
    routes under its own prefix (`/basic/.well-known/...`) instead, so the
    client's computed URL 404s without this redirect — confirmed live
    against a real unauthenticated claude.ai connector attempt.
    """

    async def _handler(request: Request) -> RedirectResponse:
        return RedirectResponse(target, status_code=308)

    return _handler


# Both protected-resource paths lead to the tier's metadata. A client derives
# .../oauth-protected-resource/<tier>/mcp from the URL it connects to (RFC
# 9728), and the tier's own 401 names .../oauth-protected-resource/<tier> --
# the SDK builds that from the tier's resource URL, which has no /mcp. Only
# the first was redirected, so the URL the 401 hands a client was a 404.
_discovery_redirects = [
    route
    for name in _SUB_SERVERS
    for route in (
        Route(
            f"/.well-known/oauth-protected-resource/{name}/mcp",
            _redirect(f"/{name}/.well-known/oauth-protected-resource"),
        ),
        Route(
            f"/.well-known/oauth-protected-resource/{name}",
            _redirect(f"/{name}/.well-known/oauth-protected-resource"),
        ),
        Route(
            f"/.well-known/oauth-authorization-server/{name}",
            _redirect(f"/{name}/.well-known/oauth-authorization-server"),
        ),
    )
]

# Discovery for /mcp. A client connecting https://host/mcp asks, per RFC 9728,
# for /.well-known/oauth-protected-resource/mcp, and the 401 names the bare
# /.well-known/oauth-protected-resource. Mounted at the root, the SDK's own
# metadata route answered the second with the origin as the resource (not
# .../mcp), and nothing answered the first. The bridge's metadata is the one
# consistent with its authorization server at the root, so both paths go to it.
_domain_discovery = (
    []
    if _domain_bridge is None
    else [
        Route("/.well-known/oauth-protected-resource/mcp", _domain_bridge.protected_resource),
        Route("/.well-known/oauth-protected-resource", _domain_bridge.protected_resource),
    ]
)

app = Starlette(
    routes=[
        Route("/health", _root_health),
        Route("/version", _root_version),
        Route("/", _root),
        # Off unless MCP_UPLOAD_URLS=1 and MCP_UPLOAD_BASE_URL are set, and then
        # outside the tiers' bearer auth: the signed, single-use token in the
        # path is the credential (shared/exchange.py, upload URLs).
        Route("/upload/{token}", upload_route, methods=["PUT", "POST"]),
        *_discovery_redirects,
        *_domain_discovery,
        *(Mount(f"/{name}", app=sub_app) for name, sub_app in _sub_apps.items()),
        # Last, so every route above wins: /mcp and the domain server's own
        # OAuth routes answer at the root.
        Mount("", app=_domain_app),
    ],
    lifespan=_combined_lifespan,
)


def main() -> None:
    import uvicorn

    parser = argparse.ArgumentParser(description="MCP_Data_Analyst unified server")
    parser.add_argument("--host", default=os.environ.get("DA_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("DA_PORT", "8810")))
    args = parser.parse_args()
    # timeout_keep_alive must exceed the reverse proxy's idle-connection pool,
    # or the proxy reuses a connection this server has already closed. uvicorn's
    # default is 5s and Caddy pools for 2 minutes, so every connection idle
    # between the two was dead here and live there. Reusing one gave Caddy
    # "aborting with incomplete response ... use of closed network connection",
    # which it turned into a 200 with zero bytes: the tool call had run, and the
    # caller hung until its own timeout believing it had failed. Measured
    # against the deployment: idle 2s reused fine, idle 7s closed.
    keepalive = int(os.environ.get("MCP_KEEPALIVE_SECONDS", "300"))
    # Serving over HTTP means the caller is remote, and a remote caller has no
    # business naming a path outside the data folder and the workspaces. On by
    # default here, not only in docker-compose.yml, so a deployment started any
    # other way is confined too. MCP_CONFINE_PATHS=0 opts out explicitly.
    os.environ.setdefault("MCP_CONFINE_PATHS", "1")
    uvicorn.run(app, host=args.host, port=args.port, timeout_keep_alive=keepalive)


if __name__ == "__main__":
    main()
