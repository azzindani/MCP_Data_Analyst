# syntax=docker/dockerfile:1.7
# ─────────────────────────────────────────────────────────────────────────────
# mcp-data-analyst — production container, ONE process for all 7 sub-servers.
#
# unified_server.py mounts basic/medium/statistics/transform/visual/
# workspace/ingest as separate MCP endpoints (/basic/mcp, /medium/mcp, ...)
# inside one Starlette app on one port, so pandas/numpy/scipy/matplotlib load
# once instead of seven times — was previously 7 containers (~1.1 GiB idle
# combined), now 1 (~260 MiB idle). Each sub-server's own /health, /version,
# /mcp routes (defined via @mcp.custom_route in its own server.py) come
# along for free under its mount prefix. Per-sub-server stdio/individual-HTTP
# servers (servers/data_*/server.py) are untouched — still usable directly
# for local LM Studio installs. data_advanced (retired stub, zero tools) and
# data_project (redirect alias of data_workspace) are intentionally excluded.
#
# Build:  docker build -t mcp-data-analyst:latest .
# Run:    docker run --rm -p 8810:8810 -e DA_TRANSPORT=http mcp-data-analyst:latest
# ─────────────────────────────────────────────────────────────────────────────

ARG PYTHON_VERSION=3.12-slim

FROM python:${PYTHON_VERSION} AS builder
WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/
COPY pyproject.toml uv.lock ./
COPY shared ./shared
COPY servers ./servers
RUN uv sync --frozen

FROM python:${PYTHON_VERSION} AS runtime
RUN groupadd -r app && useradd -r -g app app
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/shared /app/shared
COPY --from=builder /app/servers /app/servers
COPY pyproject.toml unified_server.py ./

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    DA_HOST=0.0.0.0 \
    DA_PORT=8810

USER app
EXPOSE 8810

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import os, urllib.request; urllib.request.urlopen(f'http://127.0.0.1:{os.environ[\"DA_PORT\"]}/health', timeout=3)" || exit 1

ENTRYPOINT ["python", "unified_server.py"]
