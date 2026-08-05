# syntax=docker/dockerfile:1.7
# ─────────────────────────────────────────────────────────────────────────────
# mcp-data-analyst — production container for the 7 active Data_Analyst MCP
# servers (data_advanced is a retired stub with zero tools — not deployed;
# data_project is a redirect alias of data_workspace — same tools, same
# process, not a separate deployment). One root `uv sync` covers all 7 (root
# pyproject.toml's dependencies are the union of every sub-server's deps).
#
# One image, N containers: select which sub-server a given container runs via
# SERVER_MODULE (path to its server.py). See docker-compose.yml for the
# one-service-per-sub-server layout (each with its own port).
#
# Build:  docker build -t mcp-data-analyst:latest .
# Run data_basic:
#   docker run --rm -p 8810:8810 -e SERVER_MODULE=servers/data_basic/server.py \
#     -e DA_BASIC_TRANSPORT=http -e DA_BASIC_HOST=0.0.0.0 -e DA_BASIC_PORT=8810 \
#     mcp-data-analyst:latest
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
COPY pyproject.toml ./

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1

USER app

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "\
import os, urllib.request; \
name = os.path.basename(os.path.dirname(os.environ.get('SERVER_MODULE', 'servers/data_basic/server.py'))); \
prefix = 'DA_' + name.removeprefix('data_').upper(); \
port = os.environ[f'{prefix}_PORT']; \
urllib.request.urlopen(f'http://127.0.0.1:{port}/health', timeout=3)" || exit 1

ENTRYPOINT ["sh", "-c", "exec python \"$SERVER_MODULE\""]
