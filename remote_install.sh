#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# MCP_Data_Analyst — remote bootstrap (Google Colab / any fresh Linux VM, no
# Docker needed). Installs uv, clones/updates this repo, syncs Python deps
# for all 7 sub-servers.
#
# Companion to remote_launch.sh. Same idea as azzindani/Folio's install.sh,
# adapted for a uv/Python project instead of npm.
#
# Usage:
#   REPO_DIR=/content/MCP_Data_Analyst ./remote_install.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

REPO_DIR="${REPO_DIR:-/content/MCP_Data_Analyst}"
REPO_URL="${REPO_URL:-https://github.com/azzindani/MCP_Data_Analyst.git}"

if ! command -v uv &>/dev/null; then
  echo "[remote_install] installing uv..."
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi
export PATH="${HOME}/.local/bin:${PATH}"

if [ -d "$REPO_DIR/.git" ]; then
  cd "$REPO_DIR" && git pull -q && echo "Updated: $(git log -1 --oneline)"
else
  git clone -q "$REPO_URL" "$REPO_DIR"
  echo "Cloned: $(cd "$REPO_DIR" && git log -1 --oneline)"
fi

cd "$REPO_DIR"
uv sync 2>&1 | tail -5
echo "✓ MCP_Data_Analyst installed (7 sub-servers)"
