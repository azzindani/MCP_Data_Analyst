from __future__ import annotations

import os

MCP_CONSTRAINED_MODE: bool = os.environ.get("MCP_CONSTRAINED_MODE", "0") == "1"


def get_max_rows() -> int:
    return 20 if MCP_CONSTRAINED_MODE else 100


def get_max_columns() -> int:
    return 20 if MCP_CONSTRAINED_MODE else 50


def get_max_results() -> int:
    return 10 if MCP_CONSTRAINED_MODE else 50
