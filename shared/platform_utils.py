from __future__ import annotations

# Ring-2 infrastructure utility — reads environment at call time (not import
# time) so that MCP_CONSTRAINED_MODE changes after startup are honoured and
# test monkeypatching works without module reloads.
import os


def _constrained() -> bool:
    return os.environ.get("MCP_CONSTRAINED_MODE", "0") == "1"


def get_max_rows() -> int:
    return 20 if _constrained() else 100


def get_max_columns() -> int:
    return 20 if _constrained() else 50


def get_max_results() -> int:
    return 10 if _constrained() else 50
