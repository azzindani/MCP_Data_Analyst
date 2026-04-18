"""Tier 3 data_advanced — retired. All tools live in data_visual (strict superset)."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

logging.basicConfig(stream=sys.stderr, level=logging.WARNING)

from fastmcp import FastMCP

try:
    from . import engine  # noqa: F401
except ImportError:
    _root = str(Path(__file__).resolve().parents[2])
    if _root not in sys.path:
        sys.path.insert(0, _root)

mcp = FastMCP("data_advanced")


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
