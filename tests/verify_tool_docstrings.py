"""Verify all @mcp.tool() docstrings are <= 80 characters.

CI gate: exits non-zero if any tool docstring exceeds the limit.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

MAX_LEN = 80

SERVER_FILES = [
    Path("servers/data_basic/server.py"),
    Path("servers/data_medium/server.py"),
    Path("servers/data_advanced/server.py"),
    Path("servers/data_workspace/server.py"),
    Path("servers/data_transform/server.py"),
    Path("servers/data_statistics/server.py"),
    Path("servers/data_visual/server.py"),
    Path("servers/data_ingest/server.py"),
]


def check_file(path: Path) -> list[str]:
    """Return list of error strings for docstrings exceeding MAX_LEN."""
    errors: list[str] = []
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))

    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue

        # Check if the function is decorated with @mcp.tool()
        is_tool = False
        for dec in node.decorator_list:
            if isinstance(dec, ast.Call) and isinstance(dec.func, ast.Attribute):
                if dec.func.attr == "tool":
                    is_tool = True
                    break
        if not is_tool:
            continue

        # The WHOLE docstring, not its first line. The MCP SDK sends
        # `func.__doc__` as the tool's description, so every client pays for
        # every line of it on every tools/list -- and a first-line check passes
        # a three-paragraph docstring while the gate's own summary claims to
        # verify that tool docstrings are <= 80 characters.
        #
        # This measured the first line only for as long as every tool here
        # happened to have one line. The first multi-line docstring anyone wrote
        # shipped a 301-character description that this file called OK.
        # MCP_Machine_Learning's equivalent census measures the whole string;
        # they now agree.
        docstring = ast.get_docstring(node) or ""
        if len(docstring) > MAX_LEN:
            first_line = docstring.splitlines()[0]
            errors.append(
                f"  {path}:{node.lineno} — {node.name}() "
                f"docstring is {len(docstring)} chars (max {MAX_LEN}): "
                f"{first_line!r}" + ("..." if len(docstring.splitlines()) > 1 else "")
            )

    return errors


def main() -> int:
    all_errors: list[str] = []
    for server_file in SERVER_FILES:
        if not server_file.exists():
            print(f"WARNING: {server_file} not found, skipping")
            continue
        all_errors.extend(check_file(server_file))

    if all_errors:
        print(f"FAIL — {len(all_errors)} docstring(s) exceed {MAX_LEN} chars:")
        for err in all_errors:
            print(err)
        return 1

    print(f"OK — all tool docstrings <= {MAX_LEN} chars")
    return 0


if __name__ == "__main__":
    sys.exit(main())
