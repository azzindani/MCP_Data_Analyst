"""CI gate: every @mcp.tool() docstring must be <= 80 characters."""

from __future__ import annotations

import ast
import sys
from pathlib import Path

LIMIT = 80
SERVER_DIRS = [
    "servers/data_basic",
    "servers/data_medium",
    "servers/data_advanced",
    "servers/data_transform",
    "servers/data_statistics",
    "servers/data_visual",
    "servers/data_project",
    "servers/data_workspace",
    "servers/data_ingest",
]


def check_file(path: Path) -> list[str]:
    violations: list[str] = []
    tree = ast.parse(path.read_text(encoding="utf-8"))

    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        # Must be decorated with @mcp.tool() or @<anything>.tool()
        has_tool_decorator = any(
            (isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute) and d.func.attr == "tool")
            or (isinstance(d, ast.Attribute) and d.attr == "tool")
            for d in node.decorator_list
        )
        if not has_tool_decorator:
            continue

        docstring = ast.get_docstring(node) or ""
        # Measure only the first line (the selection cue)
        first_line = docstring.splitlines()[0] if docstring else ""
        if len(first_line) > LIMIT:
            violations.append(
                f"{path}:{node.lineno}: {node.name}() docstring first line "
                f"is {len(first_line)} chars (limit {LIMIT}): {first_line!r}"
            )
    return violations


def main() -> None:
    all_violations: list[str] = []

    for server_dir in SERVER_DIRS:
        server_py = Path(server_dir) / "server.py"
        if not server_py.exists():
            continue
        all_violations.extend(check_file(server_py))

    if all_violations:
        print("Tool docstring violations found:\n", file=sys.stderr)
        for v in all_violations:
            print(f"  {v}", file=sys.stderr)
        print(f"\n{len(all_violations)} violation(s). Fix before committing.", file=sys.stderr)
        sys.exit(1)

    checked = sum(1 for d in SERVER_DIRS if (Path(d) / "server.py").exists())
    print(f"OK — all @mcp.tool() docstrings within {LIMIT} chars ({checked} servers checked).")


if __name__ == "__main__":
    main()
