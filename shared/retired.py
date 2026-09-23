"""A tool name that keeps answering after its successor took over the job.

Four medium-tier tools duplicate a newer sibling: `extended_stats` is the same
function the statistics server serves, `statistical_tests` runs a subset of
`statistical_test`'s seventeen tests without its alpha, effect size or
post-hoc, `filter_rows` is what `filter_dataset` was upgraded from, and
`compute_aggregations` is `aggregate_dataset` in groupby mode. Every name in
`tools/list` costs a model attention on every turn, so a duplicate is not free.

Retiring a name drops it from `tools/list` and nothing else: a caller that
learned it keeps getting the same answer, and that answer says which tool now
does the job, so the caller moves on. The tool is still declared in the
server's source and documented in the README as retired.
"""

from __future__ import annotations

import functools
from typing import Any


def retire(mcp: Any, successors: dict[str, str]) -> None:
    """Drop each name in `successors` from tools/list; it still answers, naming its successor.

    `successors` maps a retired name to the tool that replaces it, e.g.
    {"statistical_tests": "statistical_test on the statistics server"}.
    Raises KeyError for a name this server does not register, so a typo cannot
    silently retire nothing.
    """
    manager = mcp._tool_manager
    missing = sorted(name for name in successors if name not in manager._tools)
    if missing:
        raise KeyError(f"cannot retire what is not registered: {missing}")
    listing = manager.list_tools
    retired: set[str] = getattr(listing, "__retired__", set())
    if not hasattr(listing, "__retired__"):

        def list_tools() -> list[Any]:
            return [tool for tool in listing() if tool.name not in retired]

        list_tools.__retired__ = retired  # type: ignore[attr-defined]
        manager.list_tools = list_tools
    retired.update(successors)
    for name, successor in successors.items():
        tool = manager._tools[name]
        tool.fn = _naming_successor(tool.fn, name, successor)


def _naming_successor(fn: Any, name: str, successor: str) -> Any:
    @functools.wraps(fn)
    def answering(*a: Any, **kw: Any) -> Any:
        result = fn(*a, **kw)
        if isinstance(result, dict):
            result["retired"] = f"{name} is no longer listed; use {successor}, which does this job."
        return result

    return answering
