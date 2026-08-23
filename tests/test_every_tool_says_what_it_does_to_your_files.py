"""Every tool must declare its annotations, and they must not overclaim.

All 69 tools here shipped with a bare `@mcp.tool()`. Absent the annotations
field a client applies the MCP spec defaults --

    readOnlyHint     false
    destructiveHint  true
    idempotentHint   false
    openWorldHint    true

-- so `inspect_dataset`, which opens a CSV and returns its shape, advertised
itself as a destructive, non-repeatable operation reaching the open internet.
A client that gates destructive tools behind confirmation prompts for every
read; one that trusts openWorldHint believes these servers call out to the
network, which is the opposite of what this project is built on. Verified
against the live endpoints: 0 of 69 carried annotations, while the three
sibling repos carried them on all 47 of theirs.

The classification was settled by calling each candidate against a seeded
workspace and fingerprinting the directory before and after, because static
analysis kept getting it wrong -- import aliases hid writes, `atomic_write`
reaches `unlink` internally so everything looked destructive, and one engine
lived outside servers/ entirely.

What is guarded here is what can be checked cheaply and reliably:

  * every tool declares annotations at all (so a new tool cannot slip through)
  * openWorldHint is False everywhere (no tool reaches a network at runtime)
  * nothing claiming readOnlyHint accepts an output_path/output_dir/dry_run

That last one is the mistake that matters: a tool a client may call without
asking must not be able to write.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SERVERS = [
    "data_basic",
    "data_ingest",
    "data_medium",
    "data_statistics",
    "data_transform",
    "data_visual",
    "data_workspace",
]

# A parameter that names somewhere to write. A read-only tool cannot have one.
WRITE_PARAMS = {"output_path", "output_dir", "output_file", "dry_run", "save_path"}


def load(name: str):
    pkg = ROOT / "servers" / name
    if str(pkg) not in sys.path:
        sys.path.insert(0, str(pkg))
    return importlib.import_module(f"servers.{name}.server")


def tools_of(name: str):
    mod = load(name)
    return mod.mcp._tool_manager._tools


ALL = [(s, n, t) for s in SERVERS for n, t in tools_of(s).items()]


def annotations_of(tool) -> dict:
    ann = getattr(tool, "annotations", None)
    if ann is None:
        return {}
    if isinstance(ann, dict):
        return ann
    return {k: v for k, v in vars(ann).items() if v is not None}


class TestEveryToolIsAnnotated:
    def test_the_servers_expose_the_tools_this_covers(self):
        # An empty registry would make every case below vacuous.
        assert len(ALL) >= 60, f"only {len(ALL)} tools found"

    @pytest.mark.parametrize("server,name,tool", ALL, ids=[f"{s}.{n}" for s, n, _ in ALL])
    def test_it_declares_annotations(self, server, name, tool):
        assert annotations_of(tool), f"{server}.{name} declares none — the spec defaults apply"

    @pytest.mark.parametrize("server,name,tool", ALL, ids=[f"{s}.{n}" for s, n, _ in ALL])
    def test_it_does_not_claim_to_reach_the_network(self, server, name, tool):
        # Offline-first is a founding constraint of this project; no tool here
        # calls out at runtime.
        assert annotations_of(tool).get("openWorldHint") is False, f"{server}.{name}"


class TestNothingReadOnlyCanWrite:
    @pytest.mark.parametrize("server,name,tool", ALL, ids=[f"{s}.{n}" for s, n, _ in ALL])
    def test_a_read_only_tool_takes_no_output_path(self, server, name, tool):
        ann = annotations_of(tool)
        if not ann.get("readOnlyHint"):
            return
        props = set((tool.parameters or {}).get("properties", {}))
        offenders = sorted(props & WRITE_PARAMS)
        assert not offenders, f"{server}.{name} claims readOnlyHint but accepts {offenders}"

    @pytest.mark.parametrize("server,name,tool", ALL, ids=[f"{s}.{n}" for s, n, _ in ALL])
    def test_a_read_only_tool_is_not_also_destructive(self, server, name, tool):
        ann = annotations_of(tool)
        if ann.get("readOnlyHint"):
            assert ann.get("destructiveHint") is False, f"{server}.{name}"


class TestTheClassificationIsNotAllOneThing:
    def test_all_three_kinds_are_used(self):
        kinds = set()
        for _, _, tool in ALL:
            ann = annotations_of(tool)
            kinds.add((ann.get("readOnlyHint"), ann.get("destructiveHint")))
        # Reads, creates and edits — if a later edit collapsed everything onto
        # one constant the guards above would still pass.
        assert len(kinds) >= 3, kinds
