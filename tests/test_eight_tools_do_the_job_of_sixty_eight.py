"""The whole surface as eight domain tools on one endpoint, `action` plus `args`.

A model connected to every tier reads 68 tool names on every turn. The domain
endpoint lists eight, one per job, and each `action` is one of those 68 tools
by its own name -- its schema read from the tier, its call run by the tier's
own `run()`, so validation, wrappers and answers are the tier's.
"""

from __future__ import annotations

import asyncio
import base64
import importlib

import pandas as pd
import pytest

from servers.data_domain.server import DOMAINS, mcp

TIERS = [
    "data_basic",
    "data_medium",
    "data_statistics",
    "data_transform",
    "data_visual",
    "data_workspace",
    "data_ingest",
]


def _tier_listed() -> set[str]:
    names: set[str] = set()
    for tier in TIERS:
        server = importlib.import_module(f"servers.{tier}.server").mcp
        names |= {t.name for t in asyncio.run(server.list_tools())}
    return names


def _tier_tool(name: str):
    for tier in TIERS:
        tools = importlib.import_module(f"servers.{tier}.server").mcp._tool_manager._tools
        if name in tools:
            return tools[name]
    raise KeyError(name)


def _listed():
    return {t.name: t for t in asyncio.run(mcp.list_tools())}


def _call(tool: str, action: str, args: dict | None = None) -> dict:
    payload = {"action": action} if args is None else {"action": action, "args": args}
    return asyncio.run(mcp._tool_manager._tools[tool].run(payload))


@pytest.fixture
def csv(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path / "data"))
    path = tmp_path / "sales.csv"
    pd.DataFrame(
        {"region": ["a", "a", "b", "b", "c", "c"], "units": [1, 2, 3, 4, 5, 6], "price": [9.0, 8, 7, 6, 5, 4]}
    ).to_csv(path, index=False)
    return str(path)


class TestEightNotSixtyEight:
    def test_eight_tools_are_listed(self):
        assert sorted(_listed()) == sorted(DOMAINS)
        assert len(_listed()) == 8

    def test_every_listed_tier_tool_is_exactly_one_action(self):
        actions = [a for t in _listed().values() for a in t.inputSchema["properties"]["action"]["enum"]]
        assert len(actions) == len(set(actions)), "an action is in two domains"
        assert set(actions) == _tier_listed(), "a tier tool is missing from the domains, or a retired one crept in"

    def test_the_schema_is_the_pipeline_shape(self):
        for tool in _listed().values():
            schema = tool.inputSchema
            assert schema["required"] == ["action"]
            assert schema["properties"]["args"]["additionalProperties"] is False
            for action in schema["properties"]["action"]["enum"]:
                assert f"- {action}:" in tool.description, (tool.name, action)

    def test_each_argument_says_which_actions_take_and_require_it(self):
        for tool in _listed().values():
            actions = tool.inputSchema["properties"]["action"]["enum"]
            tiers = {a: mcp_tool for a in actions for mcp_tool in [_tier_tool(a)]}
            for param, prop in tool.inputSchema["properties"]["args"]["properties"].items():
                required = [a for a in actions if param in (tiers[a].parameters.get("required") or [])]
                text = prop["description"]
                if not required:
                    assert "required by" not in text, (tool.name, param, text)
                elif len(required) == len(actions):
                    assert text.endswith("required by every action"), (tool.name, param, text)
                else:
                    assert text.endswith(f"required by {', '.join(required)}"), (tool.name, param, text)
        alpha = _listed()["data_stats"].inputSchema["properties"]["args"]["properties"]["alpha"]
        assert "statistical_test" in alpha["description"]


class TestAnActionIsTheTierTool:
    @pytest.mark.parametrize(
        ("tool", "action", "args"),
        [
            ("data_inspect", "inspect_dataset", {}),
            ("data_stats", "statistical_test", {"test": "t_test", "column_a": "units", "group_column": "region"}),
            ("data_reshape", "aggregate_dataset", {"mode": "groupby", "group_by": ["region"], "agg": {"units": "sum"}}),
            (
                "data_edit",
                "filter_dataset",
                {"conditions": [{"column": "units", "op": "gt", "value": 2}], "dry_run": True},
            ),
        ],
    )
    def test_it_answers_as_the_tier_does(self, csv, tool, action, args):
        result = _call(tool, action, {"file_path": csv, **args})
        assert result["success"] is True, result

    def test_the_tiers_wrappers_still_run(self, csv, tmp_path):
        uri = "data:text/csv;name=sent.csv;base64," + base64.b64encode(b"x,y\n1,2\n3,4\n").decode()
        result = _call("data_inspect", "inspect_dataset", {"file_path": uri})
        assert result["success"] is True, result
        assert "base64" not in str(result)
        missing = _call("data_inspect", "inspect_dataset", {"file_path": str(tmp_path / "sale.csv")})
        assert missing["success"] is False and "did_you_mean" in missing


class TestARefusalSaysWhatToDo:
    def test_an_action_of_another_tool_is_pointed_home(self):
        result = _call("data_stats", "filter_dataset", {})
        assert result["success"] is False and "data_edit" in result["hint"]

    def test_an_unknown_action_lists_the_actions(self):
        result = _call("data_stats", "no_such_thing", {})
        assert result["success"] is False and "statistical_test" in result["hint"]

    def test_an_argument_the_action_does_not_take_is_named(self, csv):
        result = _call("data_inspect", "inspect_dataset", {"file_path": csv, "colour": "red"})
        assert result["success"] is False and "colour" in result["error"]
        assert "file_path" in result["hint"]

    def test_a_missing_required_argument_is_an_answer(self):
        result = _call("data_inspect", "inspect_dataset", {})
        assert result["success"] is False and "file_path" in result["hint"]


def test_the_unified_server_serves_it_at_mcp():
    from starlette.testclient import TestClient

    import unified_server

    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "1"}},
    }
    with TestClient(unified_server.app) as client:
        assert client.get("/").json()["mcp"] == "/mcp"
        reply = client.post("/mcp", json=body, headers={"Accept": "application/json, text/event-stream"})
        assert reply.status_code == 200 and '"serverInfo"' in reply.text
        assert client.get("/basic/health").status_code == 200
