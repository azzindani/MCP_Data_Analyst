"""A next step must name a tool the client has, with the action it takes.

The 2026-10-02 sweep called the data and ML servers as a client does -- through their domain
tools -- and followed what the answers said to do next:

* `handover.suggested_next` named `{"tool": "inspect_dataset", "server": "data_basic"}`; a client
  has `data_inspect(action="inspect_dataset")`, and `tools/call inspect_dataset` was "Unknown tool".
  `data_basic` is a sub-server that has not been a separate endpoint since the tool surface was
  trimmed.
* an insight's action, `{"tool": "apply_patch", ...}`, the same.
* a hint said "use filter_rows()".

The domain dispatcher is the one place that knows both vocabularies, so it rewrites them.
"""

from __future__ import annotations

import asyncio
import re

import numpy as np
import pandas as pd
import pytest

from servers.data_domain.server import DOMAINS
from servers.data_domain.server import mcp as domain
from shared.domain_tools import point_at_domains

ACTIONS = {tool: name for name, (_, members) in DOMAINS.items() for _, tool in members}


def _call(tool: str, action: str, **args):
    return asyncio.run(domain._tool_manager._tools[tool].run({"action": action, "args": args}))


def _resolves(pointer: dict) -> bool:
    return (
        pointer["tool"] in domain._tool_manager._tools
        and pointer.get("action")
        in (domain._tool_manager._tools[pointer["tool"]].parameters["properties"]["action"]["enum"])
    )


@pytest.fixture
def table(tmp_path, monkeypatch) -> str:
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(1)
    a = rng.normal(0, 1, 200)
    path = tmp_path / "t.csv"
    pd.DataFrame({"a": a, "b": a * 2 + 0.001 * rng.normal(0, 1, 200), "c": rng.normal(0, 1, 200)}).to_csv(
        path, index=False
    )
    return str(path)


class TestTheRewrite:
    ROUTE = {"inspect_dataset": "data_inspect", "filter_rows": "data_edit"}

    def test_a_handover_pointer_names_the_domain_and_the_action(self):
        result = {"handover": {"suggested_next": [{"tool": "inspect_dataset", "server": "data_basic", "reason": "x"}]}}
        point = point_at_domains(result, self.ROUTE, "data")["handover"]["suggested_next"][0]
        assert point == {"tool": "data_inspect", "action": "inspect_dataset", "server": "data", "reason": "x"}

    def test_an_insights_action_keeps_its_args(self):
        result = {"insights": [{"finding": "f", "action": {"tool": "filter_rows", "args": {"file_path": "/x"}}}]}
        act = point_at_domains(result, self.ROUTE, "data")["insights"][0]["action"]
        assert act == {"tool": "data_edit", "action": "filter_rows", "args": {"file_path": "/x"}}

    def test_a_hint_names_the_call_that_works(self):
        result = {"hint": "Use filter_rows() to keep the rest; pandas.read_csv() is not ours."}
        assert point_at_domains(result, self.ROUTE, "data")["hint"] == (
            "Use data_edit(action='filter_rows') to keep the rest; pandas.read_csv() is not ours."
        )

    def test_a_tool_this_server_does_not_have_is_left_alone(self):
        result = {"handover": {"suggested_next": [{"tool": "train_classifier", "server": "MCP_Machine_Learning"}]}}
        assert point_at_domains(result, self.ROUTE, "data")["handover"]["suggested_next"][0]["tool"] == (
            "train_classifier"
        )

    def test_a_pointer_already_in_domain_form_is_not_rewritten_twice(self):
        result = {"handover": {"suggested_next": [{"tool": "data_inspect", "action": "inspect_dataset"}]}}
        again = point_at_domains(point_at_domains(result, self.ROUTE, "data"), self.ROUTE, "data")
        assert again["handover"]["suggested_next"][0] == {"tool": "data_inspect", "action": "inspect_dataset"}

    def test_an_answer_that_is_not_a_dict_passes_through(self):
        assert point_at_domains("text", self.ROUTE, "data") == "text"


class TestEveryPointerResolves:
    def test_a_workspace_hands_over_to_tools_the_client_has(self, tmp_path):
        result = _call("data_workspace", "create_workspace", name="demo", base_dir=str(tmp_path))
        pointers = result["handover"]["suggested_next"]
        assert pointers
        assert all(_resolves(p) for p in pointers), pointers
        assert {p["server"] for p in pointers} == {"data"}

    def test_a_correlation_insight_can_be_called_as_written(self, table):
        result = _call("data_stats", "correlation_analysis", file_path=table, open_after=False)
        actions = [i["action"] for i in result.get("insights", []) if i.get("action")]
        assert actions, "a near-duplicate pair should carry an action"
        assert all(_resolves(a) for a in actions), actions

    def test_no_hint_names_a_tier_tool_by_a_name_the_client_lacks(self, table):
        for tool, action in (("data_stats", "check_outliers"), ("data_inspect", "inspect_dataset")):
            hint = _call(tool, action, file_path=table).get("hint", "")
            for name in re.findall(r"\b([a-z_][a-z0-9_]{2,})\(\)", hint):
                assert name not in ACTIONS, f"{action}: hint still says {name}()"
