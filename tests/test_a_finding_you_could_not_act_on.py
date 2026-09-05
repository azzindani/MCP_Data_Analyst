"""Every insight's action names a tool that exists and can be called as written.

The user review drew the line precisely:

    Finding ships with executable fix (dataprep/autoviz): not "skew 31.07" but
    `FixDQ.cap_outliers / fit_transform`. MCP: `insights:[{finding, action}]`
    where action runs in one call (`run_preprocessing`, `apply_patch`). Today's
    `suggested_actions` die in response.

An action is a stronger promise than a sentence. "Call apply_patch() with a
drop_duplicates op" is advice, and advice that names a tool which does not exist
is merely unhelpful. `{"tool": "apply_patch", "args": {...}}` says *this runs*,
and if it does not, the caller has spent a loop iteration finding out.

So this reads the actions against the servers themselves: every tool named must
be a real `@mcp.tool`, every argument must be one that tool accepts, and
`file_path` must be a path rather than the bare filename the sidecar displays --
`resolve_path` resolves a bare name against the process working directory, which
is not where the data is, so an action bound to a name would look runnable and
would not run.

This is the "no dead ops" rule from the same review, applied to the newest place
a dead op could hide.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pandas as pd
import pytest

from shared.insights import (
    ACTION_FILE_KEY,
    action,
    bind_actions,
    from_alerts,
    from_correlations,
    from_crosstab,
    from_outliers,
    insight,
    write_insights,
)

SERVERS = Path(__file__).resolve().parents[1] / "servers"


def _tool_signatures() -> dict[str, set[str]]:
    """{tool name: accepted argument names} for every @mcp.tool in the repo.

    Parsed from source rather than imported: importing every server module
    starts every FastMCP instance, and this only needs the signatures.
    """
    found: dict[str, set[str]] = {}
    for server_file in SERVERS.glob("*/server.py"):
        tree = ast.parse(server_file.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef):
                continue
            decorated = any(
                (isinstance(d, ast.Attribute) and d.attr == "tool")
                or (isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute) and d.func.attr == "tool")
                for d in node.decorator_list
            )
            if decorated:
                args = node.args
                names = {a.arg for a in (*args.posonlyargs, *args.args, *args.kwonlyargs)}
                found[node.name] = names
    return found


def _every_action() -> list[dict]:
    """One insight of every kind that carries an action, from the real readers."""
    rows = 400
    frame = pd.DataFrame(
        {
            "id": range(rows),
            "member_id": range(rows),
            "grade": ["A", "B"] * (rows // 2),
            "status": ["paid", "default"] * (rows // 2),
        }
    )
    produced = [
        *from_correlations([{"col_a": "id", "col_b": "member_id", "correlation": 0.9936}]),
        *from_correlations([{"col_a": "id", "col_b": "grade", "correlation": 0.8}]),
        *from_alerts(
            [
                {"type": "DUPLICATES", "sev": "warning", "msg": "12 duplicate rows", "col": None},
                {"type": "CONSTANT", "sev": "warning", "msg": "one value", "col": "grade"},
                {"type": "ALL NULL", "sev": "error", "msg": "never populated", "col": "status"},
                {"type": "HIGH CARDINALITY", "sev": "warning", "msg": "28,525 unique", "col": "id"},
            ]
        ),
        *from_outliers([{"column": "id", "outlier_count": 40, "lower_limit": 0, "upper_limit": 9}], rows),
        *from_crosstab(
            {"A": {"paid": 300, "default": 10}, "B": {"paid": 10, "default": 300}},
            "grade",
            "status",
        ),
    ]
    assert produced, "the readers produced nothing, so this file would pass vacuously"
    return produced


ACTIONS = [i["action"] for i in _every_action() if i.get("action")]
TOOLS = _tool_signatures()


def test_the_servers_were_actually_read():
    """A broken parser would make every assertion below pass vacuously."""
    assert len(TOOLS) > 30, sorted(TOOLS)
    assert "apply_patch" in TOOLS and "detect_anomalies" in TOOLS


def test_some_findings_carry_an_action():
    assert len(ACTIONS) >= 4, ACTIONS


@pytest.mark.parametrize("act", ACTIONS, ids=lambda a: a["tool"])
class TestEveryActionIsACallThatExists:
    def test_the_tool_is_defined_on_a_server(self, act):
        assert act["tool"] in TOOLS, (
            f"insight action names {act['tool']!r}, which is not an @mcp.tool in this repo. "
            f"A dead op burns a loop iteration every time an agent reasonably tries it."
        )

    def test_every_argument_is_one_the_tool_accepts(self, act):
        accepted = TOOLS[act["tool"]]
        unknown = sorted(set(act["args"]) - accepted - {ACTION_FILE_KEY})
        assert not unknown, f"{act['tool']} does not accept {unknown}; it accepts {sorted(accepted)}"

    def test_the_tool_takes_a_file_path(self, act):
        assert ACTION_FILE_KEY in TOOLS[act["tool"]], (
            f"{act['tool']} has no {ACTION_FILE_KEY} parameter, so binding one would be a wrong call"
        )


class TestTheActionIsBoundToSomethingOpenable:
    """A bare filename is the failure this guards, and it is a quiet one."""

    def test_unbound_actions_carry_no_file_path(self):
        act = action("apply_patch", {"ops": []})
        assert ACTION_FILE_KEY not in act["args"]

    def test_binding_fills_every_action(self, tmp_path: Path):
        csv = tmp_path / "loans.csv"
        csv.write_text("a\n1\n", encoding="utf-8")
        bound = bind_actions(_every_action(), str(csv))
        acted = [i for i in bound if i.get("action")]
        assert acted
        for item in acted:
            assert item["action"]["args"][ACTION_FILE_KEY] == str(csv)

    def test_binding_is_in_place_so_the_file_and_the_response_agree(self, tmp_path: Path):
        csv = tmp_path / "loans.csv"
        csv.write_text("a\n1\n", encoding="utf-8")
        found = _every_action()
        returned = bind_actions(found, str(csv))
        assert returned is found

    def test_write_insights_binds_the_path_not_the_display_name(self, tmp_path: Path):
        import json

        csv = tmp_path / "sub" / "loans.csv"
        csv.parent.mkdir()
        csv.write_text("a\n1\n", encoding="utf-8")
        artifact = tmp_path / "loans_correlation.html"
        artifact.write_text("<html></html>", encoding="utf-8")

        found = _every_action()
        path = write_insights(artifact, found, op="correlation_analysis", source=csv.name, source_path=str(csv))
        payload = json.loads(Path(path).read_text(encoding="utf-8"))

        # The sidecar still shows the short name, and the actions still run.
        assert payload["source"] == csv.name
        for item in payload["insights"]:
            if item.get("action"):
                bound = item["action"]["args"][ACTION_FILE_KEY]
                assert bound == str(csv), f"bound {bound!r}, which is not a path a tool can open"


class TestAnActionIsOptional:
    """Better no action than a wrong one that runs."""

    def test_a_finding_with_no_single_right_fix_carries_none(self):
        [high_card] = [
            i for i in from_alerts([{"type": "HIGH CARDINALITY", "sev": "warning", "msg": "x", "col": "emp_title"}])
        ]
        assert "action" not in high_card
        assert high_card["suggested_next"], "it still says what the options are"

    def test_insight_without_an_action_omits_the_key(self):
        assert "action" not in insight("k", "low", "h")


def test_the_readers_are_the_ones_shipping_actions():
    """Guards against the actions living only in this test's fixtures."""
    source = inspect.getsource(__import__("shared.insights", fromlist=["insights"]))
    assert source.count("act=action(") >= 4
