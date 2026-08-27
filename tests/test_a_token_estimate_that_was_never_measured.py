"""token_estimate was a literal, so it described nothing.

CLAUDE.md defines it as `len(str(response)) // 4`. It was hardcoded instead --
**325** occurrences of `"token_estimate": 15` and friends in this repo, the
largest count in the fleet. Measured against the live endpoints before the fix:

    restore_version("…/dupes.csv")   # no snapshots yet
    -> "error": "No backups found for dupes.csv"
       "hint":  "Use apply_patch first to create a snapshot."
       "token_estimate": 20          the response measures ~100

Under-reporting is the direction that hurts. A client budgets its context from
this number and admits the response on the strength of it, so an
order-of-magnitude undercount blows the ~12,000-token budget these servers are
designed around. Error responses are the worst case, because their length is
dominated by a variable-length message: a constant is wrong by construction,
and *improving* a message by making it more specific silently makes the lie
bigger.

MCP_Math is the only repo in the fleet that computes it, in
src/engine/formatter.py. This is the same idea applied here as a single choke
point, because 325 hand-edits drift out of step with the responses exactly the
way the literals already had.

These assertions call `mod.<tool>.fn(...)`, the same way the rest of this
suite dispatches -- on fastmcp 2.x the module-level name is the registered
FunctionTool, so wrapping `.fn` is what a test sees as well as what a client
gets. A fix the suite dispatches around is a fix nobody can hold to account.
"""

from __future__ import annotations

import importlib

import pytest

# One cheap, reliably-failing call per server: a path that cannot exist. The
# error message carries a variable-length path, which is precisely the shape a
# constant estimate cannot describe.
MISSING = "/nonexistent/definitely_not_here.csv"

CASES = [
    ("servers.data_basic.server", "load_dataset", {"file_path": MISSING}),
    ("servers.data_basic.server", "inspect_dataset", {"file_path": MISSING}),
    ("servers.data_ingest.server", "list_sheets", {"file_path": MISSING}),
    ("servers.data_medium.server", "value_counts", {"file_path": MISSING, "columns": ["x"]}),
    ("servers.data_statistics.server", "scan_nulls_zeros", {"file_path": MISSING}),
    (
        "servers.data_transform.server",
        "filter_dataset",
        {"file_path": MISSING, "conditions": [{"column": "x", "operator": "eq", "value": 1}]},
    ),
    ("servers.data_visual.server", "generate_chart", {"file_path": MISSING, "chart_type": "bar", "value_column": "a"}),
    ("servers.data_workspace.server", "open_workspace", {"workspace_name": "nope_not_a_workspace"}),
]


def measured(response: dict) -> int:
    """What the contract says the estimate should be for this response.

    token_estimate is dropped before measuring, so the number describes the
    payload rather than partly describing itself. recount() sets the key last,
    so removing it here leaves the other keys in their original order and
    str() renders the same bytes.
    """
    return len(str({k: v for k, v in response.items() if k != "token_estimate"})) // 4


def call(module: str, tool: str, kwargs: dict) -> dict:
    mod = importlib.import_module(module)
    return getattr(mod, tool).fn(**kwargs)


@pytest.mark.parametrize("module,tool,kwargs", CASES, ids=[f"{m.split('.')[1]}.{t}" for m, t, _ in CASES])
class TestTheEstimateIsMeasuredNotTypedIn:
    def test_it_matches_the_response_it_describes(self, module: str, tool: str, kwargs: dict) -> None:
        r = call(module, tool, kwargs)
        assert isinstance(r, dict), f"{tool} did not return a dict"
        assert r["token_estimate"] == measured(r), (
            f"{tool} reported {r['token_estimate']}, response measures {measured(r)}"
        )

    def test_it_is_not_one_of_the_stock_literals(self, module: str, tool: str, kwargs: dict) -> None:
        """15, 20, 25, 30 and 40 were the hardcoded values across 325 sites.

        A stock value is only a failure when it disagrees with the measurement;
        a genuinely 60-character response really does estimate 15.
        """
        r = call(module, tool, kwargs)
        if r["token_estimate"] in (15, 20, 25, 30, 40):
            assert r["token_estimate"] == measured(r), (
                f"{tool} returned the stock literal {r['token_estimate']}; the response measures {measured(r)}"
            )


class TestTheDirectionThatMatters:
    """Equality alone would be satisfied by any consistently wrong number."""

    def test_a_long_error_is_nowhere_near_the_literal(self) -> None:
        r = call("servers.data_basic.server", "load_dataset", {"file_path": MISSING})
        assert r["token_estimate"] > 15, r
        assert r["token_estimate"] == measured(r)


class TestRecountItself:
    def test_it_measures_without_counting_its_own_field(self) -> None:
        from shared.token_estimate import recount

        r = recount({"success": True, "note": "x" * 400, "token_estimate": 15})
        assert r["token_estimate"] == measured(r)
        assert r["token_estimate"] > 90

    def test_a_stale_literal_is_replaced_not_kept(self) -> None:
        from shared.token_estimate import recount

        assert recount({"a": 1, "token_estimate": 9999})["token_estimate"] != 9999

    def test_a_response_with_no_estimate_gets_one(self) -> None:
        from shared.token_estimate import recount

        assert "token_estimate" in recount({"success": True})

    def test_a_non_dict_is_left_alone(self) -> None:
        """The return contract is enforced elsewhere; this must not raise."""
        from shared.token_estimate import recount

        assert recount("not a dict") == "not a dict"
        assert recount(None) is None

    def test_installing_twice_does_not_double_wrap(self) -> None:
        """measure_responses is safe to call again on an already-wrapped server."""
        import fastmcp

        from shared.token_estimate import measure_responses

        m = fastmcp.FastMCP("probe")

        @m.tool
        def sample(x: int) -> dict:
            """doc"""
            return {"x": x, "token_estimate": 15}

        measure_responses(m)
        once = sample.fn(x=1)["token_estimate"]
        measure_responses(m)
        assert sample.fn(x=1)["token_estimate"] == once
