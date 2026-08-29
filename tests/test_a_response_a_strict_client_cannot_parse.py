"""Infinity and NaN are not JSON, and they took the whole response with them.

Round 17 hands one artifact to a second server and compares. apply_patch built
a column with `column_math: clicks/impressions*100`, impressions can be 0, and
the two servers then disagreed about the same numbers:

    data-basic  read_column_stats  ->  "mean": Infinity, "std": NaN, "max": Infinity
    data-statistics extended_stats ->  null for the same quantities

The model that found it called this "representation difference only". It is
not: one of those representations cannot be parsed.

    python json.loads(strict)   REJECTED   non-JSON literal 'Infinity'
    node   JSON.parse           REJECTED   Unexpected token 'I' ... not valid JSON

Python's encoder writes the bare tokens by extension and Python's decoder reads
them back, so the fault is invisible from inside Python -- which is every test
in this repo. The damage is not the field but the payload: a JavaScript, Go or
Rust client cannot parse ANY of the response, so a division by zero in one
column destroys the entire reply.

shared/small_sample.py already had `finite()` for exactly this, with a docstring
naming the bare-token problem. It was wired into one module and nowhere else,
which is why the fix here is a boundary that every tool passes through rather
than another call site.
"""

from __future__ import annotations

import json

import pytest

from shared.json_safe import json_safe


def tool_fn(mod, name: str):
    """The callable a client actually reaches, via the tool registry.

    Under fastmcp 2.x the module-level name WAS the registry entry, so
    `mod.some_tool.fn` and a client's path were the same object. The official
    MCP SDK's @mcp.tool returns the plain undecorated function, so the
    module-level name now bypasses every wrapper installed on the registry --
    sanitize_responses, measure_responses, contract_errors.

    Going through _tools keeps these tests on the path a request takes. A test
    calling the bare function would pass while the thing it guards sat switched
    off, which is the one failure mode those guards exist to prevent.
    """
    return mod.mcp._tool_manager._tools[name].fn


NON_FINITE = [float("inf"), float("-inf"), float("nan")]


class TestTheValuesThatBreakAStrictParser:
    @pytest.mark.parametrize("bad", NON_FINITE, ids=["inf", "-inf", "nan"])
    def test_each_becomes_null(self, bad: float) -> None:
        assert json_safe({"v": bad}) == {"v": None}

    @pytest.mark.parametrize("bad", NON_FINITE, ids=["inf", "-inf", "nan"])
    def test_the_result_survives_a_strict_encoder(self, bad: float) -> None:
        """allow_nan=False is what a spec-compliant encoder does."""
        json.dumps(json_safe({"mean": bad, "std": bad}), allow_nan=False)

    def test_the_reported_response_shape(self) -> None:
        payload = {"column": "ctr_pct", "mean": float("inf"), "std": float("nan"), "max": float("inf")}
        safe = json_safe(payload)
        assert safe == {"column": "ctr_pct", "mean": None, "std": None, "max": None}
        assert "Infinity" not in json.dumps(safe, allow_nan=False)


class TestItLeavesEverythingElseAlone:
    """A sanitiser that flattens real data is worse than the bug it replaces."""

    def test_finite_numbers_are_untouched(self) -> None:
        assert json_safe({"a": 1.5, "b": 0.0, "c": -3, "d": 1e308}) == {"a": 1.5, "b": 0.0, "c": -3, "d": 1e308}

    def test_booleans_stay_boolean(self) -> None:
        out = json_safe({"t": True, "f": False})
        assert out["t"] is True and out["f"] is False

    def test_the_string_nan_is_data_not_a_float(self) -> None:
        assert json_safe({"name": "NaN", "note": "Infinity"}) == {"name": "NaN", "note": "Infinity"}

    def test_none_and_text_pass_through(self) -> None:
        assert json_safe({"s": "text", "n": None}) == {"s": "text", "n": None}

    def test_it_reaches_into_nested_lists_and_dicts(self) -> None:
        got = json_safe({"rows": [{"a": float("nan")}, {"b": 2.0}], "meta": {"c": float("inf")}})
        assert got == {"rows": [{"a": None}, {"b": 2.0}], "meta": {"c": None}}

    def test_numpy_scalars_are_handled(self) -> None:
        np = pytest.importorskip("numpy")
        got = json_safe({"x": np.float64("inf"), "y": np.float64(2.5)})
        assert got["x"] is None
        assert float(got["y"]) == 2.5


class TestTheToolThatActuallyEmittedIt:
    def test_read_column_stats_is_strictly_parseable(self, tmp_path) -> None:
        """End to end: a divide-by-zero column, through the registered tool."""
        from servers.data_basic import server as s

        csv = tmp_path / "d.csv"
        csv.write_text("clicks,impressions\n5,0\n3,2\n0,0\n")
        patched = tool_fn(s, "apply_patch")(
            file_path=str(csv),
            ops=[{"op": "column_math", "target_column": "ctr", "formula": "clicks/impressions*100"}],
        )
        if not patched.get("success"):
            pytest.skip(f"column_math unavailable here: {patched.get('error')}")
        r = tool_fn(s, "read_column_stats")(file_path=str(csv), column="ctr")
        # The assertion that matters: a spec-compliant encoder must accept it.
        json.dumps(r, allow_nan=False)
