"""A saved chain is a function: params with defaults, called with new values and tables.

Without this "same job, different threshold" meant writing the chain out
again, and "same cleaning on the table I have in hand" meant writing it to a
file first. What has to hold:

- save_as stores the chain only once it ran, and a dry run stores nothing;
- a call's args set the callee's params, and nothing else: an unknown one is
  refused by name;
- a table from the calling chain replaces a load step, and that file is not read;
- a scalar of the caller can be an argument -- its value is taken when it exists;
- a path is known before the run, so it can read a param but not a scalar;
- a chain that calls itself, directly or not, is refused before anything runs;
- a call that fails leaves nothing written, even the callee's own writes.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

from servers.data_transform import engine

CLEAN = [
    {"id": "min_amount", "param": 0},
    {"id": "file", "param": "orders.csv"},
    {"id": "orders", "load": "$file"},
    {"id": "clean", "ops": [{"op": "filter", "where": "status != 'cancelled' and amount >= $min_amount"}]},
]


@pytest.fixture
def data(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
    pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4, 5, 6],
            "customer_id": ["c1", "c2", "c1", "c3", "c9", "c2"],
            "status": ["ok", "cancelled", "ok", "ok", "ok", "ok"],
            "amount": [100, 50, 200, 80, 40, 300],
        }
    ).to_csv(tmp_path / "orders.csv", index=False)
    return tmp_path


@pytest.fixture
def saved(data):
    r = engine.run_chain(CLEAN, save_as="clean.chain.json")
    assert r["success"] is True, r.get("error")
    return data / "clean.chain.json"


def _amounts(r: dict) -> list:
    assert r["success"] is True, r.get("error")
    return sorted(row["amount"] for row in r["result"]["data"])


class TestSaving:
    def test_the_file_holds_the_steps_and_the_params(self, saved):
        body = json.loads(saved.read_text(encoding="utf-8"))
        assert body["format"] == "mcp-chain/1"
        assert body["params"] == {"min_amount": 0, "file": "orders.csv"}
        assert body["steps"] == CLEAN

    def test_a_dry_run_saves_nothing(self, data):
        r = engine.run_chain(CLEAN, dry_run=True, save_as="clean.chain.json")
        assert r["would_save"].endswith("clean.chain.json")
        assert not (data / "clean.chain.json").exists()

    def test_a_chain_that_failed_is_not_saved(self, data):
        broken = [*CLEAN, {"ops": [{"op": "derive", "name": "x", "expr": "nope"}]}]
        assert engine.run_chain(broken, save_as="broken.chain.json")["success"] is False
        assert not (data / "broken.chain.json").exists()

    def test_save_as_is_a_json_file(self, data):
        r = engine.run_chain(CLEAN, save_as="clean.csv")
        assert r["success"] is False and "must end in .json" in r["error"]


class TestCalling:
    def test_the_defaults_run_when_no_args_are_given(self, saved):
        assert _amounts(engine.run_chain([{"call": "clean.chain.json"}])) == [40, 80, 100, 200, 300]

    def test_args_set_the_params(self, saved):
        assert _amounts(engine.run_chain([{"call": "clean.chain.json", "args": {"min_amount": 100}}])) == [
            100,
            200,
            300,
        ]

    def test_a_param_can_name_the_file(self, data, saved):
        pd.DataFrame({"status": ["ok", "ok"], "amount": [7, 9]}).to_csv(data / "q3.csv", index=False)
        r = engine.run_chain([{"call": "clean.chain.json", "args": {"file": "q3.csv"}}])
        assert _amounts(r) == [7, 9]

    def test_a_table_replaces_a_load_and_its_file_is_not_read(self, data, saved):
        r = engine.run_chain(
            [
                {"id": "raw", "load": "orders.csv"},
                {"id": "half", "ops": [{"op": "derive", "name": "amount", "expr": "amount / 2"}]},
                {
                    "call": "clean.chain.json",
                    "args": {"min_amount": 100, "file": "gone.csv"},
                    "tables": {"orders": "half"},
                },
            ]
        )
        assert _amounts(r) == [100.0, 150.0]  # gone.csv does not exist, and was never needed

    def test_a_scalar_of_the_caller_is_an_argument(self, saved):
        r = engine.run_chain(
            [
                {"id": "raw", "load": "orders.csv"},
                {"id": "avg", "scalar": "mean(amount)"},
                {"call": "clean.chain.json", "args": {"min_amount": "$avg"}},
            ]
        )
        assert _amounts(r) == [200, 300]  # mean 128.3
        assert r["steps"][-1]["params"]["min_amount"] == pytest.approx(128.333, abs=1e-3)

    def test_the_call_step_is_a_table_later_steps_read(self, data, saved):
        r = engine.run_chain(
            [
                {"id": "c", "call": "clean.chain.json"},
                {"group_by": "customer_id", "agg": {"n": "count()"}},
                {"write": "per_customer.csv"},
            ]
        )
        assert r["success"] is True, r.get("error")
        saved_rows = pd.read_csv(data / "per_customer.csv").set_index("customer_id")["n"].to_dict()
        assert saved_rows == {"c1": 2, "c2": 1, "c3": 1, "c9": 1}


class TestACallIsCheckedBeforeItRuns:
    def test_an_unknown_arg_is_refused_by_name(self, saved):
        r = engine.run_chain([{"call": "clean.chain.json", "args": {"min": 1}}])
        assert "has no param 'min' -- did you mean 'min_amount'?" in r["error"]

    def test_a_table_for_a_step_that_is_not_a_load(self, saved):
        r = engine.run_chain(
            [{"id": "x", "load": "orders.csv"}, {"call": "clean.chain.json", "tables": {"clean": "x"}}]
        )
        assert "has no load step 'clean' to feed -- its loads: orders" in r["error"]

    def test_a_chain_that_calls_itself(self, data):
        (data / "a.chain.json").write_text(json.dumps({"steps": [{"call": "b.chain.json"}]}), encoding="utf-8")
        (data / "b.chain.json").write_text(json.dumps({"steps": [{"call": "a.chain.json"}]}), encoding="utf-8")
        r = engine.run_chain([{"call": "a.chain.json"}])
        assert "a.chain.json -> b.chain.json -> a.chain.json calls itself" in r["error"]

    def test_a_file_that_is_not_a_chain(self, data):
        (data / "x.json").write_text('{"rows": []}', encoding="utf-8")
        assert "is not a saved chain" in engine.run_chain([{"call": "x.json"}])["error"]

    def test_a_problem_inside_the_callee_names_both(self, data):
        (data / "bad.chain.json").write_text(
            json.dumps({"steps": [{"load": "orders.csv"}, {"ops": [{"op": "fill_null"}]}]}), encoding="utf-8"
        )
        r = engine.run_chain([{"id": "c", "call": "bad.chain.json"}])
        assert "step 'c' (call) -> bad.chain.json: step 'step2' (ops) ops[0]: unknown op 'fill_null'" in r["error"]

    def test_a_path_cannot_wait_for_a_scalar(self, data):
        r = engine.run_chain([{"load": "orders.csv"}, {"id": "s", "scalar": "sum(amount)"}, {"write": "out_$s.csv"}])
        assert "$s, which is only known once the chain runs" in r["error"]


class TestAFailedCallWritesNothing:
    def test_the_callees_own_write_is_dropped(self, data):
        writer = [
            {"id": "limit", "param": "amount"},
            {"load": "orders.csv"},
            {"write": "from_callee.csv"},
            {"ops": [{"op": "derive", "name": "x", "expr": "$limit * 2"}]},
        ]
        (data / "writer.chain.json").write_text(json.dumps({"steps": writer}), encoding="utf-8")
        r = engine.run_chain([{"call": "writer.chain.json"}])
        # The callee RAN -- its write step was reached -- and failed after it.
        assert r["success"] is False and "in writer.chain.json, step 'step4' (ops) failed" in r["error"]
        assert not (data / "from_callee.csv").exists()

    def test_a_skipped_call_drops_its_writes_and_the_rest_runs(self, data):
        writer = [
            {"load": "orders.csv"},
            {"write": "from_callee.csv"},
            {"ops": [{"op": "derive", "name": "x", "expr": "nope"}]},
        ]
        (data / "writer.chain.json").write_text(json.dumps({"steps": writer}), encoding="utf-8")
        r = engine.run_chain(
            [
                {"id": "c", "call": "writer.chain.json", "on_error": "skip"},
                {"load": "orders.csv"},
                {"write": "after.csv"},
            ]
        )
        assert r["success"] is True and r["skipped"][0]["id"] == "c"
        assert (data / "after.csv").exists() and not (data / "from_callee.csv").exists()
