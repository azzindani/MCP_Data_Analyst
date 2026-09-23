"""A whole job -- load, clean, compute a value, join, group, write -- in one call.

"Clean the orders, join the customers, total by region, save it" took one
call per step, each re-reading the file the last one wrote, or a script.
run_chain says it as a list of named steps. What has to hold:

- the answer is the right number, not just a success;
- the whole chain is checked before any file is read, every problem at once;
- nothing is written until every step ran, so a failure leaves files as they were;
- a failing step is named, with the columns its input held;
- a $name is a value a scalar step computed, and nothing else;
- aggregates refuse what they cannot mean instead of guessing.
"""

from __future__ import annotations

import asyncio
import sys

import pandas as pd
import pytest

from servers.data_transform import engine
from servers.data_transform.server import mcp as transform
from shared.expr import FormulaError, aggregate


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
            "discount": [0.1, 0.0, 0.0, 0.5, 0.0, 0.2],
        }
    ).to_csv(tmp_path / "orders.csv", index=False)
    pd.DataFrame({"customer_id": ["c1", "c2", "c3"], "region": ["North", "South", "North"]}).to_csv(
        tmp_path / "customers.csv", index=False
    )
    return tmp_path


def _example(write: str = "region_summary.csv") -> list[dict]:
    return [
        {"id": "orders", "load": "orders.csv"},
        {"id": "cust", "load": "customers.csv"},
        {
            "id": "clean",
            "from": "orders",
            "ops": [
                {"op": "filter", "where": "status != 'cancelled' and amount > 0"},
                {"op": "derive", "name": "net", "expr": "amount * (1 - discount)"},
            ],
        },
        {"id": "p95", "from": "clean", "scalar": "percentile(net, 95)"},
        {"id": "joined", "join": ["clean", "cust"], "on": "customer_id", "how": "left"},
        {
            "id": "by_region",
            "from": "joined",
            "group_by": ["region"],
            "agg": {
                "revenue": "sum(net)",
                "buyers": "count_distinct(customer_id)",
                "big": "count_if(net > $p95 / 2)",
            },
        },
        {"id": "save", "from": "by_region", "write": write},
    ]


class TestTheAnswerIsRight:
    def test_the_worked_example(self, data):
        r = engine.run_chain(_example())
        assert r["success"] is True, r.get("error")
        # net: 90, 200, 40, 40 (c9), 240; percentile 95 by linear interpolation
        assert r["variables"]["p95"] == pytest.approx(232.0)
        saved = pd.read_csv(data / "region_summary.csv", keep_default_na=False)
        rows = {row["region"]: row for row in saved.to_dict(orient="records")}
        assert rows["North"]["revenue"] == pytest.approx(330.0)  # c1 90+200, c3 40
        assert rows["North"]["buyers"] == 2
        assert rows["South"]["big"] == 1  # 240 > 116
        assert rows[""]["revenue"] == pytest.approx(40.0)  # c9 has no customer row: its own group

    def test_a_count_is_a_whole_number(self, data):
        r = engine.run_chain(_example(), dry_run=True)
        big = [row["big"] for row in r["result"]["data"]]
        assert all(isinstance(v, int) for v in big), big

    def test_a_linear_chain_needs_no_ids(self, data):
        r = engine.run_chain(
            [{"load": "orders.csv"}, {"group_by": "status", "agg": {"n": "count()"}}, {"write": "counts.csv"}]
        )
        assert r["success"] is True, r.get("error")
        assert pd.read_csv(data / "counts.csv").set_index("status")["n"].to_dict() == {"cancelled": 1, "ok": 5}

    def test_a_variable_reaches_an_apply_patch_op(self, data):
        r = engine.run_chain(
            [
                {"id": "o", "load": "orders.csv"},
                {"id": "avg", "scalar": "mean(amount)"},
                {"from": "o", "ops": [{"op": "filter_between", "column": "amount", "min": "$avg", "max": 1000}]},
            ]
        )
        assert r["success"] is True, r.get("error")
        assert sorted(row["amount"] for row in r["result"]["data"]) == [200, 300]  # mean is 128.3


class TestNothingIsWrittenUnlessEveryStepRan:
    def test_a_write_above_a_failing_step_is_not_made(self, data):
        r = engine.run_chain(
            [
                {"load": "orders.csv"},
                {"write": "early.csv"},
                {"id": "late", "ops": [{"op": "derive", "name": "x", "expr": "amont * 2"}]},
            ]
        )
        assert r["success"] is False
        assert not (data / "early.csv").exists()
        assert r["failed_step"] == "late"
        assert "Nothing was written" in r["hint"]

    def test_a_dry_run_writes_nothing_and_says_what_it_would(self, data):
        r = engine.run_chain(_example(), dry_run=True)
        assert r["success"] is True and r["dry_run"] is True
        assert not (data / "region_summary.csv").exists()
        assert r["would_write"][0]["rows"] == 3
        clean = next(s for s in r["steps"] if s["id"] == "clean")
        assert clean["rows"] == 5 and clean["rows_before"] == 6 and clean["added"] == ["net"]
        assert len(clean["sample"]) == 3

    def test_a_file_it_replaces_is_snapshotted_first(self, data):
        (data / "region_summary.csv").write_text("old\n1\n", encoding="utf-8")
        r = engine.run_chain(_example())
        backup = r["written"][0]["backup"]
        assert open(backup, encoding="utf-8").read() == "old\n1\n"

    def test_a_zero_padded_id_survives_the_trip(self, data):
        (data / "staff.csv").write_text("employee_id,hours\n0007,5\n0012,3\n", encoding="utf-8")
        engine.run_chain([{"load": "staff.csv"}, {"write": "staff_copy.csv"}])
        assert (data / "staff_copy.csv").read_text(encoding="utf-8").splitlines()[1].startswith("0007,")


class TestTheWholeChainIsCheckedFirst:
    def test_every_problem_is_named_at_once(self, data):
        r = engine.run_chain(
            [
                {"id": "o", "lod": "orders.csv"},
                {"id": "x", "from": "nope", "ops": [{"op": "fill_null", "column": "a"}]},
                {"id": "s", "from": "x", "scalar": "sum(a) > $missing"},
                {"write": "out.xlsx"},
            ]
        )
        assert r["success"] is False and "steps" not in r
        text = " | ".join(r["problems"])
        assert "did you mean load" in text
        assert "'nope', which is not a step" in text
        assert "did you mean 'fill_nulls'" in text
        assert "$missing, which no scalar step above it computes" in text
        assert "a chain writes .csv" in text

    def test_a_step_cannot_read_one_below_it(self, data):
        r = engine.run_chain(
            [{"id": "a", "from": "b", "ops": [{"op": "sort", "by": ["amount"]}]}, {"id": "b", "load": "orders.csv"}]
        )
        assert "which comes after it" in r["error"]

    def test_a_scalar_is_not_a_table(self, data):
        r = engine.run_chain(
            [{"load": "orders.csv"}, {"id": "t", "scalar": "sum(amount)"}, {"from": "t", "write": "x.csv"}]
        )
        assert "a scalar step: use its value inside a formula as $t" in r["error"]

    def test_one_action_per_step(self, data):
        r = engine.run_chain([{"id": "a", "load": "orders.csv", "write": "b.csv"}])
        assert "one action per step" in r["error"]

    def test_a_missing_file_is_answered_with_the_one_that_exists(self, data):
        r = asyncio.run(transform._tool_manager._tools["run_chain"].run({"steps": [{"load": "order.csv"}]}))
        assert r["success"] is False and r["error"] == "File not found: order.csv"
        assert "orders.csv" in r.get("did_you_mean", []), r


class TestAFailureKeepsTheModelGoing:
    def test_the_failing_step_names_its_input_columns(self, data):
        r = engine.run_chain(
            [{"load": "orders.csv"}, {"id": "d", "ops": [{"op": "derive", "name": "x", "expr": "amont"}]}]
        )
        assert r["failed_step"] == "d" and "amount" in r["columns_available"]
        assert "ops[0] (derive)" in r["error"]

    def test_skip_carries_on_and_a_reader_of_it_is_told(self, data):
        r = engine.run_chain(
            [
                {"id": "o", "load": "orders.csv"},
                {"id": "bad", "ops": [{"op": "derive", "name": "y", "expr": "nope"}], "on_error": "skip"},
                {"id": "after", "from": "o", "ops": [{"op": "sort", "by": ["amount"]}]},
            ]
        )
        assert r["success"] is True and r["skipped"][0]["id"] == "bad"
        r = engine.run_chain(
            [
                {"id": "o", "load": "orders.csv"},
                {"id": "bad", "ops": [{"op": "derive", "name": "y", "expr": "nope"}], "on_error": "skip"},
                {"id": "reader", "from": "bad", "write": "never.csv"},
            ]
        )
        assert r["success"] is False and "which was skipped" in r["error"]

    def test_a_fallback_runs_when_the_ops_fail(self, data):
        r = engine.run_chain(
            [
                {"load": "orders.csv"},
                {
                    "id": "f",
                    "ops": [{"op": "derive", "name": "y", "expr": "nope"}],
                    "fallback": [{"op": "derive", "name": "y", "expr": "amount"}],
                },
            ]
        )
        assert r["success"] is True
        assert "fallback ran" in r["steps"][1]["notes"][0]

    def test_until_stops_after_the_named_step(self, data):
        r = engine.run_chain(_example(), until="p95")
        assert [s["id"] for s in r["steps"]] == ["orders", "cust", "clean", "p95"]
        assert not (data / "region_summary.csv").exists()


class TestAJoinThatWouldNotFit:
    def test_it_is_refused_before_it_is_built(self, data, monkeypatch):
        monkeypatch.setattr(sys.modules[engine.run_chain.__module__], "_MAX_MERGE_ROWS", 10)
        r = engine.run_chain(
            [
                {"id": "a", "load": "orders.csv"},
                {"id": "b", "load": "orders.csv"},
                {"join": ["a", "b"], "on": "status", "how": "inner"},
            ]
        )
        assert r["success"] is False
        assert "would build 26 rows" in r["error"]  # 5 x 5 'ok' + 1 x 1 'cancelled'

    def test_a_key_missing_on_one_side_names_that_side(self, data):
        r = engine.run_chain(
            [
                {"id": "a", "load": "orders.csv"},
                {"id": "b", "load": "customers.csv"},
                {"join": ["a", "b"], "on": "status"},
            ]
        )
        assert "'b' has no column status" in r["error"]


class TestAnAggregateSaysWhatItCannotMean:
    @pytest.fixture
    def df(self):
        return pd.DataFrame({"g": ["a", "a", "b"], "x": [1.0, 3.0, 5.0], "y": [2.0, 2.0, 0.0]})

    def test_a_bare_column_is_told_to_wrap_it(self, df):
        with pytest.raises(FormulaError, match=r"wrap it, e\.g\. sum\(x\)"):
            aggregate("x + 1", df)

    def test_aggregates_do_not_nest(self, df):
        with pytest.raises(FormulaError, match="aggregates do not nest"):
            aggregate("sum(mean(x))", df)

    def test_a_percentile_is_between_0_and_100(self, df):
        with pytest.raises(FormulaError, match="from 0 to 100"):
            aggregate("percentile(x, 950)", df)

    def test_count_if_takes_a_condition(self, df):
        with pytest.raises(FormulaError, match="takes a condition"):
            aggregate("count_if(x)", df)

    def test_arithmetic_between_aggregates_per_group(self, df):
        out = aggregate("sum(x) / count()", df, ["g"])
        assert out.to_dict() == {"a": 2.0, "b": 5.0}

    def test_a_ratio_of_sums_is_not_a_mean_of_ratios(self, df):
        assert aggregate("sum(x) / sum(y)", df) == pytest.approx(9.0 / 4.0)


class TestTheDomainToolHasIt:
    def test_data_edit_runs_a_chain(self, data):
        from servers.data_domain.server import mcp as domain

        r = asyncio.run(
            domain._tool_manager._tools["data_edit"].run(
                {"action": "run_chain", "args": {"steps": [{"load": "orders.csv"}], "dry_run": True}}
            )
        )
        assert r["success"] is True and r["op"] == "run_chain"
