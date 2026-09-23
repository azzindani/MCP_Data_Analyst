"""export_pandas: the chain as a pandas script that writes the same files.

"The same" is the whole claim, so every case here runs the exported script
in a copy of the chain's inputs and compares the files it writes with the
files the chain wrote, byte for byte. A formula's meaning is where an export
drifts first -- integer arithmetic where the chain used floats, `and` read
as Python's `and`, a missing value read as true -- so the cases lean on
formulas, aggregates and the ops the export claims to cover. An op it does
not cover is refused by name, not approximated.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys

import pandas as pd
import pytest

from servers.data_transform import engine
from servers.data_transform._chain_export import EXPORTED_OPS

INPUTS = ("orders.csv", "customers.csv", "staff.csv")

# One case per op the export claims -- every fill_nulls strategy among them.
OPS = [
    {"op": "drop_column", "columns": ["discount"]},
    {"op": "sort", "by": ["amount", "order_id"], "ascending": False},
    {"op": "drop_duplicates", "subset": ["customer_id"], "keep": "last"},
    {"op": "dedup_subset", "columns": ["status"]},
    {"op": "filter_isin", "column": "status", "values": ["ok"]},
    {"op": "filter_not_isin", "column": "customer_id", "values": ["c1"]},
    {"op": "filter_between", "column": "amount", "min": 50, "max": 200, "inclusive": "left"},
    {"op": "filter_top_n", "column": "amount", "n": 3, "keep": "bottom"},
    {"op": "clip_values", "column": "amount", "min": 60, "max": 250},
    {"op": "round_values", "column": "discount", "decimals": 0},
    {"op": "abs_values", "column": "amount", "new_column": "size"},
    {"op": "fill_nulls", "column": "amount", "strategy": "median"},
    {"op": "fill_nulls", "column": "amount", "strategy": "mean", "fill_zeros": True},
    {"op": "fill_nulls", "column": "customer_id", "strategy": "mode"},
    {"op": "fill_nulls", "column": "amount", "strategy": "ffill"},
    {"op": "fill_nulls", "column": "amount", "strategy": "bfill"},
    {"op": "fill_nulls", "column": "customer_id", "strategy": "drop"},
    {"op": "fill_nulls", "column": "customer_id", "strategy": "value", "value": "unknown"},
]


@pytest.fixture
def data(tmp_path, monkeypatch):
    here = tmp_path / "chain"
    here.mkdir()
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(here))
    monkeypatch.setenv("MCP_DATA_ROOT", str(here))
    pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4, 5, 6, 7],
            "customer_id": ["c1", "c2", "c1", "c3", "c9", "c2", None],
            "status": ["ok", "cancelled", "ok", "ok", "ok", "ok", "ok"],
            "amount": [100, 50, 200, 80, None, 300, 0],
            "discount": [0.1, 0.0, 0.0, 0.5, 0.0, 0.2, 0.0],
        }
    ).to_csv(here / "orders.csv", index=False)
    pd.DataFrame({"customer_id": ["c1", "c2", "c3"], "region": ["North", "South", "North"]}).to_csv(
        here / "customers.csv", index=False
    )
    (here / "staff.csv").write_text("employee_id,hours\n0007,5\n0012,3\n0031,12\n", encoding="utf-8")
    return here


def _same(data, steps: list[dict]) -> dict:
    """Run the chain, run its export in a copy of the inputs, and require identical written files."""
    r = engine.run_chain(steps, export_pandas=True)
    assert r["success"] is True, r.get("error")
    assert "pandas" in r, r.get("pandas_refused")
    assert r.get("written"), "a case must write something to compare"
    elsewhere = data.parent / "script"
    elsewhere.mkdir(exist_ok=True)
    for name in [*INPUTS, *(p.name for p in data.glob("*.chain.json"))]:
        shutil.copy(data / name, elsewhere / name)
    (elsewhere / "chain.py").write_text(r["pandas"], encoding="utf-8")
    ran = subprocess.run([sys.executable, "chain.py"], cwd=elsewhere, capture_output=True, text=True, timeout=120)
    assert ran.returncode == 0, ran.stderr[-2000:]
    for written in r["written"]:
        name = written["path"].replace("\\", "/").split("/")[-1]
        ours = (data / name).read_bytes().replace(b"\r\n", b"\n")
        theirs = (elsewhere / name).read_bytes().replace(b"\r\n", b"\n")
        assert theirs == ours, f"{name} differs:\nchain:\n{ours.decode()}\nscript:\n{theirs.decode()}"
    return r


class TestTheScriptWritesTheSameFiles:
    def test_formulas_aggregates_and_a_join(self, data):
        _same(
            data,
            [
                {"id": "minimum", "param": 0},
                {"id": "orders", "load": "orders.csv"},
                {"id": "cust", "load": "customers.csv"},
                {
                    "id": "clean",
                    "from": "orders",
                    "ops": [
                        {"op": "filter", "where": "status != 'cancelled' and amount > $minimum"},
                        {"op": "derive", "name": "net", "expr": "amount * (1 - discount)"},
                        {"op": "derive", "name": "band", "expr": "if_else(net > 100, 'big', 'small')"},
                        {"op": "derive", "name": "odd", "expr": "-(order_id % 2) + round(net / 3, 1) ** 1"},
                        {"op": "derive", "name": "safe", "expr": "coalesce(customer_id, 'none')"},
                        {"op": "derive", "name": "flag", "expr": "not isnull(customer_id) or 1 < order_id < 3"},
                    ],
                },
                {"id": "p95", "scalar": "percentile(net, 95)"},
                {"id": "joined", "join": ["clean", "cust"], "on": "customer_id", "how": "left"},
                {
                    "group_by": ["region", "band"],
                    "agg": {
                        "revenue": "sum(net)",
                        "buyers": "count_distinct(customer_id)",
                        "big": "count_if(net > $p95 / 2)",
                        "avg": "sum(net) / count()",
                        "spread": "max(net) - min(net)",
                        "first": "first(order_id)",
                        "mid": "median(net)",
                    },
                },
                {"write": "summary.csv"},
            ],
        )

    @pytest.mark.parametrize(
        "op",
        OPS,
        ids=lambda op: f"{op['op']}-{op.get('strategy', '')}",
    )
    def test_every_op_it_claims(self, data, op):
        _same(data, [{"load": "orders.csv"}, {"ops": [op]}, {"write": "out.csv"}])

    def test_it_claims_exactly_the_ops_tested_here(self):
        # A translation added without a case here is a claim nothing checks.
        assert set(EXPORTED_OPS) == {"filter", "derive"} | {op["op"] for op in OPS}

    def test_a_variable_as_an_op_value(self, data):
        _same(
            data,
            [
                {"id": "o", "load": "orders.csv"},
                {"id": "avg", "scalar": "mean(amount)"},
                {"from": "o", "ops": [{"op": "filter_between", "column": "amount", "min": "$avg", "max": 1000}]},
                {"write": "out.csv"},
            ],
        )

    def test_a_zero_padded_id_stays_text(self, data):
        _same(
            data,
            [{"load": "staff.csv"}, {"ops": [{"op": "derive", "name": "x2", "expr": "hours * 2"}]}, {"write": "s.csv"}],
        )

    def test_a_called_chain_becomes_a_function(self, data):
        callee = [
            {"id": "min_amount", "param": 0},
            {"id": "file", "param": "orders.csv"},
            {"id": "orders", "load": "$file"},
            {"id": "clean", "ops": [{"op": "filter", "where": "status != 'cancelled' and amount >= $min_amount"}]},
        ]
        (data / "clean.chain.json").write_text(json.dumps({"steps": callee}), encoding="utf-8")
        r = _same(
            data,
            [
                {"id": "raw", "load": "orders.csv"},
                {"id": "avg", "scalar": "mean(amount)"},
                {"id": "half", "from": "raw", "ops": [{"op": "derive", "name": "amount", "expr": "amount / 2"}]},
                {"id": "c", "call": "clean.chain.json", "args": {"min_amount": "$avg"}, "tables": {"orders": "half"}},
                {"id": "again", "call": "clean.chain.json", "args": {"min_amount": 150}},
                {"from": "c", "write": "caller_out.csv"},
                {"from": "again", "write": "again_out.csv"},
            ],
        )
        assert "def clean_chain(min_amount=0, file='orders.csv', orders=None):" in r["pandas"]
        assert r["pandas"].count("def clean_chain") == 1

    def test_skip_and_fallback(self, data):
        _same(
            data,
            [
                {"load": "orders.csv"},
                {"id": "bad", "ops": [{"op": "derive", "name": "x", "expr": "nope"}], "on_error": "skip"},
                {
                    "id": "fb",
                    "from": "step1",
                    "ops": [{"op": "derive", "name": "y", "expr": "nope"}],
                    "fallback": [{"op": "derive", "name": "y", "expr": "amount * 3"}],
                },
                {"write": "out.csv"},
            ],
        )

    def test_an_id_that_is_a_name_the_script_uses(self, data):
        _same(
            data,
            [
                {"id": "t", "param": 100},
                {"id": "df", "load": "orders.csv"},
                {"ops": [{"op": "filter", "where": "amount >= $t"}]},
                {"write": "out.csv"},
            ],
        )


class TestWhatItCannotWriteIsRefusedByName:
    def test_an_op_without_a_translation(self, data):
        r = engine.run_chain(
            [{"load": "orders.csv"}, {"ops": [{"op": "cast_column", "column": "amount", "dtype": "str"}]}],
            dry_run=True,
            export_pandas=True,
        )
        assert r["success"] is True and "pandas" not in r
        assert "op 'cast_column' has no pandas translation yet" in r["pandas_refused"]
        assert "pandas export was refused" in r["hint"]

    def test_a_dry_run_exports_and_writes_nothing(self, data):
        r = engine.run_chain(
            [{"load": "orders.csv"}, {"write": "never.csv"}],
            dry_run=True,
            export_pandas=True,
        )
        assert "_writes.append((t['step2'], 'never.csv'))" in r["pandas"]
        assert not (data / "never.csv").exists()

    def test_no_export_unless_asked(self, data):
        assert "pandas" not in engine.run_chain([{"load": "orders.csv"}], dry_run=True)
