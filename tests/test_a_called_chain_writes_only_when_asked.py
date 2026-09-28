"""A saved chain called as a function runs its own writes only when the call asks.

The sweep saved a chain that wrote three files, then called it with
min_spend=100: the call ran the callee's write steps too and overwrote the three
files the first run had written -- at paths the caller never named. They were
snapshotted and listed in `written`, so nothing was lost, but a function should
not write where its caller did not say. A call now runs its chain's writes only
with `"writes": true`, says which it held back, and the exported script does the
same.
"""

from __future__ import annotations

import json
import subprocess
import sys

import pandas as pd
import pytest

from servers.data_transform import engine

CALLEE = [
    {"id": "min_amount", "param": 0},
    {"id": "orders", "load": "orders.csv"},
    {"id": "clean", "ops": [{"op": "filter", "where": "amount >= $min_amount"}]},
    {"id": "saved", "from": "clean", "write": "callee_out.csv"},
]


@pytest.fixture
def data(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
    pd.DataFrame({"order_id": [1, 2, 3], "amount": [40, 150, 300]}).to_csv(tmp_path / "orders.csv", index=False)
    (tmp_path / "callee.chain.json").write_text(json.dumps({"steps": CALLEE}), encoding="utf-8")
    return tmp_path


def _call(**extra) -> list[dict]:
    return [
        {"id": "c", "call": "callee.chain.json", "args": {"min_amount": 100}, **extra},
        {"from": "c", "write": "caller_out.csv"},
    ]


def test_a_call_does_not_write_the_callees_files(data):
    r = engine.run_chain(_call())
    assert r["success"] is True, r.get("error")
    assert [w["path"].replace("\\", "/").split("/")[-1] for w in r["written"]] == ["caller_out.csv"]
    assert not (data / "callee_out.csv").exists()
    step = next(s for s in r["steps"] if s["id"] == "c")
    assert step["writes_not_run"] == ["callee_out.csv"]
    assert "writes: true" in step["note"]
    # The table still flows through the held-back write step.
    assert pd.read_csv(data / "caller_out.csv")["amount"].tolist() == [150, 300]


def test_writes_true_runs_them(data):
    r = engine.run_chain(_call(writes=True))
    assert r["success"] is True, r.get("error")
    assert pd.read_csv(data / "callee_out.csv")["amount"].tolist() == [150, 300]
    assert "writes_not_run" not in next(s for s in r["steps"] if s["id"] == "c")


def test_a_dry_run_lists_only_the_writes_that_would_run(data):
    names = lambda r: [w["path"].replace("\\", "/").split("/")[-1] for w in r["would_write"]]  # noqa: E731
    assert names(engine.run_chain(_call(), dry_run=True)) == ["caller_out.csv"]
    assert sorted(names(engine.run_chain(_call(writes=True), dry_run=True))) == ["callee_out.csv", "caller_out.csv"]


def test_a_call_inside_a_call_that_holds_writes_holds_them_too(data):
    (data / "outer.chain.json").write_text(
        json.dumps({"steps": [{"id": "inner", "call": "callee.chain.json", "writes": True}]}), encoding="utf-8"
    )
    r = engine.run_chain([{"id": "o", "call": "outer.chain.json"}, {"from": "o", "write": "caller_out.csv"}])
    assert r["success"] is True, r.get("error")
    assert not (data / "callee_out.csv").exists()


def test_writes_is_true_or_false(data):
    r = engine.run_chain(_call(writes="yes"))
    assert r["success"] is False and "writes is true or false" in r["error"]


@pytest.mark.parametrize("writes", [False, True])
def test_the_exported_script_writes_what_the_chain_wrote(data, tmp_path_factory, writes):
    r = engine.run_chain(_call(writes=writes), export_pandas=True)
    assert r["success"] is True and "pandas" in r, r.get("pandas_refused") or r.get("error")
    elsewhere = tmp_path_factory.mktemp("script")
    for name in ("orders.csv", "callee.chain.json"):
        (elsewhere / name).write_bytes((data / name).read_bytes())
    (elsewhere / "chain.py").write_text(r["pandas"], encoding="utf-8")
    ran = subprocess.run([sys.executable, "chain.py"], cwd=elsewhere, capture_output=True, text=True, timeout=120)
    assert ran.returncode == 0, ran.stderr[-2000:]
    assert (elsewhere / "callee_out.csv").exists() is writes
    assert (elsewhere / "caller_out.csv").read_bytes() == (data / "caller_out.csv").read_bytes()
