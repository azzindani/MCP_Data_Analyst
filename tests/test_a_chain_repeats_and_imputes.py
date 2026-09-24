"""A chain repeats ops over columns with for_each, and fills gaps with impute.

"Clip every money column", "log every measure", "fill every gap the way
smart_impute does" meant writing the same op once per column, or leaving the
chain for another tool. Two ops close that:

- `for_each {columns, as, do}` repeats `do` once per column. `$column` (or
  the name `as` gives) is the column: backticked inside a formula, as-is in
  any other field, and `${column}` inside longer text such as a new column's
  name. It is expanded before anything runs, so every expanded op is checked
  like any other and a failure names the column it was for.
- `impute {columns}` fills as smart_impute does: a number column by its
  median, a date by the value before, text by its mode.

What has to hold: the answer is pandas's own, the saved chain keeps the
for_each as written, and whatever cannot work is refused by name.
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from servers.data_medium._med_transform import smart_impute
from servers.data_transform import engine
from servers.data_transform._chain import MAX_EXPANDED_OPS, MAX_FOR_EACH_COLUMNS


@pytest.fixture
def data(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
    pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4, 5, 6],
            "region": ["North", None, "South", "North", None, "East"],
            "amount": [100.0, np.nan, 250.0, 80.0, 40.0, np.nan],
            "unit price": [10.0, 20.0, 5.0, np.nan, 8.0, 12.0],
            "empty": [np.nan] * 6,
        }
    ).to_csv(tmp_path / "orders.csv", index=False)
    return tmp_path


def _run(steps: list[dict], **kw) -> dict:
    return engine.run_chain([{"id": "orders", "load": "orders.csv"}, *steps], **kw)


def _table(data, name: str = "out.csv") -> pd.DataFrame:
    return pd.read_csv(data / name)


class TestForEach:
    def test_each_column_gets_the_ops_with_its_own_name(self, data):
        steps = [
            {
                "id": "clean",
                "ops": [
                    {
                        "op": "for_each",
                        "columns": ["amount", "unit price"],
                        "do": [
                            {"op": "clip_values", "column": "$column", "min": 0, "max": 90},
                            {"op": "derive", "name": "${column}_x2", "expr": "$column * 2"},
                        ],
                    }
                ],
            },
            {"write": "out.csv"},
        ]
        r = _run(steps)
        assert r["success"] is True, r.get("error")
        got, raw = _table(data), pd.read_csv(data / "orders.csv")
        for col in ("amount", "unit price"):
            clipped = raw[col].clip(0, 90)
            pd.testing.assert_series_equal(got[col], clipped, check_names=False)
            pd.testing.assert_series_equal(got[f"{col}_x2"], clipped * 2, check_names=False)

    def test_as_names_the_placeholder(self, data):
        op = {"op": "for_each", "columns": ["amount"], "as": "c", "do": [{"op": "filter", "where": "$c > 50"}]}
        r = _run([{"ops": [op]}, {"write": "out.csv"}])
        assert r["success"] is True, r.get("error")
        assert _table(data)["amount"].tolist() == [100.0, 250.0, 80.0]

    def test_a_failure_names_the_column_it_was_for(self, data):
        op = {"op": "for_each", "columns": ["amount", "gone"], "do": [{"op": "round_values", "column": "$column"}]}
        r = _run([{"id": "clean", "ops": [op]}])
        assert r["success"] is False
        assert "ops[0] for_each 'gone' do[0] (round_values)" in r["error"], r["error"]

    def test_the_saved_chain_keeps_it_as_written(self, data):
        op = {"op": "for_each", "columns": ["amount"], "do": [{"op": "round_values", "column": "$column"}]}
        r = _run([{"ops": [op]}], save_as="rounded.chain.json")
        assert r["success"] is True, r.get("error")
        saved = json.loads((data / "rounded.chain.json").read_text(encoding="utf-8"))
        assert saved["steps"][1]["ops"] == [op]

    @pytest.mark.parametrize(
        ("op", "says"),
        [
            ({"op": "for_each", "do": [{"op": "round_values", "column": "$column"}]}, "columns must list the columns"),
            (
                {"op": "for_each", "columns": ["a", "a"], "do": [{"op": "drop_column", "columns": ["$column"]}]},
                "names 'a' more than once",
            ),
            ({"op": "for_each", "columns": ["amount"]}, "do must be the ops to repeat"),
            (
                {"op": "for_each", "columns": ["amount"], "as": "1x", "do": [{"op": "round_values"}]},
                "as '1x' must be a name",
            ),
            (
                {"op": "for_each", "columns": ["amount"], "each": 1, "do": [{"op": "round_values"}]},
                "unknown field(s) each",
            ),
            (
                {"op": "for_each", "columns": ["a`b"], "do": [{"op": "round_values", "column": "$column"}]},
                "holds a backtick",
            ),
            (
                {"op": "for_each", "columns": ["amount"], "do": [{"op": "for_each", "columns": ["x"], "do": []}]},
                "a for_each inside a for_each",
            ),
            (
                {"op": "for_each", "columns": ["amount"], "do": [{"op": "round_value", "column": "$column"}]},
                "ops[0] for_each 'amount' do[0]: unknown op 'round_value' -- did you mean 'round_values'?",
            ),
            (
                {
                    "op": "for_each",
                    "columns": [f"c{i}" for i in range(MAX_FOR_EACH_COLUMNS + 1)],
                    "do": [{"op": "round_values", "column": "$column"}],
                },
                f"a for_each takes at most {MAX_FOR_EACH_COLUMNS}",
            ),
            (
                {
                    "op": "for_each",
                    "columns": [f"c{i}" for i in range(100)],
                    "do": [{"op": "round_values", "column": "$column"}] * 6,
                },
                f"expands to 600 ops; a step runs at most {MAX_EXPANDED_OPS}",
            ),
        ],
    )
    def test_refused_by_name_before_anything_runs(self, data, op, says):
        r = _run([{"ops": [op]}, {"write": "out.csv"}])
        assert r["success"] is False and says in r["error"], r.get("error")
        assert not (data / "out.csv").exists()

    def test_a_name_that_is_already_a_value_is_refused(self, data):
        steps = [
            {"id": "column", "scalar": "max(amount)"},
            {
                "from": "orders",
                "ops": [{"op": "for_each", "columns": ["amount"], "do": [{"op": "round_values", "column": "$column"}]}],
            },
        ]
        r = _run(steps)
        assert r["success"] is False and "as 'column' is also a step above it" in r["error"]


class TestImpute:
    def test_it_fills_as_smart_impute_does(self, data):
        r = _run([{"id": "filled", "ops": [{"op": "impute"}]}, {"write": "out.csv"}])
        assert r["success"] is True, r.get("error")
        tool = smart_impute(str(data / "orders.csv"), output_path=str(data / "tool.csv"), open_after=False)
        assert tool["success"] is True, tool
        pd.testing.assert_frame_equal(_table(data), _table(data, "tool.csv"))

    def test_the_note_says_what_it_filled_and_what_it_could_not(self, data):
        r = _run([{"id": "filled", "ops": [{"op": "impute"}]}])
        note = next(s for s in r["steps"] if s["id"] == "filled")["notes"][0]
        assert "filled region (2 by mode), amount (2 by median), unit price (1 by median)" in note
        assert "left 'empty' as is: every value is null" in note

    def test_only_the_columns_named(self, data):
        r = _run([{"ops": [{"op": "impute", "columns": "amount"}]}, {"write": "out.csv"}])
        got = _table(data)
        assert got["amount"].isna().sum() == 0 and got["region"].isna().sum() == 2

    def test_a_column_that_is_not_there(self, data):
        r = _run([{"ops": [{"op": "impute", "columns": ["amount", "gone"]}]}])
        assert r["success"] is False and "impute: no column 'gone'. Columns: order_id, region" in r["error"]

    def test_a_field_impute_does_not_take(self, data):
        r = _run([{"ops": [{"op": "impute", "strategy": "mean"}]}])
        assert r["success"] is False and "(impute): unknown field(s) strategy -- impute takes: columns" in r["error"]
