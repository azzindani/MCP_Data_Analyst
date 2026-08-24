"""conditional_assign's conditions are a list of dicts nobody documented.

The catalog said `new_column, conditions: list[dict], default` and stopped, so
every key inside a condition was a guess. validate_ops checked that
`conditions` was a list and looked no further, which put the guess straight into
the handler, where `cond["label"]` raised:

    Op 0 (conditional_assign): 'label'

A bare KeyError, on an op that had already been accepted, naming a field the
caller had never been told existed and not saying what the other three are.
This is the same shape as the round-11 fill_zero/fill_zeros miss -- a list[dict]
sits one level below where strict_args and pydantic can see -- and the same
shape as the op-catalog defect it was found beside: the catalog taught a form
the tool rejects.

Two fixes, because either alone is half of one: the catalog now names the four
fields and the comparison vocabulary, and the validator refuses a bad condition
by index, naming the field. The symbol spellings are accepted because `>` is
what a caller writes first and it can only mean one thing.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_basic"), str(ROOT / "servers" / "data_medium")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _med_transform import run_cleaning_pipeline  # noqa: E402

from servers.data_basic.engine import _OP_CATALOG  # noqa: E402
from shared.patch_validator import validate_ops  # noqa: E402


@pytest.fixture
def csv(tmp_path):
    f = tmp_path / "in.csv"
    f.write_text("region,spend\n" + "\n".join(f"W,{i * 20}" for i in range(1, 6)) + "\n")
    return f


def _run(csv, tmp_path, conditions, default="lo"):
    return run_cleaning_pipeline(
        str(csv),
        ops=[{"op": "conditional_assign", "new_column": "band", "conditions": conditions, "default": default}],
        output_path=str(tmp_path / "out.csv"),
    )


def test_the_catalog_names_the_condition_fields():
    entry = next(e for ops in _OP_CATALOG.values() for e in ops if e["op"] == "conditional_assign")
    for field in ("column", "op", "value", "label"):
        assert field in entry["params"], entry["params"]


# --- the spellings a caller reaches for -------------------------------------


@pytest.mark.parametrize("symbol", [">", "gt"])
@pytest.mark.parametrize("label_key", ["label", "then", "result"])
def test_a_condition_written_the_obvious_way_runs(tmp_path, csv, symbol, label_key):
    r = _run(csv, tmp_path, [{"column": "spend", "op": symbol, "value": 50, label_key: "hi"}])
    assert r["success"] is True, r.get("error")
    written = (tmp_path / "out.csv").read_text()
    # 20 and 40 below the threshold, 60/80/100 above -- read the file, since the
    # response would say "1 condition applied" either way.
    assert written.count("lo") == 2
    assert written.count("hi") == 3


@pytest.mark.parametrize(
    ("symbol", "canonical"),
    [(">", "gt"), (">=", "gte"), ("<", "lt"), ("<=", "lte"), ("==", "equals"), ("!=", "not_equals"), ("in", "isin")],
)
def test_each_symbol_means_its_one_comparison(symbol, canonical):
    op = {
        "op": "conditional_assign",
        "new_column": "b",
        "conditions": [{"column": "spend", "op": symbol, "value": 50, "label": "hi"}],
    }
    assert validate_ops([op]) == []
    assert op["conditions"][0]["op"] == canonical


# --- and the refusals -------------------------------------------------------


def test_a_condition_missing_a_field_is_named_before_anything_is_written(tmp_path, csv):
    r = _run(csv, tmp_path, [{"column": "spend", "op": "gt", "value": 50}])
    assert r["success"] is False
    assert "condition 0" in r["error"]
    assert "label" in r["error"]
    assert r["applied"] == 0
    assert not (tmp_path / "out.csv").exists()


def test_a_misspelled_condition_field_gets_a_suggestion(tmp_path, csv):
    r = _run(csv, tmp_path, [{"colunm": "spend", "op": "gt", "value": 50, "label": "hi"}])
    assert r["success"] is False
    assert "did you mean column?" in r["error"]


def test_an_unknown_comparison_lists_the_valid_ones(tmp_path, csv):
    r = _run(csv, tmp_path, [{"column": "spend", "op": "roughly", "value": 50, "label": "hi"}])
    assert r["success"] is False
    assert "roughly" in r["error"]
    assert "not_equals" in r["error"]


def test_the_offending_condition_is_identified_by_index(tmp_path, csv):
    r = _run(
        csv,
        tmp_path,
        [
            {"column": "spend", "op": "gt", "value": 80, "label": "hi"},
            {"column": "spend", "op": "gt", "value": 50},
        ],
    )
    assert r["success"] is False
    assert "condition 1" in r["error"]
    assert "condition 0" not in r["error"]


def test_an_empty_condition_list_says_what_would_happen(tmp_path, csv):
    """Every row would take `default` -- valid Python, and never the intent."""
    r = _run(csv, tmp_path, [])
    assert r["success"] is False
    assert "default" in r["error"]
