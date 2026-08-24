"""A filter condition with nothing to compare against is not a filter.

Both filter tools take `conditions: list[dict]`, so the keys inside sit one
level below where pydantic and strict_args can see. An earlier round taught
`between`, `isin` and `regex` to name the key they were missing, because those
three read unusually-named keys (min/max, values, pattern) that a caller cannot
guess. The ten ops that read plain `value` were not the ones being debugged, so
they were left alone -- and they are the ops everybody uses.

What they did instead, depending on which tool you called:

    filter_dataset(f, [{"column": "spend", "op": "gt"}])
    -> success: false, error: "'value'"

the whole error being one quoted word, from a KeyError that escaped into the
response; and

    filter_rows(f, [{"column": "region", "op": "equals"}])
    -> success: true, rows_kept: 0

which is the dangerous one. `cond.get("value")` returned None, every value was
compared against None, nothing matched, and the tool wrote an empty CSV and
reported a filter that worked. A condition missing its operand is not a
condition that excludes everything; it is a condition nobody finished writing.

The check is shared, so both tools refuse the same conditions with the same
words, and it runs before either one writes anything.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_medium"), str(ROOT / "servers" / "data_transform")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _med_inspect import filter_rows  # noqa: E402

from servers.data_transform.engine import filter_dataset  # noqa: E402

BOTH = pytest.mark.parametrize("filter_tool", [filter_dataset, filter_rows], ids=["filter_dataset", "filter_rows"])


@pytest.fixture
def csv(tmp_path):
    f = tmp_path / "in.csv"
    rows = "\n".join(f"W{i % 2},{i * 10},2024-01-{i:02d}" for i in range(1, 9))
    f.write_text(f"region,spend,d1\n{rows}\n")
    return f


# --- refusals ---------------------------------------------------------------


@BOTH
@pytest.mark.parametrize("op", ["equals", "not_equals", "contains", "gt", "lt", "gte", "lte"])
def test_a_comparison_with_no_value_is_refused(tmp_path, csv, filter_tool, op):
    out = tmp_path / "out.csv"
    r = filter_tool(str(csv), [{"column": "spend", "op": op}], output_path=str(out))
    assert r["success"] is False
    assert op in r["error"]
    assert "'value'" in r["error"]
    # The point of refusing before dispatch: nothing on disk.
    assert not out.exists()


@BOTH
@pytest.mark.parametrize(
    ("op", "named"),
    [("isin", "'values'"), ("not_isin", "'values'"), ("regex", "'pattern'")],
)
def test_an_op_with_an_unusual_key_still_names_it(tmp_path, csv, filter_tool, op, named):
    r = filter_tool(str(csv), [{"column": "region", "op": op}], output_path=str(tmp_path / "o.csv"))
    assert r["success"] is False
    assert named in r["error"]


@BOTH
def test_a_range_with_no_bounds_names_both(tmp_path, csv, filter_tool):
    r = filter_tool(str(csv), [{"column": "spend", "op": "between"}], output_path=str(tmp_path / "o.csv"))
    assert r["success"] is False
    assert "'min'" in r["error"] and "'max'" in r["error"]


@BOTH
def test_a_date_range_with_no_dates_is_refused(tmp_path, csv, filter_tool):
    """It would keep every row -- a filter that cannot filter, reported as one."""
    r = filter_tool(str(csv), [{"column": "d1", "op": "date_range"}], output_path=str(tmp_path / "o.csv"))
    assert r["success"] is False
    assert "start" in r["error"] and "end" in r["error"]


@BOTH
def test_a_number_op_given_text_says_so(tmp_path, csv, filter_tool):
    """Was "float() argument must be a string or a real number, not 'NoneType'",
    which names neither the column nor the condition it came from."""
    r = filter_tool(str(csv), [{"column": "spend", "op": "gt", "value": "abc"}], output_path=str(tmp_path / "o.csv"))
    assert r["success"] is False
    assert "spend" in r["error"]
    assert "number" in r["error"]


@BOTH
def test_the_offending_condition_is_identified_by_index(tmp_path, csv, filter_tool):
    r = filter_tool(
        str(csv),
        [{"column": "spend", "op": "gt", "value": 10}, {"column": "region", "op": "equals"}],
        output_path=str(tmp_path / "o.csv"),
    )
    assert r["success"] is False
    assert "Condition 1" in r["error"]


# --- and everything that was already valid still is -------------------------


@BOTH
@pytest.mark.parametrize(
    ("cond", "kept"),
    [
        ({"column": "spend", "op": "gt", "value": 30}, 5),
        ({"column": "spend", "op": "between", "min": 20, "max": 50}, 4),
        ({"column": "spend", "op": "between", "value": [20, 50]}, 4),
        ({"column": "region", "op": "isin", "values": ["W0"]}, 4),
        ({"column": "d1", "op": "date_range", "start": "2024-01-04"}, 5),
        ({"column": "spend", "op": "is_null"}, 0),
        ({"column": "spend", "op": "not_null"}, 8),
    ],
)
def test_a_complete_condition_filters_as_before(tmp_path, csv, filter_tool, cond, kept):
    """Read the file: both tools must keep the same rows, not merely succeed."""
    out = tmp_path / "out.csv"
    r = filter_tool(str(csv), [cond], output_path=str(out))
    assert r["success"] is True, r.get("error")
    assert len(out.read_text().strip().splitlines()) - 1 == kept


def test_an_operandless_op_needs_no_operand():
    """is_null and not_null compare against nothing, so they must not be caught
    by a check written for the ops that do."""
    from shared.column_utils import filter_operand_error

    for op in ("is_null", "not_null"):
        assert filter_operand_error({"column": "a", "op": op}, op, 0) == ""
