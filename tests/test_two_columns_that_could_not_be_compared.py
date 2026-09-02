"""A filter could compare a column to a literal and to nothing else.

"Which rows disagree between these two columns" had no expression anywhere in
the tool surface. Every condition operand was a constant, so the codeshare
count on the SFO cargo file -- `Operating Airline` against `Published Airline`,
1,498 rows, quoted in the report that shipped -- came out of a pandas heredoc
because no tool could say it.

A condition may now name `other_column` in place of `value`. The six
comparison ops accept it; the rest refuse it by name rather than comparing
every row against the string "Published Airline". `filter_operand_error` had to
learn about it too, or a column-to-column condition was rejected for "having
nothing to compare against".
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


def kept(result: dict) -> int:
    """Rows the filter kept. filter_dataset says after_rows, filter_rows says rows_after."""
    for key in ("rows_after", "after_rows"):
        if key in result:
            return int(result[key])
    raise AssertionError(f"no row count in {sorted(result)}")


@pytest.fixture
def codeshare_csv(tmp_path):
    """Eight rows, three of which are operated by someone else -- as codeshares are."""
    rows = [
        ("United", "United", 10, 10),
        ("SkyWest", "United", 20, 15),
        ("United", "United", 30, 30),
        ("Trans States", "US Airways", 40, 55),
        ("Delta", "Delta", 50, 50),
        ("Mesa", "American", 60, 12),
        ("FedEx", "FedEx", 70, 70),
        ("Delta", "Delta", 80, 80),
    ]
    body = "\n".join(f"{o},{p},{a},{b}" for o, p, a, b in rows)
    f = tmp_path / "flights.csv"
    f.write_text(f"operating,published,lbs,tons\n{body}\n", encoding="utf-8")
    return f


@BOTH
def test_rows_where_two_columns_disagree(tmp_path, codeshare_csv, filter_tool):
    r = filter_tool(
        str(codeshare_csv),
        [{"column": "operating", "op": "not_equals", "other_column": "published"}],
        output_path=str(tmp_path / "out.csv"),
    )
    assert r["success"] is True
    assert kept(r) == 3


@BOTH
def test_rows_where_two_columns_agree(tmp_path, codeshare_csv, filter_tool):
    r = filter_tool(
        str(codeshare_csv),
        [{"column": "operating", "op": "equals", "other_column": "published"}],
        output_path=str(tmp_path / "out.csv"),
    )
    assert r["success"] is True
    assert kept(r) == 5


@BOTH
def test_a_numeric_column_ordered_against_another(tmp_path, codeshare_csv, filter_tool):
    r = filter_tool(
        str(codeshare_csv),
        [{"column": "lbs", "op": "gt", "other_column": "tons"}],
        output_path=str(tmp_path / "out.csv"),
    )
    assert r["success"] is True
    assert kept(r) == 2


@BOTH
def test_an_op_that_cannot_take_a_column_says_so(tmp_path, codeshare_csv, filter_tool):
    """`contains` against a column is not a substring test anyone means."""
    r = filter_tool(
        str(codeshare_csv),
        [{"column": "operating", "op": "contains", "other_column": "published"}],
        output_path=str(tmp_path / "out.csv"),
    )
    assert r["success"] is False
    assert "contains" in r["error"]
    assert "not_equals" in r["error"], "the error must name the ops that would have worked"


@BOTH
def test_a_missing_second_column_is_named(tmp_path, codeshare_csv, filter_tool):
    r = filter_tool(
        str(codeshare_csv),
        [{"column": "operating", "op": "not_equals", "other_column": "nope"}],
        output_path=str(tmp_path / "out.csv"),
    )
    assert r["success"] is False
    assert "nope" in r["error"]


@BOTH
def test_comparing_to_a_literal_still_works(tmp_path, codeshare_csv, filter_tool):
    """The addition must not disturb the ordinary case."""
    r = filter_tool(
        str(codeshare_csv),
        [{"column": "operating", "op": "equals", "value": "Delta"}],
        output_path=str(tmp_path / "out.csv"),
    )
    assert r["success"] is True
    assert kept(r) == 2
