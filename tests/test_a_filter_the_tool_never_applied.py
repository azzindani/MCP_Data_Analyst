"""Two numbers that were confidently wrong, both under success: true.

**search_columns(dtype=...)** filtered on numeric / datetime / object and its
if/elif chain ended there, with no else. Anything outside those three was
dropped in silence and every column came back as a match:

    search_columns(f, dtype="float64")  -> all 16 columns, success: true

float64 is not a wild guess. It is exactly what load_dataset, inspect_dataset
and this tool's own `dtypes` field print, so a caller reading a dtype out of one
tool and passing it to this one was using a name the fleet had just taught them.
The vocabulary the tools emit was not the vocabulary this one accepted.

**merge_datasets `matched`** was `merged[left_on].notna().sum()` -- the count of
rows whose JOIN KEY is present. On a left join the key comes from the left frame
and is never null, so `matched` equalled `result_rows` on every call. Three
result rows of which two found a partner were reported as "matched: 3". The one
number a caller checks to find out whether a join worked was the one number that
could not say. This is the finding class the round-14 sweep exists to catch:
not a refusal, not a crash, a wrong number reported as a success.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_basic")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from servers.data_basic.engine import (  # noqa: E402
    DTYPE_FILTER_ALIASES,
    DTYPE_FILTERS,
    inspect_dataset,
    search_columns,
)
from servers.data_medium.engine import merge_datasets  # noqa: E402


@pytest.fixture
def csv(tmp_path):
    f = tmp_path / "in.csv"
    pd.DataFrame(
        {
            "name": ["a", "b", "c", "d"],
            "label": ["p", "q", "r", "s"],
            "spends": [1.5, 2.5, 3.5, 4.5],
            "clicks": [1, 2, 3, 4],
        }
    ).to_csv(f, index=False)
    return f


# --- the dtype filter -------------------------------------------------------


@pytest.mark.parametrize("spelling", ["numeric", "number", "float64", "int64", "float", "int"])
def test_a_numeric_filter_returns_only_numeric_columns(csv, spelling):
    r = search_columns(str(csv), dtype=spelling)
    assert r["success"] is True, r.get("error")
    assert sorted(r["columns"]) == ["clicks", "spends"], f"{spelling} did not filter"


@pytest.mark.parametrize("spelling", ["object", "str", "string", "text", "categorical"])
def test_a_text_filter_returns_only_text_columns(csv, spelling):
    r = search_columns(str(csv), dtype=spelling)
    assert r["success"] is True, r.get("error")
    assert sorted(r["columns"]) == ["label", "name"], f"{spelling} did not filter"


def test_a_dtype_this_tool_cannot_filter_by_is_refused(csv):
    """Silence here meant 'every column matched', which reads as a real answer."""
    r = search_columns(str(csv), dtype="complex128")
    assert r["success"] is False
    assert "complex128" in r["error"]
    for name in DTYPE_FILTERS:
        assert name in r["hint"]


def test_no_filter_at_all_still_returns_everything(csv):
    r = search_columns(str(csv))
    assert r["success"] is True
    assert r["matched"] == 4


def test_the_dtypes_the_fleet_prints_are_all_accepted(csv):
    """The bug was a caller using a name another tool gave them, so every dtype
    inspect_dataset reports must be a dtype this one can filter by."""
    reported = set(inspect_dataset(str(csv))["dtypes"].values())
    assert reported, "no dtypes reported"
    for name in reported:
        assert name.lower() in DTYPE_FILTER_ALIASES or name.lower() in DTYPE_FILTERS, name
        r = search_columns(str(csv), dtype=name)
        assert r["success"] is True, f"{name} refused: {r.get('error')}"


# --- the matched count ------------------------------------------------------


@pytest.fixture
def pair(tmp_path):
    left, right = tmp_path / "l.csv", tmp_path / "r.csv"
    pd.DataFrame({"k": ["x", "y", "z"], "v": [1, 2, 3]}).to_csv(left, index=False)
    pd.DataFrame({"k": ["x", "y", "w"], "w": [9, 8, 7]}).to_csv(right, index=False)
    return left, right


@pytest.mark.parametrize(("how", "result_rows"), [("inner", 2), ("left", 3), ("right", 3), ("outer", 4)])
def test_matched_counts_rows_that_found_a_partner(tmp_path, pair, how, result_rows):
    """Exactly two keys are in both frames, whatever the join type does around them."""
    left, right = pair
    r = merge_datasets(str(left), str(right), left_on="k", right_on="k", how=how, output_path=str(tmp_path / "o.csv"))
    assert r["success"] is True, r.get("error")
    assert r["result_rows"] == result_rows
    assert r["matched"] == 2, f"{how}: matched should be 2, not {r['matched']}"


def test_matched_is_not_just_the_row_count(tmp_path, pair):
    """The whole defect in one assertion: on a left join the two used to agree."""
    left, right = pair
    r = merge_datasets(
        str(left), str(right), left_on="k", right_on="k", how="left", output_path=str(tmp_path / "o.csv")
    )
    assert r["matched"] != r["result_rows"]


def test_the_merge_indicator_does_not_reach_the_written_file(tmp_path, pair):
    """It is scaffolding for the count, not a column the caller asked for."""
    left, right = pair
    out = tmp_path / "o.csv"
    merge_datasets(str(left), str(right), left_on="k", right_on="k", how="outer", output_path=str(out))
    assert "_merge_side" not in pd.read_csv(out).columns


def test_a_join_where_everything_matches_still_reports_everything(tmp_path):
    left, right = tmp_path / "l.csv", tmp_path / "r.csv"
    pd.DataFrame({"k": ["x", "y"], "v": [1, 2]}).to_csv(left, index=False)
    pd.DataFrame({"k": ["x", "y"], "w": [9, 8]}).to_csv(right, index=False)
    r = merge_datasets(
        str(left), str(right), left_on="k", right_on="k", how="left", output_path=str(tmp_path / "o.csv")
    )
    assert r["matched"] == r["result_rows"] == 2
