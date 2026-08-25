"""read_column_stats promised "unique top" and gave neither to a number.

    Stats for one column: mean median std min max nulls unique top.

Eight things named, and the numeric branch returned the first six. `unique` and
`top` were built in the categorical branch alone, so the two answers a caller
wants when a column is mostly one value were the two it could not get -- and a
mostly-one-value column is a numeric column far more often than a text one. On
the reference dataset, `link_clicks` reports median, q1 and q3 all 0.0; the top
values say why in one line.

Found by asking the sibling question. ml_basic's `read_column_profile` says it
"Returns stats, null count, top values" and produced top values for categorical
columns only; the same promise, the same half-kept, in the other repo. Both are
fixed, which is the whole point of asking -- three rounds running, the answer to
"which siblings did I not touch?" has been "at least one".
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

from servers.data_basic.engine import read_column_stats  # noqa: E402


@pytest.fixture
def shaped(tmp_path) -> Path:
    f = tmp_path / "shaped.csv"
    pd.DataFrame(
        {
            "clicks": [0, 0, 0, 0, 1, 2, 450],
            "region": ["a", "a", "b", "c", "c", "c", "d"],
        }
    ).to_csv(f, index=False)
    return f


def test_a_numeric_column_reports_its_unique_count(shaped):
    r = read_column_stats(str(shaped), "clicks")
    assert r["success"] is True, r.get("error")
    assert r["unique_count"] == 4


def test_a_numeric_column_reports_its_top_values(shaped):
    r = read_column_stats(str(shaped), "clicks")
    top = r["top_values"]
    assert top["0"] == 4, "four of seven rows are zero, which the quartiles only imply"
    assert list(top)[0] == "0", "most frequent first"


def test_the_numeric_stats_are_all_still_there(shaped):
    stats = read_column_stats(str(shaped), "clicks")
    for key in ("mean", "median", "std", "min", "max", "q1", "q3", "null_count"):
        assert key in stats, key
    assert stats["median"] == 0.0
    assert stats["max"] == 450.0


def test_a_categorical_column_is_unchanged(shaped):
    stats = read_column_stats(str(shaped), "region")
    assert stats["unique_count"] == 4
    assert stats["top_values"] == {"c": 3, "a": 2, "b": 1, "d": 1}


def test_the_list_is_bounded(tmp_path):
    f = tmp_path / "wide.csv"
    pd.DataFrame({"n": list(range(500))}).to_csv(f, index=False)
    assert len(read_column_stats(str(f), "n")["top_values"]) == 10


def test_a_column_of_all_nulls_still_answers(tmp_path):
    f = tmp_path / "empty.csv"
    pd.DataFrame({"n": [None, None, None]}).to_csv(f, index=False)
    r = read_column_stats(str(f), "n")
    assert r["success"] is True, r.get("error")
    assert r["top_values"] == {}
    assert r["unique_count"] == 0


def test_every_name_in_the_docstring_has_a_key(shaped):
    """The docstring is the schema here, so walk it."""
    stats = read_column_stats(str(shaped), "clicks")
    promised = {
        "mean": "mean",
        "median": "median",
        "std": "std",
        "min": "min",
        "max": "max",
        "nulls": "null_count",
        "unique": "unique_count",
        "top": "top_values",
    }
    missing = [word for word, key in promised.items() if key not in stats]
    assert not missing, f"docstring promises {missing} and the numeric branch has no such key"
