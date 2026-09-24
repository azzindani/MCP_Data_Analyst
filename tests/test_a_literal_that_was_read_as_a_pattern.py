"""Text a caller means literally is matched literally: a `contains` value and a split delimiter.

pandas reads `str.contains(value)` as a regular expression unless told not to,
and `str.split(delimiter)` as one whenever the delimiter is longer than a
character. The tools pass a caller's value straight through, and none of them
documents a pattern -- `regex` is its own op wherever a pattern is meant. So:

- `contains 'C++'` failed outright ("bad repetition operator");
- `contains '(US)'` also kept "US office", and `contains 'a.b'` kept "axb";
- splitting "a | b | c" on " | " gave a, |, b, |, c, and splitting on "||"
  cut every character apart.

Each entry point below is checked against the literal answer, worked out by
hand. The `regex` ops keep their patterns.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_basic"), str(ROOT / "servers" / "data_medium")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _med_inspect import filter_rows  # noqa: E402
from _med_transform import run_cleaning_pipeline  # noqa: E402

from servers.data_transform.engine import filter_dataset, reshape_dataset  # noqa: E402

TITLES = ["C++ dev", "Java dev", "(US) office", "US office", "a.b", "axb"]
LITERAL = {"C++": ["C++ dev"], "(US)": ["(US) office"], "a.b": ["a.b"]}


@pytest.fixture
def jobs(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
    f = tmp_path / "jobs.csv"
    pd.DataFrame({"title": TITLES, "n": range(len(TITLES))}).to_csv(f, index=False)
    return f


def _kept(result: dict, out: Path) -> list[str]:
    assert result["success"] is True, result
    return pd.read_csv(out)["title"].tolist()


@pytest.mark.parametrize("value", list(LITERAL))
class TestContainsIsLiteral:
    def test_filter_dataset(self, jobs, value):
        out = jobs.parent / "out.csv"
        r = filter_dataset(str(jobs), [{"column": "title", "op": "contains", "value": value}], output_path=str(out))
        assert _kept(r, out) == LITERAL[value]

    def test_filter_dataset_not_contains(self, jobs, value):
        out = jobs.parent / "out.csv"
        r = filter_dataset(str(jobs), [{"column": "title", "op": "not_contains", "value": value}], output_path=str(out))
        assert _kept(r, out) == [t for t in TITLES if t not in LITERAL[value]]

    def test_filter_rows(self, jobs, value):
        out = jobs.parent / "out.csv"
        r = filter_rows(
            str(jobs), [{"column": "title", "op": "contains", "value": value}], output_path=str(out), open_after=False
        )
        assert _kept(r, out) == LITERAL[value]

    def test_a_conditional_label(self, jobs, value):
        out = jobs.parent / "out.csv"
        ops = [
            {
                "op": "conditional_assign",
                "new_column": "hit",
                "conditions": [{"column": "title", "op": "contains", "value": value, "label": "yes"}],
                "default": "no",
            }
        ]
        r = run_cleaning_pipeline(str(jobs), ops=ops, output_path=str(out))
        assert r["success"] is True, r
        df = pd.read_csv(out)
        assert df.loc[df["hit"] == "yes", "title"].tolist() == LITERAL[value]


ROWS = ["a | b | c", "x||y||z", "1. 2. 3"]


@pytest.mark.parametrize(
    ("delimiter", "row", "parts"),
    [(" | ", 0, ["a", "b", "c"]), ("||", 1, ["x", "y", "z"]), (". ", 2, ["1", "2", "3"])],
)
class TestADelimiterIsLiteral:
    @pytest.fixture
    def rows(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
        monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
        f = tmp_path / "rows.csv"
        pd.DataFrame({"packed": ROWS}).to_csv(f, index=False)
        return f

    def test_split_column_op(self, rows, delimiter, row, parts):
        out = rows.parent / "out.csv"
        ops = [{"op": "split_column", "column": "packed", "delimiter": delimiter, "new_columns": ["p0", "p1", "p2"]}]
        r = run_cleaning_pipeline(str(rows), ops=ops, output_path=str(out))
        assert r["success"] is True, r
        got = pd.read_csv(out, dtype=str).loc[row, ["p0", "p1", "p2"]].tolist()
        assert got == parts

    def test_reshape_dataset(self, rows, delimiter, row, parts):
        out = rows.parent / "out.csv"
        r = reshape_dataset(
            str(rows),
            "split_column",
            split_column="packed",
            delimiter=delimiter,
            new_columns=["p0", "p1", "p2"],
            output_path=str(out),
        )
        assert r["success"] is True, r
        got = pd.read_csv(out, dtype=str).loc[row, ["p0", "p1", "p2"]].tolist()
        assert got == parts


class TestAPatternIsStillAPattern:
    def test_the_regex_filter(self, jobs):
        out = jobs.parent / "out.csv"
        r = filter_dataset(str(jobs), [{"column": "title", "op": "regex", "pattern": r"a.b"}], output_path=str(out))
        assert _kept(r, out) == ["a.b", "axb"]
