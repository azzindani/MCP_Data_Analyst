"""A caller's regular expression is stopped when it runs away, and answers as before when it doesn't.

extract_regex matched with Python's `re`, which has no timeout. `(a+)+$` over a
29-character cell was still running when killed at 10 seconds -- the server
thread gone for good, with nothing in the response or the log. It now matches
in a worker process with a budget (shared/regex_guard.py, MCP_REGEX_SECONDS):

- a runaway pattern is refused inside its budget, naming the pattern, and
  nothing is written;
- every other pattern gives exactly the answer `re` gives, cell for cell --
  groups by number and by name, no match, a missing group, a non-text cell;
- the guard module is the same file in every repo that ships it.
"""

from __future__ import annotations

import hashlib
import re
import sys
import time
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_basic"), str(ROOT / "servers" / "data_medium")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _med_transform import run_cleaning_pipeline  # noqa: E402

from servers.data_transform import engine  # noqa: E402
from shared.regex_guard import Guard, PatternTimeout  # noqa: E402

RUNAWAY = r"(a+)+$"
STUCK = "a" * 40 + "!"


@pytest.fixture
def cells(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("MCP_REGEX_SECONDS", "1")
    f = tmp_path / "cells.csv"
    pd.DataFrame({"txt": ["order 12-A", "none here", "", STUCK, "ref 7-bb and 9-c", None], "n": range(6)}).to_csv(
        f, index=False
    )
    return f


class TestARunawayPatternIsStopped:
    def test_extract_regex_is_refused_inside_its_budget(self, cells):
        out = cells.parent / "out.csv"
        began = time.monotonic()
        r = run_cleaning_pipeline(
            str(cells),
            ops=[{"op": "extract_regex", "column": "txt", "pattern": RUNAWAY, "new_column": "x"}],
            output_path=str(out),
        )
        took = time.monotonic() - began
        assert r["success"] is False
        assert RUNAWAY in str(r) and "was still matching after 1s" in str(r)
        assert took < 8, f"stopped after {took:.1f}s against a 1s budget"
        assert not out.exists(), "nothing written"

    def test_in_a_chain_too(self, cells):
        steps = [
            {"id": "t", "load": "cells.csv"},
            {
                "id": "x",
                "from": "t",
                "ops": [{"op": "extract_regex", "column": "txt", "pattern": RUNAWAY, "new_column": "x"}],
            },
        ]
        r = engine.run_chain(steps)
        assert r["success"] is False and r["failed_step"] == "x"
        assert RUNAWAY in r["error"]

    def test_the_guard_counts_only_matching_time(self):
        with Guard(r"\d", limit=0.5) as g:
            assert g.search("a1")
            time.sleep(0.6)  # the caller's own work, between calls
            assert g.found(["x", "2"]) == [False, True]
        with pytest.raises(PatternTimeout), Guard(RUNAWAY, limit=0.5) as g:
            g.search(STUCK)


class TestEveryOtherPatternAnswersAsReDoes:
    @pytest.mark.parametrize(
        ("pattern", "group"),
        [(r"(\d+)-(\w+)", 0), (r"(\d+)-(\w+)", 2), (r"(?P<num>\d+)-(?P<code>\w+)", "code"), (r"(\d+)", 3), (r"zzz", 0)],
    )
    def test_cell_for_cell(self, cells, pattern, group):
        out = cells.parent / "out.csv"
        op = {"op": "extract_regex", "column": "txt", "pattern": pattern, "new_column": "x", "group": group}
        r = run_cleaning_pipeline(str(cells), ops=[op], output_path=str(out))
        assert r["success"] is True, r
        compiled = re.compile(pattern)

        def want(v):
            if not isinstance(v, str):
                return None
            m = compiled.search(v)
            try:
                return None if m is None else m.group(group)
            except IndexError:
                return None

        source = pd.read_csv(cells)["txt"]
        expected = [want(v) for v in source]
        got = pd.read_csv(out)["x"].tolist()
        assert [None if pd.isna(v) else v for v in got] == expected

    def test_a_bad_pattern_is_the_same_refusal(self, cells):
        r = run_cleaning_pipeline(
            str(cells),
            ops=[{"op": "extract_regex", "column": "txt", "pattern": "foo(", "new_column": "x"}],
            output_path=str(cells.parent / "out.csv"),
        )
        assert r["success"] is False and "Invalid regex pattern: missing ), unterminated subpattern" in str(r)


def test_the_guard_is_one_file_across_the_fleet():
    mine = hashlib.sha256((ROOT / "shared" / "regex_guard.py").read_bytes()).hexdigest()
    siblings = [
        ROOT.parent / repo / "shared" / "regex_guard.py"
        for repo in ("MCP_File_System", "MCP_Documents", "MCP_Web_Browser")
    ]
    present = [p for p in siblings if p.exists()]
    if not present:
        pytest.skip("no sibling repo checked out beside this one")
    for p in present:
        assert hashlib.sha256(p.read_bytes()).hexdigest() == mine, p
