"""Two servers, one filter vocabulary, three spellings of it -- and drift.

A user review opened on the cheap half of this: `filter_dataset` with
`op: "=="` is refused and demands `equals`, so every session pays one wasted
turn before it starts. The expensive half was underneath and nobody had
reported it. The op list was written down three times:

    data_transform/engine.py     _FILTER_OPS   starts_with  ends_with  not_contains
    data_medium/_med_inspect.py  if-chain      startswith   endswith   (absent)
    data_medium/_med_inspect.py  error hint    neither, and omits four more

So the two servers disagreed about how to spell the same operator. A caller
that learned `starts_with` from `filter_dataset` was refused by `filter_rows`,
and one that learned `startswith` from `filter_rows` was refused by
`filter_dataset` -- with nothing in either response saying the other spelling
existed. `not_contains` could only ever be reached through one of them.

The tests below are in three parts, because the failure had three parts:
the alias table, the two servers agreeing, and the messages rendering from
the table instead of restating it.
"""

from __future__ import annotations

import re

import pandas as pd
import pytest

from shared.value_alias import CANONICAL, UnknownOp, is_known, render_valid, resolve

# --------------------------------------------------------------------------
# 1. the table
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sent, canonical",
    [
        ("==", "equals"),
        ("=", "equals"),
        ("!=", "not_equals"),
        ("<>", "not_equals"),
        (">", "gt"),
        (">=", "gte"),
        ("<", "lt"),
        ("<=", "lte"),
        # the drift, both directions
        ("startswith", "starts_with"),
        ("starts_with", "starts_with"),
        ("endswith", "ends_with"),
        ("ends_with", "ends_with"),
        # ordinary English
        ("in", "isin"),
        ("not_in", "not_isin"),
        ("isnull", "is_null"),
        # and the canonical names still mean themselves
        ("equals", "equals"),
        ("quantile_between", "quantile_between"),
    ],
)
def test_the_spellings_a_caller_sends_all_resolve(sent, canonical):
    assert resolve(sent) == canonical
    assert is_known(sent)


def test_case_and_padding_do_not_decide_the_answer():
    assert resolve("  ==  ") == "equals"
    assert resolve("EQUALS") == "equals"
    assert resolve("StartsWith") == "starts_with"


def test_an_unknown_op_gets_the_list_and_a_guess():
    with pytest.raises(UnknownOp) as excinfo:
        resolve("euqals")
    message = str(excinfo.value)
    assert "Did you mean 'equals'?" in message
    assert "Valid:" in message
    # The message renders the table; it does not carry its own copy.
    assert render_valid() in message


def test_a_hopeless_op_still_gets_the_list():
    with pytest.raises(UnknownOp) as excinfo:
        resolve("xyzzy")
    assert render_valid() in str(excinfo.value)
    assert not is_known("xyzzy")


# --------------------------------------------------------------------------
# 2. the two servers agree -- the part that was actually broken
# --------------------------------------------------------------------------


@pytest.fixture()
def frame():
    return pd.DataFrame(
        {
            "grade": ["Alpha", "Beta", "Alpaca", "Gamma"],
            "amount": [10, 20, 30, 40],
        }
    )


def _transform_mask(frame, cond):
    from servers.data_transform.engine import _apply_condition

    return _apply_condition(frame, cond)


def _medium_mask(frame, cond):
    from servers.data_medium._med_inspect import _apply_condition

    return _apply_condition(frame, cond)


@pytest.mark.parametrize("spelling", ["starts_with", "startswith"])
def test_both_servers_accept_both_spellings_of_starts_with(frame, spelling):
    cond = {"column": "grade", "op": spelling, "value": "Alp"}
    a = _transform_mask(frame, cond).tolist()
    b = _medium_mask(frame, cond).tolist()
    assert a == b == [True, False, True, False]


@pytest.mark.parametrize("spelling", ["ends_with", "endswith"])
def test_both_servers_accept_both_spellings_of_ends_with(frame, spelling):
    cond = {"column": "grade", "op": spelling, "value": "a"}
    assert _transform_mask(frame, cond).tolist() == _medium_mask(frame, cond).tolist()


def test_not_contains_now_reaches_the_server_that_never_had_it(frame):
    cond = {"column": "grade", "op": "not_contains", "value": "Alp"}
    assert _medium_mask(frame, cond).tolist() == [False, True, False, True]


@pytest.mark.parametrize("symbol, word", [("==", "equals"), ("!=", "not_equals")])
def test_the_symbol_a_model_reaches_for_first_works_on_both(frame, symbol, word):
    """The `==` tax: one wasted turn per session, for nothing."""
    by_symbol = {"column": "grade", "op": symbol, "value": "Beta"}
    by_word = {"column": "grade", "op": word, "value": "Beta"}
    for server in (_transform_mask, _medium_mask):
        assert server(frame, by_symbol).tolist() == server(frame, by_word).tolist()


@pytest.mark.parametrize("op", [op for op in CANONICAL if op not in {"date_range", "quantile_between"}])
def test_every_canonical_op_is_answered_by_both_servers(frame, op):
    """A name in the table that a server cannot answer is the next drift.

    The two excluded ops read keys other than `value` and are covered by the
    filter tests that own them; every other name has to work on both sides.
    """
    cond = {"column": "amount", "op": op, "value": 20, "values": [20], "min": 0, "max": 50, "pattern": "2"}
    for server in (_transform_mask, _medium_mask):
        result = server(frame, cond)
        assert len(result) == len(frame)


# --------------------------------------------------------------------------
# 3. nothing writes its own copy of the list
# --------------------------------------------------------------------------


def _op_naming_lines():
    """Every non-comment server line that names three or more canonical ops."""
    import pathlib

    root = pathlib.Path(__file__).resolve().parents[1]
    for path in (root / "servers").rglob("*.py"):
        text = path.read_text(encoding="utf-8", errors="ignore")
        for line in text.splitlines():
            if line.lstrip().startswith("#"):
                continue
            named = sum(f'"{op}"' in line or f"'{op}'" in line or f" {op} " in line for op in CANONICAL)
            if named >= 3:
                yield path.relative_to(root), line.strip()


# A tool that implements part of the table declares the part, as a tuple named
# `_..._OPS`. That is a selection FROM the table -- the next test proves every
# name in it is real -- and it is the one shape allowed to list ops.
_SUBSET_DECL = re.compile(r"^_[A-Z_]*OPS(?::[^=]+)?\s*=\s*\(")


def test_no_server_file_writes_its_own_copy_of_the_op_list():
    """The check that keeps this fixed: one table, and only one.

    A future edit that adds an op to a hand-written sentence instead of to
    `CANONICAL` is exactly how the copies drifted the first time -- and how
    `data_medium`'s hint came to omit four ops that had existed for months.
    """
    offenders = [f"{path}: {line[:90]}" for path, line in _op_naming_lines() if not _SUBSET_DECL.match(line)]
    assert not offenders, "the op list is written out again in:\n" + "\n".join(offenders)


def test_every_declared_subset_is_really_a_subset():
    """A subset with a name the table does not have is the drift, renamed."""
    import ast
    import pathlib

    root = pathlib.Path(__file__).resolve().parents[1]
    checked = 0
    for path in (root / "servers").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            names = [t.id for t in node.targets if isinstance(t, ast.Name)]
            if not any(_SUBSET_DECL.match(f"{n} = (") for n in names):
                continue
            try:
                value = ast.literal_eval(node.value)
            except ValueError:
                continue
            if not isinstance(value, tuple) or not all(isinstance(v, str) for v in value):
                continue
            stray = sorted(set(value) - set(CANONICAL))
            assert not stray, f"{path.relative_to(root)}: {names[0]} names {stray}, absent from CANONICAL"
            checked += 1
    # If this drops to zero the test has stopped watching anything.
    assert checked >= 1
