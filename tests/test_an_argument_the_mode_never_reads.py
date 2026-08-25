"""An argument can be valid for the tool and mean nothing to the mode.

aggregate_dataset takes twenty arguments across five modes. The schema
describes the tool, but the vocabulary is per mode -- so strict_args and
pydantic both pass an argument the chosen branch never looks at, and the branch
runs on its defaults:

    aggregate_dataset(mode="value_counts", row_col="device")
    -> success: true, and a frequency table of every object column

The caller asked for one column and got sixteen, with nothing in the response
to tell the two apart. This is a blind spot no per-tool check can cover, which
is why it survived the round-11 strict_args work and the round-14 op-field
work: both operate one level up from the mode.

The cost is already recorded in this file for window mode, where group_by was
accepted and dropped and a 3-day rolling mean came back 167.33 where the answer
was 200.0 -- a wrong number under success: true. This refuses instead, and
names what the mode does read.

Only an argument the caller actually changed counts: passing top_n=0 to a mode
that ignores top_n asked for nothing, so it is not worth an error.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from servers.data_transform.engine import (  # noqa: E402
    _AGGREGATE_ARG_DEFAULTS,
    _AGGREGATE_MODE_ARGS,
    aggregate_dataset,
)


@pytest.fixture
def csv(tmp_path):
    f = tmp_path / "in.csv"
    pd.DataFrame(
        {
            "Date": pd.date_range("2024-01-01", periods=12).astype(str),
            "device": ["mobile", "desktop"] * 6,
            "platform": ["Google", "Google", "Meta", "Meta"] * 3,
            "spends": [10.0 * i for i in range(1, 13)],
            "clicks": list(range(1, 13)),
        }
    ).to_csv(f, index=False)
    return f


# --- the refusals -----------------------------------------------------------


@pytest.mark.parametrize(
    ("mode", "extra", "named"),
    [
        ("value_counts", {"row_col": "device"}, "row_col"),
        ("value_counts", {"normalize": "index"}, "normalize"),
        ("describe", {"columns": ["spends"]}, "columns"),
        ("groupby", {"normalize": "index"}, "normalize"),
        ("groupby", {"order_by": "Date"}, "order_by"),
        ("window", {"values_col": "spends"}, "values_col"),
        ("crosstab", {"top_n": 5}, "top_n"),
    ],
)
def test_an_argument_this_mode_ignores_is_refused(tmp_path, csv, mode, extra, named):
    base = {"mode": mode}
    if mode == "groupby":
        base["group_by"] = ["device"]
    if mode == "crosstab":
        base |= {"row_col": "device", "col_col": "platform"}
    if mode == "window":
        base["order_by"] = "Date"
    r = aggregate_dataset(str(csv), **base, **extra)
    assert r["success"] is False, f"{mode} accepted {named}"
    assert named in r["error"]
    assert mode in r["error"]
    # The hint has to say what the mode does read, or the caller is only told no.
    assert "reads" in r["hint"]


def test_the_error_names_every_ignored_argument_at_once(tmp_path, csv):
    r = aggregate_dataset(str(csv), mode="describe", columns=["spends"], top_n=3, normalize="index")
    assert r["success"] is False
    for name in ("columns", "top_n", "normalize"):
        assert name in r["error"]


def test_an_unchanged_default_is_not_an_error(tmp_path, csv):
    """Passing an argument at its own default asked for nothing."""
    r = aggregate_dataset(str(csv), mode="describe", top_n=0, sort_desc=True, include_pct=True, window=3)
    assert r["success"] is True, r.get("error")


# --- everything that already worked still does ------------------------------


@pytest.mark.parametrize(
    "call",
    [
        {"mode": "groupby", "group_by": ["device"]},
        {"mode": "groupby", "group_by": ["device"], "agg": {"spends": "sum"}, "top_n": 1, "sort_desc": False},
        {"mode": "crosstab", "row_col": "device", "col_col": "platform"},
        {"mode": "crosstab", "row_col": "device", "col_col": "platform", "values_col": "spends"},
        {"mode": "crosstab", "row_column": "device", "col_column": "platform"},
        {"mode": "value_counts", "columns": ["device"], "top_n": 2, "include_pct": False},
        {"mode": "describe"},
        {"mode": "window", "order_by": "Date", "columns": ["spends"], "window": 3, "window_agg": "mean"},
        {"mode": "window", "order_by": "Date", "group_by": ["platform"]},
    ],
)
def test_a_documented_call_is_untouched(tmp_path, csv, call):
    r = aggregate_dataset(str(csv), **call)
    assert r["success"] is True, r.get("error")


# --- the table itself -------------------------------------------------------


def test_every_mode_declares_its_arguments():
    from servers.data_transform.engine import aggregate_dataset as fn

    modes = {"groupby", "crosstab", "value_counts", "describe", "window"}
    assert set(_AGGREGATE_MODE_ARGS) == modes
    # Every argument any mode claims to read must be a real parameter, or the
    # table is describing a tool that does not exist.
    params = set(fn.__code__.co_varnames[: fn.__code__.co_argcount])
    for mode, args in _AGGREGATE_MODE_ARGS.items():
        assert args <= params, f"{mode} names arguments the function does not take: {args - params}"


def test_every_checked_argument_has_a_default_recorded():
    """A missing default would compare against None and flag an untouched arg."""
    from servers.data_transform.engine import aggregate_dataset as fn

    code = fn.__code__
    names = code.co_varnames[: code.co_argcount]
    actual = dict(zip(names[-len(fn.__defaults__) :], fn.__defaults__, strict=True))
    for arg, recorded in _AGGREGATE_ARG_DEFAULTS.items():
        assert arg in actual, f"{arg} is not a parameter of aggregate_dataset"
        assert actual[arg] == recorded, f"{arg} default drifted: {actual[arg]!r} != {recorded!r}"
