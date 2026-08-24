"""A pipeline template is checked when it is saved, not when it is run.

save_workspace_pipeline stored whatever it was handed. So this succeeded:

    save_workspace_pipeline("w", "clean", [{"op": "teleport"}])
    -> success: true, op_count: 1

and the refusal arrived later, from run_workspace_pipeline, about a template
the caller had already been told was good -- with nothing in between to
connect the error to the mistake. Its own hint had the answer all along
("ops must be a list of apply_patch op dicts. Use list_patch_ops() for
reference"), but it lived in the except branch, which nothing reached, because
storing a bad dict is not an error in Python.

The ops run through apply_patch, so apply_patch's validator is the right one,
and the right time is while the caller is still looking at what they wrote.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from servers.data_workspace.engine import (  # noqa: E402
    create_workspace,
    list_workspace_files,
    save_workspace_pipeline,
)


@pytest.fixture
def workspace(tmp_path):
    create_workspace("wtest", base_dir=str(tmp_path))
    return str(tmp_path)


@pytest.mark.parametrize(
    ("ops", "expected"),
    [
        ([{"op": "teleport", "column": "x"}], "teleport"),
        ([{"op": "fill_nulls", "column": "a", "stratgy": "mean"}], "did you mean strategy?"),
        ([{"op": "fill_nulls", "column": "a"}], "missing 'strategy'"),
        ([{"op": "cast_column", "column": "a", "dtype": "complex"}], "invalid dtype"),
    ],
)
def test_a_template_that_cannot_run_is_not_saved(workspace, ops, expected):
    r = save_workspace_pipeline("wtest", "p", ops, base_dir=workspace)
    assert r["success"] is False
    assert expected in r["error"]
    assert "list_patch_ops" in r["hint"]


@pytest.mark.parametrize(
    "ops",
    [
        [{"op": "fill_nulls", "column": "a", "strategy": "mean"}],
        # An op outside the original eight: the pipeline runner accepts all 52
        # now, so saving one must not be refused on the old vocabulary.
        [{"op": "normalize", "column": "a", "method": "minmax"}],
        [{"op": "rolling_agg", "column": "a", "window": 3, "agg": "mean", "new_column": "r"}],
        [{"op": "drop_duplicates"}, {"op": "clean_text", "scope": "headers"}],
    ],
)
def test_a_runnable_template_saves(workspace, ops):
    r = save_workspace_pipeline("wtest", "good", ops, base_dir=workspace)
    assert r["success"] is True, r.get("error")
    assert r["op_count"] == len(ops)


def test_nothing_is_stored_when_the_template_is_refused(workspace):
    """ "Nothing was stored" has to be true, or the next save silently appends
    to a template the caller thinks does not exist."""
    save_workspace_pipeline("wtest", "p", [{"op": "teleport"}], base_dir=workspace)
    listing = list_workspace_files("wtest", base_dir=workspace)
    assert listing["success"] is True
    assert "teleport" not in str(listing)
