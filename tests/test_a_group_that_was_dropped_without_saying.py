"""compute_aggregations returned 20 groups and called that all of them.

Two caps sat on top of each other. `get_max_rows()` -- the repo's own limit
helper, 100 normally and 20 constrained -- computed `truncated` and warned. Two
lines below it a second, hardcoded `_response_cap = 20` did the real cutting,
and nothing downstream knew about it::

    "groups": 20, "returned": 20, "truncated": false

with `groups` set to the length of the list it had just trimmed. Grouped by
year, the SFO cargo file has 25. The five it dropped were the five smallest --
2011-2014 and 2023 -- because the rows were sorted descending first, so the
2013 trough quoted in the report that shipped was simply not in the answer, and
the response said nothing was missing.

A ceiling used as a setting: `get_max_rows()` exists for exactly this and was
already in the function. Now it is the only cap, `groups` counts the groups
rather than the reply, and a truncated response says how many it left out.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_medium")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _med_transform import compute_aggregations  # noqa: E402

# More groups than the old hardcoded cap, fewer than the unconstrained limit.
GROUP_COUNT = 25


@pytest.fixture
def unconstrained(monkeypatch):
    """Pin normal limits.

    The constrained-mode CI job runs this whole suite with
    MCP_CONSTRAINED_MODE=1, where get_max_rows() is 20 and a 25-group answer is
    legitimately truncated. These tests are about the cap that was *not* the
    documented one, so they set the mode instead of inheriting it.
    """
    monkeypatch.setenv("MCP_CONSTRAINED_MODE", "0")


@pytest.fixture
def yearly_csv(tmp_path):
    """One row per year, each year's value smaller than the last.

    Descending sort then means the tail is what a cap silently removes -- the
    same shape as the cargo file, where the dropped years were the low ones.
    """
    rows = "\n".join(f"{1999 + i},{(GROUP_COUNT - i) * 1000}" for i in range(GROUP_COUNT))
    f = tmp_path / "yearly.csv"
    f.write_text(f"year,tons\n{rows}\n", encoding="utf-8")
    return f


def test_every_group_comes_back(unconstrained, yearly_csv):
    r = compute_aggregations(str(yearly_csv), group_by=["year"], agg_column="tons", agg_func="sum")
    assert r["success"] is True
    assert r["groups"] == GROUP_COUNT
    assert r["returned"] == GROUP_COUNT
    assert r["truncated"] is False
    assert len(r["result"]) == GROUP_COUNT


def test_the_smallest_group_is_not_the_one_that_disappears(unconstrained, yearly_csv):
    """2023 was last after a descending sort, so it was always the casualty."""
    r = compute_aggregations(str(yearly_csv), group_by=["year"], agg_column="tons", agg_func="sum")
    years = {int(row["year"]) for row in r["result"]}
    assert 1999 + GROUP_COUNT - 1 in years
    assert years == {1999 + i for i in range(GROUP_COUNT)}


def test_a_real_truncation_reports_the_total_it_cut_from(yearly_csv, monkeypatch):
    monkeypatch.setenv("MCP_CONSTRAINED_MODE", "1")  # explicit in both CI jobs
    r = compute_aggregations(str(yearly_csv), group_by=["year"], agg_column="tons", agg_func="sum")
    assert r["truncated"] is True
    assert r["returned"] == 20
    # The number that was wrong before: the caller must be able to see that
    # five rows are missing, not just that twenty came back.
    assert r["groups"] == GROUP_COUNT
    warning = [p for p in r["progress"] if p["status"] == "warn"]
    assert warning, "a truncated response must warn"
    assert f"of {GROUP_COUNT}" in warning[0]["detail"]


def test_top_n_is_a_request_not_a_truncation(unconstrained, yearly_csv):
    r = compute_aggregations(str(yearly_csv), group_by=["year"], agg_column="tons", agg_func="sum", top_n=5)
    assert r["returned"] == 5
    assert r["groups"] == GROUP_COUNT
    assert r["truncated"] is False


def test_the_totals_themselves_are_untouched(unconstrained, yearly_csv):
    r = compute_aggregations(str(yearly_csv), group_by=["year"], agg_column="tons", agg_func="sum")
    by_year = {int(row["year"]): row["tons"] for row in r["result"]}
    for i in range(GROUP_COUNT):
        assert by_year[1999 + i] == (GROUP_COUNT - i) * 1000
