"""Four duplicate tools leave tools/list and keep answering, each naming its successor.

The medium tier served four tools a newer sibling does better or identically:
`extended_stats` (the same function as the statistics server's),
`statistical_tests` (a subset of `statistical_test`), `filter_rows` (what
`filter_dataset` was upgraded from) and `compute_aggregations`
(`aggregate_dataset` in groupby mode). Every listed name costs a model
attention on every turn. Retired, they are unlisted; a caller that learned an
old name gets the same answer as before, plus the name of the tool to use.
"""

from __future__ import annotations

import asyncio
import importlib
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_medium")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from shared.retired import retire  # noqa: E402

TIERS = [
    "data_basic",
    "data_medium",
    "data_statistics",
    "data_transform",
    "data_visual",
    "data_workspace",
    "data_ingest",
]
RETIRED = {
    "extended_stats": "extended_stats on the statistics server",
    "statistical_tests": "statistical_test",
    "filter_rows": "filter_dataset",
    "compute_aggregations": "aggregate_dataset",
}


def _server(tier: str):
    return importlib.import_module(f"servers.{tier}.server").mcp


def _listed(tier: str) -> list[str]:
    return [tool.name for tool in asyncio.run(_server(tier).list_tools())]


class TestTheListIsShorter:
    def test_the_medium_tier_lists_none_of_them(self):
        assert not set(RETIRED) & set(_listed("data_medium"))

    def test_every_successor_is_still_listed(self):
        assert "extended_stats" in _listed("data_statistics")
        assert "statistical_test" in _listed("data_statistics")
        assert {"filter_dataset", "aggregate_dataset"} <= set(_listed("data_transform"))

    # 68 once the four were retired; a tool added since joins this tuple, so the
    # count still proves the retired names left the list and nothing came back.
    ADDED_SINCE = ("run_chain",)

    def test_the_repo_lists_68_tools_plus_the_ones_added_since_and_none_twice(self):
        listed = [name for tier in TIERS for name in _listed(tier)]
        assert set(self.ADDED_SINCE) <= set(listed)
        assert len(listed) == 68 + len(self.ADDED_SINCE), len(listed)
        assert len(set(listed)) == len(listed)


class TestAnOldNameStillAnswers:
    @pytest.fixture
    def csv(self, tmp_path):
        path = tmp_path / "sales.csv"
        pd.DataFrame({"region": ["a", "a", "b", "b"], "units": [1, 2, 3, 4], "price": [9.0, 8.0, 7.0, 6.0]}).to_csv(
            path, index=False
        )
        return str(path)

    @pytest.mark.parametrize(
        ("name", "args"),
        [
            ("extended_stats", {"columns": ["units"]}),
            ("statistical_tests", {"test": "ttest", "column_a": "units", "group_column": "region"}),
            ("filter_rows", {"conditions": [{"column": "units", "op": "gt", "value": 1}], "dry_run": True}),
            ("compute_aggregations", {"group_by": ["region"], "agg_column": "units", "agg_func": "sum"}),
        ],
    )
    def test_it_answers_as_before_and_names_its_successor(self, csv, name, args):
        fn = _server("data_medium")._tool_manager._tools[name].fn
        result = fn(file_path=csv, **args)
        assert result["success"] is True, result
        assert RETIRED[name] in result["retired"]


def test_retiring_a_name_that_is_not_registered_is_refused():
    with pytest.raises(KeyError, match="not registered"):
        retire(_server("data_medium"), {"no_such_tool": "anything"})
