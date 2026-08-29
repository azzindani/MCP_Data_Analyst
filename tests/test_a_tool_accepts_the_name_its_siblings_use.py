"""A parameter census over this repo, and the aliases it made necessary.

Counting every `@mcp.tool()` parameter and grouping by what it *means* is the
highest-yield offline check found so far -- it produced six defects in a sibling
repo in one pass and nine here. The finding is always the same shape: twenty-two
parameters spell a column `*_column`, seven spell it `*_col`, and a caller who
follows the majority is refused by pydantic before any server code runs, with no
property description in the schema to learn the right name from.

`data_statistics/server.py` is the clearest evidence that these are accidents
rather than design: `time_series_analysis` takes `date_column`, `cohort_analysis`
takes `date_column`, and `period_comparison` between them takes `date_col`.

Each test below sends the sibling spelling to the outlier and asserts it works.
The last class pins the thing that makes aliases dangerous: adding one must not
move an existing parameter, because a positional caller then binds to the wrong
argument silently. That bug shipped once in a sibling repo.
"""

from __future__ import annotations

import ast
import inspect
import sys
from collections import defaultdict
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_medium"), str(ROOT / "servers" / "data_basic")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from servers.data_medium import engine as med  # noqa: E402
from servers.data_statistics import engine as stats  # noqa: E402
from servers.data_transform import engine as trans  # noqa: E402
from servers.data_visual import engine as visual  # noqa: E402


def tool_fn(mod, name: str):
    """The callable a client actually reaches, via the tool registry.

    Under fastmcp 2.x the module-level name WAS the registry entry, so
    `mod.some_tool.fn` and a client's path were the same object. The official
    MCP SDK's @mcp.tool returns the plain undecorated function, so the
    module-level name now bypasses every wrapper installed on the registry --
    sanitize_responses, measure_responses, contract_errors.

    Going through _tools keeps these tests on the path a request takes. A test
    calling the bare function would pass while the thing it guards sat switched
    off, which is the one failure mode those guards exist to prevent.
    """
    return mod.mcp._tool_manager._tools[name].fn


FIXTURE = ROOT / "tests" / "fixtures" / "ad_data_full.csv"


@pytest.fixture
def csv(tmp_path: Path) -> str:
    import shutil

    dst = tmp_path / "data.csv"
    shutil.copy2(FIXTURE, dst)
    return str(dst)


class TestTheCensusItself:
    """The convention this repo actually follows, asserted rather than assumed."""

    @staticmethod
    def tool_params() -> dict[str, list[str]]:
        params: dict[str, list[str]] = defaultdict(list)
        for path in sorted((ROOT / "servers").rglob("server.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.FunctionDef):
                    continue
                if not any(
                    (d.func if isinstance(d, ast.Call) else d).attr == "tool"
                    for d in node.decorator_list
                    if isinstance((d.func if isinstance(d, ast.Call) else d), ast.Attribute)
                ):
                    continue
                for arg in node.args.args + node.args.kwonlyargs:
                    params[arg.arg].append(node.name)
        return params

    def test_file_path_is_the_name_for_the_dataset(self):
        params = self.tool_params()
        # 60 tools against one: file_path is not a preference, it is the convention.
        assert len(params["file_path"]) > 40
        assert len(params.get("file_path_a", [])) <= 1

    def test_every_short_column_spelling_has_its_long_form_too(self):
        params = self.tool_params()
        pairs = [
            ("row_col", "row_column"),
            ("col_col", "col_column"),
            ("values_col", "values_column"),
            ("date_col", "date_column"),
            ("value_cols", "value_columns"),
            ("y_col", "y_column"),
            ("x_cols", "x_columns"),
        ]
        for short, long_ in pairs:
            if short in params:
                assert long_ in params, f"{short} exists but {long_} does not -- callers guess the long form"


class TestTheSiblingSpellingWorks:
    def test_aggregate_dataset_takes_cross_tabulates_names(self, csv):
        r = trans.aggregate_dataset(
            csv,
            "crosstab",
            row_column="campaign_platform",
            col_column="device",
            values_column="spends",
        )
        assert r["success"] is True, r.get("error")

    def test_aggregate_dataset_still_takes_its_own_names(self, csv):
        r = trans.aggregate_dataset(csv, "crosstab", row_col="campaign_platform", col_col="device", values_col="spends")
        assert r["success"] is True, r.get("error")

    def test_period_comparison_takes_date_column(self, csv):
        r = stats.period_comparison(csv, date_column="Date", metrics=["spends"], period_unit="M")
        assert r["success"] is True, r.get("error")

    def test_period_comparison_still_takes_date_col(self, csv):
        r = stats.period_comparison(csv, date_col="Date", metrics=["spends"], period_unit="M")
        assert r["success"] is True, r.get("error")

    def test_period_comparison_without_a_date_says_which_names_it_takes(self, csv):
        r = stats.period_comparison(csv, metrics=["spends"], period_unit="M")
        assert r["success"] is False
        assert "date_col" in r["hint"] and "date_column" in r["hint"]

    def test_period_comparison_still_refuses_a_missing_metric(self, csv):
        # Accepting date_column forced metrics to take a default; the refusal
        # must stay as clear as pydantic's was.
        r = stats.period_comparison(csv, date_col="Date", period_unit="M")
        assert r["success"] is False
        assert "metrics" in r["error"]

    def test_resample_takes_the_long_names(self, csv, tmp_path):
        r = med.resample_timeseries(
            csv,
            date_column="Date",
            freq="M",
            value_columns=["spends"],
            output_path=str(tmp_path / "out.csv"),
        )
        assert r["success"] is True, r.get("error")

    def test_regression_takes_x_columns_and_y_column(self, csv):
        r = stats.regression_analysis(csv, y_column="spends", x_columns=["impressions"])
        assert r["success"] is True, r.get("error")

    def test_regression_without_a_target_names_both_spellings(self, csv):
        r = stats.regression_analysis(csv, x_cols=["impressions"])
        assert r["success"] is False
        assert "y_col" in r["hint"] and "y_column" in r["hint"]

    def test_statistical_test_takes_test_type(self, csv):
        r = stats.statistical_test(csv, test_type="shapiro_wilk", column_a="spends")
        assert r["success"] is True, r.get("error")

    def test_statistical_tests_takes_test(self, csv):
        r = med.statistical_tests(csv, test="ttest", column_a="spends", column_b="impressions")
        assert r["success"] is True, r.get("error")

    def test_statistical_tests_still_auto_selects_with_neither(self, csv):
        r = med.statistical_tests(csv, column_a="spends", column_b="impressions")
        assert r["success"] is True, r.get("error")

    def test_each_tool_takes_the_others_name_for_the_same_test(self, csv):
        # t_test/ttest and pearson/correlation are the same test under two names.
        a = med.statistical_tests(csv, test_type="t_test", column_a="spends", column_b="impressions")
        assert a["success"] is True, a.get("error")
        b = stats.statistical_test(csv, test="ttest", column_a="spends", column_b="impressions")
        assert b["success"] is True, b.get("error")
        c = med.statistical_tests(csv, test_type="pearson", column_a="spends", column_b="impressions")
        assert c["success"] is True, c.get("error")

    @pytest.mark.parametrize("name", ["paired_t_test", "one_sample_t", "spearman", "kendall"])
    def test_a_test_it_cannot_run_is_refused_not_substituted(self, csv, name):
        # These look like synonyms and are not. Answering them with the nearest
        # available test would be a wrong answer under success: true.
        r = med.statistical_tests(csv, test_type=name, column_a="spends", column_b="impressions")
        assert r["success"] is False
        assert name in r["error"]
        assert "Valid" in r["hint"]

    def test_sample_data_takes_top_n(self, csv):
        r = med.sample_data(csv, method="head", top_n=7)
        assert r["success"] is True, r.get("error")
        assert r["returned"] == 7 or r.get("rows_sampled") == 7, r

    def test_compare_datasets_takes_file_path(self, csv):
        r = med.compare_datasets(file_path=csv, right_file_path=csv)
        assert r["success"] is True, r.get("error")

    def test_compare_datasets_without_a_second_file_names_both_spellings(self, csv):
        r = med.compare_datasets(file_path=csv)
        assert r["success"] is False
        assert "file_path_b" in r["hint"] and "right_file_path" in r["hint"]

    def test_export_data_takes_output_format(self, csv, tmp_path):
        out = tmp_path / "exported.json"
        r = visual.export_data(csv, output_path=str(out), output_format="json")
        assert r["success"] is True, r.get("error")
        assert out.exists()


class TestTheDropFlagIsNotSilentlyIgnored:
    """One tool, two flags one letter apart, each read by only one mode.

    Sending the wrong one is a valid argument name, so pydantic accepts it and
    the mode ignores it: success: true, and the source columns still in the
    output. There is no error anywhere to read.
    """

    def test_combine_honours_the_singular(self, csv, tmp_path):
        out = tmp_path / "combined.csv"
        r = trans.reshape_dataset(
            csv,
            "combine_columns",
            combine_columns=["campaign_platform", "device"],
            new_column="combo",
            drop_original=True,
            output_path=str(out),
        )
        assert r["success"] is True, r.get("error")
        header = out.read_text(encoding="utf-8").splitlines()[0]
        assert "campaign_platform" not in header.split(",")
        assert "combo" in header

    def test_split_honours_the_plural(self, csv, tmp_path):
        out = tmp_path / "split.csv"
        r = trans.reshape_dataset(
            csv,
            "split_column",
            split_column="campaign_platform",
            delimiter=" ",
            new_columns=["w1", "w2"],
            drop_originals=True,
            output_path=str(out),
        )
        assert r["success"] is True, r.get("error")
        header = out.read_text(encoding="utf-8").splitlines()[0].split(",")
        assert "campaign_platform" not in header

    def test_the_patch_ops_take_either_spelling(self, csv):
        import pandas as pd

        from servers.data_basic._patch_ops import _op_combine_columns, _op_split_column

        df = pd.read_csv(csv, nrows=50)
        out, _ = _op_combine_columns(
            df.copy(), {"columns": ["campaign_platform", "device"], "new_column": "c", "drop_original": True}
        )
        assert "campaign_platform" not in out.columns

        out, _ = _op_split_column(df.copy(), {"column": "campaign_platform", "delimiter": " ", "drop_originals": True})
        assert "campaign_platform" not in out.columns


class TestAnAliasDoesNotMoveAnExistingArgument:
    """The footgun that shipped once: a new name placed in an old one's slot.

    MCP always sends named arguments so no live call could hit it, but the
    repo's own callers and tests use positional form, and the failure is silent
    -- the wrong column, under success: true.
    """

    @pytest.mark.parametrize(
        "fn,expected",
        [
            (
                trans.aggregate_dataset,
                ["file_path", "mode", "group_by", "agg", "sort_desc", "top_n", "row_col", "col_col", "values_col"],
            ),
            (med.resample_timeseries, ["file_path", "date_col", "freq", "agg_func", "value_cols"]),
            (stats.regression_analysis, ["file_path", "y_col", "x_cols", "model_type"]),
            (stats.period_comparison, ["file_path", "date_col", "metrics", "period_unit"]),
            (stats.statistical_test, ["file_path", "test", "column_a", "column_b", "group_column"]),
            (med.compare_datasets, ["file_path_a", "file_path_b", "key_columns"]),
            (med.sample_data, ["file_path", "method", "n", "random_state"]),
            (visual.export_data, ["file_path", "output_path", "format", "encoding", "separator"]),
        ],
    )
    def test_the_original_order_is_unchanged(self, fn, expected):
        names = list(inspect.signature(fn).parameters)
        assert names[: len(expected)] == expected, f"{fn.__name__} reordered its arguments"


class TestTheWorkspaceHasOneName:
    """Four of six workspace tools say `workspace_name`; two said `name`.

    The two are `create_workspace` and `open_workspace` -- the first two anyone
    calls. A caller who reads `list_workspace_files(workspace_name=...)` and
    then writes the same spelling for the tool that makes the workspace is
    refused by pydantic before any server code runs:

        create_workspace(workspace_name="probe")
        -> name: Missing required argument
           workspace_name: Unexpected keyword argument

    Found by the round-11 repeat-call probe, which could not build a second
    identical call because the first one never landed.
    """

    @staticmethod
    def load():
        import importlib

        p = str(ROOT / "servers" / "data_workspace")
        if p not in sys.path:
            sys.path.insert(0, p)
        return importlib.import_module("servers.data_workspace.server")

    def test_the_census_shows_which_spelling_is_the_convention(self):
        params = TestTheCensusItself.tool_params()
        assert len(params["workspace_name"]) >= len(params["name"]), (
            f"workspace_name={params['workspace_name']} name={params['name']}"
        )

    def test_create_workspace_takes_workspace_name(self, tmp_path):
        mod = self.load()
        r = tool_fn(mod, "create_workspace")(workspace_name="probe_ws", base_dir=str(tmp_path))
        assert r["success"] is True, r.get("error")

    def test_create_workspace_still_takes_name(self, tmp_path):
        mod = self.load()
        r = tool_fn(mod, "create_workspace")(name="probe_ws", base_dir=str(tmp_path))
        assert r["success"] is True, r.get("error")

    def test_open_workspace_takes_workspace_name(self, tmp_path):
        mod = self.load()
        tool_fn(mod, "create_workspace")(name="probe_ws", base_dir=str(tmp_path))
        r = tool_fn(mod, "open_workspace")(workspace_name="probe_ws", base_dir=str(tmp_path))
        assert r["success"] is True, r.get("error")

    def test_neither_spelling_names_the_missing_argument(self, tmp_path):
        mod = self.load()
        r = tool_fn(mod, "create_workspace")(base_dir=str(tmp_path))
        assert r["success"] is False
        assert "workspace_name" in r["hint"], r["hint"]

    def test_the_first_positional_argument_is_still_the_name(self, tmp_path):
        # Appending the alias must not move `name` out of position 0.
        mod = self.load()
        r = tool_fn(mod, "create_workspace")("positional_ws", "", str(tmp_path))
        assert r["success"] is True, r.get("error")
        assert "positional_ws" in str(r), r
