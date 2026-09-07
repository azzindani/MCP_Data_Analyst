"""A parameter with a closed value set that does not say what they are.

Round 28's census: 55 parameters across the fleet exist only to select
behaviour -- `action`, `mode`, `method`, `agg_func`, `chart_type`, `how`,
`format`, `task` -- and not one declared an `enum`. Every legal value lived in
prose, and often not even there: `fs_manage`'s docstring reads "Disk usage,
permissions, symlink info, or snapshot version list" while the tokens are
`disk_usage`, `permissions`, `symlink_info`, `versions`.

So a caller guessed the spelling from English and learned it by burning a call
-- which is the good case. The bad case was `check_outliers(method="zscore")`,
where the guess returned `success: true` and "no outliers" on a column holding
2,178 of them.

The runtime side of that was fixed in round 28. This is the schema side, and
the point of these tests is that the two cannot drift apart: **the enum must
render from the table the tool switches on**, never a second copy. This repo has
twice traced a chain of defects to a second table whose copies disagreed.

The enum advertises rather than enforces -- see `shared/schema_enum.py` for why
`Literal` is the wrong mechanism here -- so the last test below is the one that
matters most: every value the schema names has to be a value the tool takes.
"""

from __future__ import annotations

import asyncio
import importlib
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.choice import (  # noqa: E402
    AGG_FUNCS,
    ANOMALY_METHODS,
    CORRELATION_METHODS,
    NORMALIZE_MODES,
    OUTLIER_METHODS,
)

SERVERS = [
    "servers.data_basic.server",
    "servers.data_ingest.server",
    "servers.data_medium.server",
    "servers.data_statistics.server",
    "servers.data_transform.server",
    "servers.data_visual.server",
    "servers.data_workspace.server",
]

# A parameter is "dispatch" when its whole job is to pick a branch. Named here
# rather than guessed, so adding one to a tool without an enum fails loudly.
DISPATCH = re.compile(
    r"^(action|mode|method|agg_func|chart_type|how|direction|normalize|format|task|model|"
    r"model_type|models|algorithm|op|test|test_type|period_unit|rule|validation_type|to|type_?|"
    r"output_format|location_mode)$"
)

# The exceptions, each with the reason it cannot carry a fixed list.
NO_ENUM_IS_CORRECT: dict[str, str] = {
    # plotly's own vocabulary, passed through unchanged and resolved from the
    # data when empty -- naming a set here would be this repo asserting
    # something about a library it does not own.
    "data-visual/generate_geo_map.location_mode": "plotly's set, and auto-detected when omitted",
}


def _tools(module_name: str):
    module = importlib.import_module(module_name)
    return {t.name: t for t in asyncio.run(module.mcp.list_tools())}


def _dispatch_params():
    for module_name in SERVERS:
        endpoint = module_name.split(".")[1].replace("_", "-", 1)
        for name, tool in _tools(module_name).items():
            props = (tool.inputSchema or {}).get("properties") or {}
            for param, spec in props.items():
                if DISPATCH.match(param):
                    # A list parameter carries its enum on the ITEM schema --
                    # `models: list[one_of(...)]` -- so that is where to look.
                    if spec.get("type") == "array" and isinstance(spec.get("items"), dict):
                        spec = spec["items"]
                    yield f"{endpoint}/{name}.{param}", spec


class TestEveryDispatchParameterNamesItsValues:
    def test_the_census_finds_them(self):
        found = list(_dispatch_params())
        assert len(found) >= 30, f"only {len(found)} dispatch parameters seen -- did the census break?"

    def test_none_is_left_undeclared(self):
        missing = [key for key, spec in _dispatch_params() if "enum" not in spec and key not in NO_ENUM_IS_CORRECT]
        assert not missing, (
            f"{len(missing)} dispatch parameter(s) still name no values: {missing}. "
            "Add the enum with shared.schema_enum.one_of, or list it in NO_ENUM_IS_CORRECT "
            "with the reason it cannot have one."
        )

    def test_the_exceptions_are_still_real(self):
        """An exception that no longer exists is a stale excuse."""
        seen = {key for key, _ in _dispatch_params()}
        stale = [key for key in NO_ENUM_IS_CORRECT if key not in seen]
        assert not stale, f"NO_ENUM_IS_CORRECT names parameters that are gone: {stale}"

    def test_no_enum_is_empty_or_duplicated(self):
        for key, spec in _dispatch_params():
            values = spec.get("enum")
            if values is None:
                continue
            assert values, f"{key} declares an empty enum"
            assert len(values) == len(set(values)), f"{key} repeats a value: {values}"

    def test_a_default_is_one_of_the_declared_values(self):
        """A default outside its own enum is a contradiction the caller inherits."""
        for key, spec in _dispatch_params():
            values, default = spec.get("enum"), spec.get("default")
            if not values or default in (None, ""):
                continue
            assert default in values, f"{key} defaults to {default!r}, which its enum does not list"


class TestTheEnumRendersFromTheRuntimeTable:
    """The schema and the branch must not be two copies that can disagree."""

    @pytest.mark.parametrize(
        "key,table",
        [
            ("data-statistics/check_outliers.method", OUTLIER_METHODS),
            ("data-medium/detect_anomalies.method", ANOMALY_METHODS),
            ("data-statistics/correlation_analysis.method", CORRELATION_METHODS),
            ("data-visual/generate_correlation_heatmap.method", CORRELATION_METHODS),
            ("data-medium/compute_aggregations.agg_func", AGG_FUNCS),
            ("data-medium/pivot_table.agg_func", AGG_FUNCS),
            ("data-medium/cross_tabulate.agg_func", AGG_FUNCS),
            ("data-transform/reshape_dataset.agg_func", AGG_FUNCS),
            ("data-visual/generate_chart.agg_func", AGG_FUNCS),
            ("data-medium/cross_tabulate.normalize", NORMALIZE_MODES),
            ("data-transform/aggregate_dataset.normalize", NORMALIZE_MODES),
        ],
    )
    def test_the_declared_set_is_the_switched_set(self, key, table):
        found = dict(_dispatch_params())
        assert key in found, f"{key} is no longer a dispatch parameter"
        assert set(found[key]["enum"]) == set(table), (
            f"{key} advertises {sorted(found[key]['enum'])} and the runtime switches on "
            f"{sorted(table)} -- two tables that can drift"
        )


class TestEveryDeclaredValueIsAccepted:
    """The enum advertises rather than enforces, so it can lie. It must not.

    One tool per distinct set: send each declared value and assert the tool does
    not refuse it *for being that value*. A missing column or an empty frame is
    someone else's complaint and does not count.
    """

    @pytest.fixture
    def csv(self, tmp_path):
        import pandas as pd

        path = tmp_path / "d.csv"
        pd.DataFrame(
            {
                "spend": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
                "clicks": [1, 2, 3, 4, 5, 60],
                "platform": ["a", "a", "b", "b", "c", "c"],
                "kind": ["x", "y", "x", "y", "x", "y"],
            }
        ).to_csv(path, index=False)
        return str(path)

    def test_check_outliers_takes_every_method_it_names(self, csv):
        from servers.data_medium._med_inspect import check_outliers

        for value in OUTLIER_METHODS:
            r = check_outliers(csv, method=value, open_after=False)
            assert r["success"] is True, (value, r.get("error"))

    def test_detect_anomalies_takes_every_method_it_names(self, csv):
        from servers.data_medium._med_analysis import detect_anomalies

        for value in ANOMALY_METHODS:
            r = detect_anomalies(csv, method=value)
            assert r["success"] is True, (value, r.get("error"))

    def test_compute_aggregations_takes_every_function_it_names(self, csv):
        from servers.data_medium._med_transform import compute_aggregations

        for value in AGG_FUNCS:
            r = compute_aggregations(csv, group_by=["platform"], agg_column="spend", agg_func=value)
            assert r["success"] is True, (value, r.get("error"))

    def test_pivot_table_takes_every_function_it_names(self, csv):
        from servers.data_medium._med_report import pivot_table

        for value in AGG_FUNCS:
            r = pivot_table(csv, index=["platform"], values=["spend"], agg_func=value)
            assert r["success"] is True, (value, r.get("error"))

    def test_cross_tabulate_takes_every_normalize_it_names(self, csv):
        from servers.data_medium._med_report import cross_tabulate

        for value in NORMALIZE_MODES:
            r = cross_tabulate(csv, "platform", "kind", normalize=value, open_after=False)
            assert r["success"] is True, (value, r.get("error"))

    def test_correlation_analysis_takes_every_method_it_names(self, csv):
        from servers.data_medium._med_analysis import correlation_analysis

        for value in CORRELATION_METHODS:
            r = correlation_analysis(csv, method=value, open_after=False)
            assert r["success"] is True, (value, r.get("error"))

    def test_sample_data_takes_every_method_it_names(self, csv):
        from servers.data_medium._med_inspect import sample_data

        found = dict(_dispatch_params())["data-medium/sample_data.method"]
        for value in found["enum"]:
            r = sample_data(csv, method=value, n=2, open_after=False)
            assert r["success"] is True, (value, r.get("error"))

    def test_reshape_and_aggregate_take_every_mode_they_name(self, csv):
        """Modes need different companion arguments, so this checks the refusal
        is about a missing argument and never about the mode itself."""
        from servers.data_transform.engine import aggregate_dataset, reshape_dataset

        found = dict(_dispatch_params())
        for key, fn in (
            ("data-transform/reshape_dataset.mode", reshape_dataset),
            ("data-transform/aggregate_dataset.mode", aggregate_dataset),
        ):
            for value in found[key]["enum"]:
                r = fn(csv, mode=value)
                blob = f"{r.get('error', '')} {r.get('hint', '')}".lower()
                assert "unknown mode" not in blob, (key, value, r.get("error"))
