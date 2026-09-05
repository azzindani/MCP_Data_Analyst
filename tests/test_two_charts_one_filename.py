"""Every chart tool names its default output for what is in it.

A user review built four bar charts from one dataset -- different columns,
different aggregations -- and found one file:

    Every bar chart defaults to the same filename
    (`Air_Traffic_Cargo_bar.html`), so four charts collapsed into one file --
    silent data loss. I only noticed because I listed the directory.

`generate_chart` was fixed with `discriminated_suffix`, and the helper was then
used at exactly one of eleven chart call sites. A later audit called two
`value_counts` on different columns of one file against the deployed server and
got one `coll_value_counts.html` whose content hash changed between the calls:
the first chart was gone, and both responses said success.

So the fix is applied to every tool whose output depends on arguments:
`cross_tabulate`, `value_counts`, `correlation_analysis`, `time_series_analysis`,
`cohort_analysis`, `generate_geo_map`, `generate_3d_chart`, the regression plot
and the period-comparison plot.

**`check_outliers` and `scan_nulls_zeros` keep their bare names on purpose.**
They picture the whole dataset and take no column selection, so one file per
dataset is the right answer, and adding a discriminator there would produce a
new filename for a chart that has not changed.

An explicit `output_path` is untouched throughout. A caller who names a file
gets that file, including the right to overwrite it.
"""

from __future__ import annotations

import ast
import pathlib
import random
import sys

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
for _p in (str(REPO), str(REPO / "servers" / "data_medium")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _med_analysis import correlation_analysis  # noqa: E402
from _med_report import cross_tabulate, value_counts  # noqa: E402

from shared.html_layout import discriminated_suffix  # noqa: E402

# file -> the tools in it whose default name must vary with their arguments
DISCRIMINATED = {
    "servers/data_medium/_med_report.py": ("crosstab", "value_counts"),
    "servers/data_medium/_med_analysis.py": ("correlation", "time_series", "cohort"),
    "servers/data_advanced/_adv_gencharts.py": ("geo_map",),
    "servers/data_statistics/_stats_regression.py": ("regression",),
    "servers/data_statistics/_stats_comparative.py": ("period_comparison",),
}

# Whole-dataset pictures: one file per dataset is correct, so these stay.
WHOLE_DATASET = ("outliers", "nulls_zeros")


class TestTheHelperItself:
    def test_it_varies_with_every_argument(self):
        a = discriminated_suffix("bar", "sum", "tons", "year")
        b = discriminated_suffix("bar", "mean", "tons", "year")
        assert a != b

    def test_column_order_matters(self):
        """grade x purpose and purpose x grade are different tables."""
        assert discriminated_suffix("crosstab", "grade", "purpose") != discriminated_suffix(
            "crosstab", "purpose", "grade"
        )

    def test_empty_parts_fall_back_to_the_base(self):
        assert discriminated_suffix("crosstab", "", "") == "crosstab"

    def test_user_column_names_cannot_break_the_path(self):
        stem = discriminated_suffix("bar", "a/../../etc/passwd", "x" * 300)
        assert "/" not in stem
        assert ".." not in stem
        assert len(stem) <= 72

    def test_a_repeated_part_is_not_repeated_in_the_name(self):
        assert discriminated_suffix("x", "sales", "sales") == "x_sales"


def _literal_suffixes(path: pathlib.Path) -> list[str]:
    """Every string literal passed as `_save_chart`'s third argument."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
        if name != "_save_chart" or len(node.args) < 3:
            continue
        third = node.args[2]
        if isinstance(third, ast.Constant) and isinstance(third.value, str):
            found.append(third.value)
    return found


class TestNoChartToolStillNamesItselfByAConstant:
    """Reads the source, because the defect is a literal in a call."""

    @pytest.mark.parametrize("relpath, tools", sorted(DISCRIMINATED.items()))
    def test_the_argument_dependent_tools_pass_a_computed_stem(self, relpath, tools):
        literals = _literal_suffixes(REPO / relpath)
        for tool in tools:
            assert tool not in literals, (
                f"{relpath} still passes the constant '{tool}' to _save_chart; "
                "two calls with different arguments will overwrite each other"
            )

    def test_the_whole_dataset_views_keep_their_stable_names(self):
        """The other half of the rule: do not rename what has not changed."""
        literals = _literal_suffixes(REPO / "servers/data_medium/_med_inspect.py")
        for tool in WHOLE_DATASET:
            assert tool in literals, f"{tool} should keep one file per dataset"

    def test_every_module_that_computes_a_stem_imports_the_one_helper(self):
        for relpath in DISCRIMINATED:
            src = (REPO / relpath).read_text(encoding="utf-8")
            assert "from shared.html_layout import discriminated_suffix" in src, relpath


@pytest.fixture()
def frame(tmp_path):
    random.seed(4)
    rows = ["grade,purpose,amount,other"]
    for _ in range(120):
        rows.append(
            f"{random.choice('ABC')},{random.choice(['debt', 'car', 'home'])},"
            f"{random.randint(1000, 9000)},{random.randint(1, 50)}"
        )
    csv = tmp_path / "coll.csv"
    csv.write_text("\n".join(rows), encoding="utf-8")
    return csv


def _written(directory, pattern):
    return sorted(p.name for p in directory.glob(pattern))


class TestTwoCallsLeaveTwoFiles:
    """The behaviour the static tests above only imply."""

    def test_value_counts_on_two_columns(self, frame, tmp_path, monkeypatch):
        monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
        first = value_counts(str(frame), ["grade"], open_after=False)
        second = value_counts(str(frame), ["purpose"], open_after=False)
        assert first["success"] and second["success"]
        assert first["output_name"] != second["output_name"], _written(tmp_path, "*.html")

    def test_crosstab_with_the_axes_swapped(self, frame, tmp_path, monkeypatch):
        monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
        a = cross_tabulate(str(frame), "grade", "purpose", open_after=False)
        b = cross_tabulate(str(frame), "purpose", "grade", open_after=False)
        assert a["success"] and b["success"]
        assert a["output_name"] != b["output_name"]

    def test_the_same_call_twice_still_reuses_one_name(self, frame, tmp_path, monkeypatch):
        """Discrimination is by content, not by clock. Same request, same file."""
        monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
        a = value_counts(str(frame), ["grade"], open_after=False)
        b = value_counts(str(frame), ["grade"], open_after=False)
        assert a["output_name"] == b["output_name"]

    def test_pearson_and_spearman_are_two_pictures(self, frame, tmp_path, monkeypatch):
        monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
        p = correlation_analysis(str(frame), method="pearson", open_after=False)
        s = correlation_analysis(str(frame), method="spearman", open_after=False)
        assert p["success"] and s["success"]
        assert p["output_name"] != s["output_name"]

    def test_an_explicit_output_path_is_still_obeyed(self, frame, tmp_path):
        """A caller who names a file gets that file, collisions included."""
        named = tmp_path / "mine.html"
        result = value_counts(str(frame), ["grade"], output_path=str(named), open_after=False)
        assert result["success"]
        assert result["output_name"] == "mine.html"
