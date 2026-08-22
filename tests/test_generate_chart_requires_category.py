"""Calling generate_chart exactly as its schema documents failed for six types.

tools/list marks three arguments required -- file_path, chart_type,
value_column -- and gives category_column a "" default like every other
optional. An LLM reading that schema calls with the three required arguments,
which is what a coverage sweep did. Six of the thirteen chart types then failed:

    bar, line, scatter   ValueError: Value of 'x' is not the name of a column
                         in 'data_frame'. Expected one of [...] but received:
    pie                  Value of 'names' is not the name of a column ...
    funnel               Value of 'y' is not the name of a column ...
    radius               ''

Two things made this unrecoverable rather than merely wrong. The message names
'x', 'names', 'y' and 'data_frame' -- plotly express internals, none of which
is a parameter of this tool, so there is nothing for the caller to map it back
to. And the hint was "Check file_path, column names, and chart_type.", which
names the three arguments that were already correct and not the one that was
missing.

radius was worse still: it reached chart_df[""] and raised KeyError(""), whose
str() is the two characters '', so the response carried a failure with an
effectively empty error string.

The block above already guarded geo, treemap/sunburst, time_series and sankey
this way; these six were simply never added to it. geo is included here too --
it groups by category_column at the merge step -- so the guard now covers every
type that indexes the argument unconditionally.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from servers.data_visual.engine import generate_chart

# Every type whose plotly call indexes category_column with no fallback.
#
# geo is guarded too but is not in this list: with no geo_file_path it stops at
# its own earlier guard, which names the geo arguments -- the more useful answer,
# since without a geo file there is nothing to draw either way. It gets its own
# test below, with the geo arguments supplied, so the category guard is reached.
NEEDS_CATEGORY = ["bar", "pie", "line", "scatter", "funnel", "radius"]

# Types that must keep working with no category_column, so the guard cannot
# simply be applied to everything.
INDIFFERENT = ["waterfall", "parallel_coords"]

# plotly/pandas internals that must never reach a caller: none of these is a
# parameter of this tool.
LEAKED_TOKENS = ["data_frame", "Value of 'x'", "Value of 'names'", "Value of 'y'"]


@pytest.fixture()
def csv(simple_csv: Path) -> str:
    return str(simple_csv)


class TestTheSchemaValidCallIsAnswered:
    """Three required arguments and nothing else -- the call the schema invites."""

    @pytest.mark.parametrize("chart_type", NEEDS_CATEGORY)
    def test_it_fails_cleanly_instead_of_raising_plotlys_error(self, chart_type: str, csv: str, tmp_path: Path):
        r = generate_chart(csv, chart_type, "Revenue", output_path=str(tmp_path / "c.html"), open_after=False)
        assert r["success"] is False
        assert "category_column" in r["error"], r["error"]

    @pytest.mark.parametrize("chart_type", NEEDS_CATEGORY)
    def test_no_plotly_internal_reaches_the_caller(self, chart_type: str, csv: str, tmp_path: Path):
        r = generate_chart(csv, chart_type, "Revenue", output_path=str(tmp_path / "c.html"), open_after=False)
        blob = json.dumps(r)
        for token in LEAKED_TOKENS:
            assert token not in blob, f"{chart_type} leaked {token!r}: {r['error']}"

    @pytest.mark.parametrize("chart_type", NEEDS_CATEGORY)
    def test_the_error_is_never_empty(self, chart_type: str, csv: str, tmp_path: Path):
        """radius answered with '' -- a failure that says nothing at all."""
        r = generate_chart(csv, chart_type, "Revenue", output_path=str(tmp_path / "c.html"), open_after=False)
        assert len(r["error"].strip(" '\"")) > 10, f"{chart_type}: {r['error']!r}"

    @pytest.mark.parametrize("chart_type", NEEDS_CATEGORY)
    def test_the_hint_names_the_argument_that_was_missing(self, chart_type: str, csv: str, tmp_path: Path):
        """The old hint listed the three arguments that were already right."""
        r = generate_chart(csv, chart_type, "Revenue", output_path=str(tmp_path / "c.html"), open_after=False)
        assert "category_column" in r["hint"], r["hint"]

    @pytest.mark.parametrize("chart_type", NEEDS_CATEGORY)
    def test_the_hint_points_at_a_tool_that_lists_columns(self, chart_type: str, csv: str, tmp_path: Path):
        r = generate_chart(csv, chart_type, "Revenue", output_path=str(tmp_path / "c.html"), open_after=False)
        assert "inspect_dataset" in r["hint"], r["hint"]


class TestGeoReachesTheGuardOnceItsOwnIsSatisfied:
    """geo groups by category_column to build the choropleth's values, so it
    needs the argument as much as the rest -- it just has a prior requirement
    that fires first."""

    @pytest.fixture()
    def geojson(self, tmp_path: Path) -> str:
        path = tmp_path / "regions.geojson"
        path.write_text(
            json.dumps(
                {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "properties": {"name": "West"},
                            "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]},
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        return str(path)

    def test_its_own_guard_wins_when_the_geo_file_is_missing(self, csv: str, tmp_path: Path):
        r = generate_chart(csv, "geo", "Revenue", output_path=str(tmp_path / "c.html"), open_after=False)
        assert r["success"] is False
        assert "geo_file_path" in r["error"]

    def test_the_category_guard_fires_once_the_geo_arguments_are_given(self, csv: str, geojson: str, tmp_path: Path):
        r = generate_chart(
            csv,
            "geo",
            "Revenue",
            geo_file_path=geojson,
            geo_join_column="name",
            output_path=str(tmp_path / "c.html"),
            open_after=False,
        )
        assert r["success"] is False
        assert "category_column" in r["error"], r["error"]
        assert "inspect_dataset" in r["hint"], r["hint"]

    def test_it_does_not_leak_a_pandas_or_plotly_internal(self, csv: str, geojson: str, tmp_path: Path):
        r = generate_chart(
            csv,
            "geo",
            "Revenue",
            geo_file_path=geojson,
            geo_join_column="name",
            output_path=str(tmp_path / "c.html"),
            open_after=False,
        )
        blob = json.dumps(r)
        for token in LEAKED_TOKENS:
            assert token not in blob, blob


class TestSupplyingItStillWorks:
    """The guard must reject the empty case only, not the working one."""

    @pytest.mark.parametrize("chart_type", ["bar", "pie", "line", "scatter", "funnel", "radius"])
    def test_the_chart_is_produced(self, chart_type: str, csv: str, tmp_path: Path):
        out = tmp_path / f"{chart_type}.html"
        r = generate_chart(
            csv,
            chart_type,
            "Revenue",
            category_column="Region",
            output_path=str(out),
            open_after=False,
        )
        assert r["success"] is True, r.get("error")
        assert out.exists() and out.stat().st_size > 0


class TestTypesThatNeverNeededIt:
    @pytest.mark.parametrize("chart_type", INDIFFERENT)
    def test_they_are_not_caught_by_the_new_guard(self, chart_type: str, csv: str, tmp_path: Path):
        r = generate_chart(csv, chart_type, "Revenue", output_path=str(tmp_path / "c.html"), open_after=False)
        assert "requires category_column" not in (r.get("error") or "")

    def test_the_types_with_their_own_guard_keep_it(self, csv: str, tmp_path: Path):
        """time_series and treemap must still name their own missing argument,
        not be swallowed by the category_column check added after them."""
        ts = generate_chart(csv, "time_series", "Revenue", output_path=str(tmp_path / "a.html"), open_after=False)
        assert "date_column" in ts["error"], ts["error"]
        tm = generate_chart(csv, "treemap", "Revenue", output_path=str(tmp_path / "b.html"), open_after=False)
        assert "hierarchy_columns" in tm["error"], tm["error"]

    def test_an_unknown_type_still_lists_the_valid_ones(self, csv: str, tmp_path: Path):
        r = generate_chart(csv, "hexbin", "Revenue", output_path=str(tmp_path / "c.html"), open_after=False)
        assert r["success"] is False
        assert "bar" in r["hint"] and "sankey" in r["hint"]


class TestTheDocstringSaysSo:
    """The guard turns an unrecoverable failure into a recoverable one; the
    docstring is what stops the call being made wrong in the first place."""

    def test_the_tool_description_names_category_column(self):
        src = Path(__file__).resolve().parents[1] / "servers" / "data_visual" / "server.py"
        text = src.read_text(encoding="utf-8")
        body = text.split("def generate_chart(", 1)[1]
        docstring = body.split('"""')[1]
        assert "category_column" in docstring, docstring
        assert len(docstring) <= 80, f"{len(docstring)} chars: {docstring}"
