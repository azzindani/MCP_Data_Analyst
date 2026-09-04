"""Every bar chart on one dataset defaulted to the same filename.

`generate_chart` passed `chart_type` as the whole stem suffix, so a session
that built four bar charts from `Air_Traffic_Cargo.csv` -- different value
columns, different aggregations -- wrote `Air_Traffic_Cargo_bar.html` four
times. Three of the four were gone. All four calls returned success, and each
response named the file it had written; none said the name was already taken.

A user review found it by listing the directory afterwards. That is the part
that makes it a defect rather than an inconvenience: nothing in the tool's own
answer could have told the caller.

The fix is a default name that varies with the arguments that change the
content. An explicit `output_path` is deliberately left alone -- a caller who
names a file gets that file, including the right to overwrite it.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import pytest

from shared.html_layout import discriminated_suffix

logging.disable(logging.CRITICAL)


# --------------------------------------------------------------------------
# the naming rule
# --------------------------------------------------------------------------


def test_the_arguments_that_change_the_content_change_the_name():
    assert discriminated_suffix("bar", "sum", "tons", "year") == "bar_sum_tons_year"
    assert discriminated_suffix("bar", "mean", "tons", "year") == "bar_mean_tons_year"
    assert discriminated_suffix("bar", "sum", "tons", "year") != discriminated_suffix("bar", "sum", "flights", "year")


def test_a_bare_base_survives_when_there_is_nothing_to_discriminate():
    assert discriminated_suffix("correlation") == "correlation"
    assert discriminated_suffix("correlation", "", "") == "correlation"


def test_user_column_names_cannot_decide_whether_the_write_succeeds():
    """Column names come from the data. They must not reach the filesystem raw."""
    suffix = discriminated_suffix("bar", "sum", "Total Revenue ($) / FY-2026", "région")
    assert "/" not in suffix
    assert " " not in suffix
    assert "$" not in suffix
    assert Path(suffix).name == suffix


def test_a_pathological_column_name_is_capped():
    suffix = discriminated_suffix("bar", "sum", "x" * 400)
    assert len(suffix) <= 72
    assert not suffix.endswith("_")


def test_a_repeated_part_is_not_repeated_in_the_name():
    assert discriminated_suffix("bar", "sum", "tons", "tons") == "bar_sum_tons"


# --------------------------------------------------------------------------
# the tool that earned it
# --------------------------------------------------------------------------


@pytest.fixture()
def cargo(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    src = tmp_path / "Air_Traffic_Cargo.csv"
    pd.DataFrame(
        {
            "year": [1999, 2000, 2001, 2002],
            "tons": [10, 20, 30, 40],
            "flights": [3, 4, 5, 6],
        }
    ).to_csv(src, index=False)
    return src, tmp_path


def _chart(src, value_column, agg_func):
    from servers.data_advanced._adv_gencharts import generate_chart

    return generate_chart(
        str(src),
        "bar",
        value_column,
        category_column="year",
        agg_func=agg_func,
        open_after=False,
    )


def test_four_bar_charts_leave_four_files(cargo):
    """The exact session shape from the review."""
    src, outdir = cargo
    names = []
    for value_column, agg in [("tons", "sum"), ("tons", "mean"), ("flights", "sum"), ("flights", "max")]:
        result = _chart(src, value_column, agg)
        assert result.get("success") is True, result.get("error")
        names.append(result["output_name"])

    assert len(set(names)) == 4, f"charts collapsed into {sorted(set(names))}"
    on_disk = sorted(p.name for p in outdir.glob("*.html"))
    assert len(on_disk) == 4


def test_the_same_chart_twice_still_writes_one_file(cargo):
    """Idempotent by design: identical arguments mean identical content."""
    src, outdir = cargo
    first = _chart(src, "tons", "sum")
    second = _chart(src, "tons", "sum")
    assert first["output_name"] == second["output_name"]
    assert len(list(outdir.glob("*bar*.html"))) == 1


def test_an_explicit_output_path_is_still_obeyed(cargo):
    """Naming a file is the caller's prerogative, overwrite included."""
    from servers.data_advanced._adv_gencharts import generate_chart

    src, outdir = cargo
    chosen = outdir / "my_chart.html"
    result = generate_chart(
        str(src), "bar", "tons", category_column="year", agg_func="sum", output_path=str(chosen), open_after=False
    )
    assert result.get("success") is True, result.get("error")
    assert Path(result["output_path"]).name == "my_chart.html"


def test_the_name_says_what_is_in_the_chart(cargo):
    """A directory listing has to be readable without opening anything."""
    src, _ = cargo
    result = _chart(src, "tons", "mean")
    name = result["output_name"]
    assert "bar" in name and "mean" in name and "tons" in name
