"""Grouping by a column the file does not have yet had no tool at all.

Every aggregation tool takes a `file_path` and groups by columns already in it.
`feature_engineering` could add columns, but only automatically, and its
`date_parts` family only fires on columns that already parse as dates. On the
SFO cargo file the period is the integer `199907`, so "tonnage by year" -- the
first question anybody asks of it -- was unreachable.

What happened instead is the measurement: the model abandoned the tools and
wrote five pandas heredocs, deriving `Period_dt`, `year` and `month` by hand.
The yearly totals, the era comparison and the seasonality curve all left the
tool surface with those three columns.

`derive` takes named specs against a fixed dispatch table -- never `eval`.
Specs apply in order, so a later one reads an earlier one's output, and the
whole list is refused on the first bad spec rather than writing a half-derived
frame to disk.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_medium")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _med_transform import compute_aggregations, feature_engineering  # noqa: E402

from shared.derive_ops import DeriveError, apply_derivations  # noqa: E402


@pytest.fixture
def cargo_csv(tmp_path):
    """The shape that defeated the tools: YYYYMM as an integer, DD-MM-YYYY text."""
    rows = []
    for year in (2020, 2021):
        for month in range(1, 13):
            rows.append((f"{year}{month:02d}", f"01-{month:02d}-{year}", "United", "United", month * 100))
    rows.append(("202101", "01-01-2021", "SkyWest", "United", 55))
    body = "\n".join(f"{a},{b},{c},{d},{e}" for a, b, c, d, e in rows)
    f = tmp_path / "cargo.csv"
    f.write_text(f"period_code,start_date,operating,published,tons\n{body}\n", encoding="utf-8")
    return f


def _derived(path):
    return pd.read_csv(path)


# --- the five ops -----------------------------------------------------------


def test_a_year_sliced_out_of_an_integer_period(cargo_csv, tmp_path):
    out = tmp_path / "d.csv"
    r = feature_engineering(
        str(cargo_csv),
        output_path=str(out),
        open_after=False,
        derive=[
            {"name": "year", "op": "text", "column": "period_code", "how": "slice", "start": 0, "stop": 4, "as": "int"}
        ],
    )
    assert r["success"] is True
    assert r["new_columns"] == ["year"]
    assert sorted(_derived(out)["year"].unique().tolist()) == [2020, 2021]


def test_a_date_parsed_then_a_part_taken_from_it(cargo_csv, tmp_path):
    """Specs apply in order, so `month` reads the column `period` just made."""
    out = tmp_path / "d.csv"
    r = feature_engineering(
        str(cargo_csv),
        output_path=str(out),
        open_after=False,
        derive=[
            {"name": "period", "op": "parse_date", "column": "start_date"},
            {"name": "month", "op": "date_part", "column": "period", "part": "month"},
            {"name": "ym", "op": "date_part", "column": "period", "part": "yearmonth"},
        ],
    )
    assert r["success"] is True
    frame = _derived(out)
    # The dayfirst detector runs underneath: all twelve months, not just January.
    assert sorted(frame["month"].unique().tolist()) == list(range(1, 13))
    assert "2020-07" in set(frame["ym"])


def test_arithmetic_against_a_column_and_against_a_number(cargo_csv, tmp_path):
    out = tmp_path / "d.csv"
    r = feature_engineering(
        str(cargo_csv),
        output_path=str(out),
        open_after=False,
        derive=[
            {"name": "kg", "op": "arith", "column": "tons", "how": "mul", "value": 1000},
            {"name": "yr", "op": "arith", "column": "period_code", "how": "floordiv", "value": 100},
        ],
    )
    assert r["success"] is True
    frame = _derived(out)
    assert (frame["kg"] == frame["tons"] * 1000).all()
    assert sorted(frame["yr"].unique().tolist()) == [2020, 2021]


def test_a_comparison_between_two_text_columns(cargo_csv, tmp_path):
    out = tmp_path / "d.csv"
    r = feature_engineering(
        str(cargo_csv),
        output_path=str(out),
        open_after=False,
        derive=[{"name": "codeshare", "op": "compare", "column": "operating", "how": "ne", "other": "published"}],
    )
    assert r["success"] is True
    assert int(_derived(out)["codeshare"].sum()) == 1


def test_two_columns_joined_with_a_separator(cargo_csv, tmp_path):
    out = tmp_path / "d.csv"
    r = feature_engineering(
        str(cargo_csv),
        output_path=str(out),
        open_after=False,
        derive=[
            {
                "name": "pair",
                "op": "text",
                "column": "operating",
                "how": "combine",
                "other": "published",
                "separator": " / ",
            }
        ],
    )
    assert r["success"] is True
    assert "SkyWest / United" in set(_derived(out)["pair"])


# --- the point of all of it -------------------------------------------------


def test_the_derived_column_can_then_be_grouped_by(cargo_csv, tmp_path):
    out = tmp_path / "d.csv"
    feature_engineering(
        str(cargo_csv),
        output_path=str(out),
        open_after=False,
        derive=[
            {"name": "year", "op": "text", "column": "period_code", "how": "slice", "start": 0, "stop": 4, "as": "int"}
        ],
    )
    r = compute_aggregations(str(out), group_by=["year"], agg_column="tons", agg_func="sum")
    assert r["success"] is True
    assert r["groups"] == 2
    totals = {int(row["year"]): row["tons"] for row in r["result"]}
    assert totals[2020] == sum(m * 100 for m in range(1, 13))
    assert totals[2021] == sum(m * 100 for m in range(1, 13)) + 55


def test_naming_derivations_does_not_also_one_hot_the_whole_file(cargo_csv, tmp_path):
    """`features=None` used to mean "all four families", which would bury two
    named columns under a hundred dummies."""
    out = tmp_path / "d.csv"
    r = feature_engineering(
        str(cargo_csv),
        output_path=str(out),
        open_after=False,
        derive=[{"name": "year", "op": "text", "column": "period_code", "how": "slice", "start": 0, "stop": 4}],
    )
    assert r["new_columns"] == ["year"]
    assert r["one_hot_encoded"] == []


def test_the_automatic_families_still_run_when_asked(cargo_csv, tmp_path):
    r = feature_engineering(
        str(cargo_csv), output_path=str(tmp_path / "d.csv"), open_after=False, features=["text_length"]
    )
    assert r["success"] is True
    assert any(c.endswith("_len") for c in r["new_columns"])


# --- refusals ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        ({"name": "x", "op": "nope", "column": "tons"}, "nope"),
        ({"name": "x", "op": "arith", "column": "tons", "how": "cube", "value": 2}, "cube"),
        ({"name": "x", "op": "date_part", "column": "start_date", "part": "fortnight"}, "fortnight"),
        ({"name": "x", "op": "text", "column": "missing", "how": "upper"}, "missing"),
        ({"name": "x", "op": "arith", "column": "tons", "how": "add"}, "'other'"),
        ({"op": "text", "column": "tons", "how": "upper"}, "'name'"),
    ],
)
def test_a_bad_spec_is_refused_by_index(cargo_csv, tmp_path, spec, expected):
    out = tmp_path / "d.csv"
    r = feature_engineering(str(cargo_csv), output_path=str(out), open_after=False, derive=[spec])
    assert r["success"] is False
    assert expected in r["error"]
    assert "Derivation 0" in r["error"]
    # Refused before anything is written, so no half-derived frame lands.
    assert not out.exists()


def test_the_whole_list_is_refused_not_half_applied():
    frame = pd.DataFrame({"a": [1, 2, 3]})
    with pytest.raises(DeriveError):
        apply_derivations(
            frame,
            [
                {"name": "b", "op": "arith", "column": "a", "how": "mul", "value": 2},
                {"name": "c", "op": "arith", "column": "a", "how": "wat", "value": 2},
            ],
        )


def test_a_run_of_derivations_is_bounded():
    frame = pd.DataFrame({"a": [1]})
    specs = [{"name": f"c{i}", "op": "arith", "column": "a", "how": "add", "value": i} for i in range(40)]
    with pytest.raises(DeriveError, match="limit"):
        apply_derivations(frame, specs)
