"""What each column is, worked out before a chart is drawn.

The sweep's dashboard paired columns: it drew campaign_platform,
campaign_type and communication_medium as three charts of one split, headlined
a column that is "-" in 90% of rows, and did not know subchannel sits inside
platform or that the rows are one per day per ad. shared/analysis_plan.plan
works these out first; these tests hold it to each.
"""

from __future__ import annotations

import pandas as pd
import pytest

from shared.analysis_plan import plan

DAYS = pd.date_range("2024-01-01", periods=60, freq="D")


@pytest.fixture(scope="module")
def planned() -> dict:
    rows = []
    for i, day in enumerate(DAYS):
        for platform, medium, subs in (("A", "social", ("a1", "a2")), ("B", "search", ("b1", "b2"))):
            rows.append(
                {
                    "order_id": len(rows) + 1000,
                    "day": day.strftime("%Y-%m-%d"),
                    "platform": platform,
                    "medium": medium,
                    "subchannel": subs[i % 2],
                    "audience": "-" if len(rows) % 10 else "lookalike",
                    "spend": 10.0 + i + (5 if platform == "A" else 0),
                    "clicks": 3 + i % 7,
                    "age": 30 + i % 20,
                    "click_rate": 0.01 * (1 + i % 5),
                    "interest_rate": 4.5 + i % 3,
                    "net_change": (-1) ** i * (5.0 + i % 6),
                }
            )
    return plan(pd.DataFrame(rows))


def test_two_names_for_one_split_are_one_dimension(planned):
    assert planned["aliases"] == [["platform", "medium"]]
    assert planned["columns"]["medium"]["alias_of"] == "platform"
    assert "medium" not in planned["dimensions"]
    assert "platform, medium split the rows identically: one dimension, shown as platform." in planned["notes"]


def test_a_dimension_inside_another_is_its_child(planned):
    assert {"child": "subchannel", "parent": "platform"} in planned["hierarchies"]
    assert planned["dimensions"][0] == "platform"


def test_a_column_mostly_placeholder_is_left_out_of_the_segments(planned):
    info = planned["columns"]["audience"]
    assert info["placeholder_share"] == pytest.approx(0.9)
    assert "audience" not in planned["dimensions"]
    assert "audience is a placeholder in 90% of rows, so it is left out of the segment views." in planned["notes"]
    assert not any(n.startswith("audience nests") for n in planned["notes"])


def test_the_grain_is_one_row_per_day_per_platform(planned):
    g = planned["grain"]
    assert (g["date"], g["frequency"], g["periods"], g["rows_per_period"]) == ("day", "day", 60, 2.0)
    assert (g["start"], g["end"]) == ("2024-01-01", "2024-02-29")
    assert g["unique_rows"] is True


def test_what_adds_up_is_summed_and_what_does_not_is_not(planned):
    cols = planned["columns"]
    assert cols["spend"]["additive"] is True
    assert cols["clicks"]["additive"] is True
    assert cols["age"]["additive"] is False
    assert cols["click_rate"]["additive"] is False and cols["click_rate"]["unit"] == "percent"
    assert cols["interest_rate"]["unit"] == "number", "4.5 is already a percentage, not a share"
    assert cols["net_change"]["additive"] is False  # no word says so; it goes negative


def test_the_headline_measure_comes_first_and_an_id_is_no_measure(planned):
    assert planned["measures"][:2] == ["spend", "clicks"]
    assert planned["columns"]["order_id"]["role"] == "id"
    assert planned["columns"]["spend"]["unit"] == "currency"
