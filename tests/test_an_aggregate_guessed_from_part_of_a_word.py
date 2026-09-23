"""The aggregate is guessed from a column's words, not from letters inside them.

infer_agg decides whether a numeric column is summed, averaged, or reduced to
its max or min -- for the dashboard, pivot tables and lag correlation. It
matched its keywords as substrings of the name, so ordinary names picked up an
aggregate from a fragment:

    followers        -> min   ("low")      laptop_sales     -> max  ("top")
    attempts         -> mean  ("temp")     database_size    -> min  ("base")
    problems_reported-> mean  ("prob")     highway_miles    -> max  ("high")

Every one of those is a count to be summed. Keywords now match whole words
(camelCase split, plurals included); only long, distinctive keywords are still
found inside a run-together name.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.column_utils import infer_agg, name_words  # noqa: E402

COUNTS = pd.Series([3, 40, 12, 7])


@pytest.mark.parametrize(
    "name",
    [
        "followers",
        "laptop_sales",
        "attempts",
        "database_size",
        "problems_reported",
        "highway_miles",
        "flow_volume",
        "generated_leads",
        "stop_count",
        "baseline_visits",
        "capital_spend",
        "shipments_total",
    ],
)
def test_a_word_fragment_does_not_choose_the_aggregate(name):
    assert infer_agg(name, COUNTS) == "sum"


@pytest.mark.parametrize(
    ("name", "agg"),
    [
        ("click_through_rate", "mean"),
        ("conversionRate", "mean"),
        ("avg_price", "mean"),
        ("ratings", "mean"),
        ("customer satisfaction", "mean"),
        ("averageprice", "mean"),
        ("populationdensity", "mean"),
        ("peak_load", "max"),
        ("maximumTemp", "mean"),
        ("min_price", "min"),
        ("lowest_bid", "min"),
        ("f1_score", "mean"),
    ],
)
def test_real_keywords_still_decide(name, agg):
    assert infer_agg(name, COUNTS) == agg


def test_a_rate_between_zero_and_one_is_still_averaged():
    assert infer_agg("ctr", pd.Series([0.01, 0.2, 0.05])) == "mean"


def test_name_words():
    assert name_words("clickThroughRate") == {"click", "through", "rate"}
    assert name_words("Units Sold") == {"units", "unit", "sold"}
    assert name_words("f1_score") == {"f1", "score"}
