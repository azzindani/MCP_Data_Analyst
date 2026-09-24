"""A column with about one row per value does not "explain" the target -- it would explain anything.

run_eda(target_column=...) ranks every column by its association with the
target. Grouping by a column where nearly every row has its own value
explains all of the target's variance by construction, so a row reference
and a daily date column ranked first against a coin flip, at strength 1.0,
above every column that carried real signal. Cramer's V does the same for a
text target. Such a column is now reported as not measured, with the reason,
and ranks below every measured one.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from shared.association import MIN_ROWS_PER_CATEGORY, target_association


def _frame(n: int = 200) -> pd.DataFrame:
    rng = np.random.default_rng(3)
    return pd.DataFrame(
        {
            "ref": [f"r{i}" for i in range(n)],
            "day": pd.date_range("2024-01-01", periods=n),
            "region": rng.choice(["N", "S"], n),
            "amount": rng.normal(0, 1, n),
            "y": rng.integers(0, 2, n),
            "label": rng.choice(["yes", "no"], n),
        }
    )


def _by_column(df: pd.DataFrame, target: str) -> dict:
    return {a["column"]: a for a in target_association(df, target)}


def test_a_row_reference_and_a_daily_date_are_not_measured():
    got = _by_column(_frame(), "y")
    for col in ("ref", "day"):
        assert got[col]["strength"] is None
        assert got[col]["note"].startswith("not measured: 200 distinct values over 200 rows (1.0 rows each)")


def test_they_rank_below_every_measured_column():
    ranking = target_association(_frame(), "y")
    measured = [a["strength"] is not None for a in ranking]
    assert measured == sorted(measured, reverse=True), "every measured column comes first"
    assert {a["column"] for a in ranking if a["strength"] is None} == {"ref", "day"}


def test_a_text_target_is_guarded_too():
    got = _by_column(_frame().drop(columns="y"), "label")
    assert got["ref"]["measure"] == "cramers_v" and got["ref"]["strength"] is None
    assert got["region"]["strength"] is not None


def test_a_category_with_enough_rows_per_value_is_still_measured():
    n = 200
    df = _frame(n).assign(week=lambda d: (np.arange(n) // MIN_ROWS_PER_CATEGORY).astype(str))
    week = _by_column(df, "y")["week"]
    assert week["strength"] is not None and week["measure"] == "correlation_ratio"
