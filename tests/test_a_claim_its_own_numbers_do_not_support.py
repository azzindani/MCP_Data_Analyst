"""Two derived claims that contradicted the numbers printed beside them.

Both were found with every raw statistic in the response verified correct
against pandas/scipy. The failure is not in the arithmetic; it is in the one
sentence or badge a reader takes away from it.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from servers.data_statistics._stats_regression import regression_analysis


def _column_quality(*args, **kwargs):
    """Imported lazily so the regression half of this file collects without it."""
    from servers.data_advanced._adv_profile import _column_quality as impl

    return impl(*args, **kwargs)


@pytest.fixture
def scaled_predictors(tmp_path):
    """y driven mostly by `big`, but `small` carries the larger raw coefficient.

    `big` spans thousands and `small` spans single digits, so the raw betas rank
    them backwards while the standardised effect ranks them correctly.
    """
    rng = np.random.default_rng(0)
    n = 400
    big = rng.normal(5000, 1500, n)
    small = rng.normal(5, 1.5, n)
    y = 0.02 * big + 1.0 * small + rng.normal(0, 5, n)
    path = tmp_path / "scaled.csv"
    pd.DataFrame({"big": big, "small": small, "y": y}).to_csv(path, index=False)
    return str(path)


def test_the_strongest_predictor_is_not_the_one_with_the_biggest_raw_number(scaled_predictors):
    r = regression_analysis(scaled_predictors, y_column="y", x_columns=["big", "small"], open_after=False)
    assert r["success"] is True

    coefs = r["coefficients"]
    # The premise: `small` really does have the larger raw coefficient.
    assert abs(coefs["small"]["coef"]) > abs(coefs["big"]["coef"])
    # ...and `big` really does carry the larger standardised effect.
    assert abs(coefs["big"]["std_beta"]) > abs(coefs["small"]["std_beta"])

    # The claim must follow the comparable measure, not the raw one.
    assert "'big' is the strongest predictor" in r["insight"]
    assert "standardised" in r["insight"]


def test_the_ranking_agrees_with_the_t_statistics(scaled_predictors):
    r = regression_analysis(scaled_predictors, y_column="y", x_columns=["big", "small"], open_after=False)
    coefs = r["coefficients"]
    by_t = max(coefs, key=lambda p: abs(coefs[p]["t_or_z"]))
    assert f"'{by_t}' is the strongest predictor" in r["insight"]


def test_every_coefficient_carries_a_comparable_effect_size(scaled_predictors):
    r = regression_analysis(scaled_predictors, y_column="y", x_columns=["big", "small"], open_after=False)
    for name, row in r["coefficients"].items():
        assert "std_beta" in row, f"{name} has no comparable effect size"
        assert row["std_beta"] is not None


# --- the per-column quality badge -------------------------------------------


def _info(null_pct=0.0, unique=5, unique_pct=1.0):
    return {"null_pct": null_pct, "unique": unique, "unique_pct": unique_pct, "dtype": "text"}


def test_a_clean_categorical_column_is_not_marked_down_for_having_few_values():
    # campaign_platform: 2 values, no nulls, nothing wrong. It scored 70/100
    # (orange) purely for being 0.01% unique.
    score, reasons = _column_quality(_info(unique=2), rows=16834, is_numeric=False)
    assert score == 100.0
    assert reasons == []


def test_a_constant_column_is_not_scored_the_same_as_a_good_one():
    # `product` and `phase` hold one value each. check_data_quality rates that
    # `high` severity, "contains no information"; this badge gave them the same
    # 70/100 as a perfectly clean column.
    constant, reasons = _column_quality(_info(unique=1), rows=16834, is_numeric=False)
    clean, _ = _column_quality(_info(unique=2), rows=16834, is_numeric=False)

    assert constant < clean
    assert constant <= 50, "a column carrying no information must not badge orange or green"
    assert any("constant" in r for r in reasons)


def test_nulls_still_cost_something():
    scored, reasons = _column_quality(_info(null_pct=3.24, unique=160), rows=16834, is_numeric=True)
    assert scored < 100.0
    assert any("null" in r for r in reasons)


def test_a_numeric_column_is_not_punished_for_being_continuous():
    # `spends` is 54% unique because it is a measurement. Under the old formula
    # that was the only thing in the file that earned green; it must not now
    # swing the other way and be penalised as an identifier.
    score, reasons = _column_quality(_info(unique=9087, unique_pct=54.0), rows=16834, is_numeric=True)
    assert score == 100.0
    assert reasons == []


def test_an_identifier_column_is_flagged():
    score, reasons = _column_quality(_info(unique=16000, unique_pct=95.0), rows=16834, is_numeric=False)
    assert score < 100.0
    assert any("identifier" in r for r in reasons)


def test_the_score_never_leaves_the_scale():
    score, _ = _column_quality(_info(null_pct=100.0, unique=1), rows=10, is_numeric=False)
    assert 0.0 <= score <= 100.0
