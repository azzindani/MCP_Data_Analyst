"""A number with a handful of values -- a 0/1 flag -- drifts like any other column.

compare_frames measures a numeric column by PSI over the baseline's quantile
bins. A 0/1 column has no quantiles to bin, so PSI gave up on it -- and the
note said "not computable: fewer than 30 usable rows on one side" over 300
rows a side. A churn flag moving from 10% to 50% was reported as unmeasured,
for a reason that was not true, and drift_pct and the quality score's drift
component never counted it. Such a column is now compared as the categories
its values are; the row-count note is kept for the case it describes.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from shared.association import compare_frames


def _drift(base: pd.DataFrame, current: pd.DataFrame) -> dict:
    return {d["column"]: d for d in compare_frames(base, current)["drift"]}


def test_a_flag_that_moved_is_measured_and_moved():
    rng = np.random.default_rng(1)
    base = pd.DataFrame({"churned": (rng.random(300) < 0.1).astype(int)})
    current = pd.DataFrame({"churned": (rng.random(300) < 0.5).astype(int)})
    d = _drift(base, current)["churned"]
    shares = base["churned"].value_counts(normalize=True), current["churned"].value_counts(normalize=True)
    want = sum(abs(shares[0].get(k, 0) - shares[1].get(k, 0)) for k in (0, 1)) / 2
    assert (d["measure"], d["reading"]) == ("total_variation", "major shift")
    assert d["drift"] == round(want, 4)


def test_a_flag_that_held_is_stable():
    rng = np.random.default_rng(2)
    base = pd.DataFrame({"flag": (rng.random(400) < 0.3).astype(int)})
    current = pd.DataFrame({"flag": (rng.random(400) < 0.3).astype(int)})
    assert _drift(base, current)["flag"]["reading"] == "stable"


def test_too_few_rows_still_says_so():
    base = pd.DataFrame({"x": [1.0] * 10 + [None] * 290})
    current = pd.DataFrame({"x": np.linspace(0, 1, 300)})
    d = _drift(base, current)["x"]
    assert d["drift"] is None and d["measure"] == "psi"
    assert d["note"] == "not computable: fewer than 30 usable rows on one side"


def test_a_measure_with_many_values_is_still_psi():
    rng = np.random.default_rng(3)
    d = _drift(pd.DataFrame({"v": rng.normal(0, 1, 300)}), pd.DataFrame({"v": rng.normal(1, 1, 300)}))["v"]
    assert d["measure"] == "psi" and d["drift"] > 0.25
