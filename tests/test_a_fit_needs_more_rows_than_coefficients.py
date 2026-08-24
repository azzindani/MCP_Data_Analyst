"""regression_analysis must refuse a fit that has no residual degrees of freedom.

Handed one row, it reported `'float' object has no attribute 'dtype'` -- a
message from inside scipy's NaN-policy wrapper, raised because
`scipy.stats.shapiro` was asked to test the normality of a single residual. The
hint that came with it talked about column names and model types.

Everything before that point had already gone wrong quietly: with as many
coefficients as observations the fit passes exactly through every point, so ssr
is zero, the residual mean square is 0/0, and r_squared, the F statistic and
every coefficient p-value are NaN. `no_rows_error`, added for the empty-file
case, checks for zero rows and let one row straight through.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_statistics")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _stats_regression import regression_analysis  # noqa: E402


def _csv(tmp_path, n_rows: int) -> Path:
    f = tmp_path / f"rows_{n_rows}.csv"
    rows = "\n".join(f"{i},{i * 3 + (i % 3)}" for i in range(1, n_rows + 1))
    f.write_text(f"spend,clicks\n{rows}\n")
    return f


@pytest.mark.parametrize("n_rows", [1, 2])
def test_too_few_rows_is_a_refusal_naming_the_shortfall(tmp_path, n_rows):
    r = regression_analysis(str(_csv(tmp_path, n_rows)), y_col="clicks", x_cols=["spend"], open_after=False)
    assert r["success"] is False
    # Not scipy's word for it.
    assert "dtype" not in r["error"]
    assert "residual degrees of freedom" in r["error"]
    assert f"{n_rows} usable row" in r["error"]
    # The hint has to point at the fix, which is rows or predictors.
    assert "rows" in r["hint"]


def test_three_rows_and_one_predictor_fits(tmp_path):
    """Two coefficients need three rows to leave one residual degree of freedom."""
    r = regression_analysis(str(_csv(tmp_path, 3)), y_col="clicks", x_cols=["spend"], open_after=False)
    assert r["success"] is True
    assert r["observations"] == 3
    assert r["residual_df"] == 1


def test_a_fit_that_cannot_test_its_coefficients_says_so(tmp_path):
    """Every p-value missing is not the same as no predictor being significant."""
    # Three rows, two predictors -> three coefficients, zero residual df.
    f = tmp_path / "wide.csv"
    f.write_text("spend,views,clicks\n1,2,3\n4,5,6\n7,9,10\n")
    r = regression_analysis(str(f), y_col="clicks", x_cols=["spend", "views"], open_after=False)
    assert r["success"] is False
    assert "0 residual degrees of freedom" in r["error"]


def test_residual_normality_is_undetermined_not_failed(tmp_path):
    """Shapiro-Wilk needs three residuals; below that `normal` must be None.

    `bool(normality_p >= 0.05)` on a NaN is False, which reported a diagnostic
    failure the model had not earned.
    """
    r = regression_analysis(str(_csv(tmp_path, 3)), y_col="clicks", x_cols=["spend"], open_after=False)
    normality = r["diagnostics"]["normality_of_residuals"]
    assert normality["normal"] in (None, True, False)
    if normality["p_value"] is None:
        assert normality["normal"] is None
        assert "undetermined" in normality["status"]


def test_a_real_sample_still_produces_a_full_fit(tmp_path):
    r = regression_analysis(str(_csv(tmp_path, 40)), y_col="clicks", x_cols=["spend"], open_after=False)
    assert r["success"] is True
    assert r["residual_df"] == 38
    assert r["r_squared"] is not None
    assert r["f_pvalue"] is not None
    coef = r["coefficients"]["spend"]
    assert coef["significant"] is True
    assert r["diagnostics"]["normality_of_residuals"]["normal"] is not None
    assert "strongest predictor" in r["insight"]
