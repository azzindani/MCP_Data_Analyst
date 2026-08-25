"""The intercept was fitted, used, and never reported.

    OLS or logistic regression. Returns coefs p-values R2 RMSE diagnostics.

The coefficient table was built by walking `model.params.index` with

    if param == "const":
        continue

so the constant -- which is in every prediction the model makes -- was the one
number the response did not carry. The round-14 sweep found it by refitting the
same call independently in numpy: every slope matched to six places, and the
intercept it recovered, 3.7095, appeared nowhere in the tool's output. A caller
handed those coefficients cannot reproduce a single prediction.

It is reported beside the predictors rather than among them, because
`significant_predictors` is a list of predictors and the intercept is not one.
`equation` writes the whole fitted model on one line.

The same phase found the refusal for a text target pointing at the wrong thing:

    regression_analysis(y_col="campaign_platform", model_type="logistic")
      -> "0 usable row(s) cannot support 3 coefficient(s)"
      -> hint: "Give regression_analysis more than 3 rows where campaign_platform
                and every x_col are non-null"

on a file with 16,834 complete rows. `pd.to_numeric(errors="coerce")` had turned
the whole target to NaN, and the degrees-of-freedom guard downstream reported
the symptom -- sending the caller to find rows that were already there, instead
of naming the target as words.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from servers.data_statistics.engine import regression_analysis  # noqa: E402


@pytest.fixture
def ads(tmp_path) -> Path:
    rng = np.random.default_rng(0)
    n = 300
    spends = rng.normal(50, 10, n)
    impressions = rng.normal(200, 30, n)
    f = tmp_path / "ads.csv"
    pd.DataFrame(
        {
            "spends": spends,
            "impressions": impressions,
            "clicks": 3.7 + 0.03 * spends + 0.012 * impressions + rng.normal(0, 1, n),
            "platform": ["Google Ads" if i % 3 else "Facebook Ads" for i in range(n)],
            "three": ["a", "b", "c"] * (n // 3),
            "flag": [i % 2 for i in range(n)],
            "grade": [0, 1, 2] * (n // 3),
            # "unknown" rather than "n/a": pandas reads the latter as NaN, so it
            # would be dropped as a null before the coercion ever sees it.
            "mostly_numeric": [str(i) if i % 50 else "unknown" for i in range(n)],
            "gappy": [float(i) if i % 40 else None for i in range(n)],
        }
    ).to_csv(f, index=False)
    return f


def fit(path, tmp_path, **kw):
    return regression_analysis(str(path), open_after=False, output_path=str(tmp_path / "o.html"), **kw)


class TestTheInterceptIsReported:
    def test_it_is_there_at_all(self, ads, tmp_path):
        r = fit(ads, tmp_path, x_cols=["spends", "impressions"], y_col="clicks")
        assert r["success"] is True, r.get("error")
        assert r["intercept"]["coef"] is not None

    def test_it_matches_an_independent_fit(self, ads, tmp_path):
        """The sweep's own check: refit in numpy and compare."""
        r = fit(ads, tmp_path, x_cols=["spends", "impressions"], y_col="clicks")
        df = pd.read_csv(ads)
        design = np.column_stack([np.ones(len(df)), df["spends"].to_numpy(), df["impressions"].to_numpy()])
        beta, *_ = np.linalg.lstsq(design, df["clicks"].to_numpy(), rcond=None)
        assert r["intercept"]["coef"] == pytest.approx(beta[0], abs=1e-4)
        assert r["coefficients"]["spends"]["coef"] == pytest.approx(beta[1], abs=1e-4)

    def test_it_carries_the_same_fields_as_a_predictor(self, ads, tmp_path):
        r = fit(ads, tmp_path, x_cols=["spends"], y_col="clicks")
        for key in ("coef", "std_err", "t_or_z", "p_value", "ci_lower", "ci_upper", "significant"):
            assert key in r["intercept"], key

    def test_it_is_not_counted_as_a_predictor(self, ads, tmp_path):
        r = fit(ads, tmp_path, x_cols=["spends", "impressions"], y_col="clicks")
        assert "const" not in r["coefficients"]
        assert "const" not in r["significant_predictors"]
        assert set(r["coefficients"]) == {"spends", "impressions"}

    def test_the_equation_puts_the_model_on_one_line(self, ads, tmp_path):
        r = fit(ads, tmp_path, x_cols=["spends", "impressions"], y_col="clicks")
        eq = r["equation"]
        assert eq.startswith("clicks = ")
        assert str(r["intercept"]["coef"]) in eq
        assert "*spends" in eq and "*impressions" in eq

    def test_a_logistic_fit_reports_one_too(self, ads, tmp_path):
        r = fit(ads, tmp_path, x_cols=["spends"], y_col="flag", model_type="logistic")
        assert r["success"] is True, r.get("error")
        assert r["intercept"]["coef"] is not None


class TestARefusalNamesTheRealProblem:
    def test_a_text_target_is_named_as_text(self, ads, tmp_path):
        r = fit(ads, tmp_path, x_cols=["spends"], y_col="platform", model_type="logistic")
        assert r["success"] is False
        assert "holds no numbers" in r["error"]
        assert "platform" in r["error"]

    def test_the_error_does_not_blame_the_row_count(self, ads, tmp_path):
        r = fit(ads, tmp_path, x_cols=["spends"], y_col="platform", model_type="logistic")
        assert "usable row(s)" not in r["error"], "300 complete rows is not the problem"
        assert "more than" not in r["hint"]

    def test_it_names_the_op_that_fixes_it(self, ads, tmp_path):
        r = fit(ads, tmp_path, x_cols=["spends"], y_col="platform", model_type="logistic")
        assert "label_encode" in r["hint"]
        assert "platform" in r["hint"]

    def test_a_two_class_target_is_told_it_can_be_logistic(self, ads, tmp_path):
        r = fit(ads, tmp_path, x_cols=["spends"], y_col="platform", model_type="logistic")
        assert "2 distinct" in r["error"]
        assert "logistic" in r["hint"]

    def test_a_many_class_target_is_told_it_cannot(self, ads, tmp_path):
        r = fit(ads, tmp_path, x_cols=["spends"], y_col="three", model_type="logistic")
        assert "3 distinct" in r["error"]
        assert "not a regression target" in r["hint"]

    def test_a_numeric_multiclass_target_is_refused_for_logistic(self, ads, tmp_path):
        """grade is 0/1/2 -- numeric, so it passes the coercion and fails here."""
        r = fit(ads, tmp_path, x_cols=["spends"], y_col="grade", model_type="logistic")
        assert r["success"] is False
        assert "two-class" in r["error"]
        assert "model_type=ols" in r["hint"]

    def test_the_same_column_is_fine_for_ols(self, ads, tmp_path):
        r = fit(ads, tmp_path, x_cols=["spends"], y_col="grade")
        assert r["success"] is True, r.get("error")

    def test_partly_numeric_targets_say_what_was_dropped(self, ads, tmp_path):
        r = fit(ads, tmp_path, x_cols=["spends"], y_col="mostly_numeric")
        assert r["success"] is True, r.get("error")
        assert r["rows_in_file"] == 300
        assert r["rows_dropped_non_numeric"] == 6
        warnings = [p for p in r["progress"] if p.get("status") == "warn"]
        assert any("not a number" in p["detail"] for p in warnings), r["progress"]

    def test_rows_lost_to_nulls_are_counted_separately(self, ads, tmp_path):
        r = fit(ads, tmp_path, x_cols=["spends"], y_col="gappy")
        assert r["success"] is True, r.get("error")
        assert r["rows_dropped_null"] == 8
        assert r["rows_dropped_non_numeric"] == 0
        warnings = [p for p in r["progress"] if p.get("status") == "warn"]
        assert any("null in gappy" in p["detail"] for p in warnings), r["progress"]

    def test_the_counts_add_up_to_what_was_fitted(self, ads, tmp_path):
        r = fit(ads, tmp_path, x_cols=["spends"], y_col="mostly_numeric")
        assert r["observations"] == r["rows_in_file"] - r["rows_dropped_null"] - r["rows_dropped_non_numeric"]

    def test_a_clean_fit_warns_about_nothing(self, ads, tmp_path):
        r = fit(ads, tmp_path, x_cols=["spends"], y_col="clicks")
        assert r["rows_dropped_null"] == r["rows_dropped_non_numeric"] == 0
        assert not [p for p in r["progress"] if p.get("status") == "warn"]
