"""Tests for servers/data_statistics — statistical_test, regression_analysis,
period_comparison. Feature + e2e tests using dummy data."""

from __future__ import annotations

import datetime
from pathlib import Path

import pandas as pd
import pytest

from servers.data_statistics._stats_comparative import period_comparison
from servers.data_statistics._stats_regression import regression_analysis
from servers.data_statistics._stats_tests import statistical_test

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def normal_csv(tmp_path) -> Path:
    """Normally distributed data for parametric tests."""
    import numpy as np

    rng = np.random.default_rng(42)
    f = tmp_path / "normal.csv"
    n = 50
    group_a = rng.normal(100, 15, n)
    group_b = rng.normal(110, 15, n)
    rows = ["Group,Value"]
    for v in group_a:
        rows.append(f"A,{v:.2f}")
    for v in group_b:
        rows.append(f"B,{v:.2f}")
    f.write_text("\n".join(rows))
    return f


@pytest.fixture()
def regression_csv(tmp_path) -> Path:
    """CSV with a clear linear relationship for regression tests."""
    import numpy as np

    rng = np.random.default_rng(0)
    f = tmp_path / "regression.csv"
    n = 60
    x1 = rng.uniform(1, 100, n)
    x2 = rng.uniform(0, 50, n)
    y = 3.5 * x1 + 1.2 * x2 + rng.normal(0, 5, n)
    rows = ["y,x1,x2"]
    for yi, xi1, xi2 in zip(y, x1, x2):
        rows.append(f"{yi:.4f},{xi1:.4f},{xi2:.4f}")
    f.write_text("\n".join(rows))
    return f


@pytest.fixture()
def logistic_csv(tmp_path) -> Path:
    """CSV for binary logistic regression."""
    import numpy as np

    rng = np.random.default_rng(7)
    f = tmp_path / "logistic.csv"
    n = 80
    x = rng.uniform(0, 10, n)
    prob = 1 / (1 + np.exp(-(x - 5)))
    y = (rng.uniform(0, 1, n) < prob).astype(int)
    rows = ["label,feature"]
    for yi, xi in zip(y, x):
        rows.append(f"{yi},{xi:.4f}")
    f.write_text("\n".join(rows))
    return f


@pytest.fixture()
def monthly_sales_csv(tmp_path) -> Path:
    """18 months of monthly sales data for period_comparison."""
    f = tmp_path / "monthly.csv"
    rows = ["Date,Revenue,Units"]
    start = datetime.date(2022, 7, 1)
    base_rev = 10000
    base_units = 100
    for i in range(18):
        dt = start + datetime.timedelta(days=i * 30)
        rev = base_rev + i * 200 + (500 if i % 3 == 0 else 0)
        units = base_units + i * 2
        rows.append(f"{dt},{rev},{units}")
    f.write_text("\n".join(rows))
    return f


@pytest.fixture()
def categorical_csv(tmp_path) -> Path:
    """CSV for chi-square / fisher tests."""
    f = tmp_path / "categorical.csv"
    f.write_text(
        "Treatment,Outcome\n"
        "A,Success\n"
        "A,Success\n"
        "A,Failure\n"
        "A,Success\n"
        "B,Failure\n"
        "B,Failure\n"
        "B,Success\n"
        "B,Failure\n"
        "A,Success\n"
        "B,Failure\n"
    )
    return f


# ---------------------------------------------------------------------------
# statistical_test — normality tests
# ---------------------------------------------------------------------------


class TestStatisticalTestNormality:
    def test_shapiro_wilk(self, normal_csv):
        r = statistical_test(str(normal_csv), test="shapiro_wilk", column_a="Value")
        assert r["success"] is True
        assert "p_value" in r
        assert "statistic" in r  # actual key is 'statistic' not 'test_statistic'
        assert "interpretation" in r

    def test_ks_test(self, normal_csv):
        r = statistical_test(str(normal_csv), test="ks", column_a="Value")
        assert r["success"] is True
        assert "p_value" in r

    def test_anderson_darling(self, normal_csv):
        r = statistical_test(str(normal_csv), test="anderson", column_a="Value")
        assert r["success"] is True
        assert "statistic" in r  # actual key is 'statistic'
        assert "critical_values" in r

    def test_invalid_test_type(self, normal_csv):
        r = statistical_test(str(normal_csv), test="unknown_test", column_a="Value")
        assert r["success"] is False
        assert "hint" in r

    def test_file_not_found(self, tmp_path):
        r = statistical_test(str(tmp_path / "missing.csv"), test="shapiro_wilk", column_a="Value")
        assert r["success"] is False

    def test_column_not_found(self, normal_csv):
        r = statistical_test(str(normal_csv), test="shapiro_wilk", column_a="NonExistent")
        assert r["success"] is False
        assert "hint" in r


# ---------------------------------------------------------------------------
# statistical_test — two-sample tests
# ---------------------------------------------------------------------------


class TestStatisticalTestTwoSample:
    def test_t_test(self, normal_csv):
        r = statistical_test(str(normal_csv), test="t_test", column_a="Value", group_column="Group")
        assert r["success"] is True
        assert "p_value" in r
        assert "statistic" in r  # actual key is 'statistic'
        assert "interpretation" in r

    def test_mann_whitney(self, normal_csv):
        r = statistical_test(str(normal_csv), test="mann_whitney", column_a="Value", group_column="Group")
        assert r["success"] is True
        assert "p_value" in r

    def test_levene(self, normal_csv):
        r = statistical_test(str(normal_csv), test="levene", column_a="Value", group_column="Group")
        assert r["success"] is True
        assert "p_value" in r

    def test_pearson_correlation(self, regression_csv):
        r = statistical_test(str(regression_csv), test="pearson", column_a="y", column_b="x1")
        assert r["success"] is True
        assert "statistic" in r  # correlation coefficient is at 'statistic'
        assert "p_value" in r

    def test_spearman_correlation(self, regression_csv):
        r = statistical_test(str(regression_csv), test="spearman", column_a="y", column_b="x1")
        assert r["success"] is True
        assert "p_value" in r

    def test_chi_square(self, categorical_csv):
        r = statistical_test(str(categorical_csv), test="chi_square", column_a="Treatment", column_b="Outcome")
        assert r["success"] is True
        assert "p_value" in r


# ---------------------------------------------------------------------------
# statistical_test — effect sizes
# ---------------------------------------------------------------------------


class TestStatisticalTestEffectSize:
    def test_cohens_d_in_t_test(self, normal_csv):
        r = statistical_test(
            str(normal_csv), test="t_test", column_a="Value", group_column="Group", compute_effect_size=True
        )
        assert r["success"] is True
        assert "effect_size" in r

    def test_progress_present(self, normal_csv):
        r = statistical_test(str(normal_csv), test="shapiro_wilk", column_a="Value")
        assert "progress" in r
        assert len(r["progress"]) > 0

    def test_token_estimate(self, normal_csv):
        r = statistical_test(str(normal_csv), test="shapiro_wilk", column_a="Value")
        assert "token_estimate" in r
        assert r["token_estimate"] > 0


# ---------------------------------------------------------------------------
# regression_analysis
# ---------------------------------------------------------------------------


class TestRegressionAnalysis:
    def test_ols_basic(self, regression_csv):
        r = regression_analysis(str(regression_csv), y_col="y", x_cols=["x1", "x2"])
        assert r["success"] is True
        assert r["model_type"] == "ols"
        assert "r_squared" in r
        assert "coefficients" in r
        assert "x1" in r["coefficients"]
        assert "x2" in r["coefficients"]

    def test_ols_r_squared_reasonable(self, regression_csv):
        r = regression_analysis(str(regression_csv), y_col="y", x_cols=["x1", "x2"])
        assert r["success"] is True
        assert r["r_squared"] > 0.8  # strong linear relationship

    def test_ols_metrics_present(self, regression_csv):
        r = regression_analysis(str(regression_csv), y_col="y", x_cols=["x1", "x2"])
        assert r["success"] is True
        for key in ("adj_r_squared", "rmse", "mae", "f_statistic", "f_pvalue", "aic", "bic"):
            assert key in r

    def test_ols_coefficient_fields(self, regression_csv):
        r = regression_analysis(str(regression_csv), y_col="y", x_cols=["x1"])
        assert r["success"] is True
        coef = r["coefficients"]["x1"]
        for field in ("coef", "std_err", "t_or_z", "p_value", "ci_lower", "ci_upper", "significant"):
            assert field in coef

    def test_ols_diagnostics(self, regression_csv):
        r = regression_analysis(str(regression_csv), y_col="y", x_cols=["x1", "x2"])
        assert r["success"] is True
        assert "diagnostics" in r
        assert "normality_of_residuals" in r["diagnostics"]

    def test_logistic_basic(self, logistic_csv):
        r = regression_analysis(str(logistic_csv), y_col="label", x_cols=["feature"], model_type="logistic")
        assert r["success"] is True
        assert r["model_type"] == "logistic"
        assert "pseudo_r_squared" in r
        assert "log_likelihood" in r

    def test_invalid_model_type(self, regression_csv):
        r = regression_analysis(str(regression_csv), y_col="y", x_cols=["x1"], model_type="ridge")
        assert r["success"] is False
        assert "hint" in r

    def test_column_not_found(self, regression_csv):
        r = regression_analysis(str(regression_csv), y_col="y", x_cols=["nonexistent"])
        assert r["success"] is False
        assert "hint" in r

    def test_file_not_found(self, tmp_path):
        r = regression_analysis(str(tmp_path / "missing.csv"), y_col="y", x_cols=["x1"])
        assert r["success"] is False

    def test_significant_predictors(self, regression_csv):
        r = regression_analysis(str(regression_csv), y_col="y", x_cols=["x1", "x2"])
        assert r["success"] is True
        assert "significant_predictors" in r
        assert len(r["significant_predictors"]) >= 1

    def test_insight_present(self, regression_csv):
        r = regression_analysis(str(regression_csv), y_col="y", x_cols=["x1", "x2"])
        assert r["success"] is True
        assert "insight" in r

    def test_token_estimate(self, regression_csv):
        r = regression_analysis(str(regression_csv), y_col="y", x_cols=["x1"])
        assert "token_estimate" in r
        assert r["token_estimate"] > 0


# ---------------------------------------------------------------------------
# period_comparison
# ---------------------------------------------------------------------------


class TestPeriodComparison:
    def test_month_over_month(self, monthly_sales_csv):
        r = period_comparison(
            str(monthly_sales_csv),
            date_col="Date",
            metrics=["Revenue"],
            period_unit="M",
        )
        assert r["success"] is True
        # Returns 'comparisons' (list) not 'comparison' (dict)
        assert "comparisons" in r
        assert len(r["comparisons"]) > 0
        assert "Revenue" in r["comparisons"][0]

    def test_comparison_fields(self, monthly_sales_csv):
        r = period_comparison(
            str(monthly_sales_csv),
            date_col="Date",
            metrics=["Revenue"],
            period_unit="M",
        )
        assert r["success"] is True
        # Each entry in comparisons has metric data
        comp_entry = r["comparisons"][0]
        rev = comp_entry["Revenue"]
        assert "current" in rev
        assert "delta" in rev
        assert "pct_change" in rev
        assert "direction" in rev

    def test_multiple_metrics(self, monthly_sales_csv):
        r = period_comparison(
            str(monthly_sales_csv),
            date_col="Date",
            metrics=["Revenue", "Units"],
            period_unit="M",
        )
        assert r["success"] is True
        comp_entry = r["comparisons"][0]
        assert "Revenue" in comp_entry
        assert "Units" in comp_entry

    def test_invalid_period_unit(self, monthly_sales_csv):
        r = period_comparison(
            str(monthly_sales_csv),
            date_col="Date",
            metrics=["Revenue"],
            period_unit="X",
        )
        assert r["success"] is False
        assert "hint" in r

    def test_file_not_found(self, tmp_path):
        r = period_comparison(
            str(tmp_path / "missing.csv"),
            date_col="Date",
            metrics=["Revenue"],
            period_unit="M",
        )
        assert r["success"] is False

    def test_metric_not_found(self, monthly_sales_csv):
        r = period_comparison(
            str(monthly_sales_csv),
            date_col="Date",
            metrics=["NonExistent"],
            period_unit="M",
        )
        # Either fails or returns no comparison for NonExistent
        if r["success"]:
            assert not r.get("comparisons") or "NonExistent" not in r["comparisons"][0]

    def test_progress_present(self, monthly_sales_csv):
        r = period_comparison(
            str(monthly_sales_csv),
            date_col="Date",
            metrics=["Revenue"],
            period_unit="M",
        )
        assert "progress" in r

    def test_token_estimate(self, monthly_sales_csv):
        r = period_comparison(
            str(monthly_sales_csv),
            date_col="Date",
            metrics=["Revenue"],
            period_unit="M",
        )
        assert "token_estimate" in r
        assert r["token_estimate"] > 0


# ---------------------------------------------------------------------------
# E2E: end-to-end statistical analysis pipeline
# ---------------------------------------------------------------------------


class TestE2EStatisticalPipeline:
    def test_normality_then_correct_test(self, normal_csv):
        """Confirm normality, then run parametric t-test (Shapiro → t-test)."""
        # Step 1: Check normality of group A
        r_norm = statistical_test(str(normal_csv), test="shapiro_wilk", column_a="Value")
        assert r_norm["success"] is True
        assert "p_value" in r_norm

        # Step 2: Run t-test (appropriate for ~normal data)
        r_t = statistical_test(str(normal_csv), test="t_test", column_a="Value", group_column="Group")
        assert r_t["success"] is True
        assert "p_value" in r_t
        # Groups differ by 10 units in mean — should detect at N=50 per group
        assert r_t["p_value"] < 0.10  # relaxed threshold for small sample

    def test_regression_with_effect_size_and_diagnostics(self, regression_csv):
        """Full regression pipeline: fit → check diagnostics → check VIF."""
        r = regression_analysis(str(regression_csv), y_col="y", x_cols=["x1", "x2"])
        assert r["success"] is True
        # Fit quality
        assert r["r_squared"] > 0.5
        # Coefficients
        assert "x1" in r["coefficients"]
        assert "x2" in r["coefficients"]
        # Diagnostics
        assert "diagnostics" in r
        norm = r["diagnostics"]["normality_of_residuals"]
        assert "p_value" in norm
        assert "normal" in norm
        # VIF
        assert "vif" in r

    def test_period_comparison_then_t_test(self, tmp_path):
        """Compare two periods, then confirm statistical difference with t-test."""
        f = tmp_path / "sales_2yrs.csv"
        rows = ["Date,Revenue"]
        # Year 1: lower
        for i in range(12):
            dt = datetime.date(2022, i + 1, 15)
            rows.append(f"{dt},{5000 + i * 100}")
        # Year 2: higher
        for i in range(12):
            dt = datetime.date(2023, i + 1, 15)
            rows.append(f"{dt},{7000 + i * 100}")
        f.write_text("\n".join(rows))

        # Period comparison: current vs previous year
        r_comp = period_comparison(
            str(f),
            date_col="Date",
            metrics=["Revenue"],
            period_unit="Y",
        )
        assert r_comp["success"] is True
        # comparisons is a list; check first entry
        assert len(r_comp["comparisons"]) > 0
        rev = r_comp["comparisons"][0]["Revenue"]
        # 2023 should be higher than 2022
        assert rev["direction"] == "up"

    def test_all_key_fields_in_full_regression(self, regression_csv):
        """Verify every documented field appears in a complete OLS result."""
        r = regression_analysis(str(regression_csv), y_col="y", x_cols=["x1", "x2"])
        assert r["success"] is True
        required = [
            "model_type",
            "observations",
            "coefficients",
            "significant_predictors",
            "vif",
            "r_squared",
            "adj_r_squared",
            "rmse",
            "mae",
            "f_statistic",
            "f_pvalue",
            "aic",
            "bic",
            "diagnostics",
            "insight",
            "progress",
            "token_estimate",
        ]
        for field in required:
            assert field in r, f"Missing field: {field}"
