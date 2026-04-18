"""Regression analysis module. No MCP imports. Requires statsmodels."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
for _p in (str(_ROOT),):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import numpy as np
import pandas as pd

from shared.file_utils import resolve_path
from shared.progress import fail, info, ok, warn

try:
    import statsmodels.api as _sm  # type: ignore[import-untyped]
    from statsmodels.stats.outliers_influence import variance_inflation_factor as _vif  # type: ignore[import-untyped]

    _STATSMODELS_OK = True
except ImportError:
    _sm = None  # type: ignore
    _vif = None  # type: ignore
    _STATSMODELS_OK = False

try:
    from scipy import stats as _scipy_stats

    _SCIPY_OK = True
except ImportError:
    _scipy_stats = None  # type: ignore
    _SCIPY_OK = False

logger = logging.getLogger(__name__)


def regression_analysis(
    file_path: str,
    y_col: str,
    x_cols: list[str],
    model_type: str = "ols",
    interaction_terms: list[str] = None,
    output_path: str = "",
) -> dict:
    """OLS or logistic regression with coefficients, p-values, R², diagnostics."""
    progress = []
    if _sm is None or _vif is None:
        return {
            "success": False,
            "error": "statsmodels not installed",
            "hint": "Install statsmodels: uv add statsmodels",
            "progress": [fail("Missing dependency", "statsmodels")],
            "token_estimate": 20,
        }
    sm = _sm
    variance_inflation_factor = _vif
    try:
        path = resolve_path(file_path)
        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check file_path is absolute.",
                "progress": [fail("File not found", path.name)],
                "token_estimate": 20,
            }

        try:
            df = pd.read_csv(str(path), encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(str(path), encoding="latin-1")

        if model_type not in ("ols", "logistic"):
            return {
                "success": False,
                "error": f"Unknown model_type '{model_type}'",
                "hint": "Valid: ols, logistic",
                "progress": [fail("Unknown model_type", model_type)],
                "token_estimate": 20,
            }

        # Validate columns
        missing_cols = [c for c in [y_col] + x_cols if c not in df.columns]
        if missing_cols:
            return {
                "success": False,
                "error": f"Columns not found: {missing_cols}",
                "hint": f"Available: {list(df.columns)}",
                "progress": [fail("Columns not found", str(missing_cols))],
                "token_estimate": 20,
            }

        # Build feature matrix
        data = df[[y_col] + x_cols].dropna()
        y = pd.to_numeric(data[y_col], errors="coerce").dropna()
        data = data.loc[y.index]
        y = y.loc[data.index]

        X_df = data[x_cols].copy()

        # One-hot encode any object columns
        cat_cols = X_df.select_dtypes(include="object").columns.tolist()
        if cat_cols:
            X_df = pd.get_dummies(X_df, columns=cat_cols, drop_first=True)
            progress.append(info("One-hot encoded", str(cat_cols)))

        # Interaction terms
        if interaction_terms:
            for term in interaction_terms:
                if "*" in term:
                    parts = [p.strip() for p in term.split("*")]
                    if all(p in X_df.columns for p in parts):
                        new_col = "_x_".join(parts)
                        X_df[new_col] = X_df[parts].prod(axis=1)
                        progress.append(info("Interaction term", new_col))

        X = sm.add_constant(X_df, has_constant="add")

        if model_type == "ols":
            model = sm.OLS(y, X).fit()
        else:
            model = sm.Logit(y, X).fit(disp=0)

        # Build coefficient table
        coef_table = {}
        for param in model.params.index:
            if param == "const":
                continue
            coef_table[param] = {
                "coef": round(float(model.params[param]), 6),
                "std_err": round(float(model.bse[param]), 6),
                "t_or_z": round(float(model.tvalues[param]), 4),
                "p_value": round(float(model.pvalues[param]), 6),
                "ci_lower": round(float(model.conf_int().loc[param, 0]), 6),
                "ci_upper": round(float(model.conf_int().loc[param, 1]), 6),
                "significant": bool(model.pvalues[param] < 0.05),
            }

        significant_predictors = [p for p, v in coef_table.items() if v["significant"]]

        # VIF for multicollinearity
        vif_data: dict = {}
        try:
            X_no_const = X.drop(columns=["const"], errors="ignore")
            if len(X_no_const.columns) > 1:
                vif_vals = [variance_inflation_factor(X_no_const.values, i) for i in range(len(X_no_const.columns))]
                max_vif = float(max(vif_vals))
                vif_data = {
                    "max_vif": round(max_vif, 2),
                    "problematic": max_vif > 10,
                    "note": "VIF > 10 indicates severe multicollinearity.",
                }
        except Exception:
            pass

        # Build result
        result_data: dict = {
            "model_type": model_type,
            "observations": int(model.nobs),
            "coefficients": coef_table,
            "significant_predictors": significant_predictors,
            "vif": vif_data,
        }

        if model_type == "ols":
            residuals = model.resid
            result_data.update(
                {
                    "r_squared": round(float(model.rsquared), 4),
                    "adj_r_squared": round(float(model.rsquared_adj), 4),
                    "rmse": round(float(np.sqrt(model.mse_resid)), 4),
                    "mae": round(float(np.abs(residuals).mean()), 4),
                    "f_statistic": round(float(model.fvalue), 4),
                    "f_pvalue": round(float(model.f_pvalue), 6),
                    "aic": round(float(model.aic), 2),
                    "bic": round(float(model.bic), 2),
                }
            )
            # Diagnostics
            normality_p = float("nan")
            if _SCIPY_OK:
                _, normality_p = _scipy_stats.shapiro(residuals.values[: min(5000, len(residuals))])
            result_data["diagnostics"] = {
                "normality_of_residuals": {
                    "test": "shapiro_wilk",
                    "p_value": round(float(normality_p), 4),
                    "normal": bool(normality_p >= 0.05),
                },
                "multicollinearity": vif_data,
            }
        else:
            result_data.update(
                {
                    "pseudo_r_squared": round(float(model.prsquared), 4),
                    "log_likelihood": round(float(model.llf), 4),
                    "aic": round(float(model.aic), 2),
                    "bic": round(float(model.bic), 2),
                }
            )

        # Insight
        if significant_predictors:
            top = max(coef_table, key=lambda p: abs(coef_table[p]["coef"]))
            coef_val = coef_table[top]["coef"]
            direction = "positive" if coef_val > 0 else "negative"
            result_data["insight"] = (
                f"'{top}' is the strongest predictor (β={coef_val:.4f}, {direction} effect, "
                f"p={coef_table[top]['p_value']:.4f})."
            )
        else:
            result_data["insight"] = "No significant predictors found at α=0.05."

        progress.append(
            ok(
                f"{'OLS' if model_type == 'ols' else 'Logistic'} regression",
                f"n={int(model.nobs)}  {len(significant_predictors)} significant predictors",
            )
        )

        result = {
            "success": True,
            "op": "regression_analysis",
            **result_data,
            "progress": progress,
        }
        result["token_estimate"] = len(str(result)) // 4
        return result

    except Exception as exc:
        logger.exception("regression_analysis error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check y_col and x_cols are numeric (or categorical for one-hot encoding). Use model_type: ols or logistic.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }
