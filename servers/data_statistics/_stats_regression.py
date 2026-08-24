"""Regression analysis module. No MCP imports. Requires statsmodels."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
# data_medium holds the shared chart saver; engine.py puts it on the path too,
# but this module is also imported directly by the tests.
_MED = str(Path(__file__).resolve().parents[1] / "data_medium")
for _p in (str(_ROOT), _MED):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import numpy as np
import pandas as pd

from shared.arg_alias import missing, pick, pick_list
from shared.file_utils import hint_for_error, no_rows_error, resolve_path
from shared.file_utils import read_csv as _read_csv
from shared.progress import fail, info, ok, warn
from shared.stats_format import format_p, round_p

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


def _coefficient_chart(
    coef_table: dict,
    y_col: str,
    output_path: str,
    input_path: Path,
    theme: str,
    open_after: bool,
) -> tuple[str, str]:
    """Render the fitted coefficients with their confidence intervals.

    This is the standard way to read a regression: which predictors moved the
    outcome, in which direction, and how sure the fit is about each. Every
    number plotted is already computed above, so the chart cannot disagree with
    the returned statistics.
    """
    try:
        import plotly.graph_objects as go  # type: ignore[import-untyped]
        from _med_helpers import _save_chart  # type: ignore[import]
    except ImportError:
        return "", ""

    names = list(coef_table)
    coefs = [coef_table[n]["coef"] for n in names]
    # Error bars are the distance from the point to each CI edge, not the edges.
    plus = [coef_table[n]["ci_upper"] - coef_table[n]["coef"] for n in names]
    minus = [coef_table[n]["coef"] - coef_table[n]["ci_lower"] for n in names]
    # Significance is the one thing a reader should not have to compute by eye.
    colors = ["#3fb950" if coef_table[n]["significant"] else "#8b949e" for n in names]

    fig = go.Figure(
        go.Bar(
            x=coefs,
            y=names,
            orientation="h",
            marker_color=colors,
            error_x=dict(type="data", symmetric=False, array=plus, arrayminus=minus),
            hovertemplate="%{y}: %{x:.4f}<extra></extra>",
        )
    )
    fig.add_vline(x=0, line_width=1, line_dash="dash", line_color="#8b949e")
    fig.update_layout(
        title=f"Effect on {y_col} (95% CI; grey = not significant)",
        xaxis_title="coefficient",
        margin=dict(l=20, r=20, t=60, b=20),
        autosize=True,
        showlegend=False,
    )
    return _save_chart(fig, output_path, "regression", input_path, open_after, theme)


def regression_analysis(
    file_path: str,
    y_col: str = "",
    x_cols: list[str] = None,
    model_type: str = "ols",
    interaction_terms: list[str] = None,
    output_path: str = "",
    theme: str = "device",
    open_after: bool = False,
    y_column: str = "",
    x_columns: list[str] = None,
) -> dict:
    """OLS or logistic regression with coefficients, p-values, R², diagnostics."""
    progress = []
    # Every other tool that names an axis spells it x_column / y_column.
    y_col, y_note = pick("regression_analysis", "y_col", y_col, y_column)
    if not y_col:
        return missing("regression_analysis", "y_col", "y_column")
    x_cols, x_note = pick_list("regression_analysis", "x_cols", x_cols, x_columns)
    if not x_cols:
        return missing("regression_analysis", "x_cols", "x_columns")
    for note in (y_note, x_note):
        if note:
            progress.append(info("Argument alias", note))
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

        df = _read_csv(str(path))
        if err := no_rows_error("regression_analysis", df, path.name, "Fitting a model"):
            return err

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
                "p_value": round_p(float(model.pvalues[param])),
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
                    "p_value": round_p(float(normality_p)),
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

        # output_path was accepted, threaded down here and then ignored: the
        # caller asked for a report, got success:true, and no file. Nothing in
        # the response said so either, because output_path was not echoed back.
        if output_path:
            chart_path, chart_name = _coefficient_chart(coef_table, y_col, output_path, path, theme, open_after)
            if chart_path:
                result["output_path"] = chart_path
                result["output_name"] = chart_name
                progress.append(ok("Coefficient chart saved", chart_name))
            else:
                progress.append(warn("No chart written", "plotly is unavailable in this environment"))

        result["token_estimate"] = len(str(result)) // 4
        return result

    except Exception as exc:
        logger.exception("regression_analysis error")
        return {
            "success": False,
            "error": str(exc),
            "hint": hint_for_error(
                exc,
                "Check y_col and x_cols are numeric (or categorical for one-hot encoding). Use model_type: ols or logistic.",
            ),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }
