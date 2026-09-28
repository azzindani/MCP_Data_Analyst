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
from shared.file_utils import error_text, hint_for_error, no_rows_error, resolve_path
from shared.file_utils import read_csv as _read_csv
from shared.html_layout import discriminated_suffix
from shared.progress import fail, info, ok, warn
from shared.small_sample import (
    MIN_N_SHAPIRO,
    finite_split,
    is_significant,
    rounded,
    shapiro_p,
    shapiro_sample,
)
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


# A group column is a candidate for "pooled" when it splits the rows into a few
# groups, each big enough to fit the same model on its own.
_POOL_MAX_GROUPS = 8
_POOL_MIN_REDUCTION = 0.10
_POOL_ALPHA = 0.001
_POOL_MAX_CANDIDATES = 12
_PAGE_POINTS = 4000


def _pooled_groups(
    df: pd.DataFrame, rows: pd.Index, y: pd.Series, X: pd.DataFrame, ssr: float, skip: set
) -> list[dict]:
    """Columns whose groups the fit pools into one line, when each group follows a line of its own.

    The sweep regressed clicks on spends and impressions over two platforms
    whose rows lie on two separate lines; the one pooled line through both
    reported R2 0.896 and nothing about it. For each few-valued column outside
    the model, the same design is fitted per group and the residual sums are
    compared with the pooled fit's: a Chow test, and the share of the pooled
    fit's unexplained variation that separate lines remove.
    """
    if _sm is None or _scipy_stats is None or not ssr > 0:
        return []
    k = int(X.shape[1])
    n = len(y)
    found: list[dict] = []
    # One split under several names -- campaign_platform, campaign_type and
    # communication_medium are 1:1 in the ad data -- is one finding.
    by_partition: dict[bytes, dict | None] = {}
    checked = 0
    for col in df.columns:
        if col in skip or checked >= _POOL_MAX_CANDIDATES:
            continue
        groups = df.loc[rows, col].astype(str).fillna("(missing)")
        counts = groups.value_counts()
        if not 2 <= len(counts) <= _POOL_MAX_GROUPS or counts.min() < max(20, k + 2):
            continue
        partition = pd.factorize(groups)[0].tobytes()
        if partition in by_partition:
            same = by_partition[partition]
            if same is not None:
                same.setdefault("same_split_as", []).append(str(col))
            continue
        by_partition[partition] = None
        checked += 1
        try:
            split = sum(float(_sm.OLS(y[groups == g], X[groups == g]).fit().ssr) for g in counts.index)
        except Exception:  # a group whose design cannot be fitted says nothing about pooling
            continue
        extra = (len(counts) - 1) * k
        denominator = n - len(counts) * k
        if denominator <= 0 or split <= 0:
            continue
        f_stat = ((ssr - split) / extra) / (split / denominator)
        p_value = float(_scipy_stats.f.sf(f_stat, extra, denominator))
        reduction = 1.0 - split / ssr
        if reduction >= _POOL_MIN_REDUCTION and p_value < _POOL_ALPHA:
            by_partition[partition] = {
                "column": str(col),
                "groups": [str(g) for g in counts.index],
                "unexplained_removed": round(reduction, 4),
                "chow_f": round(float(f_stat), 2),
                "chow_p": round_p(p_value),
            }
            found.append(by_partition[partition])
    # By F, which charges each split for the parameters it adds: a finer column
    # always removes more, and on the sweep's ad data subchannel (4 groups,
    # 42%) would have outranked the two platforms (2 groups, 20%, F 1437 vs 1348)
    # it is nested in.
    found.sort(key=lambda f: -f["chow_f"])
    return found[:3]


def _pooled_note(pooled: list[dict]) -> str:
    top = pooled[0]
    return (
        f"The rows of '{top['column']}' follow separate lines: fitting its {len(top['groups'])} groups apart "
        f"leaves {top['unexplained_removed']:.0%} less unexplained variation (Chow p={format_p(top['chow_p'])}), "
        f"and this model pools them into one. Fit each group on its own, or add '{top['column']}' to x_cols."
    )


def _regression_page(
    model,
    X: pd.DataFrame,
    y: pd.Series,
    coef_table: dict,
    result_data: dict,
    y_col: str,
    output_path: str,
    input_path: Path,
    theme: str,
    open_after: bool,
    progress: list,
) -> tuple[str, str]:
    """The fit as a page a reader can judge it from.

    It was one bar chart of raw coefficients: $ of spend beside counts of
    impressions, so the longer bar was whichever column had the smaller unit,
    with no R2, no residuals, and nothing about the two platforms pooled into
    one line (sweep F18). Now: standardised effects with their intervals, the
    fit statistics, the residual diagnostics, and the pooled-groups finding.
    Every number is the one in the response.
    """
    try:
        import plotly.graph_objects as go  # type: ignore[import-untyped]
        from _med_helpers import _save_chart  # type: ignore[import]
        from plotly.subplots import make_subplots  # type: ignore[import-untyped]
    except ImportError:
        return "", ""

    ols = result_data["model_type"] == "ols"
    names = [n for n, v in coef_table.items() if v.get("std_beta") is not None and v["coef"] is not None]
    # The interval scales with the coefficient, by sd(x) [/ sd(y)] -- always positive.
    scale = {n: coef_table[n]["std_beta"] / coef_table[n]["coef"] if coef_table[n]["coef"] else 0.0 for n in names}
    effect = [coef_table[n]["std_beta"] for n in names]
    plus = [(coef_table[n]["ci_upper"] - coef_table[n]["coef"]) * scale[n] for n in names]
    minus = [(coef_table[n]["coef"] - coef_table[n]["ci_lower"]) * scale[n] for n in names]
    colors = ["#3fb950" if coef_table[n]["significant"] else "#8b949e" for n in names]
    effect_title = (
        "Standardised effect: SDs of y per SD of x (95% CI)" if ols else "Change in log-odds per SD of x (95% CI)"
    )

    fitted = pd.Series(np.asarray(model.fittedvalues if ols else model.predict(X)), index=y.index)
    shown = y.index if len(y) <= _PAGE_POINTS else y.sample(_PAGE_POINTS, random_state=42).index
    sampled = "" if len(shown) == len(y) else f" ({len(shown):,} of {len(y):,} points, seed 42)"

    if ols:
        residuals = y - fitted
        normality = result_data.get("diagnostics", {}).get("normality_of_residuals", {})
        verdict = {True: "normal", False: "not normal", None: "untested"}[normality.get("normal")]
        titles = (
            effect_title,
            f"Residuals vs fitted{sampled}",
            f"Residuals: {verdict} (Shapiro p={format_p(normality.get('p_value'))})",
            f"Actual vs fitted{sampled}",
        )
        fig = make_subplots(rows=2, cols=2, subplot_titles=titles, horizontal_spacing=0.12, vertical_spacing=0.14)
        fig.add_trace(
            go.Scattergl(
                x=fitted[shown], y=residuals[shown], mode="markers", marker={"size": 4, "opacity": 0.5}, name="residual"
            ),
            row=1,
            col=2,
        )
        fig.add_hline(y=0, line_dash="dash", line_color="#8b949e", row=1, col=2)
        fig.add_trace(go.Histogram(x=residuals, nbinsx=60, name="residuals"), row=2, col=1)
        fig.add_trace(
            go.Scattergl(
                x=fitted[shown], y=y[shown], mode="markers", marker={"size": 4, "opacity": 0.5}, name="actual"
            ),
            row=2,
            col=2,
        )
        low, high = float(min(fitted.min(), y.min())), float(max(fitted.max(), y.max()))
        fig.add_trace(
            go.Scatter(
                x=[low, high], y=[low, high], mode="lines", line={"dash": "dash", "color": "#8b949e"}, name="y = fitted"
            ),
            row=2,
            col=2,
        )
        fig.update_xaxes(title_text="fitted", row=1, col=2)
        fig.update_yaxes(title_text="residual", row=1, col=2)
        fig.update_xaxes(title_text="residual", row=2, col=1)
        fig.update_xaxes(title_text="fitted", row=2, col=2)
        fig.update_yaxes(title_text=f"actual {y_col}", row=2, col=2)
        stats = (
            f"R² {result_data['r_squared']} (adj {result_data['adj_r_squared']}) · RMSE {result_data['rmse']} · "
            f"n {result_data['observations']:,} · F-test p={format_p(result_data['f_pvalue'])}"
        )
        height = 860
    else:
        fig = make_subplots(rows=1, cols=2, subplot_titles=(effect_title, "Predicted probability by actual class"))
        for value in sorted(pd.unique(y)):
            fig.add_trace(
                go.Histogram(x=fitted[y == value], nbinsx=40, opacity=0.6, name=f"{y_col} = {value}"), row=1, col=2
            )
        fig.update_layout(barmode="overlay")
        fig.update_xaxes(title_text="predicted probability", row=1, col=2)
        stats = f"pseudo R² {result_data['pseudo_r_squared']} · n {result_data['observations']:,} · AIC {result_data['aic']}"
        height = 520

    fig.add_trace(
        go.Bar(
            x=effect,
            y=names,
            orientation="h",
            marker_color=colors,
            error_x={"type": "data", "symmetric": False, "array": plus, "arrayminus": minus},
            customdata=[coef_table[n]["coef"] for n in names],
            hovertemplate="%{y}: %{x:.3f} (raw β %{customdata:.4g})<extra></extra>",
            name="effect",
        ),
        row=1,
        col=1,
    )
    fig.add_vline(x=0, line_width=1, line_dash="dash", line_color="#8b949e", row=1, col=1)
    fig.update_layout(
        title=f"{y_col} regressed on {len(coef_table)} predictor(s) — {stats}",
        height=height,
        margin=dict(l=20, r=20, t=110 if result_data.get("pooled_groups") else 80, b=20),
        showlegend=not ols,
    )
    if result_data.get("pooled_groups"):
        fig.add_annotation(
            text="⚠ " + _pooled_note(result_data["pooled_groups"]),
            xref="paper",
            yref="paper",
            x=0,
            y=1.1,
            showarrow=False,
            align="left",
            font={"color": "#d29922", "size": 12},
        )
    stem = discriminated_suffix("regression", y_col)
    return _save_chart(fig, output_path, stem, input_path, open_after, theme, progress)


def _equation(y_col: str, intercept: dict, coef_table: dict) -> str:
    """The fitted model as one line a caller can read straight off."""
    if not intercept and not coef_table:
        return ""
    parts = [f"{intercept.get('coef', 0)}"] if intercept else []
    for name, v in coef_table.items():
        coef = v["coef"]
        if coef is None:
            continue
        sign = "-" if coef < 0 else "+"
        parts.append(f"{sign} {abs(coef)}*{name}" if parts else f"{coef}*{name}")
    return f"{y_col} = " + " ".join(parts)


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
        # to_numeric(errors="coerce") turns a text target into NaN and drops it.
        # A string y_col therefore arrived at the degrees-of-freedom guard below
        # as "0 usable row(s) cannot support 3 coefficient(s)" -- a message about
        # sample size, for a file with 16,834 complete rows whose only problem
        # was that the target is words. The caller is sent to find more rows
        # that already exist.
        rows_in_file = int(len(df))
        dropped_null = rows_in_file - int(len(data))
        before = int(len(data))
        y = pd.to_numeric(data[y_col], errors="coerce").dropna()
        dropped_text = before - int(len(y))
        if before and not len(y):
            sample = [str(v) for v in df[y_col].dropna().unique()[:4]]
            distinct = int(df[y_col].nunique())
            return {
                "success": False,
                "op": "regression_analysis",
                "error": (
                    f"'{y_col}' holds no numbers: all {before} value(s) are non-numeric "
                    f"({distinct} distinct, e.g. {', '.join(sample)})."
                ),
                "hint": (
                    f"Regression needs a numeric target. Encode it first with apply_patch() "
                    f"op=label_encode column={y_col}"
                    + (
                        ", then fit model_type=logistic against the 0/1 column."
                        if distinct == 2
                        else f" -- but {distinct} classes is not a regression target; "
                        "pick a numeric column, or a two-class one for logistic."
                    )
                ),
                "progress": [*progress, fail("Target is not numeric", f"{y_col}: {distinct} distinct values")],
                "token_estimate": 60,
            }
        # Rows leave the fit in two places -- dropna() on the null side and
        # to_numeric() on the text side -- and neither said so. The response
        # carried `observations`, which is what survived, with nothing to
        # compare it against.
        if dropped_null or dropped_text:
            causes = []
            if dropped_null:
                causes.append(f"{dropped_null} with a null in {y_col} or an x_col")
            if dropped_text:
                causes.append(f"{dropped_text} where {y_col} is not a number")
            progress.append(
                warn(
                    f"Fitting {len(y)} of {rows_in_file} row(s)",
                    "; ".join(causes),
                )
            )
        data = data.loc[y.index]
        y = y.loc[data.index]

        if model_type == "logistic":
            classes = sorted(str(v) for v in pd.unique(y))
            if len(classes) != 2:
                return {
                    "success": False,
                    "op": "regression_analysis",
                    "error": (
                        f"Logistic regression needs a two-class target; '{y_col}' has "
                        f"{len(classes)} distinct value(s)"
                        + (f": {', '.join(classes[:6])}" if len(classes) <= 6 else "")
                        + "."
                    ),
                    "hint": (
                        f"Use model_type=ols for a continuous target, or derive a 0/1 column with "
                        f"apply_patch() op=conditional_assign on {y_col}."
                    ),
                    "progress": [*progress, fail("Target is not binary", f"{y_col}: {len(classes)} classes")],
                    "token_estimate": 60,
                }

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

        # A fit needs more observations than it has coefficients to estimate. At
        # or below that count the surface passes exactly through every point:
        # ssr is 0, the residual mean square is 0/0, and r_squared, every
        # p-value and the F statistic all come back NaN. scipy.stats.shapiro on
        # those residuals then raised `'float' object has no attribute 'dtype'`
        # from inside its own NaN-policy wrapper, and that was the message the
        # caller got -- naming neither the sample size nor the model.
        n_params = int(X.shape[1])
        residual_df = int(len(y)) - n_params
        if residual_df <= 0:
            names = ", ".join(str(c) for c in X.columns)
            return {
                "success": False,
                "op": "regression_analysis",
                "error": (
                    f"{len(y)} usable row(s) cannot support {n_params} coefficient(s) ({names}); "
                    f"that leaves {residual_df} residual degrees of freedom."
                ),
                "hint": (
                    f"Give regression_analysis more than {n_params} rows where {y_col} and every x_col are "
                    "non-null, or fit fewer predictors."
                ),
                "progress": [*progress, fail("Not enough rows to fit", f"{len(y)} row(s), {n_params} coefficient(s)")],
                "token_estimate": 40,
            }

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
                "coef": rounded(model.params[param], 6),
                "std_err": rounded(model.bse[param], 6),
                "t_or_z": rounded(model.tvalues[param]),
                "p_value": round_p(float(model.pvalues[param])),
                "ci_lower": rounded(model.conf_int().loc[param, 0], 6),
                "ci_upper": rounded(model.conf_int().loc[param, 1], 6),
                # None, not False: a coefficient whose p-value is missing was
                # not found insignificant, it was not tested.
                "significant": is_significant(model.pvalues[param]),
            }

        # "Strongest" cannot be read off raw coefficients. Each is in the units
        # of its own predictor, so ranking by magnitude just tracks which column
        # happens to have the smallest scale. On the ad dataset it named spends
        # (b=0.0322) over impressions (b=0.0121), while impressions carried
        # nearly twice the standardised effect -- 0.66 against 0.35 -- and a t of
        # 177 against 94. Every number in the response was right and the one
        # sentence quoted out of it was wrong.
        #
        # Scaling by sd(x) puts the predictors in common units. Dividing by sd(y)
        # makes the OLS figure a standardised beta; that divisor is the same for
        # every predictor, so it never changes the ranking. For logistic, where a
        # standardised beta is not defined that way, coef * sd(x) is the change
        # in log-odds per standard deviation and is comparable as it stands.
        y_sd = float(y.std())
        for param, row in coef_table.items():
            coef = row["coef"] or 0.0
            try:
                x_sd = float(X[param].astype(float).std())
            except KeyError, TypeError, ValueError:
                x_sd = float("nan")
            if not np.isfinite(x_sd) or x_sd == 0:
                # A predictor with no spread has no comparable effect size.
                # None, not 0.0: it was not measured as zero, it was not measurable.
                row["std_beta"] = None
                continue
            scaled = coef * x_sd
            row["std_beta"] = rounded(scaled / y_sd, 6) if (model_type == "ols" and y_sd) else rounded(scaled, 6)

        significant_predictors = [p for p, v in coef_table.items() if v["significant"]]

        # The constant is fitted, it is in every prediction the model makes, and
        # the loop above skipped it -- so the response carried "coefficients"
        # from which no prediction could be reproduced. An independent refit of
        # the sweep's own call put the intercept at 3.7095, a number nothing in
        # the response mentioned. It is reported beside the predictors rather
        # than among them, because significant_predictors is a list of
        # predictors and the intercept is not one.
        intercept: dict = {}
        if "const" in model.params.index:
            intercept = {
                "coef": rounded(model.params["const"], 6),
                "std_err": rounded(model.bse["const"], 6),
                "t_or_z": rounded(model.tvalues["const"]),
                "p_value": round_p(float(model.pvalues["const"])),
                "ci_lower": rounded(model.conf_int().loc["const", 0], 6),
                "ci_upper": rounded(model.conf_int().loc["const", 1], 6),
                "significant": is_significant(model.pvalues["const"]),
            }

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
            "residual_df": residual_df,
            "rows_in_file": rows_in_file,
            "rows_dropped_null": dropped_null,
            "rows_dropped_non_numeric": dropped_text,
            "coefficients": coef_table,
            "intercept": intercept,
            "equation": _equation(y_col, intercept, coef_table),
            "significant_predictors": significant_predictors,
            "vif": vif_data,
        }

        if model_type == "ols":
            # Before the diagnostics: a model fitted over groups that each
            # follow their own line is the first thing to know about its R2.
            pooled = _pooled_groups(df, data.index, y, X, float(model.ssr), {y_col, *x_cols})
            if pooled:
                result_data["pooled_groups"] = pooled
                progress.append(warn("Separate groups pooled into one fit", _pooled_note(pooled)))
            residuals = model.resid
            result_data.update(
                {
                    "r_squared": rounded(model.rsquared),
                    "adj_r_squared": rounded(model.rsquared_adj),
                    "rmse": rounded(np.sqrt(model.mse_resid)),
                    "mae": rounded(np.abs(residuals).mean()),
                    "f_statistic": rounded(model.fvalue),
                    "f_pvalue": round_p(float(model.f_pvalue)),
                    "aic": rounded(model.aic, 2),
                    "bic": rounded(model.bic, 2),
                }
            )
            # Diagnostics. `bool(normality_p >= 0.05)` on a NaN is False, so a
            # residual sample too small to test used to be reported as
            # "normal": false -- a diagnostic failure the model never earned.
            normality_p = shapiro_p(residuals.values, _scipy_stats) if _SCIPY_OK else None
            normality: dict = {
                "test": "shapiro_wilk",
                "p_value": round_p(normality_p),
                "normal": None if normality_p is None else bool(normality_p >= 0.05),
            }
            # `shapiro_p` caps at 5,000 with a seeded draw. This response prints
            # `observations: 16834` beside the p-value, which reads as a test on
            # all 16,834 residuals; 11,834 of them were not in it.
            _n_used, _n_total, _sample_note = shapiro_sample(residuals.values)
            if _sample_note and normality_p is not None:
                normality["n_used"] = _n_used
                normality["n_total"] = _n_total
                normality["sample_note"] = _sample_note
            if normality_p is None:
                # Two reasons, two sentences. Printing the pre-drop count for a
                # residual vector emptied by non-finite values produced
                # "needs at least 3 residuals, this fit has 16834" -- a sentence
                # that refutes itself, on a fit whose residuals were all NaN
                # because the target column contained infinities.
                good, bad = finite_split(residuals.values)
                normality["status"] = (
                    f"undetermined: Shapiro-Wilk needs at least {MIN_N_SHAPIRO} finite residuals, this fit has {good}"
                    + (
                        f" ({bad} of {len(residuals)} residuals are not finite, which is what an infinity "
                        "in the input columns does to a fit)"
                        if bad
                        else ""
                    )
                )
            result_data["diagnostics"] = {
                "normality_of_residuals": normality,
                "multicollinearity": vif_data,
            }
        else:
            result_data.update(
                {
                    "pseudo_r_squared": rounded(model.prsquared),
                    "log_likelihood": rounded(model.llf),
                    "aic": rounded(model.aic, 2),
                    "bic": rounded(model.bic, 2),
                }
            )

        # Insight
        untested = [p for p, v in coef_table.items() if v["significant"] is None]
        if significant_predictors:
            comparable = [p for p in significant_predictors if coef_table[p]["std_beta"] is not None]
            if comparable:
                top = max(comparable, key=lambda p: abs(coef_table[p]["std_beta"]))
                basis = f"standardised β={coef_table[top]['std_beta']:.4f}"
            else:
                # Nothing had measurable spread, so nothing is comparable. Rank
                # by the raw coefficient and say that is what was used.
                top = max(significant_predictors, key=lambda p: abs(coef_table[p]["coef"] or 0.0))
                basis = "raw β only — no predictor had measurable spread"
            coef_val = coef_table[top]["coef"] or 0.0
            direction = "positive" if coef_val > 0 else "negative"
            top_p = coef_table[top]["p_value"]
            result_data["insight"] = (
                f"'{top}' is the strongest predictor ({basis}, raw β={coef_val:.4f}, "
                f"{direction} effect, p={format_p(top_p)})."
            )
        elif untested and len(untested) == len(coef_table):
            # "No significant predictors" is a finding. Not having tested any of
            # them is not the same finding.
            result_data["insight"] = (
                f"No predictor could be tested: {len(untested)} coefficient(s) came back without a p-value."
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
            chart_path, chart_name = _regression_page(
                model, X, y, coef_table, result_data, y_col, output_path, path, theme, open_after, progress
            )
            if chart_path:
                result["output_path"] = chart_path
                result["output_name"] = chart_name
                progress.append(ok("Regression page saved", chart_name))
            else:
                progress.append(warn("No chart written", "plotly is unavailable in this environment"))

        result["token_estimate"] = len(str(result)) // 4
        return result

    except Exception as exc:
        logger.exception("regression_analysis error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(
                exc,
                "Check y_col and x_cols are numeric (or categorical for one-hot encoding). Use model_type: ols or logistic.",
            ),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }
