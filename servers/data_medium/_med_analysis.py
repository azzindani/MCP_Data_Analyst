"""Analysis and stats tools for data_medium. No MCP imports."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_HERE = str(Path(__file__).resolve().parent)
for _p in (str(_ROOT), _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import pandas as pd

try:
    import plotly.graph_objects as go

    from shared.html_theme import calc_chart_height, plotly_template

    _PLOTLY_AVAILABLE = True
except ImportError:
    _PLOTLY_AVAILABLE = False

from _med_helpers import (
    _is_string_col,
    _read_csv,
    _save_chart,
    _token_estimate,
)

from shared.column_utils import infer_agg, is_numeric_col
from shared.file_utils import resolve_path
from shared.platform_utils import get_max_rows
from shared.progress import fail, info, ok, warn

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# correlation_analysis
# ---------------------------------------------------------------------------


def correlation_analysis(
    file_path: str,
    method: str = "pearson",
    top_n: int = 10,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
) -> dict:
    progress = []
    try:
        path = resolve_path(file_path)
        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check file_path is absolute and the file exists.",
                "progress": [fail("File not found", path.name)],
                "token_estimate": 20,
            }

        valid_methods = {"pearson", "kendall", "spearman"}
        if method not in valid_methods:
            return {
                "success": False,
                "error": f"Invalid method: {method}",
                "hint": f"Valid methods: {', '.join(sorted(valid_methods))}",
                "progress": [fail("Invalid method", method)],
                "token_estimate": 20,
            }

        df = _read_csv(str(path))
        num_df = df.select_dtypes(include="number")
        if num_df.shape[1] < 2:
            return {
                "success": False,
                "error": "At least 2 numeric columns required.",
                "hint": "Use inspect_dataset() to check column dtypes.",
                "progress": [fail("Not enough numeric columns", path.name)],
                "token_estimate": 20,
            }

        corr = num_df.corr(method=method)
        pairs = []
        cols = corr.columns.tolist()
        for i in range(len(cols)):
            for j in range(i + 1, len(cols)):
                val = corr.iloc[i, j]
                if pd.notna(val):
                    pairs.append(
                        {
                            "col_a": cols[i],
                            "col_b": cols[j],
                            "correlation": round(float(val), 4),
                        }
                    )
        pairs.sort(key=lambda x: abs(x["correlation"]), reverse=True)
        top_pairs = pairs[: max(1, top_n)]

        matrix = {
            col: {c: round(float(v), 4) if pd.notna(v) else None for c, v in row.items()}
            for col, row in corr.to_dict().items()
        }

        progress.append(ok(f"Correlation for {path.name}", f"method={method}, {len(cols)} columns"))

        result: dict = {
            "success": True,
            "op": "correlation_analysis",
            "file_path": str(path),
            "method": method,
            "columns": cols,
            "top_pairs": top_pairs,
            "matrix": matrix,
            "hint": "Call apply_patch() or run_cleaning_pipeline() to act on findings.",
            "progress": progress,
        }

        if _PLOTLY_AVAILABLE:
            z = [[matrix[r][c] if matrix[r][c] is not None else 0.0 for c in cols] for r in cols]
            fig = go.Figure(
                go.Heatmap(
                    z=z,
                    x=cols,
                    y=cols,
                    colorscale="RdBu",
                    zmid=0,
                    text=[[f"{v:.2f}" if v is not None else "" for v in row] for row in z],
                    texttemplate="%{text}",
                )
            )
            fig.update_layout(
                title=f"Correlation Heatmap — {path.name} ({method})",
                template=plotly_template(theme),
                height=calc_chart_height(len(cols), mode="heatmap"),
            )
            abs_p, fname = _save_chart(fig, output_path, "correlation", path, open_after, theme)
            result["output_path"] = abs_p
            result["output_name"] = fname
            progress.append(ok("Chart saved", fname))
        else:
            progress.append(warn("plotly not installed", "pip install plotly to enable HTML export"))

        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("correlation_analysis error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path is absolute and the file is a valid CSV.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# statistical_tests
# ---------------------------------------------------------------------------


def statistical_tests(
    file_path: str,
    test_type: str = "",
    column_a: str = "",
    column_b: str = "",
    group_column: str = "",
) -> dict:
    progress = []
    try:
        from scipy import stats as scipy_stats
    except ImportError:
        return {
            "success": False,
            "error": "scipy not installed",
            "hint": "Install scipy: uv add scipy",
            "progress": [fail("Missing dependency", "scipy")],
            "token_estimate": 20,
        }

    try:
        path = resolve_path(file_path)
        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check file_path is absolute and the file exists.",
                "progress": [fail("File not found", path.name)],
                "token_estimate": 20,
            }

        df = _read_csv(str(path))

        for col in [column_a, column_b, group_column]:
            if col and col not in df.columns:
                return {
                    "success": False,
                    "error": f"Column '{col}' not found",
                    "hint": f"Available: {', '.join(df.columns)}",
                    "progress": [fail("Column not found", col)],
                    "token_estimate": 20,
                }

        if not test_type:
            a_num = column_a and pd.api.types.is_numeric_dtype(df[column_a])
            b_num = column_b and pd.api.types.is_numeric_dtype(df[column_b])
            g_cat = group_column and not pd.api.types.is_numeric_dtype(df[group_column])

            if a_num and b_num:
                test_type = "correlation"
            elif a_num and g_cat:
                n_groups = df[group_column].nunique()
                test_type = "anova" if n_groups > 2 else "ttest"
            elif column_a and column_b and not a_num and not b_num:
                test_type = "chi_square"
            else:
                return {
                    "success": False,
                    "error": "Cannot auto-select test. Specify test_type.",
                    "hint": "Valid: ttest anova chi_square correlation shapiro_wilk ks mann_whitney kruskal wilcoxon levene fisher",
                    "progress": [fail("Auto-select failed", "")],
                    "token_estimate": 20,
                }

        test_result = {}

        if test_type == "ttest":
            groups = df[group_column].dropna().unique() if group_column else []
            if len(groups) == 2:
                g1 = df[df[group_column] == groups[0]][column_a].dropna()
                g2 = df[df[group_column] == groups[1]][column_a].dropna()
            elif column_a and column_b:
                g1 = df[column_a].dropna()
                g2 = df[column_b].dropna()
            else:
                return {
                    "success": False,
                    "error": "t-test requires two numeric columns or one numeric + binary group column.",
                    "hint": "Set column_a + column_b, or column_a + group_column (2 groups).",
                    "progress": [fail("Invalid t-test params", "")],
                    "token_estimate": 20,
                }
            stat, pval = scipy_stats.ttest_ind(g1, g2)
            test_result = {
                "test": "Independent t-test",
                "statistic": round(float(stat), 4),
                "p_value": round(float(pval), 6),
                "significant": float(pval) < 0.05,
                "interpretation": (
                    "Means differ significantly (p<0.05)"
                    if float(pval) < 0.05
                    else "No significant difference (p≥0.05)"
                ),
            }

        elif test_type == "anova":
            if not group_column or not column_a:
                return {
                    "success": False,
                    "error": "ANOVA requires column_a (numeric) and group_column (categorical).",
                    "hint": "Set column_a to numeric column and group_column to category column.",
                    "progress": [fail("Invalid ANOVA params", "")],
                    "token_estimate": 20,
                }
            groups_data = [grp[column_a].dropna().values for _, grp in df.groupby(group_column)]
            stat, pval = scipy_stats.f_oneway(*groups_data)
            test_result = {
                "test": "One-Way ANOVA",
                "statistic": round(float(stat), 4),
                "p_value": round(float(pval), 6),
                "groups": int(df[group_column].nunique()),
                "significant": float(pval) < 0.05,
                "interpretation": (
                    "Group means differ significantly (p<0.05)"
                    if float(pval) < 0.05
                    else "No significant difference between groups (p≥0.05)"
                ),
            }

        elif test_type == "chi_square":
            if not column_a or not column_b:
                return {
                    "success": False,
                    "error": "Chi-square requires column_a and column_b (both categorical).",
                    "hint": "Set column_a and column_b to categorical columns.",
                    "progress": [fail("Invalid chi-square params", "")],
                    "token_estimate": 20,
                }
            ct = pd.crosstab(df[column_a], df[column_b])
            stat, pval, dof, expected = scipy_stats.chi2_contingency(ct)
            test_result = {
                "test": "Chi-Square Test of Independence",
                "statistic": round(float(stat), 4),
                "p_value": round(float(pval), 6),
                "degrees_of_freedom": int(dof),
                "significant": float(pval) < 0.05,
                "interpretation": (
                    "Significant association (p<0.05)" if float(pval) < 0.05 else "No significant association (p≥0.05)"
                ),
            }

        elif test_type == "correlation":
            if not column_a or not column_b:
                return {
                    "success": False,
                    "error": "Correlation test requires column_a and column_b (both numeric).",
                    "hint": "Set column_a and column_b to numeric columns.",
                    "progress": [fail("Invalid correlation params", "")],
                    "token_estimate": 20,
                }
            a = pd.to_numeric(df[column_a], errors="coerce").dropna()
            b = pd.to_numeric(df[column_b], errors="coerce").dropna()
            min_len = min(len(a), len(b))
            stat, pval = scipy_stats.pearsonr(a.iloc[:min_len], b.iloc[:min_len])
            test_result = {
                "test": "Pearson Correlation",
                "statistic": round(float(stat), 4),
                "p_value": round(float(pval), 6),
                "significant": float(pval) < 0.05,
                "interpretation": (
                    f"Correlation r={round(float(stat), 3)}, "
                    f"{'significant' if float(pval) < 0.05 else 'not significant'} "
                    f"(p={'<' if float(pval) < 0.05 else '≥'}0.05)"
                ),
            }

        elif test_type == "shapiro_wilk":
            if not column_a:
                return {
                    "success": False,
                    "error": "shapiro_wilk requires column_a.",
                    "hint": "Set column_a to a numeric column.",
                    "progress": [fail("Missing column_a", "")],
                    "token_estimate": 20,
                }
            series = pd.to_numeric(df[column_a], errors="coerce").dropna()
            stat, pval = scipy_stats.shapiro(series.sample(min(len(series), 5000), random_state=42))
            test_result = {
                "test": "Shapiro-Wilk normality test",
                "statistic": round(float(stat), 4),
                "p_value": round(float(pval), 6),
                "significant": float(pval) < 0.05,
                "interpretation": (
                    f"Data in '{column_a}' is {'NOT ' if float(pval) < 0.05 else ''}normally distributed (p={'<' if float(pval) < 0.05 else '≥'}0.05)"
                ),
            }

        elif test_type == "ks":
            if not column_a:
                return {
                    "success": False,
                    "error": "ks requires column_a.",
                    "hint": "Set column_a to a numeric column.",
                    "progress": [fail("Missing column_a", "")],
                    "token_estimate": 20,
                }
            series = pd.to_numeric(df[column_a], errors="coerce").dropna()
            stat, pval = scipy_stats.kstest(series, "norm", args=(float(series.mean()), float(series.std())))
            test_result = {
                "test": "Kolmogorov-Smirnov normality test",
                "statistic": round(float(stat), 4),
                "p_value": round(float(pval), 6),
                "significant": float(pval) < 0.05,
                "interpretation": (
                    f"Data in '{column_a}' is {'NOT ' if float(pval) < 0.05 else ''}normally distributed (KS test)"
                ),
            }

        elif test_type == "mann_whitney":
            groups = df[group_column].dropna().unique() if group_column else []
            if len(groups) == 2:
                g1 = df[df[group_column] == groups[0]][column_a].dropna()
                g2 = df[df[group_column] == groups[1]][column_a].dropna()
            elif column_a and column_b:
                g1 = pd.to_numeric(df[column_a], errors="coerce").dropna()
                g2 = pd.to_numeric(df[column_b], errors="coerce").dropna()
            else:
                return {
                    "success": False,
                    "error": "mann_whitney requires two groups.",
                    "hint": "Set column_a + column_b, or column_a + group_column (2 groups).",
                    "progress": [fail("Invalid params", "")],
                    "token_estimate": 20,
                }
            stat, pval = scipy_stats.mannwhitneyu(g1, g2, alternative="two-sided")
            n1, n2 = len(g1), len(g2)
            r_biserial = round(1 - 2 * float(stat) / (n1 * n2), 4) if n1 * n2 > 0 else None
            effect_label = (
                "small" if abs(r_biserial or 0) < 0.3 else "medium" if abs(r_biserial or 0) < 0.5 else "large"
            )
            test_result = {
                "test": "Mann-Whitney U test",
                "statistic": round(float(stat), 4),
                "p_value": round(float(pval), 6),
                "significant": float(pval) < 0.05,
                "interpretation": "Groups differ significantly (p<0.05)"
                if float(pval) < 0.05
                else "No significant difference (p≥0.05)",
                "effect_size": {"rank_biserial_r": r_biserial, "interpretation": effect_label},
            }

        elif test_type == "kruskal":
            if not group_column or not column_a:
                return {
                    "success": False,
                    "error": "kruskal requires column_a and group_column.",
                    "hint": "Set column_a (numeric) and group_column (categorical).",
                    "progress": [fail("Invalid params", "")],
                    "token_estimate": 20,
                }
            groups_data = [grp[column_a].dropna().values for _, grp in df.groupby(group_column)]
            stat, pval = scipy_stats.kruskal(*groups_data)
            n_total = sum(len(g) for g in groups_data)
            eta_sq = (
                round((float(stat) - len(groups_data) + 1) / (n_total - len(groups_data)), 4)
                if n_total > len(groups_data)
                else None
            )
            test_result = {
                "test": "Kruskal-Wallis test",
                "statistic": round(float(stat), 4),
                "p_value": round(float(pval), 6),
                "groups": int(df[group_column].nunique()),
                "significant": float(pval) < 0.05,
                "interpretation": "Group distributions differ significantly (p<0.05)"
                if float(pval) < 0.05
                else "No significant group differences (p≥0.05)",
                "effect_size": {"epsilon_squared": eta_sq},
            }

        elif test_type == "wilcoxon":
            if column_a and column_b:
                a = pd.to_numeric(df[column_a], errors="coerce").dropna()
                b = pd.to_numeric(df[column_b], errors="coerce").dropna()
                min_len = min(len(a), len(b))
                stat, pval = scipy_stats.wilcoxon(a.iloc[:min_len], b.iloc[:min_len])
            else:
                return {
                    "success": False,
                    "error": "wilcoxon requires column_a and column_b (paired).",
                    "hint": "Set both column_a and column_b to numeric columns.",
                    "progress": [fail("Invalid params", "")],
                    "token_estimate": 20,
                }
            test_result = {
                "test": "Wilcoxon signed-rank test",
                "statistic": round(float(stat), 4),
                "p_value": round(float(pval), 6),
                "significant": float(pval) < 0.05,
                "interpretation": "Paired differences are significant (p<0.05)"
                if float(pval) < 0.05
                else "No significant paired difference (p≥0.05)",
            }

        elif test_type == "levene":
            if not group_column or not column_a:
                return {
                    "success": False,
                    "error": "levene requires column_a and group_column.",
                    "hint": "Set column_a (numeric) and group_column (categorical).",
                    "progress": [fail("Invalid params", "")],
                    "token_estimate": 20,
                }
            groups_data = [grp[column_a].dropna().values for _, grp in df.groupby(group_column)]
            stat, pval = scipy_stats.levene(*groups_data)
            test_result = {
                "test": "Levene's test for equal variances",
                "statistic": round(float(stat), 4),
                "p_value": round(float(pval), 6),
                "significant": float(pval) < 0.05,
                "interpretation": "Variances are NOT equal (p<0.05)"
                if float(pval) < 0.05
                else "Variances are equal (p≥0.05)",
            }

        elif test_type == "fisher":
            if not column_a or not column_b:
                return {
                    "success": False,
                    "error": "fisher requires column_a and column_b (2×2 table).",
                    "hint": "Set both columns to binary/categorical.",
                    "progress": [fail("Invalid params", "")],
                    "token_estimate": 20,
                }
            ct = pd.crosstab(df[column_a], df[column_b])
            if ct.shape != (2, 2):
                return {
                    "success": False,
                    "error": f"Fisher's test requires a 2×2 table; got {ct.shape}.",
                    "hint": "Use chi_square for larger tables.",
                    "progress": [fail("Not 2x2", str(ct.shape))],
                    "token_estimate": 20,
                }
            stat, pval = scipy_stats.fisher_exact(ct.values)
            test_result = {
                "test": "Fisher's exact test",
                "odds_ratio": round(float(stat), 4),
                "p_value": round(float(pval), 6),
                "significant": float(pval) < 0.05,
                "interpretation": "Significant association (p<0.05)"
                if float(pval) < 0.05
                else "No significant association (p≥0.05)",
            }

        else:
            return {
                "success": False,
                "error": f"Unknown test_type: {test_type}",
                "hint": "Valid: ttest anova chi_square correlation shapiro_wilk ks mann_whitney kruskal wilcoxon levene fisher",
                "progress": [fail("Invalid test type", test_type)],
                "token_estimate": 20,
            }

        progress.append(ok(f"Statistical test on {path.name}", test_result.get("test", test_type)))

        result = {
            "success": True,
            "op": "statistical_tests",
            "file_path": str(path),
            "test_type": test_type,
            **test_result,
            "hint": "Call apply_patch() or run_cleaning_pipeline() to act on findings.",
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("statistical_tests error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check column names and ensure numeric/categorical types are correct.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# time_series_analysis  (enhanced with exponential smoothing forecast)
# ---------------------------------------------------------------------------


def time_series_analysis(
    file_path: str,
    date_column: str = "",
    value_columns: list[str] = None,
    period: str = "M",
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
) -> dict:
    progress = []
    try:
        path = resolve_path(file_path)
        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check file_path is absolute and the file exists.",
                "progress": [fail("File not found", path.name)],
                "token_estimate": 20,
            }

        valid_periods = {"Y", "Q", "M", "W", "D"}
        if period not in valid_periods:
            return {
                "success": False,
                "error": f"Invalid period: {period}",
                "hint": f"Valid: {', '.join(sorted(valid_periods))}",
                "progress": [fail("Invalid period", period)],
                "token_estimate": 20,
            }

        _period_map = {"M": "ME", "Q": "QE", "Y": "YE"}
        resample_period = _period_map.get(period, period)

        df = _read_csv(str(path))

        if not date_column:
            date_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
            if not date_cols:
                for col in df.columns:
                    if _is_string_col(df[col]):
                        try:
                            pd.to_datetime(df[col].dropna().head(10), errors="raise")
                            date_column = col
                            break
                        except Exception:
                            pass
            else:
                date_column = date_cols[0]

        if not date_column or date_column not in df.columns:
            return {
                "success": False,
                "error": "No date column found or specified.",
                "hint": "Set date_column to a datetime column, or cast it first with apply_patch.",
                "progress": [fail("No date column", "")],
                "token_estimate": 20,
            }

        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
        df = df.dropna(subset=[date_column])

        if not value_columns:
            value_columns = [c for c in df.columns if is_numeric_col(df[c])][:5]

        missing_vals = [c for c in value_columns if c not in df.columns]
        if missing_vals:
            return {
                "success": False,
                "error": f"Value columns not found: {missing_vals}",
                "hint": f"Available: {', '.join(df.columns)}",
                "progress": [fail("Column not found", str(missing_vals))],
                "token_estimate": 20,
            }

        df = df.set_index(date_column).sort_index()
        col_agg_map = {c: infer_agg(c, df[c]) for c in value_columns}
        resampled_parts = []
        for _vc in value_columns:
            _agg_fn = col_agg_map.get(_vc, "sum")
            _rs = df[[_vc]].resample(resample_period)
            if _agg_fn == "mean":
                resampled_parts.append(_rs.mean())
            elif _agg_fn == "max":
                resampled_parts.append(_rs.max())
            elif _agg_fn == "min":
                resampled_parts.append(_rs.min())
            else:
                resampled_parts.append(_rs.sum())
        resampled = (
            pd.concat(resampled_parts, axis=1) if resampled_parts else df[value_columns].resample(resample_period).sum()
        )

        rolling_7 = resampled.rolling(window=7, min_periods=1).mean()  # noqa: F841
        rolling_30 = resampled.rolling(window=30, min_periods=1).mean()  # noqa: F841

        max_r = get_max_rows()
        truncated = len(resampled) > max_r
        resampled_trunc = resampled.tail(max_r)

        trend_data = {}
        try:
            from scipy.stats import linregress as _linregress

            for col in value_columns:
                ts = resampled[col].dropna()
                if len(ts) >= 2:
                    slope, _, r_val, _, _ = _linregress(range(len(ts)), ts.values)
                    trend_data[col] = {
                        "slope": round(float(slope), 4),
                        "r_squared": round(float(r_val**2), 4),
                        "direction": "up" if slope > 0 else "down" if slope < 0 else "flat",
                    }
        except ImportError:
            pass

        # STL decomposition, ACF/PACF, ADF stationarity test
        stl_results: dict = {}
        acf_results: dict = {}
        adf_results: dict = {}
        try:
            from statsmodels.tsa.seasonal import STL  # type: ignore[import-untyped]
            from statsmodels.tsa.stattools import acf, adfuller, pacf  # type: ignore[import-untyped]

            for col in value_columns:
                ts = resampled[col].dropna()
                if len(ts) < 4:
                    continue

                # ADF stationarity test
                try:
                    adf_out = adfuller(ts.values, autolag="AIC")
                    adf_results[col] = {
                        "test_statistic": round(float(adf_out[0]), 4),
                        "p_value": round(float(adf_out[1]), 4),
                        "is_stationary": bool(adf_out[1] < 0.05),
                        "critical_values": {k: round(float(v), 4) for k, v in adf_out[4].items()},  # type: ignore[index]
                    }
                except Exception:
                    pass

                # ACF/PACF — up to 12 lags, at most n//2
                n_lags = min(12, len(ts) // 2)
                if n_lags >= 2:
                    try:
                        acf_vals = acf(ts.values, nlags=n_lags, fft=True)
                        pacf_vals = pacf(ts.values, nlags=n_lags)
                        acf_results[col] = {
                            "acf": [round(float(v), 4) for v in acf_vals[1:]],
                            "pacf": [round(float(v), 4) for v in pacf_vals[1:]],
                            "lags": list(range(1, n_lags + 1)),
                        }
                    except Exception:
                        pass

                # STL decomposition — needs ≥ 2 seasonal periods
                try:
                    seasonal_period = {"M": 12, "Q": 4, "W": 52, "D": 7, "Y": 1}.get(period, 12)
                    if len(ts) >= max(4, 2 * seasonal_period) and seasonal_period > 1:
                        stl_fit = STL(ts, period=seasonal_period, robust=True).fit()
                        resid_var = float(stl_fit.resid.var())
                        seasonal_var = float((stl_fit.seasonal + stl_fit.resid).var())
                        trend_var = float((stl_fit.trend + stl_fit.resid).var())
                        stl_results[col] = {
                            "trend": [round(float(v), 4) for v in stl_fit.trend.tolist()[-12:]],
                            "seasonal": [round(float(v), 4) for v in stl_fit.seasonal.tolist()[-12:]],
                            "residual": [round(float(v), 4) for v in stl_fit.resid.tolist()[-12:]],
                            "seasonal_strength": round(
                                float(max(0.0, 1 - resid_var / seasonal_var)) if seasonal_var else 0.0, 4
                            ),
                            "trend_strength": round(
                                float(max(0.0, 1 - resid_var / trend_var)) if trend_var else 0.0, 4
                            ),
                        }
                except Exception:
                    pass

        except ImportError:
            progress.append(info("statsmodels not installed", "pip install statsmodels for STL/ACF/ADF"))

        # Exponential smoothing forecast (pure pandas, no statsmodels)
        alpha = 0.3
        forecast_periods = 3
        forecast_values_map: dict = {}
        forecast_dates_map: dict = {}

        for col in value_columns:
            ts = resampled[col].dropna()
            if len(ts) < 1:
                continue
            # Compute smoothed series
            smoothed = float(ts.iloc[0])
            for y in ts.iloc[1:]:
                smoothed = alpha * float(y) + (1 - alpha) * smoothed
            # Generate next 3 periods' date index
            try:
                last_idx = ts.index[-1]
                future_idx = pd.date_range(start=last_idx, periods=forecast_periods + 1, freq=resample_period)[1:]
                # Forecast: all 3 periods equal to last smoothed value
                fcast = [round(smoothed, 4)] * forecast_periods
                forecast_values_map[col] = fcast
                forecast_dates_map[col] = [str(d) for d in future_idx]
            except Exception:
                pass

        records = resampled_trunc.reset_index().fillna("").to_dict(orient="records")
        for rec in records:
            for k, v in list(rec.items()):
                if hasattr(v, "isoformat"):
                    rec[k] = v.isoformat()

        if truncated:
            progress.append(warn("Results truncated", f"Showing last {max_r} periods"))

        progress.append(
            ok(
                f"Time series analysis for {path.name}",
                f"{len(resampled)} periods ({period})",
            )
        )

        result: dict = {
            "success": True,
            "op": "time_series_analysis",
            "file_path": str(path),
            "date_column": date_column,
            "value_columns": value_columns,
            "period": period,
            "total_periods": len(resampled),
            "date_range": {
                "start": str(df.index.min()),
                "end": str(df.index.max()),
            },
            "trend": trend_data,
            "stl": stl_results,
            "acf": acf_results,
            "adf": adf_results,
            "data": records,
            "truncated": truncated,
            "hint": "Use a more targeted call with specific value_columns or a narrower date range.",
            "forecast_periods": forecast_periods,
            "forecast_values": forecast_values_map,
            "forecast_dates": forecast_dates_map,
            "progress": progress,
        }

        if _PLOTLY_AVAILABLE:
            fig = go.Figure()
            x_vals = [str(i) for i in resampled.index]
            for col in value_columns:
                fig.add_trace(
                    go.Scatter(
                        x=x_vals,
                        y=resampled[col].tolist(),
                        name=col,
                        mode="lines+markers",
                    )
                )
            fig.update_layout(
                title=f"Time Series — {path.name} (period={period})",
                xaxis_title=date_column,
                template=plotly_template(theme),
                height=calc_chart_height(len(value_columns), mode="subplot"),
            )
            abs_p, fname = _save_chart(fig, output_path, "time_series", path, open_after, theme)
            result["output_path"] = abs_p
            result["output_name"] = fname
            progress.append(ok("Chart saved", fname))
        else:
            progress.append(warn("plotly not installed", "pip install plotly to enable HTML export"))

        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("time_series_analysis error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check date_column is a datetime column and value_columns are numeric.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# cohort_analysis
# ---------------------------------------------------------------------------


def cohort_analysis(
    file_path: str,
    cohort_column: str = "",
    date_column: str = "",
    value_column: str = "",
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
) -> dict:
    progress = []
    try:
        path = resolve_path(file_path)
        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check file_path is absolute and the file exists.",
                "progress": [fail("File not found", path.name)],
                "token_estimate": 20,
            }

        df = _read_csv(str(path))

        if not date_column:
            date_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
            if not date_cols:
                for col in df.columns:
                    if _is_string_col(df[col]):
                        try:
                            pd.to_datetime(df[col].dropna().head(10), errors="raise")
                            date_column = col
                            break
                        except Exception:
                            pass
            else:
                date_column = date_cols[0]

        if not date_column or date_column not in df.columns:
            return {
                "success": False,
                "error": "No date column found or specified.",
                "hint": "Set date_column to a datetime column.",
                "progress": [fail("No date column", "")],
                "token_estimate": 20,
            }

        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
        df = df.dropna(subset=[date_column])

        if not cohort_column:
            cat_cols = [c for c in df.columns if _is_string_col(df[c]) and df[c].nunique() < 50]
            if cat_cols:
                cohort_column = cat_cols[0]
                progress.append(info("Auto-detected cohort column", cohort_column))

        if not cohort_column or cohort_column not in df.columns:
            df["_cohort"] = df[date_column].dt.to_period("M").astype(str)
            cohort_column = "_cohort"
            progress.append(info("Using date-based cohort", "year-month"))

        if not value_column:
            num_cols = [c for c in df.columns if is_numeric_col(df[c])]
            if num_cols:
                value_column = num_cols[0]

        df["_period"] = df[date_column].dt.to_period("M").astype(str)

        if value_column and value_column in df.columns:
            pivot = df.pivot_table(
                index=cohort_column,
                columns="_period",
                values=value_column,
                aggfunc="sum",
                fill_value=0,
            )
        else:
            pivot = df.pivot_table(
                index=cohort_column,
                columns="_period",
                values=date_column,
                aggfunc="count",
                fill_value=0,
            )

        max_r = get_max_rows()
        truncated = len(pivot) > max_r
        pivot_trunc = pivot.head(max_r)

        matrix = {
            str(idx): {str(col): int(v) if hasattr(v, "item") else v for col, v in row.items()}
            for idx, row in pivot_trunc.to_dict(orient="index").items()
        }

        if truncated:
            progress.append(warn("Results truncated", f"Showing first {max_r} cohorts"))

        progress.append(
            ok(
                f"Cohort analysis for {path.name}",
                f"{len(pivot)} cohorts × {len(pivot.columns)} periods",
            )
        )

        result: dict = {
            "success": True,
            "op": "cohort_analysis",
            "file_path": str(path),
            "cohort_column": cohort_column,
            "date_column": date_column,
            "value_column": value_column or "count",
            "cohorts": len(pivot),
            "periods": len(pivot.columns),
            "matrix": matrix,
            "truncated": truncated,
            "hint": "Use a more targeted call with a specific cohort_column or value_column.",
            "progress": progress,
        }

        if _PLOTLY_AVAILABLE:
            row_keys = list(pivot_trunc.index.astype(str))
            col_keys = list(pivot_trunc.columns.astype(str))
            z = pivot_trunc.values.tolist()
            fig = go.Figure(
                go.Heatmap(
                    z=z,
                    x=col_keys,
                    y=row_keys,
                    colorscale="Blues",
                    text=[[str(v) for v in row] for row in z],
                    texttemplate="%{text}",
                )
            )
            fig.update_layout(
                title=f"Cohort Analysis — {path.name}",
                xaxis_title="Period",
                yaxis_title=cohort_column,
                template=plotly_template(theme),
                height=calc_chart_height(len(row_keys), mode="heatmap"),
            )
            abs_p, fname = _save_chart(fig, output_path, "cohort", path, open_after, theme)
            result["output_path"] = abs_p
            result["output_name"] = fname
            progress.append(ok("Chart saved", fname))
        else:
            progress.append(warn("plotly not installed", "pip install plotly to enable HTML export"))

        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("cohort_analysis error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check date_column is a datetime column and cohort_column exists.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# detect_anomalies (new)
# ---------------------------------------------------------------------------


def detect_anomalies(
    file_path: str,
    columns: list[str] = None,
    method: str = "both",
    output_path: str = "",
    threshold: float = 3.0,
) -> dict:
    """Flag anomalous rows using IQR and/or z-score. Saves flagged CSV."""
    progress = []
    try:
        path = resolve_path(file_path)
        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check file_path is absolute and the file exists.",
                "progress": [fail("File not found", path.name)],
                "token_estimate": 20,
            }

        valid_methods = {"iqr", "zscore", "both"}
        if method not in valid_methods:
            return {
                "success": False,
                "error": f"Invalid method: {method}",
                "hint": f"Valid: {', '.join(sorted(valid_methods))}",
                "progress": [fail("Invalid method", method)],
                "token_estimate": 20,
            }

        df = _read_csv(str(path))
        numeric_cols = [c for c in df.columns if is_numeric_col(df[c])]

        if columns is not None:
            missing = [c for c in columns if c not in df.columns]
            if missing:
                return {
                    "success": False,
                    "error": f"Columns not found: {missing}",
                    "hint": f"Available: {', '.join(df.columns)}",
                    "progress": [fail("Column not found", str(missing))],
                    "token_estimate": 30,
                }
            numeric_cols = [c for c in columns if c in numeric_cols]

        result_df = df.copy()
        per_column_summary = {}

        for col in numeric_cols:
            clean = df[col].dropna()
            col_summary: dict = {"column": col}

            if method in ("iqr", "both"):
                q1 = clean.quantile(0.25)
                q3 = clean.quantile(0.75)
                iqr = q3 - q1
                lower = q1 - 1.5 * iqr
                upper = q3 + 1.5 * iqr
                iqr_flag = (df[col] < lower) | (df[col] > upper)
                result_df[f"{col}_iqr_flag"] = iqr_flag.fillna(False)
                col_summary["iqr_outliers"] = int(iqr_flag.sum())
                col_summary["iqr_lower"] = round(float(lower), 4)
                col_summary["iqr_upper"] = round(float(upper), 4)

            if method in ("zscore", "both"):
                mean_v = clean.mean()
                std_v = clean.std() if len(clean) > 1 else 0
                if std_v > 0:
                    zscores = (df[col] - mean_v) / std_v
                    zscore_flag = zscores.abs() > threshold
                else:
                    zscore_flag = pd.Series([False] * len(df), index=df.index)
                result_df[f"{col}_zscore_flag"] = zscore_flag.fillna(False)
                col_summary["zscore_outliers"] = int(zscore_flag.sum())
                col_summary["zscore_threshold"] = threshold

            per_column_summary[col] = col_summary

        flag_cols = [c for c in result_df.columns if c.endswith("_iqr_flag") or c.endswith("_zscore_flag")]
        if flag_cols:
            result_df["_anomaly_score"] = result_df[flag_cols].sum(axis=1)
        else:
            result_df["_anomaly_score"] = 0

        anomaly_count = int((result_df["_anomaly_score"] > 0).sum())

        out = str(resolve_path(output_path)) if output_path else str(path.parent / f"{path.stem}_anomalies.csv")
        result_df.to_csv(out, index=False)

        progress.append(
            ok(
                f"Anomaly detection on {path.name}",
                f"{anomaly_count}/{len(df)} anomalous rows, saved to {Path(out).name}",
            )
        )

        result = {
            "success": True,
            "op": "detect_anomalies",
            "file_path": str(path),
            "method": method,
            "total_rows": len(df),
            "anomaly_count": anomaly_count,
            "columns_scanned": len(numeric_cols),
            "per_column": per_column_summary,
            "output_path": out,
            "output_name": Path(out).name,
            "hint": "Call apply_patch() or run_cleaning_pipeline() to act on findings.",
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("detect_anomalies error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path is absolute and columns are numeric.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }
