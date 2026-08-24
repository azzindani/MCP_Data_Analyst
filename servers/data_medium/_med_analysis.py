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
    import plotly.colors as px_colors
    import plotly.graph_objects as go

    from shared.html_theme import calc_chart_height, plotly_template

    _PLOTLY_AVAILABLE = True
except ImportError:
    _PLOTLY_AVAILABLE = False

# Pre-import heavy libraries at module load so Windows Defender scans .pyc
# files once (at server start) rather than blocking the first tool call.
try:
    from scipy import stats as _scipy_stats
    from scipy.stats import linregress as _linregress

    _SCIPY_OK = True
except ImportError:
    _scipy_stats = None  # type: ignore
    _linregress = None  # type: ignore
    _SCIPY_OK = False

try:
    from statsmodels.tsa.seasonal import STL  # type: ignore[import-untyped]
    from statsmodels.tsa.stattools import acf, adfuller, pacf  # type: ignore[import-untyped]

    _STATSMODELS_OK = True
except ImportError:
    STL = acf = adfuller = pacf = None  # type: ignore
    _STATSMODELS_OK = False

from _med_helpers import (
    _is_string_col,
    _read_csv,
    _save_chart,
    _token_estimate,
)

from shared.column_utils import infer_agg, is_numeric_col, paired_numeric
from shared.file_utils import hint_for_error, resolve_path
from shared.platform_utils import get_max_rows
from shared.progress import fail, info, ok, warn
from shared.small_sample import (
    MIN_N_IQR,
    MIN_N_SHAPIRO,
    is_significant,
    min_n_for_zscore,
    need_n,
    rounded,
    settle_verdict,
    shapiro_p,
)
from shared.stats_format import format_p, round_p

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
    theme: str = "device",
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
            abs_p, fname = _save_chart(fig, output_path, "correlation", path, open_after, theme, progress)
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
            "hint": hint_for_error(exc, "Check file_path is absolute and the file is a valid CSV."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# statistical_tests
# ---------------------------------------------------------------------------


# statistical_test on the statistics server names these two differently, and
# they are the same test: its `t_test` is the independent-samples one, and
# `correlation` here is Pearson and nothing else.
#
# Only exact synonyms belong here. `paired_t_test`, `one_sample_t`, `spearman`
# and `kendall` all look mappable and are not -- this tool cannot run them, so
# accepting them would answer a different question than the caller asked under
# success: true. They stay refused, with the valid list in the hint.
_SIBLING_TEST_NAMES = {
    "t_test": "ttest",
    "pearson": "correlation",
}


def statistical_tests(
    file_path: str,
    test_type: str = "",
    column_a: str = "",
    column_b: str = "",
    group_column: str = "",
    test: str = "",
) -> dict:
    progress = []
    # statistical_test on the statistics server spells this same choice `test`.
    # Blank is legitimate here -- this tool auto-selects a test -- so only the
    # alias having supplied the value is worth logging.
    if not test_type and test:
        test_type = test
        progress.append(info("Argument alias", "Read test_type from an accepted alternative spelling"))
    # ... and spells the same tests t_test, pearson, spearman and kendall.
    test_type = _SIBLING_TEST_NAMES.get(test_type, test_type)
    if not _SCIPY_OK:
        return {
            "success": False,
            "error": "scipy not installed",
            "hint": "Install scipy: uv add scipy",
            "progress": [fail("Missing dependency", "scipy")],
            "token_estimate": 20,
        }
    scipy_stats = _scipy_stats

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

        if not test_type and not column_a and not column_b and not group_column:
            # Auto-scan: normality + top correlations for all numeric cols
            num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])][:8]
            normality: dict = {}
            for col in num_cols:
                s = pd.to_numeric(df[col], errors="coerce").dropna()
                # `normal` is None, not False, when Shapiro-Wilk could not run:
                # a column too short to test is not a column that failed it.
                _pv = shapiro_p(s.to_numpy(), scipy_stats)
                normality[col] = {
                    "n": int(len(s)),
                    "p_value": round_p(_pv) if _pv is not None else None,
                    "normal": None if _pv is None else bool(_pv >= 0.05),
                }
            correlations: list = []
            for i, ca in enumerate(num_cols):
                for cb in num_cols[i + 1 :]:
                    try:
                        _a, _b = paired_numeric(df, ca, cb)
                        # Two points always lie on a line, so pearsonr returns
                        # r=+-1 for them however unrelated the columns are. That
                        # is a property of the count, not of the data, and it
                        # sorts to the top of a list captioned "top
                        # correlations".
                        if len(_a) < 3:
                            continue
                        _r, _p = scipy_stats.pearsonr(_a, _b)
                        correlations.append(
                            {
                                "col_a": ca,
                                "col_b": cb,
                                "r": rounded(_r, 3),
                                "p_value": round_p(float(_p)),
                                "significant": is_significant(_p),
                                "n": int(len(_a)),
                            }
                        )
                    except Exception:
                        pass
            # A constant column gives r=NaN, which `rounded` reports as None.
            correlations.sort(key=lambda x: abs(x["r"] or 0.0), reverse=True)
            progress.append(ok(f"Auto-scan on {path.name}", f"{len(num_cols)} numeric cols"))
            result = {
                "success": True,
                "op": "statistical_tests",
                "file_path": str(path),
                "test_type": "auto_scan",
                "normality": normality,
                "top_correlations": correlations[:10],
                "hint": "Call with test_type + column_a/column_b for a specific test.",
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

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
            if err := need_n(
                "statistical_tests",
                "Independent t-test",
                {"group 1": len(g1), "group 2": len(g2)},
                2,
                hint="A t-test compares variances, so each group needs 2+ values. Use describe() to see the counts.",
            ):
                return err
            stat, pval = scipy_stats.ttest_ind(g1, g2)
            test_result = {
                "test": "Independent t-test",
                "statistic": rounded(stat),
                "p_value": round_p(float(pval)),
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
            if err := need_n(
                "statistical_tests",
                "One-Way ANOVA",
                {f"groups in '{group_column}'": len(groups_data)},
                2,
                demand="at least 2 groups to compare",
                hint="Use value_counts() on the group column to see how many distinct groups the data has.",
            ):
                return err
            # ANOVA does not need every group to have variance -- a singleton
            # group is legitimate. What it needs is residual degrees of freedom:
            # total values minus number of groups, the denominator of the
            # within-group mean square. At zero, F is a division by zero.
            if err := need_n(
                "statistical_tests",
                "One-Way ANOVA",
                {
                    "residual degrees of freedom (values minus groups)": sum(len(g) for g in groups_data)
                    - len(groups_data)
                },
                1,
                demand="at least 1 residual degree of freedom",
                hint="Every group holding exactly one value leaves nothing to compare within groups.",
            ):
                return err
            stat, pval = scipy_stats.f_oneway(*groups_data)
            test_result = {
                "test": "One-Way ANOVA",
                "statistic": rounded(stat),
                "p_value": round_p(float(pval)),
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
            if err := need_n(
                "statistical_tests",
                "Chi-Square Test of Independence",
                {f"distinct values in '{column_a}'": ct.shape[0], f"distinct values in '{column_b}'": ct.shape[1]},
                2,
                demand="a contingency table of at least 2x2",
                hint="Use value_counts() on both columns to see how many categories each one has.",
            ):
                return err
            stat, pval, dof, expected = scipy_stats.chi2_contingency(ct)
            test_result = {
                "test": "Chi-Square Test of Independence",
                "statistic": rounded(stat),
                "p_value": round_p(float(pval)),
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
            # Pairwise deletion: row i of one column has to be compared with
            # row i of the other. Dropping each column's nulls separately and
            # cutting both to the shorter length offsets every pair after the
            # first null, which turned r=0.9256 into r=0.0015 on the reference
            # dataset -- see paired_numeric.
            a, b = paired_numeric(df, column_a, column_b)
            if err := need_n(
                "statistical_tests",
                "Pearson Correlation",
                {"complete pairs": len(a)},
                3,
                hint="Any two points lie on a line, so r is +-1 by construction below 3 pairs.",
            ):
                return err
            stat, pval = scipy_stats.pearsonr(a, b)
            test_result = {
                "test": "Pearson Correlation",
                "statistic": rounded(stat),
                "p_value": round_p(float(pval)),
                "n": int(len(a)),
                "rows_dropped": int(len(df) - len(a)),
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
            if err := need_n(
                "statistical_tests",
                "Shapiro-Wilk normality test",
                {f"non-null values in '{column_a}'": len(series)},
                MIN_N_SHAPIRO,
                hint="Shapiro-Wilk is undefined below 3 values. Use describe() to see how many the column has.",
            ):
                return err
            stat, pval = scipy_stats.shapiro(series.sample(min(len(series), 5000), random_state=42))
            test_result = {
                "test": "Shapiro-Wilk normality test",
                "statistic": rounded(stat),
                "p_value": round_p(float(pval)),
                "significant": is_significant(pval),
                "interpretation": (
                    f"Data in '{column_a}' is {'NOT ' if is_significant(pval) else ''}normally distributed "
                    f"(p={'<' if is_significant(pval) else '≥'}0.05)"
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
            # The reference distribution is fitted from the sample itself, so a
            # sample with no spread gives it a zero standard deviation and every
            # comparison against it is NaN.
            if err := need_n(
                "statistical_tests",
                "Kolmogorov-Smirnov normality test",
                {f"non-null values in '{column_a}'": len(series)},
                3,
                hint="KS compares the sample against a normal fitted to its own mean and sd; both need 3+ values.",
            ):
                return err
            stat, pval = scipy_stats.kstest(series, "norm", args=(float(series.mean()), float(series.std())))
            test_result = {
                "test": "Kolmogorov-Smirnov normality test",
                "statistic": rounded(stat),
                "p_value": round_p(float(pval)),
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
            if err := need_n(
                "statistical_tests",
                "Mann-Whitney U test",
                {"group 1": len(g1), "group 2": len(g2)},
                2,
                hint="One value per group cannot rank against another; give each group 2+ values.",
            ):
                return err
            stat, pval = scipy_stats.mannwhitneyu(g1, g2, alternative="two-sided")
            n1, n2 = len(g1), len(g2)
            r_biserial = round(1 - 2 * float(stat) / (n1 * n2), 4) if n1 * n2 > 0 else None
            effect_label = (
                "small" if abs(r_biserial or 0) < 0.3 else "medium" if abs(r_biserial or 0) < 0.5 else "large"
            )
            test_result = {
                "test": "Mann-Whitney U test",
                "statistic": rounded(stat),
                "p_value": round_p(float(pval)),
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
            if err := need_n(
                "statistical_tests",
                "Kruskal-Wallis test",
                {f"groups in '{group_column}'": len(groups_data)},
                2,
                demand="at least 2 groups to compare",
                hint="Use value_counts() on the group column to see how many distinct groups the data has.",
            ):
                return err
            stat, pval = scipy_stats.kruskal(*groups_data)
            n_total = sum(len(g) for g in groups_data)
            eta_sq = (
                round((float(stat) - len(groups_data) + 1) / (n_total - len(groups_data)), 4)
                if n_total > len(groups_data)
                else None
            )
            test_result = {
                "test": "Kruskal-Wallis test",
                "statistic": rounded(stat),
                "p_value": round_p(float(pval)),
                "groups": int(df[group_column].nunique()),
                "significant": float(pval) < 0.05,
                "interpretation": "Group distributions differ significantly (p<0.05)"
                if float(pval) < 0.05
                else "No significant group differences (p≥0.05)",
                "effect_size": {"epsilon_squared": eta_sq},
            }

        elif test_type == "wilcoxon":
            if column_a and column_b:
                # Paired, so the pairs have to be real rows -- see paired_numeric.
                a, b = paired_numeric(df, column_a, column_b)
                if err := need_n(
                    "statistical_tests",
                    "Wilcoxon signed-rank test",
                    {"complete pairs": len(a)},
                    2,
                    hint="A signed-rank test ranks the paired differences, so it needs 2+ pairs.",
                ):
                    return err
                stat, pval = scipy_stats.wilcoxon(a, b)
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
                "statistic": rounded(stat),
                "p_value": round_p(float(pval)),
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
            if err := need_n(
                "statistical_tests",
                "Levene's test for equal variances",
                {f"groups in '{group_column}'": len(groups_data)},
                2,
                demand="at least 2 groups whose variances it can compare",
                hint="Use value_counts() on the group column to see how many distinct groups the data has.",
            ):
                return err
            if err := need_n(
                "statistical_tests",
                "Levene's test for equal variances",
                {
                    "residual degrees of freedom (values minus groups)": sum(len(g) for g in groups_data)
                    - len(groups_data)
                },
                1,
                demand="at least 1 residual degree of freedom",
                hint="Every group holding exactly one value leaves no spread to compare.",
            ):
                return err
            stat, pval = scipy_stats.levene(*groups_data)
            test_result = {
                "test": "Levene's test for equal variances",
                "statistic": rounded(stat),
                "p_value": round_p(float(pval)),
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
                "odds_ratio": rounded(stat),
                "p_value": round_p(float(pval)),
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

        # The per-test guards above refuse the sample sizes that are known to
        # produce no p-value. This catches whatever gets past them -- a
        # degenerate spread, a scipy edge case, a test added later -- because a
        # missing p-value must never be reported as "not significant".
        test_result = settle_verdict(test_result, f"{len(df)} row(s) in {path.name}")
        if test_result.get("undetermined"):
            progress.append(warn("Verdict withheld", test_result["interpretation"]))
        progress.append(ok(f"Statistical test on {path.name}", test_result.get("test", test_type)))

        hint = "Call apply_patch() or run_cleaning_pipeline() to act on findings."
        if test_result.get("undetermined"):
            hint = "There is no finding to act on. Re-run the test on a sample large enough to produce a p-value."
        result = {
            "success": True,
            "op": "statistical_tests",
            "file_path": str(path),
            "test_type": test_type,
            **test_result,
            "hint": hint,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("statistical_tests error")
        return {
            "success": False,
            "error": str(exc),
            "hint": hint_for_error(exc, "Check column names and ensure numeric/categorical types are correct."),
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
    theme: str = "device",
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

        df[date_column] = pd.to_datetime(df[date_column], format="mixed", dayfirst=False, errors="coerce")
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

        # Coerce value columns to numeric (handles string-typed numeric columns)
        for vc in value_columns:
            if not is_numeric_col(df[vc]):
                df[vc] = pd.to_numeric(df[vc], errors="coerce")
                non_num = int(df[vc].isna().sum())
                if non_num:
                    progress.append(warn(f"Coerced '{vc}' to numeric", f"{non_num} non-numeric values → NaN"))

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

        max_r = get_max_rows()
        truncated = len(resampled) > max_r
        resampled_trunc = resampled.tail(max_r)

        trend_data = {}
        if _SCIPY_OK and _linregress is not None:
            for col in value_columns:
                ts = resampled[col].dropna()
                if len(ts) >= 2:
                    slope, _, r_val, _, _ = _linregress(range(len(ts)), ts.values)
                    trend_data[col] = {
                        "slope": round(float(slope), 4),
                        "r_squared": round(float(r_val**2), 4),
                        "direction": "up" if slope > 0 else "down" if slope < 0 else "flat",
                    }

        # STL decomposition, ACF/PACF, ADF stationarity test
        stl_results: dict = {}
        acf_results: dict = {}
        adf_results: dict = {}
        if _STATSMODELS_OK and adfuller is not None and acf is not None and pacf is not None and STL is not None:
            for col in value_columns:
                ts = resampled[col].dropna()
                if len(ts) < 4:
                    continue

                # ADF stationarity test
                try:
                    adf_out = adfuller(ts.values, autolag="AIC")
                    adf_results[col] = {
                        "p_value": round_p(float(adf_out[1])),
                        "is_stationary": bool(adf_out[1] < 0.05),
                    }
                except Exception:
                    pass

                # ACF/PACF — report only significant lags (|acf| > 2/sqrt(n))
                n_lags = min(12, len(ts) // 2)
                if n_lags >= 2:
                    try:
                        threshold = 2.0 / (len(ts) ** 0.5)
                        acf_vals = acf(ts.values, nlags=n_lags, fft=True)
                        pacf_vals = pacf(ts.values, nlags=n_lags)
                        sig_acf = [i + 1 for i, v in enumerate(acf_vals[1:]) if abs(v) > threshold]
                        sig_pacf = [i + 1 for i, v in enumerate(pacf_vals[1:]) if abs(v) > threshold]
                        acf_results[col] = {
                            "significant_acf_lags": sig_acf,
                            "significant_pacf_lags": sig_pacf,
                        }
                    except Exception:
                        pass

                # STL decomposition — report only strength metrics, not raw arrays
                try:
                    seasonal_period = {"M": 12, "Q": 4, "W": 52, "D": 7, "Y": 1}.get(period, 12)
                    if len(ts) >= max(4, 2 * seasonal_period) and seasonal_period > 1:
                        stl_fit = STL(ts, period=seasonal_period, robust=True).fit()
                        resid_var = float(stl_fit.resid.var())
                        seasonal_var = float((stl_fit.seasonal + stl_fit.resid).var())
                        trend_var = float((stl_fit.trend + stl_fit.resid).var())
                        stl_results[col] = {
                            "seasonal_strength": round(
                                float(max(0.0, 1 - resid_var / seasonal_var)) if seasonal_var else 0.0, 4
                            ),
                            "trend_strength": round(
                                float(max(0.0, 1 - resid_var / trend_var)) if trend_var else 0.0, 4
                            ),
                        }
                except Exception:
                    pass
        else:
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
            "hint": (
                "HTML chart saved — open output_path for the history and the dashed forecast."
                if _PLOTLY_AVAILABLE
                else "plotly is not installed, so no chart was written; the numbers above are complete."
            ),
            "forecast_periods": forecast_periods,
            "forecast_values": forecast_values_map,
            "forecast_dates": forecast_dates_map,
            "progress": progress,
        }

        if _PLOTLY_AVAILABLE:
            fig = go.Figure()
            x_vals = [str(i) for i in resampled.index]
            # Plotly's own qualitative cycle, pinned per column so a series and
            # its forecast are drawn in the same colour.
            palette = px_colors.qualitative.Plotly
            for i, col in enumerate(value_columns):
                colour = palette[i % len(palette)]
                fig.add_trace(
                    go.Scatter(
                        x=x_vals,
                        y=resampled[col].tolist(),
                        name=col,
                        mode="lines+markers",
                        legendgroup=col,
                        line=dict(color=colour),
                    )
                )
                # The forecast was computed, returned in forecast_values, and
                # never drawn -- while the hint said to open the file "for the
                # full visualization". Continued from the last observed point,
                # so the dashed segment joins the line it extends instead of
                # floating beside it.
                fcast = forecast_values_map.get(col)
                fdates = forecast_dates_map.get(col)
                if fcast and fdates and len(resampled[col].dropna()):
                    fig.add_trace(
                        go.Scatter(
                            x=[x_vals[-1], *fdates],
                            y=[float(resampled[col].dropna().iloc[-1]), *fcast],
                            name=f"{col} (forecast)",
                            mode="lines+markers",
                            legendgroup=col,
                            line=dict(color=colour, dash="dash"),
                            marker=dict(symbol="circle-open"),
                        )
                    )
            fig.update_layout(
                title=(
                    f"Time Series — {path.name} (period={period})"
                    + (f", {forecast_periods}-period forecast dashed" if forecast_values_map else "")
                ),
                xaxis_title=date_column,
                template=plotly_template(theme),
                height=calc_chart_height(len(value_columns), mode="subplot"),
            )
            abs_p, fname = _save_chart(fig, output_path, "time_series", path, open_after, theme, progress)
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
            "hint": hint_for_error(exc, "Check date_column is a datetime column and value_columns are numeric."),
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
    theme: str = "device",
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

        df[date_column] = pd.to_datetime(df[date_column], format="mixed", dayfirst=False, errors="coerce")
        df = df.dropna(subset=[date_column])

        if not cohort_column:
            # A column with one value puts every row in the same cohort, which
            # is not a cohort analysis -- it is the total, drawn as one row. The
            # old bound was `nunique() < 50`, which admits 1, and the first
            # string column of the ad dataset is `product` ("Product 1" in all
            # 16,834 rows), so the tool answered with a 1x10 matrix and reported
            # "Auto-detected cohort column: product" as if that were a finding.
            # Excluding constants lets the real candidate through, and if none
            # qualifies the year-month fallback below is the right answer.
            # _adv_dashboard.py applies the same `nunique() > 1` rule to charts.
            cat_cols = [c for c in df.columns if _is_string_col(df[c]) and 1 < df[c].nunique() < 50]
            if cat_cols:
                cohort_column = cat_cols[0]
                progress.append(info("Auto-detected cohort column", cohort_column))

        if not cohort_column or cohort_column not in df.columns:
            df["_cohort"] = df[date_column].dt.to_period("M").astype(str)
            cohort_column = "_cohort"
            progress.append(info("Using date-based cohort", "year-month"))

        # The guard above only covers the column this function picks for itself.
        # A caller can name a constant column outright -- `phase` in the ad
        # dataset -- and reach the same dead end: the dataset total drawn as a
        # single row, under a hint reading "use a specific cohort_column", which
        # is exactly what they had just done. Say so, and keep the result: they
        # asked for that column and the matrix is not wrong, only degenerate.
        degenerate_cohort = cohort_column != "_cohort" and df[cohort_column].nunique() <= 1
        if degenerate_cohort:
            progress.append(
                warn(
                    f"'{cohort_column}' has one distinct value",
                    "every row lands in the same cohort, so this is the dataset total",
                )
            )

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
                f"{len(pivot)} cohort{'' if len(pivot) == 1 else 's'} × {len(pivot.columns)} periods",
            )
        )

        if degenerate_cohort:
            usable = [c for c in df.columns if _is_string_col(df[c]) and 1 < df[c].nunique() < 50]
            cohort_hint = f"'{cohort_column}' is the same for every row, so there is one cohort. " + (
                f"Try cohort_column={usable[0]}." if usable else "Leave cohort_column empty to cohort by year-month."
            )
        else:
            cohort_hint = "Use a more targeted call with a specific cohort_column or value_column."

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
            "hint": cohort_hint,
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
            abs_p, fname = _save_chart(fig, output_path, "cohort", path, open_after, theme, progress)
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
            "hint": hint_for_error(exc, "Check date_column is a datetime column and cohort_column exists."),
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

        min_n_z = min_n_for_zscore(threshold)
        undetermined_cols: set[str] = set()
        for col in numeric_cols:
            clean = df[col].dropna()
            col_summary: dict = {"column": col, "n": int(len(clean))}

            if method in ("iqr", "both"):
                # Under four values the 1.5*IQR fence always lands outside the
                # sample -- see MIN_N_IQR. A flag column of all False and a
                # count of zero describe the row count, not the data.
                if len(clean) < MIN_N_IQR:
                    # No flag column at all, rather than a column of False. The
                    # JSON says this column has no verdict; a saved file that
                    # says False for every row says "checked, found nothing",
                    # and the two must not disagree. The re-run caught exactly
                    # this: null in the response, concrete False in the CSV.
                    col_summary["iqr_outliers"] = None
                    col_summary["iqr_status"] = (
                        f"undetermined at n={len(clean)}: the 1.5*IQR fence cannot fall inside a sample "
                        f"smaller than {MIN_N_IQR}"
                    )
                    undetermined_cols.add(col)
                else:
                    q1 = clean.quantile(0.25)
                    q3 = clean.quantile(0.75)
                    iqr = q3 - q1
                    lower = q1 - 1.5 * iqr
                    upper = q3 + 1.5 * iqr
                    iqr_flag = (df[col] < lower) | (df[col] > upper)
                    result_df[f"{col}_iqr_flag"] = iqr_flag.fillna(False)
                    col_summary["iqr_outliers"] = int(iqr_flag.sum())
                    col_summary["iqr_lower"] = rounded(lower)
                    col_summary["iqr_upper"] = rounded(upper)
                    if float(iqr) == 0.0:
                        col_summary["iqr_status"] = "zero spread: q1 == q3, so the fence has no width"

            if method in ("zscore", "both"):
                # The largest z any of n points can reach is (n-1)/sqrt(n), so
                # below min_n_z the scan cannot reach `threshold` whatever the
                # values are.
                if len(clean) < min_n_z:
                    col_summary["zscore_outliers"] = None
                    col_summary["zscore_threshold"] = threshold
                    col_summary["zscore_status"] = (
                        f"undetermined at n={len(clean)}: the largest z-score attainable by any of n points "
                        f"is (n-1)/sqrt(n), which first exceeds {threshold} at n={min_n_z}"
                    )
                    undetermined_cols.add(col)
                else:
                    mean_v = clean.mean()
                    std_v = clean.std()
                    if std_v > 0:
                        zscores = (df[col] - mean_v) / std_v
                        zscore_flag = zscores.abs() > threshold
                    else:
                        zscore_flag = pd.Series([False] * len(df), index=df.index)
                        col_summary["zscore_status"] = "zero spread: every value is identical, so every z is 0"
                    result_df[f"{col}_zscore_flag"] = zscore_flag.fillna(False)
                    col_summary["zscore_outliers"] = int(zscore_flag.sum())
                    col_summary["zscore_threshold"] = threshold

            per_column_summary[col] = col_summary

        flag_cols = [c for c in result_df.columns if c.endswith("_iqr_flag") or c.endswith("_zscore_flag")]
        if flag_cols:
            result_df["_anomaly_score"] = result_df[flag_cols].sum(axis=1)
            anomaly_count = int((result_df["_anomaly_score"] > 0).sum())
        else:
            # Nothing was judged, so there is no score to write. A `_anomaly_score`
            # column of zeros would tell every later reader of this file that the
            # rows were checked and came back clean.
            anomaly_count = None

        out = str(resolve_path(output_path)) if output_path else str(path.parent / f"{path.stem}_anomalies.csv")
        result_df.to_csv(out, index=False)

        undetermined_shown = sorted(undetermined_cols)
        if undetermined_shown:
            progress.append(
                warn(
                    "Sample too small to detect anomalies",
                    f"{len(undetermined_shown)} column(s) undetermined: {', '.join(undetermined_shown)}",
                )
            )
        scored = "no columns could be scored" if anomaly_count is None else f"{anomaly_count}/{len(df)} anomalous rows"
        progress.append(ok(f"Anomaly detection on {path.name}", f"{scored}, saved to {Path(out).name}"))

        hint = "Call apply_patch() or run_cleaning_pipeline() to act on findings."
        if anomaly_count is None:
            hint = (
                "No column had enough rows for an anomaly verdict, so there is nothing to act on. "
                "columns_undetermined lists them, each with the n it had. The saved file carries the "
                "rows and no flag columns, because there was no verdict to record."
            )
        result = {
            "success": True,
            "op": "detect_anomalies",
            "file_path": str(path),
            "method": method,
            "total_rows": len(df),
            "anomaly_count": anomaly_count,
            "columns_scanned": len(numeric_cols),
            "columns_undetermined": undetermined_shown,
            "per_column": per_column_summary,
            "output_path": out,
            "output_name": Path(out).name,
            "hint": hint,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("detect_anomalies error")
        return {
            "success": False,
            "error": str(exc),
            "hint": hint_for_error(exc, "Check file_path is absolute and columns are numeric."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }
