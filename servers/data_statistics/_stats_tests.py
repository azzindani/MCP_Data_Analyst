"""Statistical tests module. No MCP imports. Requires scipy."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
_MED = str(Path(__file__).resolve().parents[2] / "data_medium")
for _p in (str(_ROOT), _MED):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import numpy as np
import pandas as pd

from shared.progress import fail, info, ok, warn

logger = logging.getLogger(__name__)

_VALID_TESTS = frozenset(
    {
        "shapiro_wilk",
        "ks",
        "anderson",
        "t_test",
        "paired_t_test",
        "one_sample_t",
        "anova",
        "chi_square",
        "fisher",
        "mann_whitney",
        "wilcoxon",
        "kruskal",
        "levene",
        "pearson",
        "spearman",
        "kendall",
        "proportion_z",
    }
)


def _interpret_p(p: float, alpha: float) -> str:
    reject = p < alpha
    if reject:
        return f"Reject H0 (p={p:.4f} < α={alpha})"
    return f"Fail to reject H0 (p={p:.4f} ≥ α={alpha})"


def statistical_test(  # type: ignore[reportGeneralTypeIssues]
    file_path: str,
    test: str,
    column_a: str = "",
    column_b: str = "",
    group_column: str = "",
    alpha: float = 0.05,
    alternative: str = "two-sided",
    compute_effect_size: bool = True,
    posthoc: bool = False,
    correction: str = "",
    hypothesized_mean: float = 0.0,
) -> dict:
    """Run one of 17 statistical tests. Returns statistic, p-value, effect size."""
    progress = []
    try:
        from scipy import stats as scipy_stats

        # Resolve path and load
        from shared.file_utils import resolve_path

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

        if test not in _VALID_TESTS:
            return {
                "success": False,
                "error": f"Unknown test '{test}'",
                "hint": f"Valid tests: {', '.join(sorted(_VALID_TESTS))}",
                "progress": [fail("Unknown test", test)],
                "token_estimate": 20,
            }

        def _get_series(col: str) -> pd.Series:
            if col not in df.columns:
                raise ValueError(f"Column '{col}' not found. Available: {list(df.columns)}")
            return pd.to_numeric(df[col], errors="coerce").dropna()

        statistic: float = float("nan")
        p_value: float = float("nan")
        effect_size: dict = {}
        posthoc_result: dict | None = None

        # --- Normality tests ---
        if test == "shapiro_wilk":
            a = _get_series(column_a)
            stat, p = scipy_stats.shapiro(a.values)
            statistic, p_value = float(stat), float(p)
            interp = "Normally distributed" if p >= alpha else "Not normally distributed"
            progress.append(ok("Shapiro-Wilk", interp))

        elif test == "ks":
            a = _get_series(column_a)
            if column_b and column_b in df.columns:
                b = _get_series(column_b)
                stat, p = scipy_stats.ks_2samp(a.values, b.values, alternative=alternative)
            else:
                stat, p = scipy_stats.kstest(a.values, "norm", alternative=alternative)
            statistic, p_value = float(stat), float(p)
            progress.append(ok("Kolmogorov-Smirnov", _interpret_p(p, alpha)))

        elif test == "anderson":
            a = _get_series(column_a)
            result = scipy_stats.anderson(a.values, dist="norm")
            statistic = float(result.statistic)
            # Use 5% significance level index (index 2)
            sig_idx = 2
            critical = float(result.critical_values[sig_idx])
            p_value = float(result.significance_level[sig_idx]) / 100
            interp = "Reject normality" if statistic > critical else "Cannot reject normality"
            progress.append(ok("Anderson-Darling", interp))
            return {
                "success": True,
                "test": test,
                "statistic": statistic,
                "critical_values": dict(zip(result.significance_level.tolist(), result.critical_values.tolist())),
                "interpretation": interp,
                "reject_null": statistic > critical,
                "progress": progress,
                "token_estimate": 80,
            }

        # --- t-tests ---
        elif test == "t_test":
            a = _get_series(column_a)
            if group_column and group_column in df.columns:
                groups = df[group_column].dropna().unique()
                if len(groups) < 2:
                    raise ValueError(f"Need at least 2 groups in '{group_column}'.")
                g1 = pd.to_numeric(df.loc[df[group_column] == groups[0], column_a], errors="coerce").dropna()
                g2 = pd.to_numeric(df.loc[df[group_column] == groups[1], column_a], errors="coerce").dropna()
                stat, p = scipy_stats.ttest_ind(g1.values, g2.values, alternative=alternative)
                statistic, p_value = float(stat), float(p)
                if compute_effect_size:
                    pooled_std = float(
                        np.sqrt(
                            ((len(g1) - 1) * g1.std() ** 2 + (len(g2) - 1) * g2.std() ** 2) / (len(g1) + len(g2) - 2)
                        )
                    )
                    d = float((g1.mean() - g2.mean()) / pooled_std) if pooled_std > 0 else 0.0
                    effect_size = {"cohens_d": round(d, 4), "interpretation": _cohens_d_label(d)}
            else:
                b = _get_series(column_b)
                stat, p = scipy_stats.ttest_ind(a.values, b.values, alternative=alternative)
                statistic, p_value = float(stat), float(p)
                if compute_effect_size:
                    pooled_std = float(
                        np.sqrt(((len(a) - 1) * a.std() ** 2 + (len(b) - 1) * b.std() ** 2) / (len(a) + len(b) - 2))
                    )
                    d = float((a.mean() - b.mean()) / pooled_std) if pooled_std > 0 else 0.0
                    effect_size = {"cohens_d": round(d, 4), "interpretation": _cohens_d_label(d)}
            progress.append(ok("Independent t-test", _interpret_p(p_value, alpha)))

        elif test == "paired_t_test":
            a = _get_series(column_a)
            b = _get_series(column_b)
            common_idx = a.index.intersection(b.index)
            stat, p = scipy_stats.ttest_rel(a[common_idx].values, b[common_idx].values, alternative=alternative)
            statistic, p_value = float(stat), float(p)
            if compute_effect_size:
                diffs = a[common_idx] - b[common_idx]
                d = float(diffs.mean() / diffs.std()) if diffs.std() > 0 else 0.0
                effect_size = {"cohens_d": round(d, 4), "interpretation": _cohens_d_label(d)}
            progress.append(ok("Paired t-test", _interpret_p(p_value, alpha)))

        elif test == "one_sample_t":
            a = _get_series(column_a)
            stat, p = scipy_stats.ttest_1samp(a.values, hypothesized_mean, alternative=alternative)
            statistic, p_value = float(stat), float(p)
            if compute_effect_size:
                d = float((a.mean() - hypothesized_mean) / a.std()) if a.std() > 0 else 0.0
                effect_size = {"cohens_d": round(d, 4), "interpretation": _cohens_d_label(d)}
            progress.append(ok("One-sample t-test", _interpret_p(p_value, alpha)))

        # --- ANOVA ---
        elif test == "anova":
            if not group_column or group_column not in df.columns:
                raise ValueError(f"anova requires group_column. Available: {list(df.columns)}")
            groups_data = [
                pd.to_numeric(df.loc[df[group_column] == g, column_a], errors="coerce").dropna().values
                for g in df[group_column].dropna().unique()
            ]
            stat, p = scipy_stats.f_oneway(*groups_data)
            statistic, p_value = float(stat), float(p)
            if compute_effect_size:
                grand_mean = np.concatenate(groups_data).mean()
                ss_between = sum(len(g) * (g.mean() - grand_mean) ** 2 for g in groups_data)
                ss_total = sum(((v - grand_mean) ** 2).sum() for v in groups_data)
                eta_sq = float(ss_between / ss_total) if ss_total > 0 else 0.0
                effect_size = {"eta_squared": round(eta_sq, 4), "interpretation": _eta_sq_label(eta_sq)}
            if posthoc and p < alpha:
                from scipy.stats import tukey_hsd

                posthoc_result = {
                    "method": "Tukey HSD",
                    "note": "Use scipy.stats.tukey_hsd for full pairwise comparisons.",
                }
            progress.append(ok("One-way ANOVA", _interpret_p(p_value, alpha)))

        # --- Chi-square ---
        elif test == "chi_square":
            if not row_col_available(df, column_a, column_b, group_column):
                raise ValueError(
                    f"chi_square requires column_a and column_b (categorical). Available: {list(df.columns)}"
                )
            ct = pd.crosstab(df[column_a], df[column_b if column_b else group_column])
            chi2, p, dof, _ = scipy_stats.chi2_contingency(ct)
            statistic, p_value = float(chi2), float(p)
            if compute_effect_size:
                n = ct.values.sum()
                k = min(ct.shape)
                v = float(np.sqrt(chi2 / (n * (k - 1)))) if n > 0 and k > 1 else 0.0
                effect_size = {"cramers_v": round(v, 4), "interpretation": _cramers_v_label(v)}
            progress.append(ok("Chi-square test", _interpret_p(p_value, alpha)))

        elif test == "fisher":
            ct = pd.crosstab(df[column_a], df[column_b if column_b else group_column])
            if ct.shape != (2, 2):
                raise ValueError("Fisher's exact test requires a 2×2 contingency table.")
            stat, p = scipy_stats.fisher_exact(ct.values, alternative=alternative)
            statistic, p_value = float(stat), float(p)
            if compute_effect_size:
                n = ct.values.sum()
                chi2_approx = scipy_stats.chi2_contingency(ct)[0]
                v = float(np.sqrt(chi2_approx / n)) if n > 0 else 0.0
                effect_size = {"cramers_v": round(v, 4), "interpretation": _cramers_v_label(v)}
            progress.append(ok("Fisher's exact", _interpret_p(p_value, alpha)))

        # --- Non-parametric ---
        elif test == "mann_whitney":
            a = _get_series(column_a)
            if group_column and group_column in df.columns:
                groups = df[group_column].dropna().unique()
                g1 = pd.to_numeric(df.loc[df[group_column] == groups[0], column_a], errors="coerce").dropna()
                g2 = pd.to_numeric(df.loc[df[group_column] == groups[1], column_a], errors="coerce").dropna()
                stat, p = scipy_stats.mannwhitneyu(g1.values, g2.values, alternative=alternative)
                if compute_effect_size:
                    r = float(1 - 2 * stat / (len(g1) * len(g2))) if (len(g1) * len(g2)) > 0 else 0.0
                    effect_size = {"rank_biserial_r": round(r, 4), "interpretation": _r_label(abs(r))}
            else:
                b = _get_series(column_b)
                stat, p = scipy_stats.mannwhitneyu(a.values, b.values, alternative=alternative)
                if compute_effect_size:
                    r = float(1 - 2 * stat / (len(a) * len(b))) if (len(a) * len(b)) > 0 else 0.0
                    effect_size = {"rank_biserial_r": round(r, 4), "interpretation": _r_label(abs(r))}
            statistic, p_value = float(stat), float(p)
            progress.append(ok("Mann-Whitney U", _interpret_p(p_value, alpha)))

        elif test == "wilcoxon":
            a = _get_series(column_a)
            b = _get_series(column_b)
            common_idx = a.index.intersection(b.index)
            stat, p = scipy_stats.wilcoxon(a[common_idx].values, b[common_idx].values, alternative=alternative)
            statistic, p_value = float(stat), float(p)
            if compute_effect_size:
                n = len(common_idx)
                r = float(stat / (n * (n + 1) / 2)) if n > 0 else 0.0
                effect_size = {"rank_biserial_r": round(r, 4), "interpretation": _r_label(abs(r))}
            progress.append(ok("Wilcoxon signed-rank", _interpret_p(p_value, alpha)))

        elif test == "kruskal":
            if not group_column or group_column not in df.columns:
                raise ValueError(f"kruskal requires group_column. Available: {list(df.columns)}")
            groups_data = [
                pd.to_numeric(df.loc[df[group_column] == g, column_a], errors="coerce").dropna().values
                for g in df[group_column].dropna().unique()
            ]
            stat, p = scipy_stats.kruskal(*groups_data)
            statistic, p_value = float(stat), float(p)
            if compute_effect_size:
                n = sum(len(g) for g in groups_data)
                k = len(groups_data)
                eps_sq = float((stat - k + 1) / (n - k)) if (n - k) > 0 else 0.0
                effect_size = {
                    "epsilon_squared": round(max(0.0, eps_sq), 4),
                    "interpretation": _epsilon_sq_label(eps_sq),
                }
            progress.append(ok("Kruskal-Wallis", _interpret_p(p_value, alpha)))

        elif test == "levene":
            if group_column and group_column in df.columns:
                groups_data = [
                    pd.to_numeric(df.loc[df[group_column] == g, column_a], errors="coerce").dropna().values
                    for g in df[group_column].dropna().unique()
                ]
            else:
                a = _get_series(column_a)
                b = _get_series(column_b)
                groups_data = [a.values, b.values]
            stat, p = scipy_stats.levene(*groups_data)
            statistic, p_value = float(stat), float(p)
            progress.append(ok("Levene's test", _interpret_p(p_value, alpha)))

        # --- Correlation tests ---
        elif test == "pearson":
            a = _get_series(column_a)
            b = _get_series(column_b)
            common_idx = a.index.intersection(b.index)
            stat, p = scipy_stats.pearsonr(a[common_idx].values, b[common_idx].values)
            statistic, p_value = float(stat), float(p)
            if compute_effect_size:
                effect_size = {
                    "r": round(float(stat), 4),
                    "r_squared": round(float(stat) ** 2, 4),
                    "interpretation": _r_label(abs(float(stat))),
                }
            progress.append(ok("Pearson correlation", f"r={statistic:.4f}  {_interpret_p(p_value, alpha)}"))

        elif test == "spearman":
            a = _get_series(column_a)
            b = _get_series(column_b)
            common_idx = a.index.intersection(b.index)
            stat, p = scipy_stats.spearmanr(a[common_idx].values, b[common_idx].values)
            statistic, p_value = float(stat), float(p)
            if compute_effect_size:
                effect_size = {"rho": round(float(stat), 4), "interpretation": _r_label(abs(float(stat)))}
            progress.append(ok("Spearman correlation", f"rho={statistic:.4f}  {_interpret_p(p_value, alpha)}"))

        elif test == "kendall":
            a = _get_series(column_a)
            b = _get_series(column_b)
            common_idx = a.index.intersection(b.index)
            stat, p = scipy_stats.kendalltau(a[common_idx].values, b[common_idx].values)
            statistic, p_value = float(stat), float(p)
            if compute_effect_size:
                effect_size = {"tau": round(float(stat), 4), "interpretation": _r_label(abs(float(stat)))}
            progress.append(ok("Kendall's tau", f"tau={statistic:.4f}  {_interpret_p(p_value, alpha)}"))

        elif test == "proportion_z":
            a = _get_series(column_a)
            b = _get_series(column_b) if column_b and column_b in df.columns else None
            n1 = len(a)
            p1 = float(a.mean())
            if b is not None:
                n2 = len(b)
                p2 = float(b.mean())
                p_pool = (p1 * n1 + p2 * n2) / (n1 + n2)
                z = (p1 - p2) / float(np.sqrt(p_pool * (1 - p_pool) * (1 / n1 + 1 / n2)))
                p_val = float(2 * (1 - scipy_stats.norm.cdf(abs(z))))
            else:
                p0 = hypothesized_mean
                z = (p1 - p0) / float(np.sqrt(p0 * (1 - p0) / n1))
                p_val = float(2 * (1 - scipy_stats.norm.cdf(abs(z))))
            statistic, p_value = z, p_val
            progress.append(ok("Proportion Z-test", _interpret_p(p_value, alpha)))

        reject_null = bool(p_value < alpha) if not np.isnan(p_value) else False
        interpretation = f"{'Reject' if reject_null else 'Fail to reject'} H0: {_interpret_p(p_value, alpha)}"

        result: dict = {
            "success": True,
            "test": test,
            "statistic": round(statistic, 6) if not np.isnan(statistic) else None,
            "p_value": round(p_value, 6) if not np.isnan(p_value) else None,
            "alpha": alpha,
            "reject_null": reject_null,
            "interpretation": interpretation,
            "alternative": alternative,
            "progress": progress,
        }
        if effect_size:
            result["effect_size"] = effect_size
        if posthoc_result:
            result["posthoc"] = posthoc_result
        result["token_estimate"] = len(str(result)) // 4
        return result

    except ImportError:
        return {
            "success": False,
            "error": "scipy not installed",
            "hint": "Install scipy: uv add scipy",
            "progress": [fail("Missing dependency", "scipy")],
            "token_estimate": 20,
        }
    except Exception as exc:
        logger.exception("statistical_test error")
        return {
            "success": False,
            "error": str(exc),
            "hint": f"Check column names and test type. Valid tests: {', '.join(sorted(_VALID_TESTS))}",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


def row_col_available(df: pd.DataFrame, col_a: str, col_b: str, group_col: str) -> bool:
    col2 = col_b if col_b and col_b in df.columns else group_col
    return bool(col_a and col_a in df.columns and col2 and col2 in df.columns)


def _cohens_d_label(d: float) -> str:
    d = abs(d)
    if d < 0.2:
        return "negligible"
    if d < 0.5:
        return "small"
    if d < 0.8:
        return "medium"
    return "large"


def _eta_sq_label(eta: float) -> str:
    if eta < 0.01:
        return "negligible"
    if eta < 0.06:
        return "small"
    if eta < 0.14:
        return "medium"
    return "large"


def _cramers_v_label(v: float) -> str:
    if v < 0.1:
        return "negligible"
    if v < 0.3:
        return "small"
    if v < 0.5:
        return "medium"
    return "large"


def _r_label(r: float) -> str:
    if r < 0.1:
        return "negligible"
    if r < 0.3:
        return "small"
    if r < 0.5:
        return "medium"
    return "large"


def _epsilon_sq_label(e: float) -> str:
    if e < 0.01:
        return "negligible"
    if e < 0.08:
        return "small"
    if e < 0.26:
        return "medium"
    return "large"
