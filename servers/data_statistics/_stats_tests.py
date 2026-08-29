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

from shared.arg_alias import missing, pick
from shared.file_utils import error_text, resolve_path
from shared.file_utils import read_csv as _read_csv
from shared.progress import fail, info, ok, warn
from shared.small_sample import (
    MIN_N_SHAPIRO,
    is_significant,
    need_n,
    rounded,
    undetermined_because,
)
from shared.stats_format import format_p, round_p

try:
    from scipy import stats as _scipy_stats
    from scipy.stats import tukey_hsd as _tukey_hsd

    _SCIPY_OK = True
except ImportError:
    _scipy_stats = None  # type: ignore
    _tukey_hsd = None  # type: ignore
    _SCIPY_OK = False

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


# The medium server's statistical_tests names three of these differently.
_SIBLING_TEST_NAMES = {
    "ttest": "t_test",
    "correlation": "pearson",
}


class _SampleTooSmall(Exception):
    """Carries a ready-made refusal out of whichever test branch raised it.

    The seventeen branches each build their samples differently, so the size
    check has to live inside the branch. Raising rather than returning keeps it
    to one line there, and the handler sits above the general `except Exception`
    so a refusal that names n is not reworded into "Check column names".
    """

    def __init__(self, payload: dict) -> None:
        super().__init__(payload.get("error", "sample too small"))
        self.payload = payload


def _require(test_name: str, sizes: dict[str, int], minimum: int, demand: str = "", hint: str = "") -> None:
    err = need_n("statistical_test", test_name, sizes, minimum, hint=hint, demand=demand)
    if err:
        raise _SampleTooSmall(err)


def _interpret_p(p: float, alpha: float) -> str:
    reject = p < alpha
    shown = format_p(p)
    if reject:
        return f"Reject H0 (p={shown} < α={alpha})"
    return f"Fail to reject H0 (p={shown} ≥ α={alpha})"


VALID_CORRECTIONS = ("none", "bonferroni", "holm")


def _adjust(pvals: list[float], correction: str) -> list[float]:
    """Adjust a family of p-values for multiple comparisons.

    Implemented here rather than pulled in from statsmodels because the whole
    point of this server is that the caller cannot run a stats package itself
    -- which is also why `posthoc` returning "use scipy.stats.tukey_hsd" was
    not an answer.
    """
    m = len(pvals)
    if m <= 1 or correction in ("", "none"):
        return [min(1.0, float(p)) for p in pvals]
    if correction == "bonferroni":
        return [min(1.0, float(p) * m) for p in pvals]
    # holm: step-down, each p scaled by the number still under test, then made
    # monotone so a later comparison never reports a smaller adjusted p than an
    # earlier one.
    order = sorted(range(m), key=lambda i: pvals[i])
    out = [0.0] * m
    running = 0.0
    for rank, idx in enumerate(order):
        running = max(running, min(1.0, float(pvals[idx]) * (m - rank)))
        out[idx] = running
    return out


def _posthoc_pairs(groups: list, labels: list[str], kind: str, correction: str, alpha: float, scipy_stats) -> dict:
    """Pairwise comparisons after a significant omnibus test.

    `posthoc=True` used to return {"method": "Tukey HSD", "note": "Use
    scipy.stats.tukey_hsd for full pairwise comparisons."} -- a method name
    implying a test had been run, and a note pointing at a function the caller
    has no way to call. `correction` was declared on the tool, forwarded
    through the wrapper, and read nowhere at all.
    """
    import itertools

    pairs: list[dict] = []
    if kind == "anova":
        # Tukey HSD controls the family-wise error rate itself, so an extra
        # correction on top of it would be wrong, not merely redundant.
        res = scipy_stats.tukey_hsd(*groups)
        for i, j in itertools.combinations(range(len(groups)), 2):
            pairs.append(
                {
                    "group_a": labels[i],
                    "group_b": labels[j],
                    "statistic": round(float(res.statistic[i][j]), 4),
                    "p_value": round_p(float(res.pvalue[i][j])),
                    # A pair involving a singleton group has no p-value of its
                    # own even when the omnibus test that led here did.
                    "significant": is_significant(res.pvalue[i][j], alpha),
                }
            )
        return {
            "method": "Tukey HSD",
            "correction": "tukey (family-wise, built in)",
            "comparisons": pairs,
            "n_comparisons": len(pairs),
        }

    raw: list[float] = []
    combos = list(itertools.combinations(range(len(groups)), 2))
    for i, j in combos:
        st, pv = scipy_stats.mannwhitneyu(groups[i], groups[j], alternative="two-sided")
        raw.append(float(pv))
        pairs.append(
            {
                "group_a": labels[i],
                "group_b": labels[j],
                "statistic": round(float(st), 4),
            }
        )
    adjusted = _adjust(raw, correction)
    for entry, before, after in zip(pairs, raw, adjusted):
        entry["p_value"] = round_p(after)
        entry["p_value_raw"] = round_p(before)
        entry["significant"] = is_significant(after, alpha)
    return {
        "method": "pairwise Mann-Whitney U",
        "correction": correction or "none",
        "comparisons": pairs,
        "n_comparisons": len(pairs),
    }


def statistical_test(  # type: ignore[reportGeneralTypeIssues]
    file_path: str,
    test: str = "",
    column_a: str = "",
    column_b: str = "",
    group_column: str = "",
    alpha: float = 0.05,
    alternative: str = "two-sided",
    compute_effect_size: bool = True,
    posthoc: bool = False,
    correction: str = "",
    hypothesized_mean: float = 0.0,
    test_type: str = "",
) -> dict:
    """Run one of 17 statistical tests. Returns statistic, p-value, effect size."""
    progress = []
    # statistical_tests on the medium server spells this same choice `test_type`.
    test, note = pick("statistical_test", "test", test, test_type)
    if not test:
        return missing("statistical_test", "test", "test_type")
    if correction and correction not in VALID_CORRECTIONS:
        return {
            "success": False,
            "op": "statistical_test",
            "error": f"Unknown correction: '{correction}'",
            "hint": f"Valid: {', '.join(VALID_CORRECTIONS)}. Correction applies to posthoc=True comparisons.",
            "progress": [fail("Invalid correction", correction)],
            "token_estimate": 30,
        }
    if note:
        progress.append(info("Argument alias", note))
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
                "hint": "Check file_path is absolute.",
                "progress": [fail("File not found", path.name)],
                "token_estimate": 20,
            }
        df = _read_csv(str(path))

        # statistical_tests, whose name differs by one letter, spells the same
        # test `ttest` and lumps the three correlations under `correlation`.
        # Accept its vocabulary rather than refusing a caller who learned it there.
        test = _SIBLING_TEST_NAMES.get(test, test)

        if test not in _VALID_TESTS:
            return {
                "success": False,
                "error": f"Unknown test '{test}'",
                "hint": f"Valid tests: {', '.join(sorted(_VALID_TESTS))}",
                "progress": [fail("Unknown test", test)],
                "token_estimate": 20,
            }

        def _get_series(col: str, name: str = "column_a") -> pd.Series:
            # file_path and test are the only arguments the schema marks
            # required, so the call it documents arrives with column_a still at
            # its "" default. That used to come back as "Column '' not found.
            # Available: [...]", which reads as though the caller had named a
            # column when it had named none, under a hint listing the 17 valid
            # tests -- and the test was already right. Not supplying a column
            # and naming one that is not there are different mistakes.
            if not col:
                numeric = [c for c in df.columns if pd.to_numeric(df[c], errors="coerce").notna().any()]
                raise ValueError(
                    f"Test '{test}' needs {name}. Name the column to test, e.g. "
                    f"{name}='{numeric[0] if numeric else df.columns[0]}'. Numeric columns: {numeric}"
                )
            if col not in df.columns:
                raise ValueError(f"Column '{col}' not found. Available: {list(df.columns)}")
            return pd.to_numeric(df[col], errors="coerce").dropna()

        statistic: float = float("nan")
        p_value: float = float("nan")
        effect_size: dict = {}
        posthoc_result: dict | None = None

        # --- Normality tests ---
        if test == "shapiro_wilk":
            a = _get_series(column_a, "column_a")
            # Below three values scipy does not return NaN here -- it raises
            # `'float' object has no attribute 'dtype'` from inside its own
            # NaN-policy wrapper, which says nothing about sample size.
            _require(
                "Shapiro-Wilk",
                {f"non-null values in '{column_a}'": len(a)},
                MIN_N_SHAPIRO,
                hint="Shapiro-Wilk is undefined below 3 values. Use extended_stats() to see the column's count.",
            )
            stat, p = scipy_stats.shapiro(a.values)
            statistic, p_value = float(stat), float(p)
            interp = "Normally distributed" if p >= alpha else "Not normally distributed"
            progress.append(ok("Shapiro-Wilk", interp))

        elif test == "ks":
            a = _get_series(column_a, "column_a")
            _require("Kolmogorov-Smirnov", {f"non-null values in '{column_a}'": len(a)}, 2)
            if column_b and column_b in df.columns:
                b = _get_series(column_b, "column_b")
                _require("Kolmogorov-Smirnov", {f"non-null values in '{column_b}'": len(b)}, 2)
                stat, p = scipy_stats.ks_2samp(a.values, b.values, alternative=alternative)
            else:
                # Fit the reference normal to the sample, the way the medium
                # server's statistical_tests does. Bare "norm" is the STANDARD
                # normal, so this asked whether the column was N(0, 1) rather
                # than whether it was normal at all: 300 draws from N(1000, 100)
                # came back p=0.0, "Reject H0". Every column whose mean was not
                # about zero got that answer, which is every real one.
                fitted = scipy_stats.norm(float(a.mean()), float(a.std()))
                stat, p = scipy_stats.kstest(a.values, fitted.cdf, alternative=alternative)
            statistic, p_value = float(stat), float(p)
            progress.append(ok("Kolmogorov-Smirnov", _interpret_p(p, alpha)))

        elif test == "anderson":
            a = _get_series(column_a, "column_a")
            # This branch returns its own dict below, so the undetermined-verdict
            # backstop at the end never sees it: a NaN statistic here would
            # compare False against the critical value and print "Cannot reject
            # normality", which is the same sentence a large normal sample gets.
            _require(
                "Anderson-Darling",
                {f"non-null values in '{column_a}'": len(a)},
                MIN_N_SHAPIRO,
                hint="Anderson-Darling needs 3+ values. Use extended_stats() to see the column's count.",
            )
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
            a = _get_series(column_a, "column_a")
            if group_column and group_column in df.columns:
                groups = df[group_column].dropna().unique()
                if len(groups) < 2:
                    raise ValueError(f"Need at least 2 groups in '{group_column}'.")
                g1 = pd.to_numeric(df.loc[df[group_column] == groups[0], column_a], errors="coerce").dropna()
                g2 = pd.to_numeric(df.loc[df[group_column] == groups[1], column_a], errors="coerce").dropna()
                _require(
                    "Independent t-test",
                    {f"group '{groups[0]}'": len(g1), f"group '{groups[1]}'": len(g2)},
                    2,
                    hint="A t-test compares variances, so each group needs 2+ values.",
                )
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
                b = _get_series(column_b, "column_b")
                _require(
                    "Independent t-test",
                    {f"'{column_a}'": len(a), f"'{column_b}'": len(b)},
                    2,
                    hint="A t-test compares variances, so each column needs 2+ non-null values.",
                )
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
            a = _get_series(column_a, "column_a")
            b = _get_series(column_b, "column_b")
            common_idx = a.index.intersection(b.index)
            _require(
                "Paired t-test",
                {"complete pairs": len(common_idx)},
                2,
                hint="A paired test needs 2+ rows where both columns are non-null.",
            )
            stat, p = scipy_stats.ttest_rel(a[common_idx].values, b[common_idx].values, alternative=alternative)
            statistic, p_value = float(stat), float(p)
            if compute_effect_size:
                diffs = a[common_idx] - b[common_idx]
                d = float(diffs.mean() / diffs.std()) if diffs.std() > 0 else 0.0
                effect_size = {"cohens_d": round(d, 4), "interpretation": _cohens_d_label(d)}
            progress.append(ok("Paired t-test", _interpret_p(p_value, alpha)))

        elif test == "one_sample_t":
            a = _get_series(column_a, "column_a")
            _require(
                "One-sample t-test",
                {f"non-null values in '{column_a}'": len(a)},
                2,
                hint="The test divides by the sample standard deviation, which is undefined for a single value.",
            )
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
            group_labels = [str(g) for g in df[group_column].dropna().unique()]
            groups_data = [
                pd.to_numeric(df.loc[df[group_column] == g, column_a], errors="coerce").dropna().values
                for g in df[group_column].dropna().unique()
            ]
            _require(
                "One-way ANOVA",
                {f"groups in '{group_column}'": len(groups_data)},
                2,
                demand="at least 2 groups to compare",
                hint="Use value_counts() on the group column to see how many distinct groups the data has.",
            )
            # Not "every group needs 2 values" -- a singleton group is fine. The
            # requirement is residual degrees of freedom, total values minus
            # number of groups, which is the denominator of the within-group
            # mean square.
            _require(
                "One-way ANOVA",
                {
                    "residual degrees of freedom (values minus groups)": sum(len(g) for g in groups_data)
                    - len(groups_data)
                },
                1,
                demand="at least 1 residual degree of freedom",
                hint="Every group holding exactly one value leaves nothing to compare within groups.",
            )
            stat, p = scipy_stats.f_oneway(*groups_data)
            statistic, p_value = float(stat), float(p)
            if compute_effect_size:
                grand_mean = np.concatenate(groups_data).mean()
                ss_between = sum(len(g) * (g.mean() - grand_mean) ** 2 for g in groups_data)
                ss_total = sum(((v - grand_mean) ** 2).sum() for v in groups_data)
                eta_sq = float(ss_between / ss_total) if ss_total > 0 else 0.0
                effect_size = {"eta_squared": round(eta_sq, 4), "interpretation": _eta_sq_label(eta_sq)}
            if posthoc and p < alpha and len(groups_data) > 2:
                posthoc_result = _posthoc_pairs(groups_data, group_labels, "anova", correction, alpha, scipy_stats)
                progress.append(ok("Post-hoc", f"{posthoc_result['n_comparisons']} pairwise comparison(s)"))
            progress.append(ok("One-way ANOVA", _interpret_p(p_value, alpha)))

        # --- Chi-square ---
        elif test == "chi_square":
            if not row_col_available(df, column_a, column_b, group_column):
                raise ValueError(
                    f"chi_square requires column_a and column_b (categorical). Available: {list(df.columns)}"
                )
            ct = pd.crosstab(df[column_a], df[column_b if column_b else group_column])
            _require(
                "Chi-square test",
                {"table rows": ct.shape[0], "table columns": ct.shape[1]},
                2,
                demand="a contingency table of at least 2x2",
                hint="Use value_counts() on both columns to see how many categories each one has.",
            )
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
            a = _get_series(column_a, "column_a")
            if group_column and group_column in df.columns:
                groups = df[group_column].dropna().unique()
                g1 = pd.to_numeric(df.loc[df[group_column] == groups[0], column_a], errors="coerce").dropna()
                g2 = pd.to_numeric(df.loc[df[group_column] == groups[1], column_a], errors="coerce").dropna()
                _require(
                    "Mann-Whitney U",
                    {f"group '{groups[0]}'": len(g1), f"group '{groups[1]}'": len(g2)},
                    2,
                    hint="One value per group cannot rank against another; give each group 2+ values.",
                )
                stat, p = scipy_stats.mannwhitneyu(g1.values, g2.values, alternative=alternative)
                if compute_effect_size:
                    r = float(1 - 2 * stat / (len(g1) * len(g2))) if (len(g1) * len(g2)) > 0 else 0.0
                    effect_size = {"rank_biserial_r": round(r, 4), "interpretation": _r_label(abs(r))}
            else:
                b = _get_series(column_b, "column_b")
                _require(
                    "Mann-Whitney U",
                    {f"'{column_a}'": len(a), f"'{column_b}'": len(b)},
                    2,
                    hint="One value per column cannot rank against another; give each column 2+ values.",
                )
                stat, p = scipy_stats.mannwhitneyu(a.values, b.values, alternative=alternative)
                if compute_effect_size:
                    r = float(1 - 2 * stat / (len(a) * len(b))) if (len(a) * len(b)) > 0 else 0.0
                    effect_size = {"rank_biserial_r": round(r, 4), "interpretation": _r_label(abs(r))}
            statistic, p_value = float(stat), float(p)
            progress.append(ok("Mann-Whitney U", _interpret_p(p_value, alpha)))

        elif test == "wilcoxon":
            a = _get_series(column_a, "column_a")
            b = _get_series(column_b, "column_b")
            common_idx = a.index.intersection(b.index)
            _require(
                "Wilcoxon signed-rank",
                {"complete pairs": len(common_idx)},
                2,
                hint="A signed-rank test ranks the paired differences, so it needs 2+ complete pairs.",
            )
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
            group_labels = [str(g) for g in df[group_column].dropna().unique()]
            groups_data = [
                pd.to_numeric(df.loc[df[group_column] == g, column_a], errors="coerce").dropna().values
                for g in df[group_column].dropna().unique()
            ]
            _require(
                "Kruskal-Wallis",
                {f"groups in '{group_column}'": len(groups_data)},
                2,
                demand="at least 2 groups to compare",
                hint="Use value_counts() on the group column to see how many distinct groups the data has.",
            )
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
            # posthoc was read only by the ANOVA branch, so asking for pairwise
            # comparisons after a non-parametric omnibus test did nothing at all.
            if posthoc and p_value < alpha and len(groups_data) > 2:
                posthoc_result = _posthoc_pairs(groups_data, group_labels, "kruskal", correction, alpha, scipy_stats)
                progress.append(ok("Post-hoc", f"{posthoc_result['n_comparisons']} pairwise comparison(s)"))
            progress.append(ok("Kruskal-Wallis", _interpret_p(p_value, alpha)))

        elif test == "levene":
            if group_column and group_column in df.columns:
                groups_data = [
                    pd.to_numeric(df.loc[df[group_column] == g, column_a], errors="coerce").dropna().values
                    for g in df[group_column].dropna().unique()
                ]
            else:
                a = _get_series(column_a, "column_a")
                b = _get_series(column_b, "column_b")
                groups_data = [a.values, b.values]
            _require(
                "Levene's test",
                {"groups": len(groups_data)},
                2,
                demand="at least 2 groups whose variances it can compare",
                hint="Give levene either a group_column with 2+ groups, or column_a and column_b.",
            )
            _require(
                "Levene's test",
                {
                    "residual degrees of freedom (values minus groups)": sum(len(g) for g in groups_data)
                    - len(groups_data)
                },
                1,
                demand="at least 1 residual degree of freedom",
                hint="Every group holding exactly one value leaves no spread to compare.",
            )
            stat, p = scipy_stats.levene(*groups_data)
            statistic, p_value = float(stat), float(p)
            progress.append(ok("Levene's test", _interpret_p(p_value, alpha)))

        # --- Correlation tests ---
        elif test == "pearson":
            a = _get_series(column_a, "column_a")
            b = _get_series(column_b, "column_b")
            common_idx = a.index.intersection(b.index)
            _require(
                "Pearson correlation",
                {"complete pairs": len(common_idx)},
                3,
                hint="Any two points lie on a line, so the coefficient is +-1 by construction below 3 complete pairs.",
            )
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
            a = _get_series(column_a, "column_a")
            b = _get_series(column_b, "column_b")
            common_idx = a.index.intersection(b.index)
            _require(
                "Spearman correlation",
                {"complete pairs": len(common_idx)},
                3,
                hint="Any two points lie on a line, so the coefficient is +-1 by construction below 3 complete pairs.",
            )
            stat, p = scipy_stats.spearmanr(a[common_idx].values, b[common_idx].values)
            statistic, p_value = float(stat), float(p)
            if compute_effect_size:
                effect_size = {"rho": round(float(stat), 4), "interpretation": _r_label(abs(float(stat)))}
            progress.append(ok("Spearman correlation", f"rho={statistic:.4f}  {_interpret_p(p_value, alpha)}"))

        elif test == "kendall":
            a = _get_series(column_a, "column_a")
            b = _get_series(column_b, "column_b")
            common_idx = a.index.intersection(b.index)
            _require(
                "Kendall's tau",
                {"complete pairs": len(common_idx)},
                3,
                hint="Any two points lie on a line, so the coefficient is +-1 by construction below 3 complete pairs.",
            )
            stat, p = scipy_stats.kendalltau(a[common_idx].values, b[common_idx].values)
            statistic, p_value = float(stat), float(p)
            if compute_effect_size:
                effect_size = {"tau": round(float(stat), 4), "interpretation": _r_label(abs(float(stat)))}
            progress.append(ok("Kendall's tau", f"tau={statistic:.4f}  {_interpret_p(p_value, alpha)}"))

        elif test == "proportion_z":
            a = _get_series(column_a, "column_a")
            b = _get_series(column_b, "column_b") if column_b and column_b in df.columns else None
            n1 = len(a)
            _require(
                "Proportion Z-test",
                {f"non-null values in '{column_a}'": n1},
                2,
                hint="A proportion from a single observation is either 0 or 1, so the z-statistic is degenerate.",
            )
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

        # A NaN p-value used to be reported as reject_null: False under "Fail to
        # reject H0", which is the same answer a large sample with no effect
        # gets. The line already knew the p-value was missing -- it tested for
        # it -- and then chose the negative verdict anyway. Failing to reject is
        # a finding; having nothing to reject with is not.
        reject_null = is_significant(p_value, alpha)
        if reject_null is None:
            interpretation = undetermined_because(f"{len(df)} row(s) in {path.name}")
            progress.append(warn("Verdict withheld", interpretation))
        else:
            interpretation = f"{'Reject' if reject_null else 'Fail to reject'} H0: {_interpret_p(p_value, alpha)}"

        result: dict = {
            "success": True,
            "test": test,
            "statistic": rounded(statistic, 6),
            "p_value": round_p(p_value) if not np.isnan(p_value) else None,
            "alpha": alpha,
            "reject_null": reject_null,
            "interpretation": interpretation,
            "alternative": alternative,
            "progress": progress,
        }
        if reject_null is None:
            result["undetermined"] = True
        if effect_size:
            result["effect_size"] = effect_size
        if posthoc_result:
            result["posthoc"] = posthoc_result
        result["token_estimate"] = len(str(result)) // 4
        return result

    except _SampleTooSmall as too_small:
        return too_small.payload
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
            "error": error_text(exc),
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
