"""A two-group t-test names its groups, their means, and which one is higher.

A t-test of clicks by campaign_platform answered `statistic: -33.2, Means
differ significantly` -- no group names, no means, no direction. The sign of
the statistic follows whichever group the file happened to list first, so it
could not be read, and "which platform gets more clicks" was the question
(Facebook Ads 44.76, Google Ads 8.22). Both Data_Analyst tools did this.

It was also Student's equal-variance t, run on groups whose standard
deviations differ seven-fold (125.6 vs 17.1) and whose sizes differ nine-fold
(1,733 vs 15,101) -- the case where Student's t is wrong, not merely
conservative. Welch's t does not assume equal variances, costs nothing when
they are equal, and is what R's t.test runs by default; the answer says which
test ran.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from shared.small_sample import rounded

WELCH = "Welch's t-test (unequal variances)"


def welch_t(a: pd.Series, b: pd.Series, alternative: str = "two-sided") -> tuple[float, float]:
    """(statistic, p) of Welch's two-sample t-test of `a` against `b`."""
    from scipy import stats as scipy_stats

    stat, p = scipy_stats.ttest_ind(a.values, b.values, equal_var=False, alternative=alternative)
    return float(stat), float(p)


def two_groups(a: pd.Series, b: pd.Series, name_a: Any, name_b: Any) -> dict[str, Any]:
    """Each group's name, size, mean and spread, and a sentence on which mean is higher.

    The t statistic is positive when the first group's mean is the higher, so
    the sentence also says which group is first.
    """
    first, second = str(name_a), str(name_b)
    mean_a, mean_b = float(a.mean()), float(b.mean())
    groups = [
        {"name": first, "n": int(len(a)), "mean": rounded(mean_a), "std": rounded(float(a.std()))},
        {"name": second, "n": int(len(b)), "mean": rounded(mean_b), "std": rounded(float(b.std()))},
    ]
    if mean_a == mean_b:
        direction = f"'{first}' and '{second}' have the same mean ({mean_a:.4g})."
    else:
        high, low = (first, second) if mean_a > mean_b else (second, first)
        high_mean, low_mean = max(mean_a, mean_b), min(mean_a, mean_b)
        direction = f"'{high}' has the higher mean ({high_mean:.4g} vs {low_mean:.4g} for '{low}')."
    direction += f" The statistic is positive when '{first}', the first group, is higher."
    return {"groups": groups, "direction": direction}


def not_two_groups(group_column: str, groups: list[Any]) -> str:
    """Why a t-test cannot run on this grouping, naming the groups it found."""
    shown = ", ".join(f"'{g}'" for g in groups[:8]) + (", ..." if len(groups) > 8 else "")
    return f"A t-test compares two groups; '{group_column}' has {len(groups)}: {shown}."
