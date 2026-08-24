"""A sample too small to test must not come back as "no significant difference".

Both statistical tools took the p-value scipy returns when a test cannot run --
NaN -- and evaluated `p < 0.05` on it. That is False, so the caller was told
`significant: false` with the sentence "No significant difference (p>=0.05)"
beside a `p_value: null` that said the opposite. `statistical_test` even tested
for the NaN first and still chose the negative verdict.

The tools now refuse before running a test the sample cannot support, naming
which sample was short; and where a p-value still comes back missing, the
verdict is withheld rather than defaulted.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_medium"), str(ROOT / "servers" / "data_statistics")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _med_analysis import statistical_tests  # noqa: E402
from _stats_tests import statistical_test  # noqa: E402


@pytest.fixture()
def one_row(tmp_path) -> Path:
    f = tmp_path / "one_row.csv"
    f.write_text("region,spend,clicks\nWest,120,4\n")
    return f


@pytest.fixture()
def two_rows(tmp_path) -> Path:
    f = tmp_path / "two_rows.csv"
    f.write_text("region,spend,clicks\nWest,120,4\nEast,80,9\n")
    return f


# --- the medium server's statistical_tests ---------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {"test_type": "ttest", "column_a": "spend", "column_b": "clicks"},
        {"test_type": "correlation", "column_a": "spend", "column_b": "clicks"},
        {"test_type": "shapiro_wilk", "column_a": "spend"},
        {"test_type": "ks", "column_a": "spend"},
        {"test_type": "mann_whitney", "column_a": "spend", "column_b": "clicks"},
        {"test_type": "wilcoxon", "column_a": "spend", "column_b": "clicks"},
        {"test_type": "anova", "column_a": "spend", "group_column": "region"},
        {"test_type": "kruskal", "column_a": "spend", "group_column": "region"},
        {"test_type": "levene", "column_a": "spend", "group_column": "region"},
        {"test_type": "chi_square", "column_a": "region", "column_b": "region"},
    ],
)
def test_medium_refuses_a_single_row_rather_than_calling_it_insignificant(one_row, kwargs):
    r = statistical_tests(str(one_row), **kwargs)
    assert r["success"] is False
    # The refusal has to say how small the sample was, or the caller cannot tell
    # this apart from a bad column name.
    assert re.search(r"= \d+\.?$|= \d+,", r["error"]), r["error"]
    assert r.get("significant") is None
    assert "no significant" not in r.get("interpretation", "").lower()


def test_medium_never_says_not_significant_without_a_p_value(one_row):
    """Whatever gets past the guards must not arrive as a negative verdict."""
    r = statistical_tests(str(one_row))  # auto-scan: always succeeds
    assert r["success"] is True
    for col, entry in r["normality"].items():
        assert entry["p_value"] is None, col
        # None, not False: a column too short to test did not fail the test.
        assert entry["normal"] is None, col
    assert r["top_correlations"] == []


def test_medium_still_reaches_a_verdict_on_a_real_sample(tmp_path):
    f = tmp_path / "many.csv"
    rows = "\n".join(f"West,{i},{i * 3}" for i in range(1, 41))
    f.write_text(f"region,spend,clicks\n{rows}\n")
    r = statistical_tests(str(f), test_type="correlation", column_a="spend", column_b="clicks")
    assert r["success"] is True
    assert r["significant"] is True
    assert r.get("undetermined") is not True


# --- the statistics server's statistical_test ------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {"test": "shapiro_wilk", "column_a": "spend"},
        {"test": "anderson", "column_a": "spend"},
        {"test": "t_test", "column_a": "spend", "column_b": "clicks"},
        {"test": "paired_t_test", "column_a": "spend", "column_b": "clicks"},
        {"test": "one_sample_t", "column_a": "spend"},
        {"test": "pearson", "column_a": "spend", "column_b": "clicks"},
        {"test": "spearman", "column_a": "spend", "column_b": "clicks"},
        {"test": "kendall", "column_a": "spend", "column_b": "clicks"},
        {"test": "mann_whitney", "column_a": "spend", "column_b": "clicks"},
        {"test": "wilcoxon", "column_a": "spend", "column_b": "clicks"},
        {"test": "anova", "column_a": "spend", "group_column": "region"},
        {"test": "kruskal", "column_a": "spend", "group_column": "region"},
        {"test": "levene", "column_a": "spend", "column_b": "clicks"},
        {"test": "proportion_z", "column_a": "clicks"},
        {"test": "ks", "column_a": "spend"},
    ],
)
def test_statistics_refuses_a_single_row_rather_than_failing_to_reject(one_row, kwargs):
    r = statistical_test(str(one_row), **kwargs)
    assert r["success"] is False
    assert re.search(r"= \d+\.?$|= \d+,", r["error"]), r["error"]
    assert r.get("reject_null") is None
    assert "fail to reject" not in r.get("interpretation", "").lower()


def test_shapiro_below_three_values_is_a_refusal_not_a_scipy_crash(two_rows):
    """scipy raises `'float' object has no attribute 'dtype'` here, not NaN.

    That message reached the caller verbatim from regression_analysis, with a
    hint about column names and nothing about the sample size that caused it.
    """
    r = statistical_test(str(two_rows), test="shapiro_wilk", column_a="spend")
    assert r["success"] is False
    assert "dtype" not in r["error"]
    assert "Shapiro-Wilk" in r["error"]
    assert "3" in r["error"]


def test_statistics_still_reaches_a_verdict_on_a_real_sample(tmp_path):
    f = tmp_path / "many.csv"
    rows = "\n".join(f"West,{i},{i * 3}" for i in range(1, 41))
    f.write_text(f"region,spend,clicks\n{rows}\n")
    r = statistical_test(str(f), test="pearson", column_a="spend", column_b="clicks")
    assert r["success"] is True
    assert r["reject_null"] is True
    assert r.get("undetermined") is not True


def test_a_singleton_group_does_not_block_anova(tmp_path):
    """One group of one is legitimate; what ANOVA needs is residual df.

    The first version of this guard demanded 2+ values in every group, which
    refused an eight-row fixture with four regions because one region appeared
    once -- a valid ANOVA with four residual degrees of freedom.
    """
    f = tmp_path / "groups.csv"
    f.write_text(
        "region,spend\nWest,5000\nWest,3200\nWest,6000\nEast,7500\nEast,3000\nSouth,2100\nSouth,2500\nNorth,4800\n"
    )
    r = statistical_tests(str(f), test_type="anova", column_a="spend", group_column="region")
    assert r["success"] is True
    assert r["groups"] == 4
    assert r["p_value"] is not None
