"""Outlier scans and shape labels must not answer from the row count alone.

At one row every outlier tool returned the same shape: `lower_limit ==
upper_limit == the value itself`, and "0 outliers". Zero was guaranteed there --
a fence with no width cannot have anything outside it -- so the number described
the input's size, not its contents.

Two thresholds make that precise, and both are properties of the arithmetic
rather than judgement calls:

  * the 1.5*IQR fence cannot fall inside a sample of fewer than four values, so
    below n=4 nothing can ever be flagged;
  * the largest z-score any of n points can reach is (n-1)/sqrt(n), which first
    exceeds 3 at n=11, so a 3-sigma scan over ten rows or fewer is guaranteed to
    find nothing.

Alongside them, extended_stats read labels off statistics that were NaN: a
single row was "approximately symmetric" with "approximately normal tails" and a
Shapiro-Wilk verdict, beside the honest nulls the same row produced for std and
variance.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_medium")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _med_analysis import detect_anomalies  # noqa: E402
from _med_inspect import check_outliers, extended_stats  # noqa: E402

from shared.small_sample import MIN_N_IQR, min_n_for_zscore  # noqa: E402


def _csv(tmp_path, n_rows: int) -> Path:
    f = tmp_path / f"rows_{n_rows}.csv"
    rows = "\n".join(f"{i * 10}" for i in range(1, n_rows + 1))
    f.write_text(f"spend\n{rows}\n")
    return f


def test_the_z_score_threshold_is_where_three_sigma_becomes_reachable():
    """(n-1)/sqrt(n) > t, solved for n. Checked against the definition itself."""
    for threshold in (1.0, 2.0, 2.5, 3.0, 4.0):
        n = min_n_for_zscore(threshold)
        assert (n - 1) / n**0.5 > threshold, (threshold, n)
        assert (n - 2) / (n - 1) ** 0.5 <= threshold, (threshold, n)
    assert min_n_for_zscore(3.0) == 11


def test_check_outliers_withholds_a_verdict_it_could_not_have_reached(tmp_path):
    r = check_outliers(str(_csv(tmp_path, 1)), open_after=False)
    assert r["success"] is True
    col = r["results"]["spend"]
    assert col["n"] == 1
    # None, not 0: nothing was measured.
    assert col["outlier_count_iqr"] is None
    assert col["has_outliers_iqr"] is None
    assert col["outlier_count_std"] is None
    assert col["has_outliers_std"] is None
    # And no bounds are offered, because the ones that existed sat on the value.
    assert "lower_limit_iqr" not in col
    assert "upper_limit_std" not in col
    assert r["columns_undetermined"] == ["spend"]
    assert "nothing to act on" in r["hint"]


@pytest.mark.parametrize("n_rows", [1, 2, 3])
def test_iqr_stays_undetermined_below_four_rows(tmp_path, n_rows):
    col = check_outliers(str(_csv(tmp_path, n_rows)), method="iqr", open_after=False)["results"]["spend"]
    assert col["outlier_count_iqr"] is None
    assert str(MIN_N_IQR) in col["iqr_status"]


def test_iqr_reports_a_real_count_from_four_rows(tmp_path):
    """Four is the first size where a point can sit outside the fence."""
    f = tmp_path / "four.csv"
    f.write_text("spend\n0\n0\n0\n100\n")
    col = check_outliers(str(f), method="iqr", open_after=False)["results"]["spend"]
    assert col["outlier_count_iqr"] == 1
    assert col["has_outliers_iqr"] is True


@pytest.mark.parametrize("n_rows", [1, 5, 10])
def test_three_sigma_stays_undetermined_below_eleven_rows(tmp_path, n_rows):
    col = check_outliers(str(_csv(tmp_path, n_rows)), method="std", open_after=False)["results"]["spend"]
    assert col["outlier_count_std"] is None, n_rows
    assert "11" in col["std_status"]


def test_three_sigma_reports_a_real_count_from_eleven_rows(tmp_path):
    f = tmp_path / "eleven.csv"
    f.write_text("spend\n" + "1\n" * 10 + "1000\n")
    col = check_outliers(str(f), method="std", open_after=False)["results"]["spend"]
    assert col["outlier_count_std"] == 1


def test_a_constant_column_with_enough_rows_still_answers_zero(tmp_path):
    """Zero spread over 20 rows is a real "no outliers", and says why."""
    f = tmp_path / "flat.csv"
    f.write_text("spend\n" + "7\n" * 20)
    col = check_outliers(str(f), open_after=False)["results"]["spend"]
    assert col["outlier_count_iqr"] == 0
    assert col["outlier_count_std"] == 0
    assert "zero spread" in col["iqr_status"]
    assert "zero spread" in col["std_status"]


def test_detect_anomalies_withholds_the_same_verdict(tmp_path):
    out = tmp_path / "flagged.csv"
    r = detect_anomalies(str(_csv(tmp_path, 1)), output_path=str(out))
    assert r["success"] is True
    col = r["per_column"]["spend"]
    assert col["iqr_outliers"] is None
    assert col["zscore_outliers"] is None
    assert r["columns_undetermined"] == ["spend"]
    assert r["anomaly_count"] is None
    assert "nothing to act on" in r["hint"]
    # The saved file must not carry a verdict the response withholds: a
    # `_iqr_flag` column of False says "checked, found nothing".
    written = out.read_text().splitlines()[0]
    assert "_iqr_flag" not in written
    assert "_zscore_flag" not in written
    assert "_anomaly_score" not in written
    assert "spend" in written


def test_detect_anomalies_threshold_moves_the_boundary(tmp_path):
    """The z guard is derived from the caller's threshold, not hardcoded at 11."""
    col = detect_anomalies(str(_csv(tmp_path, 7)), method="zscore", threshold=2.0, output_path=str(tmp_path / "o.csv"))[
        "per_column"
    ]["spend"]
    # min_n_for_zscore(2.0) is 6, so seven rows is enough for a real answer.
    assert col["zscore_outliers"] == 0
    assert "undetermined" not in col.get("zscore_status", "")

    col2 = detect_anomalies(
        str(_csv(tmp_path, 5)), method="zscore", threshold=2.0, output_path=str(tmp_path / "o2.csv")
    )["per_column"]["spend"]
    assert col2["zscore_outliers"] is None


def test_extended_stats_does_not_label_a_nan(tmp_path):
    stats = extended_stats(str(_csv(tmp_path, 1)))["stats"]["spend"]
    assert stats["n"] == 1
    assert stats["std"] is None
    assert stats["variance"] is None
    assert stats["skewness"] is None
    assert stats["kurtosis"] is None
    # The labels were the bug: NaN fails every comparison, so the if/elif chain
    # fell through to its else and described the shape of one number.
    assert stats["skewness_label"] is None
    assert stats["kurtosis_label"] is None
    assert "undetermined" in stats["distribution_hint"]
    assert "Shapiro" in stats["distribution_hint"]


def test_extended_stats_still_labels_a_real_distribution(tmp_path):
    f = tmp_path / "skewed.csv"
    f.write_text("spend\n" + "\n".join(["1"] * 30 + ["500", "600", "700"]) + "\n")
    stats = extended_stats(str(f))["stats"]["spend"]
    assert stats["skewness"] is not None
    assert "skewed" in stats["skewness_label"]
    assert stats["kurtosis_label"] is not None
    assert "undetermined" not in stats["distribution_hint"]
