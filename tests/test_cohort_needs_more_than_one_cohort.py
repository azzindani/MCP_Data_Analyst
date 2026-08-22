"""cohort_analysis auto-detected a constant column as the cohort key.

The candidate filter was `_is_string_col(df[c]) and df[c].nunique() < 50`,
which admits a column with exactly one value, and `cat_cols[0]` then takes the
first one in column order. In the real ad dataset that is `product`, which holds
"Product 1" in all 16,834 rows. So a coverage sweep asking for a cohort
retention matrix got one cohort by ten periods -- the dataset total, laid out in
a row -- under the progress line "Auto-detected cohort column: product", which
reads like a finding rather than a dead end.

The function already had the right answer built in: when nothing categorical
qualifies it cohorts by year-month, which is what a cohort analysis normally
means. That branch was simply unreachable, because a constant column always
passed the filter first.

`_adv_dashboard.py` applies exactly this `nunique() > 1` rule before it builds
charts, for the same reason spelled out in its own comment.
"""

from __future__ import annotations

import csv as csvmod
from pathlib import Path

import pytest

from servers.data_statistics.engine import cohort_analysis


def matrix(result: dict) -> dict:
    return result.get("cohort_matrix") or result.get("matrix") or {}


@pytest.fixture()
def constant_first_column(tmp_path: Path) -> str:
    """The shape of the real dataset: a constant column ahead of a useful one."""
    path = tmp_path / "campaigns.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csvmod.writer(fh)
        w.writerow(["Date", "product", "campaign_platform", "spends"])
        for month in ("2019-10", "2019-11", "2019-12"):
            for platform in ("Google Ads", "Facebook Ads"):
                w.writerow([f"{month}-15", "Product 1", platform, 1000])
    return str(path)


class TestAConstantColumnIsNotACohort:
    def test_it_is_not_chosen(self, constant_first_column: str, tmp_path: Path):
        r = cohort_analysis(constant_first_column, output_path=str(tmp_path / "c.html"), open_after=False)
        assert r["success"] is True, r.get("error")
        assert r.get("cohort_column") != "product", r.get("cohort_column")

    def test_the_column_that_varies_is_chosen_instead(self, constant_first_column: str, tmp_path: Path):
        r = cohort_analysis(constant_first_column, output_path=str(tmp_path / "c.html"), open_after=False)
        assert r.get("cohort_column") == "campaign_platform", r.get("cohort_column")

    def test_the_matrix_has_more_than_one_cohort(self, constant_first_column: str, tmp_path: Path):
        """One row is the total, not a cohort analysis."""
        r = cohort_analysis(constant_first_column, output_path=str(tmp_path / "c.html"), open_after=False)
        assert len(matrix(r)) > 1, matrix(r)

    def test_the_progress_log_does_not_claim_to_have_found_product(self, constant_first_column: str, tmp_path: Path):
        r = cohort_analysis(constant_first_column, output_path=str(tmp_path / "c.html"), open_after=False)
        detected = [s for s in r["progress"] if "cohort column" in str(s.get("message", ""))]
        assert all("product" not in str(s.get("detail", "")) for s in detected), detected


class TestTheDateFallbackIsReachableAgain:
    """With nothing categorical left to choose, year-month cohorts are correct."""

    @pytest.fixture()
    def only_constants(self, tmp_path: Path) -> str:
        path = tmp_path / "flat.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            w = csvmod.writer(fh)
            w.writerow(["Date", "product", "phase", "spends"])
            for month in ("2019-10", "2019-11", "2019-12"):
                w.writerow([f"{month}-15", "Product 1", "Performance", 1000])
        return str(path)

    def test_it_cohorts_by_month(self, only_constants: str, tmp_path: Path):
        r = cohort_analysis(only_constants, output_path=str(tmp_path / "c.html"), open_after=False)
        assert r["success"] is True, r.get("error")
        assert len(matrix(r)) == 3, matrix(r)

    def test_it_says_it_used_a_date_based_cohort(self, only_constants: str, tmp_path: Path):
        r = cohort_analysis(only_constants, output_path=str(tmp_path / "c.html"), open_after=False)
        assert any("date-based" in str(s.get("message", "")).lower() for s in r["progress"]), r["progress"]


class TestAnExplicitCohortColumnIsStillObeyed:
    def test_the_caller_may_still_ask_for_the_constant_column(self, constant_first_column: str, tmp_path: Path):
        """Excluding it from auto-detection must not forbid it outright."""
        r = cohort_analysis(
            constant_first_column,
            cohort_column="product",
            output_path=str(tmp_path / "c.html"),
            open_after=False,
        )
        assert r["success"] is True, r.get("error")
        assert r.get("cohort_column") == "product"


class TestTheRealDataset:
    def test_the_ad_data_no_longer_cohorts_on_product(self, ad_data_full_csv: Path, tmp_path: Path):
        r = cohort_analysis(str(ad_data_full_csv), output_path=str(tmp_path / "c.html"), open_after=False)
        assert r["success"] is True, r.get("error")
        assert r.get("cohort_column") == "campaign_platform", r.get("cohort_column")
        assert len(matrix(r)) > 1, matrix(r)
