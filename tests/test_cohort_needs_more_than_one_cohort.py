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


class TestTheExplicitPathSaysWhenItIsADeadEnd:
    """The auto-detect guard never sees a column the caller names itself.

    A later sweep asked for `phase` outright -- constant in the ad dataset -- and
    got the 1x10 total back as a clean PASS, under the stock hint "Use a more
    targeted call with a specific cohort_column or value_column", which is what
    the caller had just done. The result is kept (they asked for it); what
    changes is that the response says it is degenerate and names a way out.
    """

    def explicit_constant(self, path: str, tmp_path: Path) -> dict:
        return cohort_analysis(path, cohort_column="product", output_path=str(tmp_path / "c.html"), open_after=False)

    def test_the_matrix_is_still_returned(self, constant_first_column: str, tmp_path: Path):
        r = self.explicit_constant(constant_first_column, tmp_path)
        assert r["success"] is True
        assert r["cohorts"] == 1
        assert len(matrix(r)) == 1

    def test_a_warning_says_the_column_is_constant(self, constant_first_column: str, tmp_path: Path):
        r = self.explicit_constant(constant_first_column, tmp_path)
        warns = [s for s in r["progress"] if s.get("status") == "warn"]
        assert any("one distinct value" in str(s.get("message", "")) for s in warns), r["progress"]

    def test_the_hint_no_longer_asks_for_what_it_was_given(self, constant_first_column: str, tmp_path: Path):
        r = self.explicit_constant(constant_first_column, tmp_path)
        assert "Use a more targeted call" not in r["hint"], r["hint"]

    def test_the_hint_names_the_constant_column(self, constant_first_column: str, tmp_path: Path):
        r = self.explicit_constant(constant_first_column, tmp_path)
        assert "product" in r["hint"], r["hint"]

    def test_the_hint_names_a_column_that_would_work(self, constant_first_column: str, tmp_path: Path):
        r = self.explicit_constant(constant_first_column, tmp_path)
        assert "campaign_platform" in r["hint"], r["hint"]

    def test_one_cohort_is_not_reported_as_cohorts(self, constant_first_column: str, tmp_path: Path):
        r = self.explicit_constant(constant_first_column, tmp_path)
        details = [str(s.get("detail", "")) for s in r["progress"]]
        assert any("1 cohort ×" in d for d in details), details

    def test_a_varying_column_gets_no_warning(self, constant_first_column: str, tmp_path: Path):
        r = cohort_analysis(
            constant_first_column,
            cohort_column="campaign_platform",
            output_path=str(tmp_path / "c.html"),
            open_after=False,
        )
        assert [s for s in r["progress"] if s.get("status") == "warn"] == []
        assert "Use a more targeted call" in r["hint"], r["hint"]

    def test_the_date_fallback_is_never_called_degenerate(self, tmp_path: Path):
        """A single-month dataset cohorts to one _cohort row; that is the fallback
        working, not the caller naming a constant column."""
        path = tmp_path / "one_month.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            w = csvmod.writer(fh)
            w.writerow(["Date", "product", "phase", "spends"])
            for day in ("01", "02", "03"):
                w.writerow([f"2019-10-{day}", "Product 1", "Performance", 1000])
        r = cohort_analysis(str(path), output_path=str(tmp_path / "c.html"), open_after=False)
        assert r["success"] is True, r.get("error")
        assert r["cohort_column"] == "_cohort"
        assert [s for s in r["progress"] if s.get("status") == "warn"] == []

    def test_the_real_dataset_phase_column(self, ad_data_full_csv: Path, tmp_path: Path):
        r = cohort_analysis(
            str(ad_data_full_csv),
            cohort_column="phase",
            value_column="clicks",
            output_path=str(tmp_path / "c.html"),
            open_after=False,
        )
        assert r["success"] is True, r.get("error")
        assert r["cohorts"] == 1
        assert "phase" in r["hint"] and "campaign_platform" in r["hint"], r["hint"]


class TestTheRealDataset:
    def test_the_ad_data_no_longer_cohorts_on_product(self, ad_data_full_csv: Path, tmp_path: Path):
        r = cohort_analysis(str(ad_data_full_csv), output_path=str(tmp_path / "c.html"), open_after=False)
        assert r["success"] is True, r.get("error")
        assert r.get("cohort_column") == "campaign_platform", r.get("cohort_column")
        assert len(matrix(r)) > 1, matrix(r)
