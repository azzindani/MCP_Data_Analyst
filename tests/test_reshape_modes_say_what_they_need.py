"""Two reshape modes answered a missing argument badly; one of them silently.

reshape_dataset's docstring is "Reshape data. mode: pivot melt split_column
combine_columns transpose." It names the modes; each mode then needs its own
extra argument, and the modes did not agree about how to say so. pivot answers
"pivot requires 'index' parameter" and crosstab (in aggregate_dataset next door)
answers "crosstab requires 'row_col' and 'col_col'". The other two did not:

* split_column with no split_column argument answered "split_column '' not in
  dataset", sending a caller who omitted the argument off to look for a column
  named "".

* combine_columns with no combine_columns list **reported success**. `df[[]]`
  has nothing to join, so it wrote a new column full of NaN into the output file
  and told the caller the reshape had worked. A wrong answer presented as a
  right one is worse than a refusal.

Both were found by calling every mode the docstring names with nothing but the
required arguments.
"""

from __future__ import annotations

import csv as csvmod
from pathlib import Path

import pandas as pd
import pytest

from servers.data_transform.engine import reshape_dataset


@pytest.fixture()
def csv_path(tmp_path: Path) -> str:
    path = tmp_path / "campaigns.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csvmod.writer(fh)
        w.writerow(["campaign_platform", "campaign_type", "spends"])
        w.writerow(["Google Ads", "Search", 100])
        w.writerow(["Facebook Ads", "Conversions", 200])
    return str(path)


class TestCombineColumnsWithNothingToCombine:
    def test_it_no_longer_reports_success(self, csv_path: str, tmp_path: Path):
        r = reshape_dataset(csv_path, "combine_columns", output_path=str(tmp_path / "o.csv"))
        assert r["success"] is False, r

    def test_it_writes_no_file(self, csv_path: str, tmp_path: Path):
        out = tmp_path / "o.csv"
        reshape_dataset(csv_path, "combine_columns", output_path=str(out))
        assert not out.exists()

    def test_the_error_names_the_missing_argument(self, csv_path: str, tmp_path: Path):
        r = reshape_dataset(csv_path, "combine_columns", output_path=str(tmp_path / "o.csv"))
        assert "combine_columns" in r["error"], r["error"]

    def test_the_hint_shows_real_columns_to_use(self, csv_path: str, tmp_path: Path):
        r = reshape_dataset(csv_path, "combine_columns", output_path=str(tmp_path / "o.csv"))
        assert "campaign_platform" in r["hint"], r["hint"]


class TestCombineColumnsStillWorks:
    def test_it_joins_the_named_columns(self, csv_path: str, tmp_path: Path):
        out = tmp_path / "o.csv"
        r = reshape_dataset(
            csv_path,
            "combine_columns",
            combine_columns=["campaign_platform", "campaign_type"],
            output_path=str(out),
        )
        assert r["success"] is True, r.get("error")
        assert list(pd.read_csv(out)["combined"]) == ["Google Ads_Search", "Facebook Ads_Conversions"]

    def test_a_column_that_is_not_there_is_still_reported(self, csv_path: str, tmp_path: Path):
        r = reshape_dataset(csv_path, "combine_columns", combine_columns=["ghost"], output_path=str(tmp_path / "o.csv"))
        assert r["success"] is False
        assert "ghost" in r["error"], r["error"]


class TestSplitColumnWithNothingToSplit:
    def test_the_error_does_not_pretend_a_column_was_named(self, csv_path: str, tmp_path: Path):
        r = reshape_dataset(csv_path, "split_column", output_path=str(tmp_path / "o.csv"))
        assert r["success"] is False
        assert "''" not in r["error"], r["error"]

    def test_it_says_the_parameter_is_required(self, csv_path: str, tmp_path: Path):
        r = reshape_dataset(csv_path, "split_column", output_path=str(tmp_path / "o.csv"))
        assert "requires" in r["error"] and "split_column" in r["error"], r["error"]

    def test_a_column_that_is_not_there_still_says_so(self, csv_path: str, tmp_path: Path):
        r = reshape_dataset(csv_path, "split_column", split_column="ghost", output_path=str(tmp_path / "o.csv"))
        assert r["success"] is False
        assert "ghost" in r["error"], r["error"]

    def test_splitting_a_real_column_still_works(self, csv_path: str, tmp_path: Path):
        out = tmp_path / "o.csv"
        r = reshape_dataset(
            csv_path,
            "split_column",
            split_column="campaign_platform",
            delimiter=" ",
            output_path=str(out),
        )
        assert r["success"] is True, r.get("error")
        assert "campaign_platform_0" in pd.read_csv(out).columns


class TestTheModesThatNeedNothingExtraAreUnchanged:
    @pytest.mark.parametrize("mode", ["melt", "transpose"])
    def test_they_still_run(self, mode: str, csv_path: str, tmp_path: Path):
        r = reshape_dataset(csv_path, mode, output_path=str(tmp_path / f"{mode}.csv"))
        assert r["success"] is True, r.get("error")

    def test_pivot_still_names_its_own_missing_argument(self, csv_path: str, tmp_path: Path):
        r = reshape_dataset(csv_path, "pivot", output_path=str(tmp_path / "o.csv"))
        assert r["success"] is False
        assert "index" in r["error"], r["error"]
