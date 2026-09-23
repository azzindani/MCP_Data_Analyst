"""Every tool that says which columns are dates gives the same answer.

A CSV's dates arrive as text. generate_dashboard and auto_detect_schema read
Ad_Data.csv's `Date` (257 ISO dates) as a date; inspect_dataset, run_eda and
generate_auto_profile asked only the dtype and filed it as categorical, so a
caller reading inspect concluded the file had no time axis. The three now use
the dashboard's rule (shared/column_utils.read_dates / date_like).

The fixture has ISO dates, month-name dates, an ID that is not a date, and a
column only half of which are dates -- which is not a date column.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (
    str(ROOT),
    str(ROOT / "servers" / "data_basic"),
    str(ROOT / "servers" / "data_medium"),
    str(ROOT / "servers" / "data_advanced"),
):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import _adv_eda  # noqa: E402
import _adv_profile  # noqa: E402
import _med_inspect  # noqa: E402

from servers.data_basic.engine import inspect_dataset  # noqa: E402

DATES = {"Date", "when"}


@pytest.fixture
def csv(tmp_path, monkeypatch) -> str:
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    frame = pd.DataFrame(
        {
            "Date": ["2019-10-16", "2019-10-17", "2019-11-02", "2020-01-05"] * 5,
            "when": ["16 Oct 2019", "17 Oct 2019", "2 Nov 2019", "5 Jan 2020"] * 5,
            "sku": ["SKU-001", "SKU-002", "SKU-003", "SKU-004"] * 5,
            "half": ["2019-10-16", "north", "2019-11-02", "south"] * 5,
            "region": ["north", "south", "east", "west"] * 5,
            "units": [1, 2, 3, 4] * 5,
        }
    )
    path = tmp_path / "t.csv"
    frame.to_csv(path, index=False)
    return str(path)


def _schema_dates(csv: str) -> set[str]:
    columns = _med_inspect.auto_detect_schema(csv)["columns"]
    return {name for name, info in columns.items() if info["inferred_type"] == "datetime"}


def test_the_reference_is_what_the_fixture_says(csv):
    assert _schema_dates(csv) == DATES


class TestInspectDataset:
    def test_it_lists_the_dates(self, csv):
        result = inspect_dataset(csv)
        assert set(result["datetime_columns"]) == _schema_dates(csv)

    def test_a_date_is_not_also_a_category(self, csv):
        result = inspect_dataset(csv)
        assert not DATES & set(result["categorical_columns"])
        assert {"sku", "half", "region"} <= set(result["categorical_columns"])


class TestTheReports:
    def test_run_eda_counts_them(self, csv):
        result = _adv_eda.run_eda(csv, mode="minimal")
        assert result["success"] is True, result.get("error")
        assert result["datetime_columns"] == len(_schema_dates(csv))

    def test_generate_auto_profile_counts_them(self, csv, tmp_path):
        result = _adv_profile.generate_auto_profile(csv, output_path=str(tmp_path / "p.html"))
        assert result["success"] is True, result.get("error")
        assert result["datetime_columns"] == len(_schema_dates(csv))

    def test_the_full_eda_page_still_renders(self, csv, tmp_path):
        result = _adv_eda.run_eda(csv, output_path=str(tmp_path / "eda.html"))
        assert result["success"] is True, result.get("error")
        assert (tmp_path / "eda.html").stat().st_size > 0
