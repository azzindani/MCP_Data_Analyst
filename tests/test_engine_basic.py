"""Tests for servers/data_basic/engine.py — ≥90% coverage required."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pandas as pd
import pytest

from servers.data_basic.engine import (
    apply_patch,
    inspect_dataset,
    load_dataset,
    read_column_stats,
    read_receipt,
    restore_version,
    search_columns,
)

# ---------------------------------------------------------------------------
# load_dataset
# ---------------------------------------------------------------------------


class TestLoadDataset:
    def test_success_simple(self, simple_csv):
        r = load_dataset(str(simple_csv))
        assert r["success"] is True
        assert r["rows"] == 5
        assert r["columns"] == 7
        assert "Revenue" in r["dtypes"]
        assert "token_estimate" in r
        assert len(r["sample"]) == 2

    def test_encoding_iso(self, tmp_path):
        # Write a CSV with ISO-8859-1 encoding (accented char)
        f = tmp_path / "iso.csv"
        f.write_bytes("Région,Value\ncafé,100\n".encode("ISO-8859-1"))
        r = load_dataset(str(f), encoding="ISO-8859-1")
        assert r["success"] is True
        assert r["rows"] == 1

    def test_file_not_found(self, tmp_path):
        r = load_dataset(str(tmp_path / "missing.csv"))
        assert r["success"] is False
        assert "not found" in r["error"].lower()
        assert "hint" in r

    def test_wrong_extension(self, tmp_path):
        f = tmp_path / "data.xlsx"
        f.write_text("fake")
        r = load_dataset(str(f))
        assert r["success"] is False
        assert ".xlsx" in r["error"]

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.csv"
        f.write_text("")
        r = load_dataset(str(f))
        assert r["success"] is False

    def test_max_rows_sampling(self, large_csv):
        r = load_dataset(str(large_csv), max_rows=10)
        assert r["success"] is True
        assert r["rows"] == 10
        # warn should appear in progress
        assert any(p["status"] == "warn" for p in r["progress"])

    def test_constrained_mode_warn(self, large_csv, monkeypatch):
        monkeypatch.setenv("MCP_CONSTRAINED_MODE", "1")
        import importlib

        import shared.platform_utils as pu

        importlib.reload(pu)
        r = load_dataset(str(large_csv))
        assert r["success"] is True
        # Progress may contain a warn about large dataset
        assert "token_estimate" in r

    def test_null_counts_present(self, messy_csv):
        r = load_dataset(str(messy_csv))
        assert r["success"] is True
        assert "null_counts" in r
        # revenue column has nulls in messy fixture
        assert r["null_counts"].get("revenue", 0) > 0

    def test_progress_contains_ok(self, simple_csv):
        r = load_dataset(str(simple_csv))
        assert any(p["status"] == "ok" for p in r["progress"])


# ---------------------------------------------------------------------------
# inspect_dataset
# ---------------------------------------------------------------------------


class TestInspectDataset:
    def test_success(self, simple_csv):
        r = inspect_dataset(str(simple_csv))
        assert r["success"] is True
        assert r["rows"] == 5
        assert "numeric_columns" in r
        assert "categorical_columns" in r
        assert "datetime_columns" in r
        assert "null_pct" in r

    def test_include_sample_false(self, simple_csv):
        r = inspect_dataset(str(simple_csv), include_sample=False)
        assert "sample" not in r

    def test_include_sample_true(self, simple_csv):
        r = inspect_dataset(str(simple_csv), include_sample=True)
        assert "sample" in r
        assert len(r["sample"]) == 2

    def test_file_not_found(self, tmp_path):
        r = inspect_dataset(str(tmp_path / "no.csv"))
        assert r["success"] is False

    def test_column_classification(self, simple_csv):
        r = inspect_dataset(str(simple_csv))
        assert "Revenue" in r["numeric_columns"]
        assert "Region" in r["categorical_columns"]

    def test_null_pct_computation(self, messy_csv):
        r = inspect_dataset(str(messy_csv))
        assert r["success"] is True
        # revenue has nulls → null_pct should be > 0
        assert r["null_pct"].get("revenue", 0) > 0

    def test_token_estimate_present(self, simple_csv):
        r = inspect_dataset(str(simple_csv))
        assert r["token_estimate"] > 0


# ---------------------------------------------------------------------------
# read_column_stats
# ---------------------------------------------------------------------------


class TestReadColumnStats:
    def test_numeric_column(self, simple_csv):
        r = read_column_stats(str(simple_csv), "Revenue")
        assert r["success"] is True
        assert "mean" in r
        assert "median" in r
        assert "std" in r
        assert "q1" in r
        assert "q3" in r
        assert "iqr" in r
        assert "outlier_count_iqr" in r
        assert "zero_count" in r

    def test_categorical_column(self, simple_csv):
        r = read_column_stats(str(simple_csv), "Region")
        assert r["success"] is True
        assert "top_values" in r
        assert "unique_count" in r
        assert "mean" not in r

    def test_column_not_found(self, simple_csv):
        r = read_column_stats(str(simple_csv), "NonExistent")
        assert r["success"] is False
        assert "hint" in r
        assert "Revenue" in r["hint"] or "inspect_dataset" in r["hint"]

    def test_all_nulls_no_crash(self, tmp_path):
        f = tmp_path / "nulls.csv"
        f.write_text("A,B\n,\n,\n,\n")
        r = read_column_stats(str(f), "A")
        assert r["success"] is True
        assert r["null_count"] == 3

    def test_values_correct(self, simple_csv):
        r = read_column_stats(str(simple_csv), "Revenue")
        assert r["mean"] == pytest.approx(5320.0, rel=0.01)

    def test_token_estimate(self, simple_csv):
        r = read_column_stats(str(simple_csv), "Revenue")
        assert r["token_estimate"] > 0


# ---------------------------------------------------------------------------
# search_columns
# ---------------------------------------------------------------------------


class TestSearchColumns:
    def test_has_nulls(self, messy_csv):
        r = search_columns(str(messy_csv), has_nulls=True)
        assert r["success"] is True
        # messy has nulls in revenue and region
        assert r["matched"] > 0
        for col in r["columns"]:
            assert r["null_counts"][col] > 0

    def test_has_zeros(self, messy_csv):
        r = search_columns(str(messy_csv), has_zeros=True)
        assert r["success"] is True
        for col in r["columns"]:
            assert r["zero_counts"][col] > 0

    def test_dtype_numeric(self, simple_csv):
        r = search_columns(str(simple_csv), dtype="numeric")
        assert r["success"] is True
        assert "Revenue" in r["columns"]
        assert "Region" not in r["columns"]

    def test_dtype_object(self, simple_csv):
        r = search_columns(str(simple_csv), dtype="object")
        assert "Region" in r["columns"]
        assert "Revenue" not in r["columns"]

    def test_name_contains_case_insensitive(self, simple_csv):
        r = search_columns(str(simple_csv), name_contains="revenue")
        assert r["success"] is True
        assert "Revenue" in r["columns"]

    def test_no_criteria_returns_all(self, simple_csv):
        r = search_columns(str(simple_csv))
        assert r["matched"] == 7  # all 7 columns

    def test_no_match_returns_empty_not_error(self, simple_csv):
        r = search_columns(str(simple_csv), name_contains="xyzzy_no_match")
        assert r["success"] is True
        assert r["matched"] == 0
        assert r["columns"] == []

    def test_truncated_flag(self, large_csv, monkeypatch):
        monkeypatch.setenv("MCP_CONSTRAINED_MODE", "1")
        import importlib

        import shared.platform_utils as pu

        importlib.reload(pu)
        r = search_columns(str(large_csv))
        # large csv has 5 columns so constrained mode (10) won't truncate, but verify field exists
        assert "truncated" in r


# ---------------------------------------------------------------------------
# apply_patch
# ---------------------------------------------------------------------------


class TestApplyPatch:
    def test_fill_nulls_success(self, messy_csv):
        r = apply_patch(
            str(messy_csv),
            [{"op": "fill_nulls", "column": "revenue", "strategy": "median"}],
        )
        assert r["success"] is True
        assert r["applied"] == 1
        assert "backup" in r
        assert ".mcp_versions" in r["backup"]
        # Verify file was changed
        df = pd.read_csv(str(messy_csv))
        assert df["revenue"].isna().sum() == 0

    def test_backup_created(self, messy_csv):
        apply_patch(
            str(messy_csv),
            [{"op": "fill_nulls", "column": "revenue", "strategy": "mean"}],
        )
        versions_dir = messy_csv.parent / ".mcp_versions"
        assert versions_dir.exists()
        assert len(list(versions_dir.glob("*.bak"))) >= 1

    def test_drop_duplicates(self, messy_csv):
        r = apply_patch(str(messy_csv), [{"op": "drop_duplicates"}])
        assert r["success"] is True
        df_after = pd.read_csv(str(messy_csv))
        assert len(df_after) < 7  # messy has 2 duplicate rows

    def test_drop_column(self, simple_csv):
        r = apply_patch(str(simple_csv), [{"op": "drop_column", "columns": ["Discount"]}])
        assert r["success"] is True
        df = pd.read_csv(str(simple_csv))
        assert "Discount" not in df.columns

    def test_drop_column_nonexistent(self, simple_csv):
        r = apply_patch(str(simple_csv), [{"op": "drop_column", "columns": ["NoSuchCol"]}])
        assert r["success"] is False
        assert "backup" in r  # snapshot still returned

    def test_cast_column_to_datetime(self, simple_csv):
        r = apply_patch(
            str(simple_csv),
            [{"op": "cast_column", "column": "Order Date", "dtype": "datetime"}],
        )
        assert r["success"] is True
        result = r["results"][0]
        assert "datetime" in result["to"].lower()

    def test_cast_column_bad_values_tracked(self, messy_csv):
        r = apply_patch(
            str(messy_csv),
            [{"op": "cast_column", "column": "Order Date", "dtype": "datetime"}],
        )
        assert r["success"] is True
        # "not-a-date" row should increment failed count
        assert r["results"][0]["failed"] >= 1

    def test_fill_nulls_fill_zeros(self, messy_csv):
        r = apply_patch(
            str(messy_csv),
            [
                {
                    "op": "fill_nulls",
                    "column": "Units Sold",
                    "strategy": "mean",
                    "fill_zeros": True,
                }
            ],
        )
        assert r["success"] is True

    def test_cap_outliers_iqr(self, large_csv):
        r = apply_patch(
            str(large_csv),
            [{"op": "cap_outliers", "column": "Revenue", "method": "iqr"}],
        )
        assert r["success"] is True
        res = r["results"][0]
        assert "lower_limit" in res
        assert "upper_limit" in res

    def test_add_column_math(self, simple_csv):
        r = apply_patch(
            str(simple_csv),
            [
                {
                    "op": "add_column",
                    "name": "Rev_Per_Unit",
                    "expr": "Revenue / Units Sold",
                    "mode": "math",
                }
            ],
        )
        assert r["success"] is True
        df = pd.read_csv(str(simple_csv))
        assert "Rev_Per_Unit" in df.columns
        # First row: 5000 / 10 = 500
        assert df["Rev_Per_Unit"].iloc[0] == pytest.approx(500.0)

    def test_add_column_threshold(self, simple_csv):
        r = apply_patch(
            str(simple_csv),
            [
                {
                    "op": "add_column",
                    "name": "Top_Region",
                    "source": "Region",
                    "mode": "threshold",
                    "threshold": 2,
                }
            ],
        )
        assert r["success"] is True
        df = pd.read_csv(str(simple_csv))
        assert "Top_Region" in df.columns

    def test_clean_text_headers(self, messy_csv):
        r = apply_patch(str(messy_csv), [{"op": "clean_text", "scope": "headers"}])
        assert r["success"] is True
        df = pd.read_csv(str(messy_csv))
        # Columns should be title-cased
        for col in df.columns:
            assert col == col.strip()

    def test_replace_values(self, simple_csv):
        r = apply_patch(
            str(simple_csv),
            [
                {
                    "op": "replace_values",
                    "column": "Region",
                    "mapping": {"West": "Pacific"},
                }
            ],
        )
        assert r["success"] is True
        df = pd.read_csv(str(simple_csv))
        assert "Pacific" in df["Region"].values

    def test_dry_run_no_changes(self, simple_csv):
        original_content = simple_csv.read_text()
        r = apply_patch(
            str(simple_csv),
            [{"op": "fill_nulls", "column": "Revenue", "strategy": "mean"}],
            dry_run=True,
        )
        assert r["success"] is True
        assert r["dry_run"] is True
        assert "would_change" in r
        assert simple_csv.read_text() == original_content  # file unchanged

    def test_unknown_op_rejected(self, simple_csv):
        r = apply_patch(str(simple_csv), [{"op": "explode_table"}])
        assert r["success"] is False
        assert "hint" in r

    def test_multi_op_success(self, messy_csv):
        r = apply_patch(
            str(messy_csv),
            [
                {"op": "fill_nulls", "column": "revenue", "strategy": "median"},
                {"op": "drop_duplicates"},
            ],
        )
        assert r["success"] is True
        assert r["applied"] == 2

    def test_receipt_appended(self, simple_csv):
        apply_patch(str(simple_csv), [{"op": "drop_duplicates"}])
        r = read_receipt(str(simple_csv))
        assert r["total_entries"] >= 1

    def test_drop_duplicates_subset(self, messy_csv):
        r = apply_patch(str(messy_csv), [{"op": "drop_duplicates", "subset": ["State", "City"]}])
        assert r["success"] is True

    def test_file_not_found(self, tmp_path):
        r = apply_patch(str(tmp_path / "nope.csv"), [{"op": "drop_duplicates"}])
        assert r["success"] is False


# ---------------------------------------------------------------------------
# restore_version
# ---------------------------------------------------------------------------


class TestRestoreVersion:
    def test_restore_most_recent(self, simple_csv):
        _ = simple_csv.read_text()
        apply_patch(str(simple_csv), [{"op": "drop_column", "columns": ["Discount"]}])
        assert "Discount" not in simple_csv.read_text()
        r = restore_version(str(simple_csv))
        assert r["success"] is True
        assert "Discount" in simple_csv.read_text()

    def test_restore_content_matches_backup(self, simple_csv):
        original_content = simple_csv.read_text()
        apply_patch(str(simple_csv), [{"op": "drop_duplicates"}])
        restore_version(str(simple_csv))
        assert simple_csv.read_text() == original_content

    def test_restore_no_backups(self, tmp_path):
        f = tmp_path / "fresh.csv"
        f.write_text("a,b\n1,2")
        r = restore_version(str(f))
        assert r["success"] is False
        assert "hint" in r

    def test_restore_timestamp_not_found(self, simple_csv):
        apply_patch(str(simple_csv), [{"op": "drop_duplicates"}])
        r = restore_version(str(simple_csv), timestamp="9999-01-01")
        assert r["success"] is False
        assert "available_versions" in r

    def test_available_versions_in_response(self, simple_csv):
        apply_patch(str(simple_csv), [{"op": "drop_duplicates"}])
        r = restore_version(str(simple_csv))
        assert "available_versions" in r
        assert len(r["available_versions"]) >= 1

    def test_counter_snapshot_created(self, simple_csv):
        apply_patch(str(simple_csv), [{"op": "drop_duplicates"}])
        versions_before = len(list((simple_csv.parent / ".mcp_versions").glob("*.bak")))
        restore_version(str(simple_csv))
        versions_after = len(list((simple_csv.parent / ".mcp_versions").glob("*.bak")))
        assert versions_after > versions_before


# ---------------------------------------------------------------------------
# read_receipt
# ---------------------------------------------------------------------------


class TestReadReceipt:
    def test_no_receipt_returns_empty(self, simple_csv):
        r = read_receipt(str(simple_csv))
        assert r["success"] is True
        assert r["entries"] == []
        assert r["total_entries"] == 0

    def test_receipt_after_patch(self, simple_csv):
        apply_patch(str(simple_csv), [{"op": "drop_duplicates"}])
        r = read_receipt(str(simple_csv))
        assert r["total_entries"] >= 1
        assert r["entries"][0]["tool"] == "apply_patch"

    def test_last_n_respected(self, simple_csv):
        for _ in range(5):
            apply_patch(str(simple_csv), [{"op": "drop_duplicates"}])
        r = read_receipt(str(simple_csv), last_n=3)
        assert r["returned"] == 3

    def test_entries_descending(self, simple_csv):
        apply_patch(str(simple_csv), [{"op": "drop_duplicates"}])
        apply_patch(str(simple_csv), [{"op": "drop_column", "columns": ["Discount"]}])
        r = read_receipt(str(simple_csv))
        ts_list = [e["ts"] for e in r["entries"]]
        assert ts_list == sorted(ts_list, reverse=True)

    def test_token_estimate_present(self, simple_csv):
        r = read_receipt(str(simple_csv))
        assert "token_estimate" in r


# ---------------------------------------------------------------------------
# apply_patch — new ops
# ---------------------------------------------------------------------------


class TestApplyPatchNewOps:
    def test_normalize_minmax(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("Value\n10\n20\n30\n40\n50\n")
        r = apply_patch(str(f), [{"op": "normalize", "column": "Value", "method": "minmax"}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert df["Value"].min() >= 0.0
        assert df["Value"].max() <= 1.0
        res = r["results"][0]
        assert res["op"] == "normalize"
        assert res["method"] == "minmax"
        assert "min" in res and "max" in res

    def test_normalize_zscore(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("Value\n10\n20\n30\n40\n50\n")
        r = apply_patch(str(f), [{"op": "normalize", "column": "Value", "method": "zscore"}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert abs(df["Value"].mean()) < 1e-9

    def test_label_encode(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("Color\nRed\nBlue\nGreen\nRed\nBlue\n")
        r = apply_patch(str(f), [{"op": "label_encode", "column": "Color"}])
        assert r["success"] is True
        res = r["results"][0]
        assert res["op"] == "label_encode"
        assert "encoding" in res
        assert res["unique_count"] == 3
        df = pd.read_csv(str(f))
        # All values should be integers now
        assert pd.api.types.is_numeric_dtype(df["Color"])
        # Encoding should be alphabetical: Blue=0, Green=1, Red=2
        assert res["encoding"]["Blue"] == 0
        assert res["encoding"]["Green"] == 1
        assert res["encoding"]["Red"] == 2

    def test_extract_regex(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("Text\nOrder123\nItem456\nNoMatch\n")
        r = apply_patch(
            str(f),
            [
                {
                    "op": "extract_regex",
                    "column": "Text",
                    "pattern": r"\d+",
                    "new_column": "Digits",
                }
            ],
        )
        assert r["success"] is True
        res = r["results"][0]
        assert res["op"] == "extract_regex"
        assert res["matched"] == 2
        assert res["failed"] == 1
        df = pd.read_csv(str(f), dtype={"Digits": str})
        assert "Digits" in df.columns
        assert df["Digits"].iloc[0] == "123"
        assert df["Digits"].iloc[1] == "456"

    def test_date_diff_days(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("Start,End\n2024-01-10,2024-01-01\n2024-03-15,2024-03-01\n")
        r = apply_patch(
            str(f),
            [
                {
                    "op": "date_diff",
                    "date_col_a": "Start",
                    "date_col_b": "End",
                    "new_column": "DiffDays",
                    "unit": "days",
                }
            ],
        )
        assert r["success"] is True
        res = r["results"][0]
        assert res["op"] == "date_diff"
        assert res["unit"] == "days"
        assert res["null_count"] == 0
        df = pd.read_csv(str(f))
        assert "DiffDays" in df.columns
        assert int(df["DiffDays"].iloc[0]) == 9
        assert int(df["DiffDays"].iloc[1]) == 14

    def test_rank_column(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("Score\n30\n10\n20\n10\n")
        r = apply_patch(
            str(f),
            [
                {
                    "op": "rank_column",
                    "column": "Score",
                    "ascending": True,
                    "method": "dense",
                }
            ],
        )
        assert r["success"] is True
        res = r["results"][0]
        assert res["op"] == "rank_column"
        assert res["new_column"] == "Score_rank"
        assert res["ascending"] is True
        df = pd.read_csv(str(f))
        assert "Score_rank" in df.columns
        # Dense rank ascending: 10→1, 10→1, 20→2, 30→3
        ranks = df.set_index("Score")["Score_rank"].to_dict()
        assert ranks[10] == 1.0
        assert ranks[20] == 2.0
        assert ranks[30] == 3.0


# ---------------------------------------------------------------------------
# apply_patch — extended ops (filtering, numeric, encoding, temporal, structural)
# ---------------------------------------------------------------------------


class TestApplyPatchFilterSort:
    def test_sort_ascending(self, tmp_path):
        f = tmp_path / "sort.csv"
        f.write_text("Score,Name\n30,C\n10,A\n20,B\n")
        r = apply_patch(str(f), [{"op": "sort", "by": ["Score"], "ascending": True}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert list(df["Score"]) == [10, 20, 30]

    def test_sort_descending(self, tmp_path):
        f = tmp_path / "sort.csv"
        f.write_text("Score,Name\n30,C\n10,A\n20,B\n")
        r = apply_patch(str(f), [{"op": "sort", "by": ["Score"], "ascending": False}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert list(df["Score"]) == [30, 20, 10]

    def test_filter_isin(self, tmp_path):
        f = tmp_path / "isin.csv"
        f.write_text("Region,Revenue\nWest,100\nEast,200\nNorth,300\n")
        r = apply_patch(str(f), [{"op": "filter_isin", "column": "Region", "values": ["West", "East"]}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert len(df) == 2
        assert "North" not in df["Region"].values

    def test_filter_not_isin(self, tmp_path):
        f = tmp_path / "isin.csv"
        f.write_text("Region,Revenue\nWest,100\nEast,200\nNorth,300\n")
        r = apply_patch(str(f), [{"op": "filter_not_isin", "column": "Region", "values": ["West"]}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert "West" not in df["Region"].values
        assert len(df) == 2

    def test_filter_between(self, tmp_path):
        f = tmp_path / "between.csv"
        f.write_text("Score\n5\n15\n25\n35\n")
        r = apply_patch(str(f), [{"op": "filter_between", "column": "Score", "min": 10, "max": 30}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert all(10 <= v <= 30 for v in df["Score"])

    def test_filter_date_range(self, tmp_path):
        f = tmp_path / "dates.csv"
        f.write_text("Date,Value\n2023-01-01,1\n2023-06-01,2\n2024-01-01,3\n")
        r = apply_patch(
            str(f),
            [{"op": "filter_date_range", "column": "Date", "start": "2023-01-01", "end": "2023-12-31"}],
        )
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert len(df) == 2

    def test_filter_regex(self, tmp_path):
        f = tmp_path / "regex.csv"
        f.write_text("Code\nABC123\nXYZ456\nABC789\n")
        r = apply_patch(str(f), [{"op": "filter_regex", "column": "Code", "pattern": "^ABC"}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert len(df) == 2
        assert all(str(v).startswith("ABC") for v in df["Code"])

    def test_filter_quantile(self, tmp_path):
        f = tmp_path / "quant.csv"
        f.write_text("Value\n" + "\n".join(str(i) for i in range(1, 101)) + "\n")
        r = apply_patch(str(f), [{"op": "filter_quantile", "column": "Value", "min_q": 0.25, "max_q": 0.75}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        # Should keep middle 50%
        assert len(df) <= 52  # allow boundary inclusion

    def test_filter_top_n(self, tmp_path):
        f = tmp_path / "topn.csv"
        f.write_text("Score\n10\n30\n20\n50\n40\n")
        r = apply_patch(str(f), [{"op": "filter_top_n", "column": "Score", "n": 3, "keep": "top"}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert len(df) == 3
        assert df["Score"].min() >= 20

    def test_dedup_subset(self, tmp_path):
        f = tmp_path / "dup.csv"
        f.write_text("A,B,C\n1,x,10\n1,x,20\n2,y,30\n")
        r = apply_patch(str(f), [{"op": "dedup_subset", "columns": ["A", "B"], "keep": "first"}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert len(df) == 2


class TestApplyPatchNumericTransforms:
    def test_log_transform_log1p(self, tmp_path):
        f = tmp_path / "log.csv"
        f.write_text("Value\n0\n9\n99\n")
        r = apply_patch(str(f), [{"op": "log_transform", "column": "Value", "method": "log1p"}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        import math

        assert abs(df["Value"].iloc[0] - math.log1p(0)) < 1e-9
        assert abs(df["Value"].iloc[1] - math.log1p(9)) < 1e-9

    def test_sqrt_transform(self, tmp_path):
        f = tmp_path / "sqrt.csv"
        f.write_text("Value\n4\n9\n16\n")
        r = apply_patch(str(f), [{"op": "sqrt_transform", "column": "Value"}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert abs(df["Value"].iloc[0] - 2.0) < 1e-9
        assert abs(df["Value"].iloc[1] - 3.0) < 1e-9

    def test_robust_scale(self, tmp_path):
        f = tmp_path / "robust.csv"
        f.write_text("Value\n10\n20\n30\n40\n50\n")
        r = apply_patch(str(f), [{"op": "robust_scale", "column": "Value"}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        # Median of scaled values should be ~0
        assert abs(df["Value"].median()) < 0.01

    def test_winsorize(self, tmp_path):
        f = tmp_path / "winsor.csv"
        f.write_text("Value\n1\n2\n3\n4\n5\n6\n7\n8\n9\n100\n")
        r = apply_patch(str(f), [{"op": "winsorize", "column": "Value", "upper_q": 0.9}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert df["Value"].max() < 100

    def test_bin_column(self, tmp_path):
        f = tmp_path / "bin.csv"
        f.write_text("Score\n10\n30\n50\n70\n90\n")
        r = apply_patch(str(f), [{"op": "bin_column", "column": "Score", "bins": 3}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert "Score_bin" in df.columns

    def test_qbin_column(self, tmp_path):
        f = tmp_path / "qbin.csv"
        f.write_text("Score\n" + "\n".join(str(i) for i in range(1, 21)) + "\n")
        r = apply_patch(str(f), [{"op": "qbin_column", "column": "Score", "q": 4}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert "Score_qbin" in df.columns

    def test_clip_values(self, tmp_path):
        f = tmp_path / "clip.csv"
        f.write_text("Value\n-10\n5\n50\n")
        r = apply_patch(str(f), [{"op": "clip_values", "column": "Value", "min": 0, "max": 10}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert df["Value"].min() >= 0
        assert df["Value"].max() <= 10

    def test_round_values(self, tmp_path):
        f = tmp_path / "round.csv"
        f.write_text("Value\n3.14159\n2.71828\n1.41421\n")
        r = apply_patch(str(f), [{"op": "round_values", "column": "Value", "decimals": 2}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert df["Value"].iloc[0] == pytest.approx(3.14)

    def test_abs_values(self, tmp_path):
        f = tmp_path / "abs.csv"
        f.write_text("Value\n-5\n3\n-7\n")
        r = apply_patch(str(f), [{"op": "abs_values", "column": "Value"}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert all(v >= 0 for v in df["Value"])


class TestApplyPatchEncoding:
    def test_ordinal_encode(self, tmp_path):
        f = tmp_path / "ordinal.csv"
        f.write_text("Size\nSmall\nMedium\nLarge\nSmall\n")
        r = apply_patch(
            str(f),
            [{"op": "ordinal_encode", "column": "Size", "order": ["Small", "Medium", "Large"]}],
        )
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert df["Size"].iloc[0] == 0  # Small → 0
        assert df["Size"].iloc[1] == 1  # Medium → 1
        assert df["Size"].iloc[2] == 2  # Large → 2

    def test_binary_encode_categorical(self, tmp_path):
        f = tmp_path / "binary.csv"
        f.write_text("Flag\nyes\nno\nyes\nno\n")
        r = apply_patch(str(f), [{"op": "binary_encode", "column": "Flag", "value": "yes"}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert "Flag_binary" in df.columns
        assert df["Flag_binary"].iloc[0] == 1
        assert df["Flag_binary"].iloc[1] == 0

    def test_frequency_encode(self, tmp_path):
        f = tmp_path / "freq.csv"
        f.write_text("Color\nRed\nBlue\nRed\nGreen\nRed\n")
        r = apply_patch(str(f), [{"op": "frequency_encode", "column": "Color"}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert "Color_freq" in df.columns
        # Red appears 3x → should have highest freq
        red_freq = df.loc[df["Color"] == "Red", "Color_freq"].iloc[0]
        blue_freq = df.loc[df["Color"] == "Blue", "Color_freq"].iloc[0]
        assert red_freq > blue_freq


class TestApplyPatchTemporal:
    @pytest.fixture()
    def ts_csv(self, tmp_path):
        f = tmp_path / "ts.csv"
        f.write_text("Value\n10\n20\n30\n40\n50\n")
        return f

    def test_lag(self, ts_csv):
        r = apply_patch(str(ts_csv), [{"op": "lag", "column": "Value", "periods": 1}])
        assert r["success"] is True
        df = pd.read_csv(str(ts_csv))
        assert "Value_lag1" in df.columns
        assert pd.isna(df["Value_lag1"].iloc[0])
        assert df["Value_lag1"].iloc[1] == 10.0

    def test_lead(self, ts_csv):
        r = apply_patch(str(ts_csv), [{"op": "lead", "column": "Value", "periods": 1}])
        assert r["success"] is True
        df = pd.read_csv(str(ts_csv))
        assert "Value_lead1" in df.columns
        assert df["Value_lead1"].iloc[0] == 20.0

    def test_diff(self, ts_csv):
        r = apply_patch(str(ts_csv), [{"op": "diff", "column": "Value", "periods": 1}])
        assert r["success"] is True
        df = pd.read_csv(str(ts_csv))
        assert "Value_diff1" in df.columns
        assert df["Value_diff1"].iloc[1] == 10.0  # 20-10

    def test_pct_change(self, ts_csv):
        r = apply_patch(str(ts_csv), [{"op": "pct_change", "column": "Value", "periods": 1}])
        assert r["success"] is True
        df = pd.read_csv(str(ts_csv))
        assert "Value_pct1" in df.columns
        assert abs(df["Value_pct1"].iloc[1] - 100.0) < 0.01  # 100% change

    def test_rolling_agg(self, ts_csv):
        r = apply_patch(
            str(ts_csv),
            [{"op": "rolling_agg", "column": "Value", "window": 3, "agg": "sum"}],
        )
        assert r["success"] is True
        df = pd.read_csv(str(ts_csv))
        assert "Value_roll3_sum" in df.columns

    def test_ewm(self, ts_csv):
        r = apply_patch(str(ts_csv), [{"op": "ewm", "column": "Value", "span": 3}])
        assert r["success"] is True
        df = pd.read_csv(str(ts_csv))
        assert "Value_ewm3" in df.columns

    def test_cumulative_sum(self, ts_csv):
        r = apply_patch(str(ts_csv), [{"op": "cumulative", "column": "Value", "agg": "sum"}])
        assert r["success"] is True
        df = pd.read_csv(str(ts_csv))
        assert "Value_cumsum" in df.columns
        assert df["Value_cumsum"].iloc[-1] == 150.0  # 10+20+30+40+50


class TestApplyPatchStructural:
    def test_column_math(self, tmp_path):
        f = tmp_path / "math.csv"
        f.write_text("A,B\n10,5\n20,4\n30,3\n")
        r = apply_patch(str(f), [{"op": "column_math", "formula": "A * B", "target_column": "Product"}])
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert "Product" in df.columns
        assert df["Product"].iloc[0] == 50.0

    def test_conditional_assign(self, tmp_path):
        f = tmp_path / "cond.csv"
        f.write_text("Score\n90\n60\n40\n75\n")
        r = apply_patch(
            str(f),
            [
                {
                    "op": "conditional_assign",
                    "new_column": "Grade",
                    "conditions": [
                        {"column": "Score", "op": "gte", "value": 80, "label": "A"},
                        {"column": "Score", "op": "gte", "value": 60, "label": "B"},
                    ],
                    "default": "C",
                }
            ],
        )
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert "Grade" in df.columns
        assert df["Grade"].iloc[0] == "A"
        assert df["Grade"].iloc[1] == "B"
        assert df["Grade"].iloc[2] == "C"

    def test_split_column(self, tmp_path):
        f = tmp_path / "split.csv"
        f.write_text("FullName\nJohn Doe\nJane Smith\nBob Jones\n")
        r = apply_patch(
            str(f),
            [{"op": "split_column", "column": "FullName", "delimiter": " ", "new_columns": ["First", "Last"]}],
        )
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert "First" in df.columns
        assert "Last" in df.columns
        assert df["First"].iloc[0] == "John"
        assert df["Last"].iloc[0] == "Doe"

    def test_combine_columns(self, tmp_path):
        f = tmp_path / "combine.csv"
        f.write_text("First,Last\nJohn,Doe\nJane,Smith\n")
        r = apply_patch(
            str(f),
            [{"op": "combine_columns", "columns": ["First", "Last"], "delimiter": " ", "new_column": "FullName"}],
        )
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert "FullName" in df.columns
        assert df["FullName"].iloc[0] == "John Doe"

    def test_regex_replace(self, tmp_path):
        f = tmp_path / "regex.csv"
        f.write_text("Code\nABC-DEF\nXYZ-GHI\n")  # non-numeric so result stays string
        r = apply_patch(
            str(f),
            [{"op": "regex_replace", "column": "Code", "pattern": r"-", "replacement": "_"}],
        )
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert df["Code"].iloc[0] == "ABC_DEF"
        assert "-" not in str(df["Code"].iloc[0])

    def test_str_slice(self, tmp_path):
        f = tmp_path / "slice.csv"
        f.write_text("Code\nABC123\nXYZ456\n")
        # str_slice writes to new_column (default: Code_slice), not in-place
        r = apply_patch(
            str(f),
            [{"op": "str_slice", "column": "Code", "start": 0, "end": 3, "new_column": "Code_slice"}],
        )
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert "Code_slice" in df.columns
        assert df["Code_slice"].iloc[0] == "ABC"
        assert df["Code_slice"].iloc[1] == "XYZ"

    def test_melt(self, tmp_path):
        f = tmp_path / "wide.csv"
        f.write_text("ID,Q1,Q2,Q3\n1,100,200,300\n2,400,500,600\n")
        r = apply_patch(
            str(f),
            [{"op": "melt", "id_vars": ["ID"], "value_vars": ["Q1", "Q2", "Q3"]}],
        )
        assert r["success"] is True
        df = pd.read_csv(str(f))
        # melt converts wide to long: 2 IDs × 3 quarters = 6 rows
        assert len(df) == 6
        assert "variable" in df.columns
        assert "value" in df.columns

    def test_concat_file(self, tmp_path):
        f = tmp_path / "main.csv"
        f.write_text("A,B\n1,2\n3,4\n")
        extra = tmp_path / "extra.csv"
        extra.write_text("A,B\n5,6\n7,8\n")
        r = apply_patch(
            str(f),
            [{"op": "concat_file", "file_path": str(extra), "direction": "rows"}],
        )
        assert r["success"] is True
        df = pd.read_csv(str(f))
        assert len(df) == 4


# ---------------------------------------------------------------------------
# Docstring length CI check
# ---------------------------------------------------------------------------


def test_server_docstrings_lte_80_chars():
    """All @mcp.tool() docstrings must be ≤ 80 characters."""
    from servers.data_basic import server

    tool_funcs = [
        server.load_dataset,
        server.load_geo_dataset,
        server.inspect_dataset,
        server.read_column_stats,
        server.search_columns,
        server.apply_patch,
        server.restore_version,
        server.read_receipt,
    ]
    for fn in tool_funcs:
        doc = fn.__doc__ or ""
        assert len(doc) <= 80, f"{fn.__name__} docstring too long ({len(doc)} chars): {doc!r}"


# ---------------------------------------------------------------------------
# list_patch_ops
# ---------------------------------------------------------------------------


class TestListPatchOps:
    def test_all_ops_returned(self):
        from servers.data_basic.engine import list_patch_ops

        r = list_patch_ops()
        assert r["success"] is True
        assert "ops" in r
        assert "total_ops" in r
        assert r["total_ops"] > 0

    def test_category_filter_numeric(self):
        from servers.data_basic.engine import list_patch_ops

        r = list_patch_ops(category="numeric")
        assert r["success"] is True
        assert len(r["ops"]) == 1
        assert "numeric" in r["ops"]

    def test_invalid_category(self):
        from servers.data_basic.engine import list_patch_ops

        r = list_patch_ops(category="nonexistent_cat")
        assert r["success"] is False
        assert "hint" in r

    def test_op_catalog_contains_fill_nulls(self):
        from servers.data_basic.engine import list_patch_ops

        r = list_patch_ops()
        all_op_names = [entry["op"] for ops in r["ops"].values() for entry in ops]
        assert "fill_nulls" in all_op_names

    def test_op_catalog_contains_boxcox(self):
        from servers.data_basic.engine import list_patch_ops

        r = list_patch_ops()
        all_op_names = [entry["op"] for ops in r["ops"].values() for entry in ops]
        assert "boxcox_transform" in all_op_names

    def test_token_estimate_present(self):
        from servers.data_basic.engine import list_patch_ops

        r = list_patch_ops()
        assert "token_estimate" in r
        assert r["token_estimate"] > 0


# ---------------------------------------------------------------------------
# boxcox_transform and yeojohnson_transform (via apply_patch)
# ---------------------------------------------------------------------------


@pytest.fixture()
def positive_csv(tmp_path) -> Path:
    """CSV with strictly positive values (required for Box-Cox)."""
    f = tmp_path / "positive.csv"
    f.write_text("id,sales,cost\n1,100,50\n2,200,80\n3,300,120\n4,400,150\n5,500,200\n")
    return f


@pytest.fixture()
def mixed_sign_csv(tmp_path) -> Path:
    """CSV with mixed positive/negative values (for Yeo-Johnson only)."""
    f = tmp_path / "mixed.csv"
    f.write_text("id,score\n1,-50\n2,-10\n3,0\n4,30\n5,100\n6,200\n")
    return f


class TestBoxCoxTransform:
    def test_success(self, positive_csv):
        r = apply_patch(str(positive_csv), [{"op": "boxcox_transform", "column": "sales"}])
        assert r["success"] is True
        df = pd.read_csv(str(positive_csv))
        # Column should be transformed (non-integer values)
        assert df["sales"].dtype == float

    def test_lambda_in_result(self, positive_csv):
        r = apply_patch(str(positive_csv), [{"op": "boxcox_transform", "column": "sales"}])
        assert r["success"] is True
        assert "results" in r
        ops_r = r["results"]
        assert len(ops_r) == 1
        assert "lambda" in ops_r[0]

    def test_dry_run(self, positive_csv):
        original = pd.read_csv(str(positive_csv))["sales"].tolist()
        r = apply_patch(str(positive_csv), [{"op": "boxcox_transform", "column": "sales"}], dry_run=True)
        assert r["success"] is True
        assert r.get("dry_run") is True
        after = pd.read_csv(str(positive_csv))["sales"].tolist()
        assert after == original

    def test_column_not_found(self, positive_csv):
        r = apply_patch(str(positive_csv), [{"op": "boxcox_transform", "column": "nonexistent"}])
        assert r["success"] is False

    def test_non_positive_values_rejected(self, mixed_sign_csv):
        r = apply_patch(str(mixed_sign_csv), [{"op": "boxcox_transform", "column": "score"}])
        assert r["success"] is False
        assert "hint" in r

    def test_backup_created(self, positive_csv):
        r = apply_patch(str(positive_csv), [{"op": "boxcox_transform", "column": "sales"}])
        assert r["success"] is True
        assert "backup" in r


class TestYeoJohnsonTransform:
    def test_success_positive(self, positive_csv):
        r = apply_patch(str(positive_csv), [{"op": "yeojohnson_transform", "column": "sales"}])
        assert r["success"] is True

    def test_success_mixed_sign(self, mixed_sign_csv):
        r = apply_patch(str(mixed_sign_csv), [{"op": "yeojohnson_transform", "column": "score"}])
        assert r["success"] is True
        df = pd.read_csv(str(mixed_sign_csv))
        assert df["score"].dtype == float

    def test_lambda_in_result(self, positive_csv):
        r = apply_patch(str(positive_csv), [{"op": "yeojohnson_transform", "column": "sales"}])
        assert r["success"] is True
        assert "results" in r
        ops_r = r["results"]
        assert "lambda" in ops_r[0]

    def test_dry_run(self, mixed_sign_csv):
        original = pd.read_csv(str(mixed_sign_csv))["score"].tolist()
        r = apply_patch(str(mixed_sign_csv), [{"op": "yeojohnson_transform", "column": "score"}], dry_run=True)
        assert r["success"] is True
        assert r.get("dry_run") is True
        after = pd.read_csv(str(mixed_sign_csv))["score"].tolist()
        assert after == original

    def test_column_not_found(self, positive_csv):
        r = apply_patch(str(positive_csv), [{"op": "yeojohnson_transform", "column": "noexist"}])
        assert r["success"] is False

    def test_backup_created(self, mixed_sign_csv):
        r = apply_patch(str(mixed_sign_csv), [{"op": "yeojohnson_transform", "column": "score"}])
        assert r["success"] is True
        assert "backup" in r


# ---------------------------------------------------------------------------
# E2E: Four-Tool Pattern — locate → inspect → patch → verify
# ---------------------------------------------------------------------------


class TestE2EFourToolPattern:
    def test_locate_inspect_patch_verify(self, tmp_path):
        """Full LOCATE→INSPECT→PATCH→VERIFY cycle using nulls in revenue column."""
        f = tmp_path / "sales.csv"
        f.write_text("Region,Revenue,Units\nWest,5000,10\nEast,,15\nSouth,2100,5\nNorth,4800,12\nWest,,9\n")

        # LOCATE — find columns with nulls
        r_locate = search_columns(str(f), has_nulls=True)
        assert r_locate["success"] is True
        assert "Revenue" in r_locate["columns"]

        # INSPECT — get stats on the nulled column
        r_inspect = read_column_stats(str(f), "Revenue")
        assert r_inspect["success"] is True
        assert r_inspect["null_count"] == 2

        # PATCH — fill nulls with median
        r_patch = apply_patch(str(f), [{"op": "fill_nulls", "column": "Revenue", "strategy": "median"}])
        assert r_patch["success"] is True
        assert "backup" in r_patch

        # VERIFY — confirm no nulls remain
        r_verify = read_column_stats(str(f), "Revenue")
        assert r_verify["success"] is True
        assert r_verify["null_count"] == 0

    def test_boxcox_e2e_locate_transform_verify(self, tmp_path):
        """E2E: detect skewed column → boxcox transform → verify dtype changes."""
        f = tmp_path / "skewed.csv"
        # Create right-skewed data
        f.write_text("id,amount\n1,1\n2,2\n3,3\n4,5\n5,8\n6,13\n7,21\n8,34\n9,55\n10,89\n")

        # Inspect original
        r_pre = read_column_stats(str(f), "amount")
        assert r_pre["success"] is True
        original_mean = r_pre["mean"]

        # Apply Box-Cox
        r_patch = apply_patch(str(f), [{"op": "boxcox_transform", "column": "amount"}])
        assert r_patch["success"] is True

        # Verify data changed
        r_post = read_column_stats(str(f), "amount")
        assert r_post["success"] is True
        assert r_post["mean"] != original_mean
