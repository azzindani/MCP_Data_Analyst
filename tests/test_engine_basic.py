"""Tests for servers/data_basic/engine.py — ≥90% coverage required."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest
import pandas as pd

from servers.data_basic.engine import (
    load_dataset,
    inspect_dataset,
    read_column_stats,
    search_columns,
    apply_patch,
    restore_version,
    read_receipt,
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
        r = apply_patch(
            str(simple_csv), [{"op": "drop_column", "columns": ["Discount"]}]
        )
        assert r["success"] is True
        df = pd.read_csv(str(simple_csv))
        assert "Discount" not in df.columns

    def test_drop_column_nonexistent(self, simple_csv):
        r = apply_patch(
            str(simple_csv), [{"op": "drop_column", "columns": ["NoSuchCol"]}]
        )
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
        r = apply_patch(
            str(messy_csv), [{"op": "drop_duplicates", "subset": ["State", "City"]}]
        )
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
            [{"op": "extract_regex", "column": "Text", "pattern": r"\d+", "new_column": "Digits"}],
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
            [{"op": "date_diff", "date_col_a": "Start", "date_col_b": "End",
              "new_column": "DiffDays", "unit": "days"}],
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
            [{"op": "rank_column", "column": "Score", "ascending": True, "method": "dense"}],
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
        assert len(doc) <= 80, (
            f"{fn.__name__} docstring too long ({len(doc)} chars): {doc!r}"
        )
