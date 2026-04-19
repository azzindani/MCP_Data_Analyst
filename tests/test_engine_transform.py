"""Tests for servers/data_transform/engine.py — feature and e2e tests."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from servers.data_transform.engine import (
    aggregate_dataset,
    filter_dataset,
    reshape_dataset,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sales_csv(tmp_path) -> Path:
    f = tmp_path / "sales.csv"
    f.write_text(
        "Region,Product,Revenue,Units,Quarter\n"
        "West,Widget A,5000,10,Q1\n"
        "West,Widget B,3200,8,Q1\n"
        "East,Widget A,7500,15,Q2\n"
        "South,Widget C,2100,5,Q2\n"
        "North,Widget A,4800,12,Q3\n"
        "West,Widget A,6000,12,Q3\n"
        "East,Widget B,3000,7,Q4\n"
        "South,Widget A,2500,6,Q4\n"
        "North,Widget C,1800,4,Q1\n"
        "West,Widget C,4200,9,Q2\n"
    )
    return f


@pytest.fixture()
def wide_csv(tmp_path) -> Path:
    """Wide-format CSV for pivot/melt tests."""
    f = tmp_path / "wide.csv"
    f.write_text(
        "Region,Q1,Q2,Q3,Q4\n"
        "West,5000,4200,6000,3200\n"
        "East,7500,7500,4800,3000\n"
        "South,2100,2100,1800,2500\n"
        "North,4800,4800,4800,1800\n"
    )
    return f


@pytest.fixture()
def full_name_csv(tmp_path) -> Path:
    """CSV with compound column for split tests."""
    f = tmp_path / "names.csv"
    f.write_text("FullName,Score\nAlice Smith,95\nBob Jones,88\nCarol White,72\n")
    return f


@pytest.fixture()
def date_sales_csv(tmp_path) -> Path:
    """CSV with date column for date_range filter tests."""
    f = tmp_path / "date_sales.csv"
    f.write_text(
        "Date,Revenue,Region\n"
        "2023-01-15,5000,West\n"
        "2023-03-20,3200,East\n"
        "2023-06-05,7500,West\n"
        "2023-09-10,4800,South\n"
        "2023-12-25,2100,North\n"
    )
    return f


# ---------------------------------------------------------------------------
# filter_dataset
# ---------------------------------------------------------------------------


class TestFilterDataset:
    def test_basic_gt_filter(self, sales_csv):
        r = filter_dataset(str(sales_csv), conditions=[{"column": "Revenue", "op": "gt", "value": 4000}])
        assert r["success"] is True
        assert r["after_rows"] == 5  # 5000, 7500, 4800, 6000, 4200
        assert r["before_rows"] == 10

    def test_dry_run_no_change(self, sales_csv):
        original = pd.read_csv(str(sales_csv))
        r = filter_dataset(
            str(sales_csv),
            conditions=[{"column": "Revenue", "op": "lt", "value": 3000}],
            dry_run=True,
        )
        assert r["success"] is True
        assert r["dry_run"] is True
        after = pd.read_csv(str(sales_csv))
        assert len(after) == len(original)

    def test_file_not_found(self, tmp_path):
        r = filter_dataset(str(tmp_path / "missing.csv"), conditions=[])
        assert r["success"] is False

    def test_isin_filter(self, sales_csv):
        r = filter_dataset(
            str(sales_csv),
            conditions=[{"column": "Region", "op": "isin", "values": ["West", "East"]}],
        )
        assert r["success"] is True
        df = pd.read_csv(r["output_path"])
        assert all(df["Region"].isin(["West", "East"]))

    def test_between_filter(self, sales_csv):
        r = filter_dataset(
            str(sales_csv),
            conditions=[{"column": "Revenue", "op": "between", "min": 3000, "max": 5500}],
        )
        assert r["success"] is True
        df = pd.read_csv(r["output_path"])
        assert all((df["Revenue"] >= 3000) & (df["Revenue"] <= 5500))

    def test_contains_filter(self, sales_csv):
        r = filter_dataset(
            str(sales_csv),
            conditions=[{"column": "Product", "op": "contains", "value": "Widget A"}],
        )
        assert r["success"] is True
        assert r["after_rows"] == 5

    def test_sort_by_revenue_desc(self, sales_csv):
        r = filter_dataset(
            str(sales_csv),
            conditions=[{"column": "Revenue", "op": "gt", "value": 0}],
            sort_by=["Revenue"],
            sort_ascending=[False],
        )
        assert r["success"] is True
        df = pd.read_csv(r["output_path"])
        assert df["Revenue"].iloc[0] == df["Revenue"].max()

    def test_date_range_filter(self, date_sales_csv):
        r = filter_dataset(
            str(date_sales_csv),
            conditions=[{"column": "Date", "op": "date_range", "start": "2023-01-01", "end": "2023-06-30"}],
        )
        assert r["success"] is True
        assert r["after_rows"] == 3  # Jan, Mar, Jun

    def test_not_null_filter(self, tmp_path):
        f = tmp_path / "nulls.csv"
        f.write_text("Name,Score\nAlice,90\nBob,\nCarol,85\n")
        r = filter_dataset(str(f), conditions=[{"column": "Score", "op": "not_null"}])
        assert r["success"] is True
        assert r["after_rows"] == 2

    def test_output_to_separate_file(self, sales_csv, tmp_path):
        out = tmp_path / "filtered_output.csv"
        r = filter_dataset(
            str(sales_csv),
            conditions=[{"column": "Region", "op": "equals", "value": "West"}],
            output_path=str(out),
        )
        assert r["success"] is True
        assert out.exists()
        df_out = pd.read_csv(str(out))
        assert all(df_out["Region"] == "West")

    def test_backup_created(self, sales_csv):
        r = filter_dataset(
            str(sales_csv),
            conditions=[{"column": "Revenue", "op": "gt", "value": 3000}],
        )
        assert r["success"] is True
        assert "backup" in r

    def test_token_estimate_present(self, sales_csv):
        r = filter_dataset(str(sales_csv), conditions=[])
        assert "token_estimate" in r


# ---------------------------------------------------------------------------
# reshape_dataset
# ---------------------------------------------------------------------------


class TestReshapeDataset:
    def test_melt_wide_to_long(self, wide_csv):
        r = reshape_dataset(
            str(wide_csv),
            mode="melt",
            id_vars=["Region"],
            value_vars=["Q1", "Q2", "Q3", "Q4"],
        )
        assert r["success"] is True
        df = pd.read_csv(r["output_path"])
        assert len(df) == 4 * 4  # 4 regions × 4 quarters
        assert "variable" in df.columns
        assert "value" in df.columns

    def test_split_column(self, full_name_csv):
        r = reshape_dataset(
            str(full_name_csv),
            mode="split_column",
            split_column="FullName",
            delimiter=" ",
            new_columns=["FirstName", "LastName"],
            drop_original=True,
        )
        assert r["success"] is True
        df = pd.read_csv(r["output_path"])
        assert "FirstName" in df.columns
        assert "LastName" in df.columns
        assert df["FirstName"].iloc[0] == "Alice"

    def test_combine_columns(self, full_name_csv):
        r = reshape_dataset(
            str(full_name_csv),
            mode="combine_columns",
            combine_columns=["FullName", "Score"],
            combine_delimiter="_",
            new_column="Name_Score",
        )
        assert r["success"] is True
        df = pd.read_csv(r["output_path"])
        assert "Name_Score" in df.columns
        assert df["Name_Score"].iloc[0] == "Alice Smith_95"

    def test_invalid_mode(self, sales_csv):
        r = reshape_dataset(str(sales_csv), mode="unknown_mode")
        assert r["success"] is False
        assert "hint" in r

    def test_pivot_requires_index(self, sales_csv):
        r = reshape_dataset(str(sales_csv), mode="pivot")
        assert r["success"] is False
        assert "index" in r.get("hint", "") or "index" in r.get("error", "")

    def test_pivot_sales_by_region(self, sales_csv):
        r = reshape_dataset(
            str(sales_csv),
            mode="pivot",
            index=["Region"],
            columns=["Quarter"],
            values=["Revenue"],
            agg_func="sum",
        )
        assert r["success"] is True
        df = pd.read_csv(r["output_path"])
        # Result should have Region column and one column per quarter
        assert "Region" in df.columns

    def test_dry_run_no_change(self, wide_csv):
        original_content = wide_csv.read_text()
        r = reshape_dataset(
            str(wide_csv),
            mode="melt",
            id_vars=["Region"],
            value_vars=["Q1", "Q2"],
            dry_run=True,
        )
        assert r["success"] is True
        assert r["dry_run"] is True
        assert wide_csv.read_text() == original_content

    def test_file_not_found(self, tmp_path):
        r = reshape_dataset(str(tmp_path / "missing.csv"), mode="melt")
        assert r["success"] is False


# ---------------------------------------------------------------------------
# aggregate_dataset
# ---------------------------------------------------------------------------


class TestAggregateDataset:
    def test_groupby_sum(self, sales_csv):
        r = aggregate_dataset(
            str(sales_csv),
            mode="groupby",
            group_by=["Region"],
            agg={"Revenue": "sum"},
        )
        assert r["success"] is True
        assert "data" in r
        # data is dict with {"rows": N, "data": [...]}
        assert r["data"]["rows"] == 4  # 4 regions
        assert len(r["data"]["data"]) == 4

    def test_groupby_mean(self, sales_csv):
        r = aggregate_dataset(
            str(sales_csv),
            mode="groupby",
            group_by=["Quarter"],
            agg={"Revenue": "mean"},
        )
        assert r["success"] is True
        assert r["data"]["rows"] == 4  # Q1, Q2, Q3, Q4

    def test_value_counts(self, sales_csv):
        r = aggregate_dataset(str(sales_csv), mode="value_counts", columns=["Region"])
        assert r["success"] is True
        assert "data" in r

    def test_describe_mode(self, sales_csv):
        r = aggregate_dataset(str(sales_csv), mode="describe")
        assert r["success"] is True

    def test_crosstab_mode(self, sales_csv):
        r = aggregate_dataset(
            str(sales_csv),
            mode="crosstab",
            row_col="Region",
            col_col="Product",
        )
        assert r["success"] is True
        assert "data" in r

    def test_invalid_mode(self, sales_csv):
        r = aggregate_dataset(str(sales_csv), mode="nonexistent")
        assert r["success"] is False
        assert "hint" in r

    def test_file_not_found(self, tmp_path):
        r = aggregate_dataset(str(tmp_path / "missing.csv"), mode="groupby")
        assert r["success"] is False

    def test_output_path_in_result(self, sales_csv, tmp_path):
        out = tmp_path / "agg_out.csv"
        r = aggregate_dataset(
            str(sales_csv),
            mode="groupby",
            group_by=["Region"],
            agg={"Revenue": "sum"},
            output_path=str(out),
        )
        assert r["success"] is True
        assert "output_path" in r

    def test_token_estimate(self, sales_csv):
        r = aggregate_dataset(str(sales_csv), mode="groupby", group_by=["Region"])
        assert "token_estimate" in r


# ---------------------------------------------------------------------------
# E2E: filter → aggregate → verify pipeline
# ---------------------------------------------------------------------------


class TestE2ETransformPipeline:
    def test_filter_then_aggregate(self, tmp_path):
        """Filter to one region, then aggregate by product — full pipeline."""
        f = tmp_path / "data.csv"
        f.write_text(
            "Region,Product,Revenue,Units\n"
            "West,Widget A,5000,10\n"
            "West,Widget B,3200,8\n"
            "East,Widget A,7500,15\n"
            "East,Widget B,3000,7\n"
            "South,Widget A,2100,5\n"
        )

        # Step 1: Filter to West only
        r_filter = filter_dataset(
            str(f),
            conditions=[{"column": "Region", "op": "equals", "value": "West"}],
        )
        assert r_filter["success"] is True
        assert r_filter["after_rows"] == 2

        # Step 2: Aggregate by product (on the West-only data)
        r_agg = aggregate_dataset(
            str(f),
            mode="groupby",
            group_by=["Product"],
            agg={"Revenue": "sum"},
        )
        assert r_agg["success"] is True
        # West data has Widget A and Widget B
        assert r_agg["data"]["rows"] == 2

    def test_reshape_then_aggregate(self, tmp_path):
        """Melt wide data, then aggregate by quarter."""
        wide = tmp_path / "wide_data.csv"
        wide.write_text("Region,Q1,Q2,Q3\nWest,5000,4000,6000\nEast,7500,8000,5000\nSouth,2000,2500,1800\n")

        # Melt wide → long (using custom column names Quarter/Revenue)
        r_reshape = reshape_dataset(
            str(wide),
            mode="melt",
            id_vars=["Region"],
            value_vars=["Q1", "Q2", "Q3"],
            var_name="Quarter",
            value_name="Revenue",
        )
        assert r_reshape["success"] is True
        df_long = pd.read_csv(r_reshape["output_path"])
        # After melt: 3 regions × 3 quarters = 9 rows
        assert len(df_long) == 9
        # Custom var_name was "Quarter" so that should be the column name
        assert "Quarter" in df_long.columns

    def test_multi_condition_filter_and_sort(self, tmp_path):
        """Multiple conditions AND'd together with sort — real data flow."""
        f = tmp_path / "inventory.csv"
        f.write_text(
            "Category,Item,Price,Stock\n"
            "Electronics,Phone,599,50\n"
            "Electronics,Laptop,1200,30\n"
            "Clothing,Shirt,45,200\n"
            "Clothing,Pants,80,150\n"
            "Electronics,Tablet,350,80\n"
            "Clothing,Jacket,120,60\n"
        )

        # Filter: Category = Electronics AND Price < 700, sort by Price desc
        r = filter_dataset(
            str(f),
            conditions=[
                {"column": "Category", "op": "equals", "value": "Electronics"},
                {"column": "Price", "op": "lt", "value": 700},
            ],
            sort_by=["Price"],
            sort_ascending=[False],
        )
        assert r["success"] is True
        assert r["after_rows"] == 2  # Phone (599) and Tablet (350)
        df = pd.read_csv(str(f))
        # Should be sorted desc: Phone first
        assert df["Price"].iloc[0] == 599
