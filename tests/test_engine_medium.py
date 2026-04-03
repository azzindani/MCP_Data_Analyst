"""Tests for servers/data_medium/engine.py — ≥90% coverage required."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import pandas as pd

from servers.data_medium.engine import (
    check_outliers,
    scan_nulls_zeros,
    enrich_with_geo,
    validate_dataset,
    compute_aggregations,
    run_cleaning_pipeline,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def outlier_csv(tmp_path) -> Path:
    f = tmp_path / "outlier_data.csv"
    f.write_text("""Region,Revenue,Units,Discount
West,5000,10,0.1
West,4800,9,0.12
East,7500,15,0.05
South,2100,5,0.2
North,4800,12,0.08
West,5200,11,0.1
East,7200,14,0.06
South,1900,4,0.25
North,5100,13,0.07
West,500,1,0.3
East,50000,100,0.01
South,,3,0.15
North,4900,11,0.09
West,5100,10,0.11
""")
    return f


@pytest.fixture()
def agg_csv(tmp_path) -> Path:
    f = tmp_path / "sales_agg.csv"
    f.write_text("""Region,Product,Revenue,Units_Sold,Quarter
West,Widget A,5000,10,Q1
West,Widget B,3200,8,Q1
East,Widget A,7500,15,Q2
South,Widget C,2100,5,Q2
North,Widget A,4800,12,Q3
West,Widget A,6000,12,Q3
East,Widget B,3000,7,Q4
South,Widget A,2500,6,Q4
North,Widget C,1800,4,Q1
West,Widget C,4200,9,Q2
""")
    return f


@pytest.fixture()
def geo_json(tmp_path) -> Path:
    f = tmp_path / "states.geojson"
    f.write_text("""{
  "type": "FeatureCollection",
  "features": [
    {"type":"Feature","properties":{"name":"California","code":"CA"},"geometry":{"type":"Polygon","coordinates":[[[-120,35],[-120,40],[-115,40],[-115,35],[-120,35]]]}},
    {"type":"Feature","properties":{"name":"Texas","code":"TX"},"geometry":{"type":"Polygon","coordinates":[[[-105,26],[-105,36],[-95,36],[-95,26],[-105,26]]]}},
    {"type":"Feature","properties":{"name":"New York","code":"NY"},"geometry":{"type":"Polygon","coordinates":[[[-80,40],[-80,45],[-72,45],[-72,40],[-80,40]]]}}
  ]
}""")
    return f


@pytest.fixture()
def geo_csv(tmp_path) -> Path:
    f = tmp_path / "geo_data.csv"
    f.write_text("""State,Population
California,39500000
Texas,29000000
New York,19450000
""")
    return f


@pytest.fixture()
def workflow_csv(tmp_path) -> Path:
    f = tmp_path / "workflow_data.csv"
    f.write_text("""Region,Product,Revenue,Units,Discount
West,Widget A,5000,10,0.1
west,widget a,4800,9,0.12
East,Widget B,,15,0.05
South,Widget C,2100,0,0.2
North,Widget A,4800,12,0.08
West,Widget A,5000,10,0.1
East,Widget A,7500,15,0.05
South,Widget C,1900,4,0.25
North,Widget A,5100,13,0.07
""")
    return f


# ---------------------------------------------------------------------------
# check_outliers
# ---------------------------------------------------------------------------


class TestCheckOutliers:
    def test_both_methods(self, outlier_csv):
        r = check_outliers(str(outlier_csv))
        assert r["success"] is True
        assert r["scanned_columns"] == 3
        assert r["columns_with_outliers"] >= 1
        assert "Revenue" in r["results"]
        assert "has_outliers_iqr" in r["results"]["Revenue"]
        assert "has_outliers_std" in r["results"]["Revenue"]

    def test_iqr_only(self, outlier_csv):
        r = check_outliers(str(outlier_csv), method="iqr")
        assert r["success"] is True
        assert "has_outliers_std" not in r["results"]["Revenue"]

    def test_specific_columns(self, outlier_csv):
        r = check_outliers(str(outlier_csv), columns=["Revenue"])
        assert len(r["results"]) == 1
        assert "Revenue" in r["results"]

    def test_column_not_found(self, outlier_csv):
        r = check_outliers(str(outlier_csv), columns=["NonExistent"])
        assert r["success"] is False
        assert "hint" in r

    def test_file_not_found(self, tmp_path):
        r = check_outliers(str(tmp_path / "missing.csv"))
        assert r["success"] is False


# ---------------------------------------------------------------------------
# scan_nulls_zeros
# ---------------------------------------------------------------------------


class TestScanNullsZeros:
    def test_full_scan(self, outlier_csv):
        r = scan_nulls_zeros(str(outlier_csv))
        assert r["success"] is True
        assert r["flagged_columns"] >= 1
        assert "Revenue" in r["results"]
        assert "suggested_actions" in r

    def test_include_zeros_false(self, outlier_csv):
        r = scan_nulls_zeros(str(outlier_csv), include_zeros=False)
        for col, data in r["results"].items():
            assert data.get("zero_count") is None

    def test_clean_dataset(self, agg_csv):
        r = scan_nulls_zeros(str(agg_csv))
        assert r["flagged_columns"] == 0
        assert r["results"] == {}

    def test_file_not_found(self, tmp_path):
        r = scan_nulls_zeros(str(tmp_path / "missing.csv"))
        assert r["success"] is False


# ---------------------------------------------------------------------------
# validate_dataset
# ---------------------------------------------------------------------------


class TestValidateDataset:
    def test_messy_data(self, outlier_csv):
        r = validate_dataset(str(outlier_csv))
        assert r["success"] is True
        assert r["passed"] is False
        assert r["score"] < 100
        assert len(r["issues"]) >= 1

    def test_clean_data(self, agg_csv):
        r = validate_dataset(str(agg_csv))
        assert r["passed"] is True
        assert r["score"] == 100

    def test_dtype_check(self, agg_csv):
        r = validate_dataset(str(agg_csv), expected_dtypes={"Revenue": "float64"})
        assert "dtype_mismatches" in r
        assert "Revenue" in r["dtype_mismatches"]

    def test_skip_duplicates(self, outlier_csv):
        r = validate_dataset(str(outlier_csv), check_duplicates=False)
        for issue in r["issues"]:
            assert "duplicate" not in issue["issue"].lower()

    def test_file_not_found(self, tmp_path):
        r = validate_dataset(str(tmp_path / "missing.csv"))
        assert r["success"] is False


# ---------------------------------------------------------------------------
# compute_aggregations
# ---------------------------------------------------------------------------


class TestComputeAggregations:
    def test_sum_by_region(self, agg_csv):
        r = compute_aggregations(
            str(agg_csv), group_by=["Region"], agg_column="Revenue", agg_func="sum"
        )
        assert r["success"] is True
        assert r["returned"] == 4
        assert r["result"][0]["Revenue"] == 18400

    def test_mean(self, agg_csv):
        r = compute_aggregations(
            str(agg_csv), group_by=["Region"], agg_column="Revenue", agg_func="mean"
        )
        assert r["success"] is True
        assert r["result"][0]["Revenue"] == 5250.0

    def test_count(self, agg_csv):
        r = compute_aggregations(
            str(agg_csv), group_by=["Product"], agg_column="Revenue", agg_func="count"
        )
        assert r["success"] is True
        assert r["result"][0]["Revenue"] == 5

    def test_top_n(self, agg_csv):
        r = compute_aggregations(
            str(agg_csv),
            group_by=["Region"],
            agg_column="Revenue",
            agg_func="sum",
            top_n=2,
        )
        assert len(r["result"]) == 2

    def test_multi_column_groupby(self, agg_csv):
        r = compute_aggregations(
            str(agg_csv),
            group_by=["Region", "Product"],
            agg_column="Revenue",
            agg_func="sum",
        )
        assert r["returned"] == 9

    def test_invalid_agg_func(self, agg_csv):
        r = compute_aggregations(
            str(agg_csv), group_by=["Region"], agg_column="Revenue", agg_func="median"
        )
        assert r["success"] is False
        assert "hint" in r

    def test_missing_groupby_column(self, agg_csv):
        r = compute_aggregations(
            str(agg_csv), group_by=["NonExistent"], agg_column="Revenue", agg_func="sum"
        )
        assert r["success"] is False

    def test_missing_agg_column(self, agg_csv):
        r = compute_aggregations(
            str(agg_csv), group_by=["Region"], agg_column="NonExistent", agg_func="sum"
        )
        assert r["success"] is False


# ---------------------------------------------------------------------------
# enrich_with_geo
# ---------------------------------------------------------------------------


class TestEnrichWithGeo:
    def test_dry_run(self, geo_csv, geo_json):
        r = enrich_with_geo(
            str(geo_csv),
            str(geo_json),
            join_column="State",
            geo_join_column="name",
            dry_run=True,
        )
        if not r["success"]:
            pytest.skip(f"geopandas not installed: {r['error']}")
        assert r["dry_run"] is True
        assert r["matched"] == 3

    def test_full_enrichment(self, geo_csv, geo_json):
        r = enrich_with_geo(
            str(geo_csv), str(geo_json), join_column="State", geo_join_column="name"
        )
        if not r["success"]:
            pytest.skip(f"geopandas not installed: {r['error']}")
        assert r["success"] is True
        assert r["matched"] == 3
        df = pd.read_csv(str(geo_csv))
        assert "geometry" in df.columns

    def test_missing_join_column(self, geo_csv, geo_json):
        r = enrich_with_geo(
            str(geo_csv),
            str(geo_json),
            join_column="NonExistent",
            geo_join_column="name",
        )
        if not r["success"] and "geopandas" in r.get("error", ""):
            pytest.skip("geopandas not installed")
        assert r["success"] is False

    def test_file_not_found(self, tmp_path, geo_json):
        r = enrich_with_geo(
            str(tmp_path / "missing.csv"),
            str(geo_json),
            join_column="State",
            geo_join_column="name",
        )
        assert r["success"] is False


# ---------------------------------------------------------------------------
# run_cleaning_pipeline
# ---------------------------------------------------------------------------


class TestRunCleaningPipeline:
    def _write_pipeline_csv(self, path):
        path.write_text("""Region,Revenue,Units,Discount
West,5000,10,0.1
west,500,1,0.3
East,7500,15,0.05
South,,5,0.2
North,4800,12,0.08
West,5000,10,0.1
""")

    def test_dry_run(self, tmp_path):
        f = tmp_path / "pipeline.csv"
        self._write_pipeline_csv(f)
        r = run_cleaning_pipeline(
            str(f),
            [
                {"op": "clean_text", "scope": "both"},
                {"op": "fill_nulls", "column": "Revenue", "strategy": "median"},
                {"op": "drop_duplicates"},
            ],
            dry_run=True,
        )
        assert r["success"] is True
        assert r["dry_run"] is True
        assert "would_change" in r

    def test_full_run(self, tmp_path):
        f = tmp_path / "pipeline.csv"
        self._write_pipeline_csv(f)
        r = run_cleaning_pipeline(
            str(f),
            [
                {"op": "clean_text", "scope": "both"},
                {"op": "fill_nulls", "column": "Revenue", "strategy": "median"},
                {"op": "drop_duplicates"},
            ],
        )
        assert r["success"] is True
        assert r["applied"] == 3
        df = pd.read_csv(str(f))
        assert len(df) == 5

    def test_empty_ops(self, tmp_path):
        f = tmp_path / "pipeline.csv"
        self._write_pipeline_csv(f)
        r = run_cleaning_pipeline(str(f), [])
        assert r["success"] is False

    def test_failure_rollback(self, tmp_path):
        f = tmp_path / "pipeline.csv"
        f.write_text("""Region,Revenue,Units
West,5000,10
East,7500,15
South,2100,5
""")
        before = f.read_text()
        r = run_cleaning_pipeline(
            str(f),
            [
                {"op": "clean_text", "scope": "both"},
                {"op": "explode_table"},
            ],
        )
        assert r["success"] is False
        assert f.read_text() == before

    def test_file_not_found(self, tmp_path):
        r = run_cleaning_pipeline(
            str(tmp_path / "missing.csv"),
            [
                {"op": "clean_text", "scope": "both"},
            ],
        )
        assert r["success"] is False


# ---------------------------------------------------------------------------
# Full workflow integration
# ---------------------------------------------------------------------------


class TestFullWorkflow:
    def test_scan_validate_clean_aggregate(self, workflow_csv):
        # 1. Scan
        r = scan_nulls_zeros(str(workflow_csv))
        assert r["flagged_columns"] >= 2

        # 2. Validate
        r = validate_dataset(str(workflow_csv))
        assert r["score"] < 100

        # 3. Clean
        r = run_cleaning_pipeline(
            str(workflow_csv),
            [
                {"op": "clean_text", "scope": "both"},
                {"op": "fill_nulls", "column": "Revenue", "strategy": "median"},
                {"op": "drop_duplicates"},
            ],
        )
        assert r["success"] is True

        # 4. Verify
        r = validate_dataset(str(workflow_csv))
        assert r["score"] > 90

        # 5. Aggregate
        r = compute_aggregations(
            str(workflow_csv), group_by=["Region"], agg_column="Revenue", agg_func="sum"
        )
        assert r["success"] is True
        assert r["returned"] == 4
