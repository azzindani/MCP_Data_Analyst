"""Tests for servers/data_medium/engine.py — ≥90% coverage required."""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest

from servers.data_medium.engine import (
    analyze_text_column,
    auto_detect_schema,
    check_outliers,
    cohort_analysis,
    compare_datasets,
    compute_aggregations,
    correlation_analysis,
    cross_tabulate,
    detect_anomalies,
    enrich_with_geo,
    feature_engineering,
    filter_rows,
    merge_datasets,
    pivot_table,
    run_cleaning_pipeline,
    sample_data,
    scan_nulls_zeros,
    smart_impute,
    statistical_tests,
    time_series_analysis,
    validate_dataset,
    value_counts,
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

    def test_html_export(self, outlier_csv, tmp_path):
        out = tmp_path / "outliers.html"
        r = check_outliers(str(outlier_csv), output_path=str(out), open_after=False)
        assert r["success"] is True
        assert out.exists()
        assert "output_name" in r


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
        r = scan_nulls_zeros(str(agg_csv), open_after=False)
        assert r["flagged_columns"] == 0
        assert r["results"] == {}

    def test_html_export(self, outlier_csv, tmp_path):
        out = tmp_path / "nulls.html"
        r = scan_nulls_zeros(str(outlier_csv), output_path=str(out), open_after=False)
        assert r["success"] is True
        assert out.exists()
        assert "output_name" in r

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
        r = compute_aggregations(str(agg_csv), group_by=["Region"], agg_column="Revenue", agg_func="sum")
        assert r["success"] is True
        assert r["returned"] == 4
        assert r["result"][0]["Revenue"] == 18400

    def test_mean(self, agg_csv):
        r = compute_aggregations(str(agg_csv), group_by=["Region"], agg_column="Revenue", agg_func="mean")
        assert r["success"] is True
        assert r["result"][0]["Revenue"] == 5250.0

    def test_count(self, agg_csv):
        r = compute_aggregations(str(agg_csv), group_by=["Product"], agg_column="Revenue", agg_func="count")
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
        r = compute_aggregations(str(agg_csv), group_by=["Region"], agg_column="Revenue", agg_func="median")
        assert r["success"] is False
        assert "hint" in r

    def test_missing_groupby_column(self, agg_csv):
        r = compute_aggregations(str(agg_csv), group_by=["NonExistent"], agg_column="Revenue", agg_func="sum")
        assert r["success"] is False

    def test_missing_agg_column(self, agg_csv):
        r = compute_aggregations(str(agg_csv), group_by=["Region"], agg_column="NonExistent", agg_func="sum")
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
        r = enrich_with_geo(str(geo_csv), str(geo_json), join_column="State", geo_join_column="name")
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
        r = compute_aggregations(str(workflow_csv), group_by=["Region"], agg_column="Revenue", agg_func="sum")
        assert r["success"] is True
        assert r["returned"] == 4


# ---------------------------------------------------------------------------
# Shared fixtures for new function tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def numeric_csv(tmp_path) -> Path:
    f = tmp_path / "numeric.csv"
    f.write_text("""Region,Revenue,Units,Score
West,5000,10,0.9
East,7500,15,0.7
South,2100,5,0.5
North,4800,12,0.8
West,6000,12,0.85
East,3000,7,0.6
South,2500,6,0.55
North,1800,4,0.4
""")
    return f


@pytest.fixture()
def cat_csv(tmp_path) -> Path:
    f = tmp_path / "categories.csv"
    f.write_text("""Region,Product,Revenue
West,Widget A,5000
West,Widget B,3200
East,Widget A,7500
South,Widget C,2100
North,Widget A,4800
West,Widget A,6000
East,Widget B,3000
South,Widget A,2500
""")
    return f


@pytest.fixture()
def date_csv(tmp_path) -> Path:
    f = tmp_path / "timeseries.csv"
    f.write_text("""Date,Revenue,Units
2023-01-15,5000,10
2023-02-10,6200,12
2023-03-05,4800,9
2023-04-20,7100,14
2023-05-11,5500,11
2023-06-08,6800,13
2023-07-03,7200,15
2023-08-19,5900,12
""")
    return f


@pytest.fixture()
def cohort_csv(tmp_path) -> Path:
    f = tmp_path / "cohort.csv"
    f.write_text("""Cohort,Date,Revenue
A,2023-01-15,500
A,2023-02-10,620
A,2023-03-05,480
B,2023-02-10,700
B,2023-03-05,550
B,2023-04-20,610
C,2023-03-05,400
C,2023-04-20,350
""")
    return f


@pytest.fixture()
def right_csv(tmp_path) -> Path:
    f = tmp_path / "right.csv"
    f.write_text("""Region,Manager
West,Alice
East,Bob
North,Carol
""")
    return f


# ---------------------------------------------------------------------------
# correlation_analysis
# ---------------------------------------------------------------------------


class TestCorrelationAnalysis:
    def test_basic(self, numeric_csv):
        r = correlation_analysis(str(numeric_csv))
        assert r["success"] is True
        assert "top_pairs" in r
        assert "matrix" in r
        assert len(r["top_pairs"]) >= 1
        assert all("col_a" in p and "col_b" in p and "correlation" in p for p in r["top_pairs"])

    def test_pearson_method(self, numeric_csv):
        r = correlation_analysis(str(numeric_csv), method="pearson")
        assert r["success"] is True
        assert r["method"] == "pearson"

    def test_spearman_method(self, numeric_csv):
        r = correlation_analysis(str(numeric_csv), method="spearman")
        assert r["success"] is True

    def test_invalid_method(self, numeric_csv):
        r = correlation_analysis(str(numeric_csv), method="invalid")
        assert r["success"] is False
        assert "hint" in r

    def test_html_export(self, numeric_csv, tmp_path):
        out = tmp_path / "corr.html"
        r = correlation_analysis(str(numeric_csv), output_path=str(out), open_after=False)
        assert r["success"] is True
        assert out.exists()
        assert "output_name" in r

    def test_file_not_found(self, tmp_path):
        r = correlation_analysis(str(tmp_path / "missing.csv"))
        assert r["success"] is False


# ---------------------------------------------------------------------------
# cross_tabulate
# ---------------------------------------------------------------------------


class TestCrossTabulate:
    def test_basic_count(self, cat_csv):
        r = cross_tabulate(str(cat_csv), row_column="Region", col_column="Product", open_after=False)
        assert r["success"] is True
        assert "table" in r
        assert r["agg_func"] == "count"

    def test_with_values(self, cat_csv):
        r = cross_tabulate(
            str(cat_csv),
            row_column="Region",
            col_column="Product",
            values_column="Revenue",
            agg_func="sum",
            open_after=False,
        )
        assert r["success"] is True

    def test_normalize(self, cat_csv):
        r = cross_tabulate(
            str(cat_csv),
            row_column="Region",
            col_column="Product",
            normalize="index",
            open_after=False,
        )
        assert r["success"] is True

    def test_html_export(self, cat_csv, tmp_path):
        out = tmp_path / "crosstab.html"
        r = cross_tabulate(
            str(cat_csv),
            row_column="Region",
            col_column="Product",
            output_path=str(out),
            open_after=False,
        )
        assert r["success"] is True
        assert out.exists()
        assert "output_name" in r

    def test_column_not_found(self, cat_csv):
        r = cross_tabulate(str(cat_csv), row_column="Missing", col_column="Product", open_after=False)
        assert r["success"] is False
        assert "hint" in r

    def test_file_not_found(self, tmp_path):
        r = cross_tabulate(str(tmp_path / "missing.csv"), row_column="A", col_column="B")
        assert r["success"] is False


# ---------------------------------------------------------------------------
# pivot_table
# ---------------------------------------------------------------------------


class TestPivotTable:
    def test_basic(self, cat_csv):
        r = pivot_table(str(cat_csv), index=["Region"], values=["Revenue"], agg_func="sum")
        assert r["success"] is True
        assert "result" in r
        assert r["returned"] >= 1

    def test_with_columns(self, cat_csv):
        r = pivot_table(str(cat_csv), index=["Region"], columns=["Product"], values=["Revenue"])
        assert r["success"] is True

    def test_missing_index_col(self, cat_csv):
        r = pivot_table(str(cat_csv), index=["NonExistent"])
        assert r["success"] is False
        assert "hint" in r

    def test_file_not_found(self, tmp_path):
        r = pivot_table(str(tmp_path / "missing.csv"), index=["A"])
        assert r["success"] is False


# ---------------------------------------------------------------------------
# value_counts
# ---------------------------------------------------------------------------


class TestValueCounts:
    def test_single_column(self, cat_csv):
        r = value_counts(str(cat_csv), columns=["Region"], open_after=False)
        assert r["success"] is True
        assert "Region" in r["results"]
        assert r["results"]["Region"][0]["count"] >= 1
        assert "pct" in r["results"]["Region"][0]

    def test_multiple_columns(self, cat_csv):
        r = value_counts(str(cat_csv), columns=["Region", "Product"], open_after=False)
        assert r["success"] is True
        assert "Region" in r["results"]
        assert "Product" in r["results"]

    def test_no_pct(self, cat_csv):
        r = value_counts(str(cat_csv), columns=["Region"], include_pct=False, open_after=False)
        assert r["success"] is True
        assert "pct" not in r["results"]["Region"][0]

    def test_top_n(self, cat_csv):
        r = value_counts(str(cat_csv), columns=["Region"], top_n=2, open_after=False)
        assert len(r["results"]["Region"]) <= 2

    def test_html_export(self, cat_csv, tmp_path):
        out = tmp_path / "vc.html"
        r = value_counts(str(cat_csv), columns=["Region"], output_path=str(out), open_after=False)
        assert r["success"] is True
        assert out.exists()
        assert "output_name" in r

    def test_column_not_found(self, cat_csv):
        r = value_counts(str(cat_csv), columns=["NonExistent"])
        assert r["success"] is False

    def test_file_not_found(self, tmp_path):
        r = value_counts(str(tmp_path / "missing.csv"), columns=["A"])
        assert r["success"] is False


# ---------------------------------------------------------------------------
# filter_rows
# ---------------------------------------------------------------------------


class TestFilterRows:
    def test_equals(self, cat_csv):
        r = filter_rows(
            str(cat_csv),
            [{"column": "Region", "op": "equals", "value": "West"}],
            open_after=False,
        )
        assert r["success"] is True
        assert r["rows_after"] == 3
        assert r["rows_before"] == 8

    def test_gt(self, cat_csv):
        r = filter_rows(
            str(cat_csv),
            [{"column": "Revenue", "op": "gt", "value": 5000}],
            open_after=False,
        )
        assert r["success"] is True
        assert r["rows_after"] >= 1

    def test_contains(self, cat_csv):
        r = filter_rows(
            str(cat_csv),
            [{"column": "Product", "op": "contains", "value": "Widget A"}],
            open_after=False,
        )
        assert r["success"] is True
        assert r["rows_after"] >= 1

    def test_dry_run(self, cat_csv):
        before = Path(str(cat_csv)).read_text()
        r = filter_rows(
            str(cat_csv),
            [{"column": "Region", "op": "equals", "value": "West"}],
            dry_run=True,
            open_after=False,
        )
        assert r["success"] is True
        assert r["dry_run"] is True
        assert Path(str(cat_csv)).read_text() == before

    def test_missing_column(self, cat_csv):
        r = filter_rows(
            str(cat_csv),
            [{"column": "Missing", "op": "equals", "value": "X"}],
            open_after=False,
        )
        assert r["success"] is False

    def test_no_conditions(self, cat_csv):
        r = filter_rows(str(cat_csv), [], open_after=False)
        assert r["success"] is False

    def test_file_not_found(self, tmp_path):
        r = filter_rows(
            str(tmp_path / "missing.csv"),
            [{"column": "A", "op": "equals", "value": "x"}],
            open_after=False,
        )
        assert r["success"] is False


# ---------------------------------------------------------------------------
# sample_data
# ---------------------------------------------------------------------------


class TestSampleData:
    def test_random(self, cat_csv):
        r = sample_data(str(cat_csv), method="random", n=3, open_after=False)
        assert r["success"] is True
        assert r["sampled"] == 3

    def test_head(self, cat_csv):
        r = sample_data(str(cat_csv), method="head", n=2, open_after=False)
        assert r["success"] is True
        assert r["sampled"] == 2

    def test_tail(self, cat_csv):
        r = sample_data(str(cat_csv), method="tail", n=2, open_after=False)
        assert r["success"] is True
        assert r["sampled"] == 2

    def test_save_output(self, cat_csv, tmp_path):
        out = tmp_path / "sample_out.csv"
        r = sample_data(str(cat_csv), method="head", n=3, output_path=str(out), open_after=False)
        assert r["success"] is True
        assert out.exists()

    def test_invalid_method(self, cat_csv):
        r = sample_data(str(cat_csv), method="invalid", open_after=False)
        assert r["success"] is False

    def test_file_not_found(self, tmp_path):
        r = sample_data(str(tmp_path / "missing.csv"), open_after=False)
        assert r["success"] is False


# ---------------------------------------------------------------------------
# auto_detect_schema
# ---------------------------------------------------------------------------


class TestAutoDetectSchema:
    def test_basic(self, cat_csv):
        r = auto_detect_schema(str(cat_csv))
        assert r["success"] is True
        assert "columns" in r
        assert "Region" in r["columns"]

    def test_date_detection(self, date_csv):
        r = auto_detect_schema(str(date_csv))
        assert r["success"] is True
        date_info = r["columns"].get("Date", {})
        assert date_info.get("inferred_type") == "datetime"

    def test_file_not_found(self, tmp_path):
        r = auto_detect_schema(str(tmp_path / "missing.csv"))
        assert r["success"] is False


# ---------------------------------------------------------------------------
# smart_impute
# ---------------------------------------------------------------------------


class TestSmartImpute:
    def _make_csv_with_nulls(self, path):
        path.write_text("""Region,Revenue,Units
West,5000,10
East,,15
South,2100,
North,4800,12
""")

    def test_dry_run(self, tmp_path):
        f = tmp_path / "nulls.csv"
        self._make_csv_with_nulls(f)
        before = f.read_text()
        r = smart_impute(str(f), dry_run=True, open_after=False)
        assert r["success"] is True
        assert r["dry_run"] is True
        assert f.read_text() == before

    def test_impute(self, tmp_path):
        f = tmp_path / "nulls.csv"
        self._make_csv_with_nulls(f)
        r = smart_impute(str(f), open_after=False)
        assert r["success"] is True
        assert r["columns_imputed"] >= 1
        df = pd.read_csv(str(f))
        assert df["Revenue"].isna().sum() == 0

    def test_specific_columns(self, tmp_path):
        f = tmp_path / "nulls.csv"
        self._make_csv_with_nulls(f)
        r = smart_impute(str(f), columns=["Revenue"], open_after=False)
        assert r["success"] is True

    def test_missing_column(self, tmp_path):
        f = tmp_path / "nulls.csv"
        self._make_csv_with_nulls(f)
        r = smart_impute(str(f), columns=["NonExistent"], open_after=False)
        assert r["success"] is False

    def test_file_not_found(self, tmp_path):
        r = smart_impute(str(tmp_path / "missing.csv"), open_after=False)
        assert r["success"] is False


# ---------------------------------------------------------------------------
# merge_datasets
# ---------------------------------------------------------------------------


class TestMergeDatasets:
    def test_left_join(self, cat_csv, right_csv):
        r = merge_datasets(
            str(cat_csv),
            str(right_csv),
            left_on="Region",
            right_on="Region",
            open_after=False,
        )
        assert r["success"] is True
        assert r["result_rows"] == 8
        df = pd.read_csv(str(cat_csv))
        assert "Manager" in df.columns

    def test_dry_run(self, cat_csv, right_csv):
        before = Path(str(cat_csv)).read_text()
        r = merge_datasets(
            str(cat_csv),
            str(right_csv),
            left_on="Region",
            right_on="Region",
            dry_run=True,
            open_after=False,
        )
        assert r["success"] is True
        assert r["dry_run"] is True
        assert Path(str(cat_csv)).read_text() == before

    def test_auto_detect_key(self, cat_csv, right_csv):
        r = merge_datasets(str(cat_csv), str(right_csv), open_after=False)
        assert r["success"] is True

    def test_invalid_join_type(self, cat_csv, right_csv):
        r = merge_datasets(str(cat_csv), str(right_csv), how="invalid", open_after=False)
        assert r["success"] is False

    def test_left_col_not_found(self, cat_csv, right_csv):
        r = merge_datasets(
            str(cat_csv),
            str(right_csv),
            left_on="Missing",
            right_on="Region",
            open_after=False,
        )
        assert r["success"] is False

    def test_file_not_found(self, tmp_path, right_csv):
        r = merge_datasets(str(tmp_path / "missing.csv"), str(right_csv), open_after=False)
        assert r["success"] is False


# ---------------------------------------------------------------------------
# feature_engineering
# ---------------------------------------------------------------------------


class TestFeatureEngineering:
    def test_text_length(self, cat_csv):
        r = feature_engineering(str(cat_csv), features=["text_length"], open_after=False)
        assert r["success"] is True
        assert any("_len" in c for c in r["new_columns"])

    def test_bins(self, cat_csv):
        r = feature_engineering(str(cat_csv), features=["bins"], open_after=False)
        assert r["success"] is True
        assert any("_bin" in c for c in r["new_columns"])

    def test_one_hot(self, cat_csv):
        r = feature_engineering(str(cat_csv), features=["one_hot"], open_after=False)
        assert r["success"] is True
        assert len(r["new_columns"]) >= 1

    def test_dry_run(self, cat_csv):
        before = Path(str(cat_csv)).read_text()
        r = feature_engineering(str(cat_csv), features=["bins"], dry_run=True, open_after=False)
        assert r["success"] is True
        assert r["dry_run"] is True
        assert Path(str(cat_csv)).read_text() == before

    def test_invalid_feature(self, cat_csv):
        r = feature_engineering(str(cat_csv), features=["invalid_feature"], open_after=False)
        assert r["success"] is False

    def test_file_not_found(self, tmp_path):
        r = feature_engineering(str(tmp_path / "missing.csv"), open_after=False)
        assert r["success"] is False


# ---------------------------------------------------------------------------
# statistical_tests
# ---------------------------------------------------------------------------


class TestStatisticalTests:
    def test_ttest_two_columns(self, numeric_csv):
        r = statistical_tests(str(numeric_csv), test_type="ttest", column_a="Revenue", column_b="Units")
        assert r["success"] is True
        assert "p_value" in r
        assert "significant" in r

    def test_correlation(self, numeric_csv):
        r = statistical_tests(
            str(numeric_csv),
            test_type="correlation",
            column_a="Revenue",
            column_b="Units",
        )
        assert r["success"] is True
        assert "statistic" in r

    def test_chi_square(self, cat_csv):
        r = statistical_tests(str(cat_csv), test_type="chi_square", column_a="Region", column_b="Product")
        assert r["success"] is True
        assert "degrees_of_freedom" in r

    def test_anova(self, cat_csv):
        r = statistical_tests(str(cat_csv), test_type="anova", column_a="Revenue", group_column="Region")
        assert r["success"] is True
        assert "groups" in r

    def test_auto_select_correlation(self, numeric_csv):
        r = statistical_tests(str(numeric_csv), column_a="Revenue", column_b="Units")
        assert r["success"] is True
        assert r["test_type"] == "correlation"

    def test_invalid_test_type(self, numeric_csv):
        r = statistical_tests(str(numeric_csv), test_type="bad_test")
        assert r["success"] is False

    def test_column_not_found(self, numeric_csv):
        r = statistical_tests(
            str(numeric_csv),
            test_type="correlation",
            column_a="Missing",
            column_b="Units",
        )
        assert r["success"] is False

    def test_file_not_found(self, tmp_path):
        r = statistical_tests(str(tmp_path / "missing.csv"))
        assert r["success"] is False


# ---------------------------------------------------------------------------
# time_series_analysis
# ---------------------------------------------------------------------------


class TestTimeSeriesAnalysis:
    def test_basic(self, date_csv):
        r = time_series_analysis(
            str(date_csv),
            date_column="Date",
            value_columns=["Revenue"],
            open_after=False,
        )
        assert r["success"] is True
        assert "data" in r
        assert "trend" in r
        assert "Revenue" in r["trend"]

    def test_auto_detect_date(self, date_csv):
        r = time_series_analysis(str(date_csv), value_columns=["Revenue"], open_after=False)
        assert r["success"] is True

    def test_period_quarterly(self, date_csv):
        r = time_series_analysis(str(date_csv), date_column="Date", period="Q", open_after=False)
        assert r["success"] is True
        assert r["period"] == "Q"

    def test_html_export(self, date_csv, tmp_path):
        out = tmp_path / "ts.html"
        r = time_series_analysis(
            str(date_csv),
            date_column="Date",
            value_columns=["Revenue"],
            output_path=str(out),
            open_after=False,
        )
        assert r["success"] is True
        assert out.exists()
        assert "output_name" in r

    def test_invalid_period(self, date_csv):
        r = time_series_analysis(str(date_csv), period="X")
        assert r["success"] is False

    def test_file_not_found(self, tmp_path):
        r = time_series_analysis(str(tmp_path / "missing.csv"))
        assert r["success"] is False


# ---------------------------------------------------------------------------
# cohort_analysis
# ---------------------------------------------------------------------------


class TestCohortAnalysis:
    def test_basic(self, cohort_csv):
        r = cohort_analysis(
            str(cohort_csv),
            cohort_column="Cohort",
            date_column="Date",
            value_column="Revenue",
        )
        assert r["success"] is True
        assert "matrix" in r
        assert r["cohorts"] >= 1

    def test_auto_detect(self, cohort_csv):
        r = cohort_analysis(str(cohort_csv))
        assert r["success"] is True

    def test_save_output(self, cohort_csv, tmp_path):
        out = tmp_path / "cohort_out.html"
        r = cohort_analysis(
            str(cohort_csv),
            cohort_column="Cohort",
            date_column="Date",
            output_path=str(out),
            open_after=False,
        )
        assert r["success"] is True
        assert out.exists()

    def test_file_not_found(self, tmp_path):
        r = cohort_analysis(str(tmp_path / "missing.csv"))
        assert r["success"] is False


# ---------------------------------------------------------------------------
# time_series_analysis — forecast extension
# ---------------------------------------------------------------------------


class TestTimeSeriesForecast:
    def test_has_forecast(self, date_csv):
        r = time_series_analysis(
            str(date_csv),
            date_column="Date",
            value_columns=["Revenue"],
            open_after=False,
        )
        assert r["success"] is True
        assert "forecast_values" in r
        assert "forecast_periods" in r
        assert r["forecast_periods"] == 3
        assert "Revenue" in r["forecast_values"]
        assert len(r["forecast_values"]["Revenue"]) == 3


# ---------------------------------------------------------------------------
# analyze_text_column
# ---------------------------------------------------------------------------


@pytest.fixture()
def text_csv(tmp_path) -> Path:
    f = tmp_path / "text_data.csv"
    f.write_text(
        "ID,Description,Email\n"
        "1,hello world foo bar,user@example.com\n"
        "2,foo baz qux,admin@test.org\n"
        "3,hello foo world,\n"
        "4,,other@domain.net\n"
    )
    return f


class TestAnalyzeTextColumn:
    def test_basic(self, text_csv):
        r = analyze_text_column(str(text_csv), column="Description")
        assert r["success"] is True
        assert "word_freq" in r
        assert "char_stats" in r
        assert r["char_stats"]["min"] >= 0
        assert r["char_stats"]["max"] >= r["char_stats"]["min"]
        assert "foo" in r["word_freq"]

    def test_patterns(self, text_csv):
        r = analyze_text_column(str(text_csv), column="Email")
        assert r["success"] is True
        assert "patterns" in r
        assert r["patterns"]["emails"] >= 1

    def test_null_count(self, text_csv):
        r = analyze_text_column(str(text_csv), column="Description")
        assert r["success"] is True
        assert r["null_count"] >= 1

    def test_missing_column(self, text_csv):
        r = analyze_text_column(str(text_csv), column="NonExistent")
        assert r["success"] is False
        assert "hint" in r

    def test_file_not_found(self, tmp_path):
        r = analyze_text_column(str(tmp_path / "missing.csv"), column="col")
        assert r["success"] is False


# ---------------------------------------------------------------------------
# detect_anomalies
# ---------------------------------------------------------------------------


@pytest.fixture()
def anomaly_csv(tmp_path) -> Path:
    f = tmp_path / "anomaly_data.csv"
    f.write_text("A,B\n10,100\n11,105\n12,98\n9,102\n10,103\n1000,99\n11,9999\n")
    return f


class TestDetectAnomalies:
    def test_iqr(self, anomaly_csv):
        r = detect_anomalies(str(anomaly_csv), method="iqr")
        assert r["success"] is True
        assert r["anomaly_count"] > 0
        assert "A" in r["per_column"]
        assert "iqr_outliers" in r["per_column"]["A"]

    def test_zscore(self, anomaly_csv):
        r = detect_anomalies(str(anomaly_csv), method="zscore")
        assert r["success"] is True
        assert "A" in r["per_column"]
        assert "zscore_outliers" in r["per_column"]["A"]

    def test_both_flags_in_output(self, anomaly_csv, tmp_path):
        out = tmp_path / "flagged.csv"
        r = detect_anomalies(str(anomaly_csv), method="both", output_path=str(out))
        assert r["success"] is True
        assert out.exists()
        df = pd.read_csv(str(out))
        assert "_anomaly_score" in df.columns
        assert any(c.endswith("_iqr_flag") for c in df.columns)
        assert any(c.endswith("_zscore_flag") for c in df.columns)

    def test_file_not_found(self, tmp_path):
        r = detect_anomalies(str(tmp_path / "missing.csv"))
        assert r["success"] is False


# ---------------------------------------------------------------------------
# compare_datasets
# ---------------------------------------------------------------------------


@pytest.fixture()
def compare_csv_a(tmp_path) -> Path:
    f = tmp_path / "a.csv"
    f.write_text("Region,Revenue,Units\nWest,5000,10\nEast,7500,15\nSouth,2100,5\n")
    return f


@pytest.fixture()
def compare_csv_b(tmp_path) -> Path:
    f = tmp_path / "b.csv"
    f.write_text("Region,Revenue,Units\nWest,5000,10\nEast,7500,15\nSouth,2100,5\n")
    return f


@pytest.fixture()
def compare_csv_c(tmp_path) -> Path:
    f = tmp_path / "c.csv"
    f.write_text("Region,Revenue,Manager\nWest,5000,Alice\nEast,9000,Bob\n")
    return f


class TestCompareDatasets:
    def test_identical(self, compare_csv_a, compare_csv_b):
        r = compare_datasets(str(compare_csv_a), str(compare_csv_b))
        assert r["success"] is True
        assert r["columns_only_in_a"] == []
        assert r["columns_only_in_b"] == []
        assert r["dtype_changes"] == {}
        assert r["rows_a"] == r["rows_b"]

    def test_schema_diff(self, compare_csv_a, compare_csv_c):
        r = compare_datasets(str(compare_csv_a), str(compare_csv_c))
        assert r["success"] is True
        assert "Units" in r["columns_only_in_a"]
        assert "Manager" in r["columns_only_in_b"]

    def test_row_diff(self, compare_csv_a, compare_csv_c):
        r = compare_datasets(str(compare_csv_a), str(compare_csv_c))
        assert r["success"] is True
        assert r["row_diff"] == r["rows_b"] - r["rows_a"]

    def test_file_not_found(self, tmp_path, compare_csv_a):
        r = compare_datasets(str(compare_csv_a), str(tmp_path / "missing.csv"))
        assert r["success"] is False
