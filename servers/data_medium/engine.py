"""Tier 2 engine — public API. Zero MCP imports."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_HERE = str(Path(__file__).resolve().parent)
for _p in (str(_ROOT), _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _med_inspect import (
    analyze_text_column,
    auto_detect_schema,
    check_outliers,
    filter_rows,
    sample_data,
    scan_nulls_zeros,
    validate_dataset,
)
from _med_transform import (
    compute_aggregations,
    enrich_with_geo,
    feature_engineering,
    merge_datasets,
    run_cleaning_pipeline,
    smart_impute,
)
from _med_analysis import (
    cohort_analysis,
    correlation_analysis,
    detect_anomalies,
    statistical_tests,
    time_series_analysis,
)
from _med_report import (
    compare_datasets,
    cross_tabulate,
    pivot_table,
    value_counts,
)

__all__ = [
    "check_outliers",
    "scan_nulls_zeros",
    "validate_dataset",
    "auto_detect_schema",
    "filter_rows",
    "sample_data",
    "analyze_text_column",
    "detect_anomalies",
    "compare_datasets",
    "enrich_with_geo",
    "compute_aggregations",
    "run_cleaning_pipeline",
    "smart_impute",
    "merge_datasets",
    "feature_engineering",
    "correlation_analysis",
    "statistical_tests",
    "time_series_analysis",
    "cohort_analysis",
    "cross_tabulate",
    "pivot_table",
    "value_counts",
]
