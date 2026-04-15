"""Tier 2 engine — public API. Zero MCP imports."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_HERE = str(Path(__file__).resolve().parent)
for _p in (str(_ROOT), _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _med_analysis import (
    cohort_analysis,
    correlation_analysis,
    detect_anomalies,
    statistical_tests,
    time_series_analysis,
)
from _med_inspect import (
    analyze_text_column,
    auto_detect_schema,
    check_outliers,
    extended_stats,
    filter_rows,
    sample_data,
    scan_nulls_zeros,
    validate_dataset,
)
from _med_report import (
    compare_datasets,
    cross_tabulate,
    pivot_table,
    value_counts,
)
from _med_transform import (
    compute_aggregations,
    concat_datasets,
    enrich_with_geo,
    feature_engineering,
    merge_datasets,
    resample_timeseries,
    run_cleaning_pipeline,
    smart_impute,
)

__all__ = [
    # inspect
    "check_outliers",
    "scan_nulls_zeros",
    "validate_dataset",
    "auto_detect_schema",
    "filter_rows",
    "sample_data",
    "analyze_text_column",
    "extended_stats",
    # analysis
    "detect_anomalies",
    "correlation_analysis",
    "statistical_tests",
    "time_series_analysis",
    "cohort_analysis",
    # report
    "compare_datasets",
    "cross_tabulate",
    "pivot_table",
    "value_counts",
    # transform
    "enrich_with_geo",
    "compute_aggregations",
    "run_cleaning_pipeline",
    "smart_impute",
    "merge_datasets",
    "feature_engineering",
    "resample_timeseries",
    "concat_datasets",
]
