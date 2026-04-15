"""T3 data_statistics engine — all statistics logic. Zero MCP imports."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_MED = str(Path(__file__).resolve().parents[1] / "data_medium")
_HERE = str(Path(__file__).resolve().parent)
for _p in (str(_ROOT), _MED, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Re-export from data_medium for tools that are just moved here
from _med_analysis import (  # type: ignore[import]
    cohort_analysis,
    correlation_analysis,
    time_series_analysis,
)
from _med_inspect import (  # type: ignore[import]
    auto_detect_schema,
    check_outliers,
    extended_stats,
    scan_nulls_zeros,
    validate_dataset,
)
from _stats_comparative import period_comparison
from _stats_regression import regression_analysis

# New statistics tools
from _stats_tests import statistical_test

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stderr, level=logging.WARNING)


__all__ = [
    # moved from data_medium
    "extended_stats",
    "validate_dataset",
    "auto_detect_schema",
    "check_outliers",
    "scan_nulls_zeros",
    "correlation_analysis",
    "time_series_analysis",
    "cohort_analysis",
    # new
    "statistical_test",
    "regression_analysis",
    "period_comparison",
]
