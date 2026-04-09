"""Tier 3 engine — public API. Zero MCP imports."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

logging.basicConfig(stream=sys.stderr, level=logging.WARNING)

_ROOT = Path(__file__).resolve().parents[2]
_HERE = str(Path(__file__).resolve().parent)
for _p in (str(_ROOT), _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _adv_eda import run_eda
from _adv_profile import generate_auto_profile
from _adv_charts import (
    generate_distribution_plot,
    generate_correlation_heatmap,
    generate_pairwise_plot,
    generate_multi_chart,
    export_data,
)
from _adv_gencharts import generate_chart, generate_geo_map, generate_3d_chart
from _adv_dashboard import generate_dashboard

__all__ = [
    "run_eda",
    "generate_auto_profile",
    "generate_distribution_plot",
    "generate_correlation_heatmap",
    "generate_pairwise_plot",
    "generate_multi_chart",
    "export_data",
    "generate_chart",
    "generate_geo_map",
    "generate_3d_chart",
    "generate_dashboard",
]
