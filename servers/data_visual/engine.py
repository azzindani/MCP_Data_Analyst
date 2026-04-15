"""T4 data_visual engine — public API. Zero MCP imports."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

logging.basicConfig(stream=sys.stderr, level=logging.WARNING)

_ROOT = Path(__file__).resolve().parents[2]
_ADV = str(Path(__file__).resolve().parents[1] / "data_advanced")
_HERE = str(Path(__file__).resolve().parent)
for _p in (str(_ROOT), _ADV, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Re-export all tools from data_advanced
from _adv_charts import (  # type: ignore[import]
    export_data,
    generate_correlation_heatmap,
    generate_distribution_plot,
    generate_multi_chart,
    generate_pairwise_plot,
)

# New T4 tool
from _adv_customize import customize_chart
from _adv_dashboard import generate_dashboard  # type: ignore[import]
from _adv_eda import run_eda  # type: ignore[import]
from _adv_gencharts import generate_3d_chart, generate_chart, generate_geo_map  # type: ignore[import]
from _adv_profile import generate_auto_profile  # type: ignore[import]

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
    "customize_chart",
]
