"""Shared helpers for data_advanced sub-modules. No MCP imports."""

from __future__ import annotations

import logging
import subprocess
import sys
import webbrowser
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd

from shared.column_utils import agg_label, infer_agg, parse_agg_overrides  # noqa: F401
from shared.html_layout import (  # noqa: F401  (re-exported for sub-modules)
    PLOTLY_CFG_JS,
    css_dashboard,
    css_report,
    get_output_path,
    get_plotlyjs_script,
    plotly_layout_base,
)
from shared.html_theme import (
    _BACK_TO_TOP_HTML,
    _BACK_TO_TOP_JS,
    _COLLAPSIBLE_SECTIONS_JS,
    _COPY_CLIPBOARD_JS,
    _KPI_COUNTER_JS,
    _SCROLL_SPY_JS,
    _SIDEBAR_JS,
    _SORTABLE_TABLES_JS,
    VIEWPORT_META,
    calc_chart_height,
    css_vars,
    device_mode_js,
    plotly_template,
)
from shared.html_theme import (
    save_chart as _html_save_chart,
)
from shared.progress import fail, info, ok, warn

logger = logging.getLogger(__name__)


def _token_estimate(obj) -> int:
    return len(str(obj)) // 4


def _read_csv(
    file_path: str,
    encoding: str = "utf-8",
    separator: str = ",",
    max_rows: int = 0,
) -> pd.DataFrame:
    kwargs: dict = {"encoding": encoding, "sep": separator, "low_memory": False}
    if max_rows > 0:
        kwargs["nrows"] = max_rows
    return pd.read_csv(file_path, **kwargs)


def _open_file(path: Path) -> None:
    """Open file in default browser/app."""
    try:
        webbrowser.open(f"file://{path.resolve()}")
    except Exception:
        try:
            if sys.platform == "win32":
                subprocess.Popen(["start", str(path.resolve())], shell=True)
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(path.resolve())])
            else:
                subprocess.Popen(["xdg-open", str(path.resolve())])
        except Exception:
            pass


def _save_chart(
    fig,
    output_path: str,
    stem_suffix: str,
    input_path: Path,
    open_after: bool,
    theme: str = "dark",
) -> tuple[str, str]:
    """Save plotly figure to themed responsive HTML."""
    return _html_save_chart(fig, output_path, stem_suffix, input_path, theme, open_after, _open_file)


def _dtype_label(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "int64"
    if pd.api.types.is_float_dtype(series):
        return "float64"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime64"
    return "object"


# ---------------------------------------------------------------------------
# Geo detection helpers
# ---------------------------------------------------------------------------

_GEO_LAT = {"lat", "latitude"}
_GEO_LON = {"lon", "lng", "long", "longitude"}
_GEO_COUNTRY = {
    "country",
    "nation",
    "country_name",
    "country_code",
    "iso3",
    "iso_code",
    "iso",
    "iso_3",
    "iso3_code",
}
_GEO_STATE = {"state", "state_code", "state_abbr", "state_name"}
_US_STATES = {
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "DC",
}


def _find_geo_cols(df: pd.DataFrame) -> tuple[str, str, str]:
    """Return (lat_col, lon_col, loc_col) from column names, any may be ''."""
    low = {c.lower(): c for c in df.columns}
    lat = next((low[k] for k in _GEO_LAT if k in low), "")
    lon = next((low[k] for k in _GEO_LON if k in low), "")
    loc = next((low[k] for k in _GEO_COUNTRY if k in low), "")
    if not loc:
        loc = next((low[k] for k in _GEO_STATE if k in low), "")
    return lat, lon, loc


def _detect_location_mode(df: pd.DataFrame, col: str) -> str:
    """Guess Plotly locationmode from sample values in col."""
    sample = df[col].dropna().astype(str).unique()[:20].tolist()
    if not sample:
        return "country names"
    if all(len(v) == 3 and v.isupper() for v in sample):
        return "ISO-3"
    if all(len(v) == 2 and v.upper() in _US_STATES for v in sample):
        return "USA-states"
    return "country names"
