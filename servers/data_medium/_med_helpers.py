"""Shared helpers for data_medium sub-modules. No MCP imports."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd

from shared.column_utils import is_numeric_col  # noqa: F401
from shared.html_theme import save_chart as _html_save_chart


def _token_estimate(obj) -> int:
    return len(str(obj)) // 4


def _is_string_col(series: pd.Series) -> bool:
    """Return True for object and pandas 3.x StringDtype columns."""
    return series.dtype == object or isinstance(series.dtype, pd.StringDtype)


from shared.file_utils import read_csv as _read_csv  # noqa: E402


def _dtype_label(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "int64"
    if pd.api.types.is_float_dtype(series):
        return "float64"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime64"
    return "object"


def _open_file(path: Path) -> None:
    """Open file in default system app. Silently ignored on failure."""
    # A test run must never launch a browser or an Office app. Under Windows
    # this reached the COM layer on a CI runner and killed the interpreter
    # mid-suite with an access violation -- which the `except` below cannot
    # catch, so the suite died with no failing test named.
    import os

    if os.environ.get("PYTEST_CURRENT_TEST"):
        return
    try:
        if sys.platform == "win32":
            # A child process rather than in-process os.startfile(): the shell
            # handler it invokes can fault, and a fault there must cost the
            # child, not this server.
            subprocess.Popen(["cmd", "/c", "start", "", str(path.resolve())], shell=False)
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
    theme: str = "device",
    progress: list | None = None,
) -> tuple[str, str]:
    """Save plotly figure to themed responsive HTML.

    `progress` is optional so the 18 existing call sites keep working; pass it
    and the caller is told when an output_path extension had to be corrected.
    """
    return _html_save_chart(fig, output_path, stem_suffix, input_path, theme, open_after, _open_file, progress)
