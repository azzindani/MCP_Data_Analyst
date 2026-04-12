"""Shared helpers for data_medium sub-modules. No MCP imports."""

from __future__ import annotations

import subprocess
import sys
import webbrowser
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd

from shared.html_theme import save_chart as _html_save_chart


def _token_estimate(obj) -> int:
    return len(str(obj)) // 4


def _is_string_col(series: pd.Series) -> bool:
    """Return True for object and pandas 3.x StringDtype columns."""
    return series.dtype == object or isinstance(series.dtype, pd.StringDtype)


def _read_csv(file_path: str, encoding: str = "utf-8", separator: str = ",", max_rows: int = 0) -> pd.DataFrame:
    kwargs: dict = {"encoding": encoding, "sep": separator, "low_memory": False}
    if max_rows > 0:
        kwargs["nrows"] = max_rows
    return pd.read_csv(file_path, **kwargs)


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
