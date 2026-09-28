"""A page heading built from a column or file name is text, never markup.

A chart page's <h1> and <title> are the figure's title, and a title names the
columns it was drawn from: a column called `<img src=x onerror=alert(1)>`
became an element in the heading and ran when the page was opened. The
dashboard escaped its <title> and not its <h1>, whose default is the file's
name. Found while building the regression page (sweep F22).
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
for _p in (
    str(ROOT),
    str(ROOT / "servers" / "data_statistics"),
    str(ROOT / "servers" / "data_medium"),
    str(ROOT / "servers" / "data_advanced"),
):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _stats_regression import regression_analysis  # type: ignore[import]  # noqa: E402

from servers.data_advanced._adv_dashboard import generate_dashboard  # noqa: E402

EVIL = "<img src=x onerror=alert(1)>"


def test_a_chart_page_heading(tmp_path):
    n = 40
    x = np.arange(n, dtype=float)
    pd.DataFrame({EVIL: 2 * x + np.random.default_rng(0).normal(0, 1, n), "x": x}).to_csv(
        tmp_path / "d.csv", index=False
    )
    r = regression_analysis(str(tmp_path / "d.csv"), EVIL, ["x"], output_path=str(tmp_path / "r.html"))
    page = Path(r["output_path"]).read_text(encoding="utf-8")
    head = page[: page.index('<div class="chart-wrap">')]
    assert EVIL not in head
    assert "&lt;img src=x onerror=alert(1)&gt;" in head


def test_a_dashboard_heading(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}).to_csv(tmp_path / "t.csv", index=False)
    r = generate_dashboard(str(tmp_path / "t.csv"), title=EVIL, output_path=str(tmp_path / "t.html"), open_after=False)
    page = Path(r["output_path"]).read_text(encoding="utf-8")
    assert f"<h1>{EVIL}</h1>" not in page
    assert "<h1>&lt;img src=x onerror=alert(1)&gt;</h1>" in page
