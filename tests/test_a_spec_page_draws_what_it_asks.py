"""A dashboard laid out by the caller carries what the layout asks for, and quality on request.

The sweep's spec asked for six panels and got them under a 16-row
data-quality alert wall and a Quality Score card it never asked for. The
detected page still leads with both; a caller's layout gets them with
`quality: true`.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_advanced")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from servers.data_advanced._adv_dashboard import generate_dashboard  # noqa: E402

LAYOUT = [{"chart": "bar", "cols": {"category": "region", "value": "sales"}}]


@pytest.fixture
def data(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    # A constant column, so the detected page has an alert to show.
    pd.DataFrame({"region": ["N", "S", "E", "W"] * 10, "sales": range(40), "flag": [1] * 40}).to_csv(
        tmp_path / "s.csv", index=False
    )
    return tmp_path


def _page(data, spec=None) -> str:
    r = generate_dashboard(str(data / "s.csv"), spec=spec, output_path=str(data / "d.html"), open_after=False)
    assert r["success"] is True, r.get("error")
    return Path(r["output_path"]).read_text(encoding="utf-8")


def test_the_detected_page_leads_with_quality(data):
    page = _page(data, {"story": False})
    assert "Quality Score" in page and "Data quality" in page


def test_a_storyline_keeps_it_for_its_appendix(data):
    page = _page(data)
    assert "Quality Score" not in page and ">Data quality</h3>" in page


def test_a_laid_out_page_does_not(data):
    page = _page(data, {"layout": LAYOUT})
    assert "Quality Score" not in page and "Data quality" not in page


def test_quality_true_puts_it_back(data):
    page = _page(data, {"layout": LAYOUT, "quality": True})
    assert "Quality Score" in page and "Data quality" in page


def test_quality_is_true_or_false(data):
    r = generate_dashboard(str(data / "s.csv"), spec={"layout": LAYOUT, "quality": "yes"}, open_after=False)
    assert r["success"] is False and "quality is true or false" in r["error"]
