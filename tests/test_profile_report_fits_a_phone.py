"""The profile report scrolled sideways on a phone.

Rendered at 390x844, t3_profile.html measured scrollWidth 609 against a
clientWidth of 390 -- 219px of horizontal overflow, so the whole page slid under
the thumb. The offender was a ten-column summary-statistics table 1733px wide,
emitted as a bare <table>.

_BASE_CSS has said "Tables always inside .tbl-wrap for horizontal scroll on
mobile" since it was written, and defines `.tbl-wrap{overflow-x:auto}`. Three
different things were happening instead:

  * `.tbl-wrap`               used in exactly one place (_adv_eda.py)
  * `.table-wrap`             emitted by shared data_table_html -- no CSS rule
                              anywhere in the repo defines it, so the wrapper
                              did nothing at all
  * `style="overflow-x:auto"` inline on the sample table

These assert the wide tables are wrapped in the class that is actually defined.
A page-width check belongs in a browser and this suite never opens one, so the
markup contract is what is pinned here; the rendering was verified by hand.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pytest

from servers.data_advanced.engine import generate_auto_profile
from shared.html_theme import data_table_html


@pytest.fixture()
def csv(tmp_path: Path) -> Path:
    p = tmp_path / "wide.csv"
    pd.DataFrame(
        {
            "alpha": [1.0, 2.0, 3.0, 4.0],
            "beta": [10.0, 20.0, 30.0, 40.0],
            "gamma": [5.0, 5.0, 6.0, 100.0],
            "label": ["a", "b", "a", "c"],
        }
    ).to_csv(p, index=False)
    return p


@pytest.fixture()
def profile(csv: Path, tmp_path: Path) -> str:
    out = tmp_path / "p.html"
    result = generate_auto_profile(str(csv), output_path=str(out), open_after=False)
    assert result["success"] is True
    return out.read_text(encoding="utf-8")


def _tables_outside_a_wrapper(page: str) -> int:
    """Count <table> tags not immediately preceded by a scrolling wrapper."""
    count = 0
    for match in re.finditer(r"<table[ >]", page):
        before = page[max(0, match.start() - 200) : match.start()]
        if 'class="tbl-wrap"' not in before and "overflow-x:auto" not in before:
            count += 1
    return count


class TestWideTablesScrollThemselves:
    def test_the_summary_statistics_table_is_wrapped(self, profile: str):
        stats = profile[profile.find('id="stats"') :]
        assert 'class="tbl-wrap"' in stats[: stats.find("<table")]

    def test_the_quality_table_is_wrapped(self, profile: str):
        quality = profile[profile.find('id="quality"') :]
        assert 'class="tbl-wrap"' in quality[: quality.find("<table")]

    def test_no_wide_section_table_is_left_bare(self, profile: str):
        """The two multi-column sortable tables are the ones that overflowed."""
        for section in ("stats", "quality"):
            start = profile.find(f'id="{section}"')
            end = profile.find("</div>", profile.find("</table>", start))
            assert _tables_outside_a_wrapper(profile[start:end]) == 0

    def test_the_wrappers_are_closed(self, profile: str):
        """An unclosed wrapper swallows the rest of the report."""
        assert profile.count('<div class="tbl-wrap">') >= 2
        assert profile.count("</tbody></table></div></div>") >= 2


class TestSharedTableHelper:
    def test_it_uses_the_class_the_css_defines(self):
        """It emitted .table-wrap, which no stylesheet in this repo defines."""
        html = data_table_html([{"a": 1, "b": 2}])
        assert 'class="tbl-wrap"' in html
        assert "table-wrap" not in html.replace("tbl-wrap", "")

    def test_the_wrapper_actually_wraps_the_table(self):
        html = data_table_html([{"a": 1}])
        assert html.index('class="tbl-wrap"') < html.index("<table")
        assert html.rstrip().endswith("</div>")

    def test_the_class_it_emits_is_defined_in_the_stylesheet(self):
        """The whole defect was a wrapper whose class had no rule."""
        from shared.html_layout import css_report
        from shared.html_theme import css_vars

        assert ".tbl-wrap{overflow-x:auto" in css_report(css_vars("light"))
