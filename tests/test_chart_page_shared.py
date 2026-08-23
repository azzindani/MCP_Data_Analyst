"""The standalone chart page is shared verbatim with MCP_Machine_Learning.

Data Analyst and Machine Learning ship as separate distributions with no package
between them, so "shared" means the same file vendored in both repos. That works
right up until someone edits one copy, which is how the two products ended up
rendering the same kind of artifact at different sizes with different padding and
different chrome.

Nothing in either repo's CI can see the other repo, so this test pins the parts
that must not move to a checksum. Editing the shell deliberately means updating
`SHELL_DIGEST` here — and the identical test in the other repo will fail until
that copy is synced too, which is the reminder this needs to be.

    md5sum /root/MCP_{Data_Analyst,Machine_Learning}/shared/chart_page.py
"""

from __future__ import annotations

import hashlib

from shared.chart_page import (
    CHART_MARGIN,
    CHART_PAGE_CSS,
    chart_page_html,
    chart_title_from,
    take_page_title,
)

SHELL_DIGEST = "82f934a50895fd901dfce0ef86baad61"


def _digest() -> str:
    payload = CHART_PAGE_CSS + repr(sorted(CHART_MARGIN.items()))
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


class TestTheShellIsShared:
    def test_the_page_shell_has_not_drifted(self):
        assert _digest() == SHELL_DIGEST, (
            "chart_page.py changed. Copy it to the other repo and update "
            "SHELL_DIGEST in both, or the two products drift apart again."
        )

    def test_margins_leave_room_for_tick_labels(self):
        """20px on the left rendered "5000" as "000"."""
        assert CHART_MARGIN["l"] >= 48
        assert CHART_MARGIN["b"] >= 40


class TestHeightIsAFloorNotACap:
    def test_the_chart_gets_a_viewport_proportional_height(self):
        assert "--chart-h:clamp(" in CHART_PAGE_CSS

    def test_it_is_applied_as_a_minimum(self):
        """A tall multi-panel figure must be allowed to exceed it and scroll,
        so this can never become a plain `height`."""
        assert "min-height:var(--chart-h)" in CHART_PAGE_CSS
        assert "\n  height:var(--chart-h)" not in CHART_PAGE_CSS


class TestAFigureThatAsksForLessGetsIt:
    """A floor only works when nothing deliberately sits below it.

    calc_chart_height() sizes a heatmap to its row count, so a 2x4 crosstab
    asked for 280px. The 72vh floor stretched its card to 648px and the SVG
    stayed at 280 -- 368px of empty bordered box under a chart that had been
    the right size all along. Measured, not eyeballed: card 648, svg 280.
    """

    def test_a_declared_height_is_pinned_on_the_page(self):
        page = chart_page_html("<div>c</div>", "T", ":root{}", "", chart_height=280)
        assert "--chart-h:280px" in page

    def test_it_is_written_after_the_floor_so_it_wins(self):
        page = chart_page_html("<div>c</div>", "T", ":root{}", "", chart_height=280)
        assert page.index("--chart-h:clamp(") < page.index("--chart-h:280px")

    def test_it_stays_inside_the_style_block(self):
        page = chart_page_html("<div>c</div>", "T", ":root{}", "", chart_height=280)
        assert page.index("--chart-h:280px") < page.index("</style>")

    def test_it_does_not_need_an_important(self):
        page = chart_page_html("<div>c</div>", "T", ":root{}", "", chart_height=280)
        assert "--chart-h:280px!important" not in page

    def test_a_tall_figure_pins_its_own_height_too(self):
        """The floor is not a cap either way -- a 1200px subplot grid asks for
        1200 and the card follows it up, not just down."""
        page = chart_page_html("<div>c</div>", "T", ":root{}", "", chart_height=1200)
        assert "--chart-h:1200px" in page

    def test_a_float_height_becomes_whole_pixels(self):
        page = chart_page_html("<div>c</div>", "T", ":root{}", "", chart_height=280.6)
        assert "--chart-h:281px" in page

    def test_a_figure_with_no_height_keeps_the_viewport_floor(self):
        page = chart_page_html("<div>c</div>", "T", ":root{}", "")
        assert "--chart-h:clamp(" in page
        assert "body{--chart-h:" not in page

    def test_none_and_zero_are_both_treated_as_no_height(self):
        for value in (None, 0):
            page = chart_page_html("<div>c</div>", "T", ":root{}", "", chart_height=value)
            assert "body{--chart-h:" not in page, value

    def test_the_floor_declaration_is_still_the_only_one_in_the_css(self):
        """If CHART_PAGE_CSS ever gains a second --chart-h, the override has to
        be re-checked for order -- this fails loudly rather than silently."""
        assert CHART_PAGE_CSS.count("--chart-h:") == 1


class TestPageTitle:
    def test_a_figure_title_becomes_the_heading_and_leaves_the_figure(self):
        fig = _FakeFig("Spends Over Time")
        assert take_page_title(fig, "line") == "Spends Over Time"
        assert fig.cleared is True

    def test_an_untitled_figure_falls_back_to_the_filename_stem(self):
        fig = _FakeFig("")
        assert take_page_title(fig, "roc_curve") == "Roc Curve"
        assert fig.cleared is False

    def test_a_figure_with_no_title_attribute_at_all(self):
        assert take_page_title(object(), "learning_curve") == "Learning Curve"

    def test_blank_suffix_still_yields_a_heading(self):
        assert chart_title_from("") == "Chart"


class TestPageMarkup:
    def test_the_page_carries_one_heading_and_one_chart(self):
        page = chart_page_html("<div>chart</div>", "Title", ":root{}", "")
        assert page.count('class="chart-title"') == 1
        assert page.count('class="chart-wrap"') == 1

    def test_the_viewport_meta_is_present(self):
        page = chart_page_html("", "T", ":root{}", "")
        assert 'name="viewport"' in page

    def test_device_js_is_only_included_when_given(self):
        assert "<script>dev</script>" in chart_page_html("", "T", ":root{}", "<script>dev</script>")
        assert "<script>" not in chart_page_html("", "T", ":root{}", "")


class _FakeFig:
    """Stands in for a Plotly figure without building one."""

    def __init__(self, title: str):
        self.layout = type("L", (), {"title": type("T", (), {"text": title})()})()
        self.cleared = False

    def update_layout(self, **kwargs):
        if kwargs.get("title_text", "unset") is None:
            self.cleared = True
