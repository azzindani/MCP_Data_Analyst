"""Customising a chart's title left the reader looking at the old one.

Every standalone chart page is built by save_chart(), which hoists the figure's
title into the page's <h1 class="chart-title"> and clears it from the layout --
"the page has one caption and the chart gets the vertical space back".

customize_chart rewrites an existing page's HTML rather than rebuilding the
figure, and it put the new title in two places, neither of them that heading:

    <title> tag        updated          (browser tab only)
    layout["title"]    set to the new   (inside the SVG)
    <h1 chart-title>   left alone       (what the reader actually sees)

So a customised page showed two different titles at once -- the stale heading
above the chart, and the new one inside it. The tool reported success and listed
the title among changes_applied.

The one inside the SVG was clipped as well. Plotly centres a title and never
wraps it, so at 390px wide a 52-character title measured 420px inside a 352px
plot and lost 34px off each end, beginning and ending mid-word. The heading has
no such problem: it is HTML, and it wraps.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from servers.data_visual.engine import customize_chart, generate_chart

NEW_TITLE = "Customized: Spends Distribution by Campaign Platform"


def _page_heading(path: Path) -> str | None:
    m = re.search(r'<h1 class="chart-title">([^<]*)</h1>', path.read_text(encoding="utf-8"))
    return m.group(1) if m else None


def _tab_title(path: Path) -> str | None:
    m = re.search(r"<title>([^<]*)</title>", path.read_text(encoding="utf-8"))
    return m.group(1) if m else None


def _layout_title(path: Path):
    """The figure's own title, parsed rather than pattern-matched.

    A regex for '"title": {"text": ...}' also matches xaxis.title and
    yaxis.title, which are legitimately present -- it reported the axis label
    as the chart title and failed a passing case.
    """
    import json

    from shared.plotly_payload import split_newplot

    _, _, _, _, layout, _ = split_newplot(path.read_text(encoding="utf-8"))
    return json.loads(layout).get("title")


@pytest.fixture()
def source_chart(simple_csv: Path, tmp_path: Path) -> Path:
    out = tmp_path / "chart.html"
    r = generate_chart(
        str(simple_csv),
        "bar",
        "Revenue",
        category_column="Region",
        output_path=str(out),
        open_after=False,
    )
    assert r["success"] is True, r.get("error")
    return out


class TestTheHeadingIsTheTitle:
    def test_the_visible_heading_changes(self, source_chart: Path, tmp_path: Path):
        out = tmp_path / "custom.html"
        r = customize_chart(str(source_chart), title=NEW_TITLE, output_path=str(out))
        assert r["success"] is True, r.get("error")
        assert _page_heading(out) == NEW_TITLE

    def test_the_old_heading_is_gone(self, source_chart: Path, tmp_path: Path):
        before = _page_heading(source_chart)
        out = tmp_path / "custom.html"
        customize_chart(str(source_chart), title=NEW_TITLE, output_path=str(out))
        assert _page_heading(out) != before

    def test_the_browser_tab_matches_the_heading(self, source_chart: Path, tmp_path: Path):
        out = tmp_path / "custom.html"
        customize_chart(str(source_chart), title=NEW_TITLE, output_path=str(out))
        assert _tab_title(out) == _page_heading(out)

    def test_the_title_is_not_pushed_back_into_the_chart(self, source_chart: Path, tmp_path: Path):
        """Inside the SVG it cannot wrap, and the page would caption twice."""
        out = tmp_path / "custom.html"
        customize_chart(str(source_chart), title=NEW_TITLE, output_path=str(out))
        assert _layout_title(out) is None, _layout_title(out)

    def test_the_page_says_it_once(self, source_chart: Path, tmp_path: Path):
        out = tmp_path / "custom.html"
        customize_chart(str(source_chart), title=NEW_TITLE, output_path=str(out))
        body = out.read_text(encoding="utf-8").split("</head>", 1)[1]
        assert body.count(NEW_TITLE) == 1, body.count(NEW_TITLE)


class TestTheRestOfThePageSurvives:
    def test_the_chart_still_renders_as_json(self, source_chart: Path, tmp_path: Path):
        import json

        from shared.plotly_payload import split_newplot

        out = tmp_path / "custom.html"
        customize_chart(str(source_chart), title=NEW_TITLE, output_path=str(out))
        _, _, data, _, layout, _ = split_newplot(out.read_text(encoding="utf-8"))
        assert json.loads(data)
        assert json.loads(layout)

    def test_customising_something_else_leaves_the_heading_alone(self, source_chart: Path, tmp_path: Path):
        before = _page_heading(source_chart)
        out = tmp_path / "custom.html"
        r = customize_chart(str(source_chart), x_label="Platform", output_path=str(out))
        assert r["success"] is True, r.get("error")
        assert _page_heading(out) == before

    def test_the_axis_label_still_applies(self, source_chart: Path, tmp_path: Path):
        out = tmp_path / "custom.html"
        customize_chart(str(source_chart), x_label="Platform", output_path=str(out))
        assert "Platform" in out.read_text(encoding="utf-8")


class TestATitleCannotBreakThePage:
    @pytest.mark.parametrize(
        "hostile",
        [
            "Spend <script>alert(1)</script>",
            r"Spend \1 backslash",
            "Q1 & Q2 > Q3",
        ],
    )
    def test_markup_and_group_references_are_neutralised(self, source_chart: Path, tmp_path: Path, hostile: str):
        """re.sub reads \\1 in a replacement string as a group reference, and an
        unescaped < opens a tag in the heading."""
        out = tmp_path / "custom.html"
        r = customize_chart(str(source_chart), title=hostile, output_path=str(out))
        assert r["success"] is True, r.get("error")
        text = out.read_text(encoding="utf-8")
        assert "<script>alert(1)</script>" not in text
        heading = _page_heading(out)
        assert heading is not None and heading.strip()
