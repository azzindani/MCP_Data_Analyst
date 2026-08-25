"""Where a generated page gets Plotly from, and what a caller gets inline.

**A file this server writes has to render on its own.** The sidecar broke that
and nothing here caught it: pages loaded `plotly.min.js` from beside themselves,
which is right for a directory served whole and wrong for a file that travels --
downloaded alone, copied elsewhere, attached to a message. Every one of those
was a page with a title, an empty bordered box, and `Plotly is not defined` in a
console nobody has open. It was every chart the fleet produced, and it failed in
silence.

The tests below asserted the sidecar was there, which is why they all passed
while the artifacts were unusable. They now assert the property that actually
matters -- the page references nothing outside itself -- by walking every src
and href in the written file.

The size problem the sidecar solved is real and separate: it is about what
travels in a *tool result*, not what is on disk. `_self_contained` swaps in the
few-KB SVG drawing for the returned copy and `MAX_EMBED_BYTES` refuses anything
oversized, so the file can be 4.86 MB and the response stays small.

`shared/plotly_bundle.py`, `shared/svg_chart.py` and `shared/plotly_payload.py`
are vendored byte-for-byte in both repos, so this file is identical to Machine
Learning's copy. Neither repo's CI can see the other, which is what
SHELL_DIGEST in test_chart_page_shared.py exists to catch.
"""

from __future__ import annotations

import base64
import re
from pathlib import Path

import numpy as np
import plotly.graph_objects as go
import pytest

from shared.file_utils import embed_content
from shared.html_theme import save_chart
from shared.plotly_bundle import (
    INLINE,
    MAX_EMBED_BYTES,
    include_plotlyjs_for,
    is_plotly_page,
    plotly_script_tag,
    references_sidecar,
)

# Anything a browser would have to fetch to finish drawing the page. A
# self-contained file has none of these; data: URIs travel with the file.
_EXTERNAL_REF = re.compile(r"""\b(?:src|href)\s*=\s*["']([^"']+)["']""", re.I)


_SCRIPT_BODY = re.compile(r"<script\b[^>]*>.*?</script>", re.I | re.S)


def page_markup(html: str) -> str:
    """The page's own markup, with script bodies removed.

    A script's *body* travels inside the file; only tag attributes name things
    the browser has to go and fetch. The inlined library is 4.85 MB of minified
    JavaScript that mentions plotly.com and cdn.plot.ly in its own source, so
    scanning the raw text finds URLs no browser ever requests.
    """
    return _SCRIPT_BODY.sub("<script></script>", html)


def external_refs(html: str) -> list[str]:
    """Every URL this page needs from somewhere other than itself."""
    return [u for u in _EXTERNAL_REF.findall(page_markup(html)) if not u.startswith(("data:", "#", "javascript:"))]


@pytest.fixture()
def chart(tmp_path: Path) -> Path:
    fig = go.Figure(go.Bar(x=["West", "East", "North"], y=[5000.0, 7500.0, 3200.0]))
    fig.update_layout(title="Revenue by region", xaxis_title="region", yaxis_title="revenue")
    out = tmp_path / "chart.html"
    save_chart(fig, str(out), "chart", tmp_path / "src.csv", "device", False, lambda _p: None)
    return out


def _inline(path: Path) -> dict:
    result: dict = {"success": True}
    embed_content(result, path, True)
    return result


class TestTheWrittenPageStandsAlone:
    """The guard that was missing. Each of these fails on a sidecar page."""

    def test_it_references_nothing_outside_itself(self, chart: Path):
        refs = external_refs(chart.read_text(encoding="utf-8"))
        assert refs == [], f"page needs files it does not carry: {refs}"

    def test_no_sidecar_is_written_beside_it(self, chart: Path):
        assert not (chart.parent / "plotly.min.js").exists()

    def test_it_carries_the_library_itself(self, chart: Path):
        text = chart.read_text(encoding="utf-8")
        assert not references_sidecar(text)
        assert "Plotly.newPlot" in text
        assert is_plotly_page(text)

    def test_it_is_the_size_of_a_page_carrying_a_library(self, chart: Path):
        """12 KB meant the library was somewhere else. 4.86 MB means it is here."""
        assert chart.stat().st_size > 4_000_000

    def test_moving_it_somewhere_empty_changes_nothing(self, chart: Path, tmp_path: Path):
        """How the defect reached a user: one file, copied on its own."""
        alone = tmp_path / "alone"
        alone.mkdir()
        moved = alone / "chart.html"
        moved.write_bytes(chart.read_bytes())
        assert external_refs(moved.read_text(encoding="utf-8")) == []
        assert sorted(p.name for p in alone.iterdir()) == ["chart.html"]

    def test_include_mode_is_inline(self, tmp_path: Path):
        assert include_plotlyjs_for(tmp_path) is INLINE

    def test_a_hand_built_page_gets_the_library_not_a_link(self, tmp_path: Path):
        tag = plotly_script_tag(tmp_path)
        # The library's own source mentions cdn.plot.ly, so assert on the shape
        # of the tag: an inline <script> body, never a remote or relative src.
        assert tag.startswith("<script>")
        assert "src=" not in tag[:200]
        assert len(tag) > 1_000_000

    def test_a_page_written_before_this_is_still_recognised(self):
        legacy = '<div class="plotly-graph-div"></div><script src="plotly.min.js"></script>'
        assert references_sidecar(legacy) is True
        assert is_plotly_page(legacy) is True


class TestInlineContentIsSelfContained:
    def test_returned_content_needs_no_sibling_file(self, chart: Path):
        html = base64.b64decode(_inline(chart)["content_base64"]).decode("utf-8")
        assert "plotly.min.js" not in html
        assert "<svg" in html

    def test_it_is_small_enough_to_return(self, chart: Path):
        assert len(_inline(chart)["content_base64"]) < 200_000

    def test_the_caller_is_told_what_it_received(self, chart: Path):
        assert "public_url" in _inline(chart)["content_note"]

    def test_the_file_on_disk_is_untouched(self, chart: Path):
        before = chart.read_bytes()
        _inline(chart)
        assert chart.read_bytes() == before

    def test_nothing_is_embedded_without_return_content(self, chart: Path):
        result: dict = {"success": True}
        embed_content(result, chart, False)
        assert "content_base64" not in result

    def test_values_survive_into_the_drawing(self, chart: Path):
        """A chart that renders but shows the wrong data is worse than none.
        The bars carry no printed values, so the categories and one bar per
        category are what prove the figure travelled."""
        html = base64.b64decode(_inline(chart)["content_base64"]).decode("utf-8")
        for region in ("West", "East", "North"):
            assert region in html
        assert html.count("<rect") >= 3


class TestTheSizeBackstop:
    def test_oversized_content_is_refused_rather_than_encoded(self, tmp_path: Path):
        big = tmp_path / "big.bin"
        big.write_bytes(b"\0" * (MAX_EMBED_BYTES + 1))
        result = _inline(big)
        assert "content_base64" not in result
        assert "exceeds" in result["content_note"]

    def test_content_within_the_limit_still_embeds(self, tmp_path: Path):
        small = tmp_path / "small.bin"
        small.write_bytes(b"\0" * 1024)
        assert "content_base64" in _inline(small)


class TestNonHtmlIsPassedThroughUnchanged:
    def test_a_csv_is_embedded_verbatim(self, tmp_path: Path):
        csv = tmp_path / "d.csv"
        csv.write_text("a,b\n1,2\n", encoding="utf-8")
        result = _inline(csv)
        assert base64.b64decode(result["content_base64"]) == csv.read_bytes()

    def test_a_model_file_is_embedded_verbatim(self, tmp_path: Path):
        blob = tmp_path / "m.pkl"
        blob.write_bytes(np.arange(64, dtype=np.int64).tobytes())
        result = _inline(blob)
        assert base64.b64decode(result["content_base64"]) == blob.read_bytes()


class TestWebGlChartsActuallyDraw:
    """plotGlPixelRatio was pinned to 0 in this repo's Plotly config.

    It sets the WebGL backing-store resolution, so 0 gives a 0x0 canvas and any
    WebGL trace draws nothing -- axes, ticks and title render fine, the data does
    not. Confirmed in a headless browser: canvas 0x0 with it against 1768x1168
    without, and a 3000-point scattergl came out as empty axes.

    It mattered because plotly.express switches scatter to WebGL on its own above
    roughly a thousand points, and this repo calls px.scatter. Data Analyst never
    set it, which is also how the two repos came to disagree.
    """

    def test_the_js_config_does_not_pin_the_gl_pixel_ratio(self):
        from shared.html_layout import PLOTLY_CFG_JS

        assert "plotGlPixelRatio" not in PLOTLY_CFG_JS

    def test_the_python_config_does_not_either(self):
        from shared.html_layout import plotly_config

        assert "plotGlPixelRatio" not in plotly_config()

    def test_saved_charts_do_not_pin_it(self, chart: Path):
        """The page's own config, not the library's source.

        plotly.min.js declares the attribute itself, so once the library is
        inlined the bare substring is always present -- read the config the
        page passes to newPlot instead.
        """
        from shared.plotly_payload import split_newplot

        _, _, _, _, _, after = split_newplot(chart.read_text(encoding="utf-8"))
        config = after[: after.find(")")]
        assert "plotGlPixelRatio" not in config

    def test_the_config_matches_the_sibling_repo(self):
        """Both repos ship this string; it is the one the other one uses."""
        from shared.html_layout import PLOTLY_CFG_JS

        assert PLOTLY_CFG_JS == '{"responsive":true,"displayModeBar":true,"scrollZoom":true}'
