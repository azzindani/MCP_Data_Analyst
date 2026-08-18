"""Where a generated page gets Plotly from, and what a caller gets inline.

This repo already referenced a sidecar from its charts, but still inlined the
4.85 MB library into every report -- a profile report was 5.2 MB, an EDA report
4.9 MB. Machine Learning inlined it in both, so the same kind of chart was 10 KB
here and 4.86 MB there, and asking that repo for the bytes of one put 6.21 MB of
base64 into a single tool result.

Both repos now do the same thing everywhere: the page loads `plotly.min.js` from
beside itself, and the copy travelling inline is a self-contained SVG instead.

`shared/plotly_bundle.py`, `shared/svg_chart.py` and `shared/plotly_payload.py`
are vendored byte-for-byte in both repos, so this file is identical to Machine
Learning's copy. Neither repo's CI can see the other, which is what
SHELL_DIGEST in test_chart_page_shared.py exists to catch.
"""

from __future__ import annotations

import base64
from pathlib import Path

import numpy as np
import plotly.graph_objects as go
import pytest

from shared.file_utils import embed_content
from shared.html_theme import save_chart
from shared.plotly_bundle import (
    INLINE,
    MAX_EMBED_BYTES,
    SIDECAR,
    ensure_plotly_js,
    include_plotlyjs_for,
    plotly_script_tag,
    references_sidecar,
)


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


class TestTheSidecar:
    def test_the_library_is_written_beside_the_page(self, chart: Path):
        assert (chart.parent / "plotly.min.js").exists()

    def test_the_page_references_it_rather_than_inlining_it(self, chart: Path):
        text = chart.read_text(encoding="utf-8")
        assert references_sidecar(text)
        assert "Plotly.newPlot" in text

    def test_the_page_is_kilobytes_not_megabytes(self, chart: Path):
        """4.86 MB before; the library is now paid for once per directory."""
        assert chart.stat().st_size < 200_000

    def test_it_is_written_once_not_per_page(self, tmp_path: Path):
        assert ensure_plotly_js(tmp_path) is True
        first = (tmp_path / "plotly.min.js").stat().st_mtime_ns
        assert ensure_plotly_js(tmp_path) is True
        assert (tmp_path / "plotly.min.js").stat().st_mtime_ns == first

    def test_it_creates_a_directory_that_does_not_exist_yet(self, tmp_path: Path):
        target = tmp_path / "nested" / "deeper"
        assert ensure_plotly_js(target) is True
        assert (target / "plotly.min.js").exists()

    def test_include_mode_matches_what_plotly_expects(self, tmp_path: Path):
        assert include_plotlyjs_for(tmp_path) == SIDECAR

    def test_a_hand_built_page_gets_a_script_tag(self, tmp_path: Path):
        tag = plotly_script_tag(tmp_path)
        assert tag == '<script src="plotly.min.js"></script>'


class TestFallbackWhenTheSidecarCannotBeWritten:
    def test_it_inlines_rather_than_pointing_at_a_cdn(self, tmp_path, monkeypatch):
        """This server's founding constraint is that it works fully offline. A
        CDN fallback is a page that can never render; inlining always works."""
        monkeypatch.setattr("shared.plotly_bundle.ensure_plotly_js", lambda _d: False)
        from shared import plotly_bundle

        assert plotly_bundle.include_plotlyjs_for(tmp_path) is INLINE

    def test_the_inline_tag_carries_the_real_library(self, tmp_path, monkeypatch):
        monkeypatch.setattr("shared.plotly_bundle.ensure_plotly_js", lambda _d: False)
        from shared import plotly_bundle

        tag = plotly_bundle.plotly_script_tag(tmp_path)
        # The library's own source mentions cdn.plot.ly, so assert on the shape
        # of the tag: an inline <script> body, never a remote src.
        assert tag.startswith("<script>")
        assert "src=" not in tag[:200]
        assert len(tag) > 1_000_000


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
        csv.write_text("a,b\n1,2\n")
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
        assert "plotGlPixelRatio" not in chart.read_text(encoding="utf-8")

    def test_the_config_matches_the_sibling_repo(self):
        """Both repos ship this string; it is the one the other one uses."""
        from shared.html_layout import PLOTLY_CFG_JS

        assert PLOTLY_CFG_JS == '{"responsive":true,"displayModeBar":true,"scrollZoom":true}'
