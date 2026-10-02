"""Two components the mockups had and the engine did not: a ranked list, and a donut that says its total.

`ranking` lists the leaders of a category with a rank, a bar, the value and -- for what adds up -- its
share of the whole; it follows the filters like every panel. A pie's `hole` is a percentage of its radius
and `center` is the words in the middle ("total" is the sum the ring shows).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from servers.data_advanced._adv_dashboard import generate_dashboard
from shared.dashboard_spec import CHART_KINDS, CHART_NEEDS, CHART_STYLE, SpecError, validate_panel_style


@pytest.fixture
def data(tmp_path, monkeypatch) -> str:
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(5)
    path = tmp_path / "s.csv"
    pd.DataFrame(
        {
            "region": rng.choice(["North", "South", "East", "West"], 400),
            "spend": rng.integers(10, 500, 400),
            "day": pd.date_range("2024-01-01", periods=400).strftime("%Y-%m-%d"),
        }
    ).to_csv(path, index=False)
    return str(path)


def _page(data, tmp_path, panels):
    result = generate_dashboard(data, output_path=str(tmp_path / "p.html"), open_after=False, spec={"layout": panels})
    assert result["success"] is True, result.get("error")
    return result, (tmp_path / "p.html").read_text(encoding="utf-8")


class TestTheRanking:
    def test_it_is_in_the_vocabulary_with_its_roles_and_style(self):
        assert "ranking" in CHART_KINDS and CHART_NEEDS["ranking"] == ("category", "value")
        assert {"top_n", "sort", "format", "color"} <= set(CHART_STYLE["ranking"])

    def test_a_page_carries_the_panel_and_the_drawing_of_it(self, data, tmp_path):
        result, html = _page(
            data,
            tmp_path,
            [{"chart": "ranking", "cols": {"category": "region", "value": "spend"}, "style": {"top_n": 3}}],
        )
        panel = result["spec"]["layout"][0]
        assert panel["chart"] == "ranking" and panel["style"]["top_n"] == 3
        assert "HTMLP.ranking=function" in html and '"type":"ranking"' in html and ".rank{" in html

    def test_a_missing_role_is_refused_by_name(self, data, tmp_path):
        result = generate_dashboard(
            data,
            output_path=str(tmp_path / "x.html"),
            open_after=False,
            spec={"layout": [{"chart": "ranking", "cols": {"category": "region"}}]},
        )
        assert result["success"] is False and "value" in result["error"]

    def test_only_what_it_draws_is_style(self):
        validate_panel_style("layout[0]", "ranking", {"top_n": 5, "color": "#112233"})
        with pytest.raises(SpecError, match="does not draw 'hole'"):
            validate_panel_style("layout[0]", "ranking", {"hole": 50})

    def test_the_share_is_for_what_adds_up(self):
        from servers.data_advanced._dash_ext import EXT_JS

        assert "var adds=!p.metric&&(!p.agg||p.agg==='sum'||p.agg==='count')" in EXT_JS


class TestTheDonut:
    def test_hole_and_center_are_pie_style(self):
        assert {"hole", "center"} <= set(CHART_STYLE["pie"])
        validate_panel_style("layout[0]", "pie", {"hole": 65, "center": "total"})
        validate_panel_style("layout[0]", "pie", {"hole": 0})

    @pytest.mark.parametrize("bad", [{"hole": 90}, {"hole": -1}, {"hole": 50.5}, {"hole": True}, {"center": "x" * 41}])
    def test_values_a_ring_cannot_draw_are_refused(self, bad):
        with pytest.raises(SpecError):
            validate_panel_style("layout[0]", "pie", bad)

    def test_the_renderer_draws_the_centre(self, data, tmp_path):
        _, html = _page(
            data,
            tmp_path,
            [
                {
                    "chart": "pie",
                    "cols": {"category": "region", "value": "spend"},
                    "style": {"hole": 60, "center": "total"},
                }
            ],
        )
        assert "hole=s.hole!==undefined?s.hole/100:0.38" in html and "s.center==='total'" in html
