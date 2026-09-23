"""Every card is a panel in the page's own document, drawn by one renderer.

The dashboard's charts were twelve hand-written JavaScript templates with
their columns pasted into code and their colours, caps and bin counts baked
in: an option meant editing template code for every kind, and a spec could be
validated, echoed and then ignored, because the templates read what the
detector found, not what the spec said.

Now the page carries _PANELS -- each card's columns, aggregate, title and
style -- and one renderer draws each figure from its panel. What has to hold,
checked on the figures the page's own script draws (tests/dashboard_page.py):

- one panel per card, same id and title, with its style as a field;
- a panel field IS the figure: change it in the document and the figure follows;
- each kind's numbers are the numbers pandas computes from the same rows;
- the renderer's code names no column: names are data, never code;
- the filter bar's subset reaches every figure.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from servers.data_advanced._adv_dashboard import PANEL_STYLE, generate_dashboard
from tests.dashboard_page import NODE, drawn, main_script

pytestmark = pytest.mark.skipif(NODE is None, reason="node is not installed")


@pytest.fixture
def sales(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(7)
    n = 240
    path = tmp_path / "sales.csv"
    pd.DataFrame(
        {
            "day": pd.date_range("2024-01-01", periods=n).strftime("%Y-%m-%d"),
            "region": rng.choice(["North", "South", "East"], n),
            "channel": rng.choice(["web", "store"], n),
            "country": rng.choice(["France", "Germany", "Japan"], n),
            "revenue": rng.normal(1000, 200, n).round(2),
            "units": rng.integers(1, 50, n).astype(float),
        }
    ).to_csv(path, index=False)
    return path


def _page(path: Path, **kw) -> str:
    out = path.parent / f"dash_{abs(hash(json.dumps(kw, sort_keys=True, default=str)))}.html"
    r = generate_dashboard(str(path), output_path=str(out), open_after=False, **kw)
    assert r["success"] is True, r
    return out.read_text(encoding="utf-8")


def _layout(*panels: dict) -> dict:
    return {"spec": {"layout": list(panels)}}


def _with_panels(html: str, edit) -> str:
    """The page with its _PANELS document edited -- what a later customization writes."""
    match = re.search(r"const _PANELS=(.*?);\n", html)
    assert match
    panels = json.loads(match.group(1))
    edit(panels)
    return html.replace(match.group(0), f"const _PANELS={json.dumps(panels)};\n")


class TestEveryCardIsAPanel:
    def test_one_panel_per_card_with_its_title_and_style(self, sales):
        html = _page(sales)
        cards = re.findall(r'<div id="([^"]+)" style="width:100%;height:100%"></div>', html)
        titles = re.findall(r'<div class="cc-hdr"><h3>([^<]+)</h3>', html)
        out = drawn(html)
        assert [p["id"] for p in out["panels"]] == cards
        assert [p["title"] for p in out["panels"]] == titles
        for p in out["panels"]:
            assert p["style"] == PANEL_STYLE.get(p["type"], {}), p["id"]
        assert set(out["figures"]) == set(cards) and out["warnings"] == []

    def test_a_panel_names_its_columns_by_role(self, sales):
        out = drawn(_page(sales, **_layout({"chart": "bar", "cols": {"category": "channel", "value": "units"}})))
        panel = out["panels"][0]
        assert (panel["type"], panel["category"], panel["value"], panel["agg"]) == ("bar", "channel", "units", "sum")


class TestAPanelFieldIsTheFigure:
    """Edit the page's document and the figure follows -- no template to change."""

    def test_colour(self, sales):
        html = _page(sales, **_layout({"chart": "bar", "cols": {"category": "region", "value": "revenue"}}))
        edited = _with_panels(html, lambda ps: ps[0]["style"].update(color="#123456"))
        assert drawn(edited)["figures"]["p0_bar"]["data"][0]["marker"]["color"] == "#123456"

    def test_top_n(self, sales):
        html = _page(sales, **_layout({"chart": "bar", "cols": {"category": "region", "value": "revenue"}}))
        edited = _with_panels(html, lambda ps: ps[0]["style"].update(top_n=1))
        trace = drawn(edited)["figures"]["p0_bar"]["data"][0]
        top = pd.read_csv(sales).groupby("region")["revenue"].sum().idxmax()
        assert trace["x"] == [top]

    def test_moving_average_window(self, sales):
        html = _page(sales, **_layout({"chart": "line", "cols": {"date": "day", "value": "revenue"}}))
        edited = _with_panels(html, lambda ps: ps[0]["style"].update(ma=2))
        trend = drawn(edited)["figures"]["p0_line"]["data"][1]
        assert trend["name"] == "2-period MA"

    def test_histogram_bins(self, sales):
        html = _page(sales, **_layout({"chart": "histogram", "cols": {"value": "units"}}))
        edited = _with_panels(html, lambda ps: ps[0]["style"].update(bins=7))
        assert drawn(edited)["figures"]["p0_histogram"]["data"][0]["nbinsx"] == 7

    def test_the_panels_own_layout_is_laid_over_the_theme(self, sales):
        html = _page(sales, **_layout({"chart": "bar", "cols": {"category": "region", "value": "revenue"}}))
        edited = _with_panels(html, lambda ps: ps[0]["style"].update(layout={"yaxis": {"type": "log"}}))
        layout = drawn(edited)["figures"]["p0_bar"]["layout"]
        assert layout["yaxis"]["type"] == "log"
        assert layout["yaxis"]["automargin"] is True and "gridcolor" in layout["yaxis"], "merged, not replaced"


class TestTheNumbersAreTheData:
    @pytest.mark.parametrize("agg", ["sum", "mean", "median", "min", "max", "count", "count_distinct"])
    def test_bar(self, sales, agg):
        spec = _layout({"chart": "bar", "cols": {"category": "region", "value": "units"}, "agg": agg})
        trace = drawn(_page(sales, **spec))["figures"]["p0_bar"]["data"][0]
        how = "nunique" if agg == "count_distinct" else agg
        want = pd.read_csv(sales).groupby("region")["units"].agg(how)
        assert dict(zip(trace["x"], trace["y"], strict=True)) == pytest.approx(want.to_dict())

    def test_time_series_by_month(self, sales):
        spec = _layout({"chart": "line", "cols": {"date": "day", "value": "revenue"}, "agg": "sum"})
        trace = drawn(_page(sales, **spec))["figures"]["p0_line"]["data"][0]
        df = pd.read_csv(sales)
        want = df.groupby(df["day"].str[:7])["revenue"].sum()
        assert dict(zip(trace["x"], trace["y"], strict=True)) == pytest.approx(want.to_dict())

    def test_pie_counts_rows(self, sales):
        out = drawn(_page(sales, **_layout({"chart": "pie"})))
        trace = out["figures"]["p0_pie"]["data"][0]
        want = pd.read_csv(sales)[out["panels"][0]["category"]].value_counts()
        assert dict(zip(trace["labels"], trace["values"], strict=True)) == want.to_dict()

    def test_choropleth(self, sales):
        spec = _layout({"chart": "choropleth", "cols": {"location": "country", "value": "revenue"}, "agg": "mean"})
        trace = drawn(_page(sales, **spec))["figures"]["p0_choropleth"]["data"][0]
        want = pd.read_csv(sales).groupby("country")["revenue"].mean()
        assert dict(zip(trace["locations"], trace["z"], strict=True)) == pytest.approx(want.to_dict())

    def test_detected_grouped_bar_heatmap_and_correlation(self, sales):
        out = drawn(_page(sales))
        df = pd.read_csv(sales)
        grp = next(f for cid, f in out["figures"].items() if cid.startswith("grp_"))
        panel = next(p for p in out["panels"] if p["id"].startswith("grp_"))
        want = df.groupby([panel["group"], panel["category"]])[panel["value"]].sum()
        for trace in grp["data"]:
            for x, y in zip(trace["x"], trace["y"], strict=True):
                assert y == pytest.approx(want.get((trace["name"], x), 0))
        hm_panel = next(p for p in out["panels"] if p["type"] == "agg_hm")
        hm = out["figures"][hm_panel["id"]]["data"][0]
        cells = df.groupby([hm_panel["category"], hm_panel["group"]])[hm_panel["value"]].sum()
        for i, row in enumerate(hm["y"]):
            for j, col in enumerate(hm["x"]):
                assert hm["z"][i][j] == pytest.approx(cells.get((row, col), 0))
        corr = out["figures"]["corr_hm"]["data"][0]
        want_corr = df[corr["x"]].corr()
        assert np.allclose(np.array(corr["z"]), want_corr.to_numpy(), atol=1e-9)


class TestNamesAreDataNotCode:
    def test_the_renderer_code_names_no_column(self, sales):
        script = main_script(_page(sales))
        # The script opens with its data -- _RAW, _PANELS, _KPIS, _THEME -- and
        # everything from _TOTAL on is code.
        code = script[script.index("const _TOTAL=") :]
        # A name pasted into code is a string literal there (r['region']).
        for column in ("region", "channel", "country", "revenue", "units"):
            assert f"'{column}'" not in code and f'"{column}"' not in code, f"{column!r} is written into the code"


class TestTheFilteredRowsReachEveryFigure:
    def test_a_subset_redraws_from_the_subset(self, sales):
        html = _page(sales, **_layout({"chart": "bar", "cols": {"category": "region", "value": "units"}}))
        rows = pd.read_csv(sales).to_dict(orient="records")
        north = [r for r in rows if r["region"] == "North"]
        trace = drawn(html, rows=north)["figures"]["p0_bar"]["data"][0]
        assert trace["x"] == ["North"] and trace["y"] == pytest.approx([sum(r["units"] for r in north)])


class TestTheThemeIsTheFrame:
    @pytest.mark.parametrize("theme", ["dark", "light"])
    def test_every_figure_is_drawn_on_the_themes_colours(self, sales, theme):
        from shared.html_theme import theme_plot_colors

        bg, font, _ = theme_plot_colors(theme)
        for cid, f in drawn(_page(sales, theme=theme))["figures"].items():
            assert (f["layout"]["paper_bgcolor"], f["layout"]["font"]["color"]) == (bg, font), cid

    @pytest.mark.parametrize("dark", [False, True], ids=["reader-light", "reader-dark"])
    def test_a_device_page_draws_in_the_readers_scheme(self, sales, dark):
        # It drew every chart light, on a page that turned dark around them.
        from shared.html_theme import theme_plot_colors

        bg, font, _ = theme_plot_colors("dark" if dark else "light")
        figures = drawn(_page(sales, theme="device"), dark=dark)["figures"]
        assert figures
        for cid, f in figures.items():
            assert (f["layout"]["paper_bgcolor"], f["layout"]["font"]["color"]) == (bg, font), cid
