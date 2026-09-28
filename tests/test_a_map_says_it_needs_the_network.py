"""A map carries its outlines, and says so when it cannot.

plotly fetches a map's outlines from `https://cdn.plot.ly/un/` when the page is
opened, and a tiled map fetches raster tiles. Rendered with the network
blocked, a choropleth was a colour bar beside an empty white rectangle:

    generate_geo_map(...)         success: true
    open it offline               console: "unexpected error while fetching
                                  topojson file at cdn.plot.ly/un/world_110m.json"
                                  page: a legend, and nothing else

This file used to pin the answer "say so": `self_contained: false`,
`needs_network_for`, a warn. The sweep's screenshots showed that answer still
hands a reader a blank map (F20). plotly.js reads `window.PlotlyGeoAssets`
before it fetches, so the world outlines now travel in the page -- the same
file plotly would have fetched, matched by plotly itself -- and the response
is honest the other way: self-contained, nothing to fetch. A map whose outlines
are not carried (tiles, another scope) still says it needs the network.
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

from servers.data_visual import engine as vis  # noqa: E402
from shared.plotly_bundle import REMOTE_BASEMAP_TRACES, remote_basemap_traces  # noqa: E402


@pytest.fixture
def countries(tmp_path) -> Path:
    f = tmp_path / "geo.csv"
    pd.DataFrame({"country": ["France", "Germany", "Spain", "Italy"], "spends": [100, 300, 50, 220]}).to_csv(
        f, index=False
    )
    return f


@pytest.fixture
def points(tmp_path) -> Path:
    f = tmp_path / "points.csv"
    pd.DataFrame(
        {
            "lat": [48.85, 52.52, 40.41],
            "lon": [2.35, 13.40, -3.70],
            "spends": [100.0, 300.0, 50.0],
        }
    ).to_csv(f, index=False)
    return f


def geo(path, tmp_path, **kw):
    return vis.generate_geo_map(str(path), output_path=str(tmp_path / "m.html"), open_after=False, **kw)


class TestAMapCarriesItsOutlines:
    def test_a_choropleth_is_self_contained(self, countries, tmp_path):
        r = geo(countries, tmp_path, location_column="country", value_column="spends")
        assert r["success"] is True, r.get("error")
        assert r["self_contained"] is True
        assert r["needs_network_for"] == []
        assert "hint" not in r
        assert not any("network" in p["message"] for p in r["progress"] if p.get("status") == "warn")

    def test_the_page_holds_the_outlines_plotly_would_have_fetched(self, countries, tmp_path):
        r = geo(countries, tmp_path, location_column="country", value_column="spends")
        page = Path(r["output_path"]).read_text(encoding="utf-8")
        assert 'a.topojson["world_110m"]={"type":"Topology"' in page
        # Set before the figure is drawn, or plotly has already fetched.
        assert page.index("PlotlyGeoAssets=window.PlotlyGeoAssets") < page.index("Plotly.newPlot")

    def test_a_point_map_carries_the_same(self, points, tmp_path):
        r = geo(points, tmp_path, lat_column="lat", lon_column="lon", value_column="spends")
        assert r["success"] is True, r.get("error")
        assert r["self_contained"] is True
        assert "world_110m" in Path(r["output_path"]).read_text(encoding="utf-8")

    def test_a_chart_that_is_not_a_map_carries_nothing(self, tmp_path):
        import plotly.graph_objects as go

        from shared.geo_assets import assets_script, topojson_names

        assert assets_script(topojson_names(go.Figure(go.Bar(x=["a"], y=[1])))) == ""


class TestAMapWhoseOutlinesAreNotCarriedStillSaysSo:
    def test_another_scope_is_not_carried(self):
        import plotly.graph_objects as go

        from shared.geo_assets import inlined_traces, topojson_names

        fig = go.Figure(go.Choropleth(locations=["FRA"], z=[1]))
        fig.update_geos(scope="europe")
        assert topojson_names(fig) == ["europe_110m"]
        assert inlined_traces(fig) == []
        assert remote_basemap_traces(fig) == ["choropleth"]

    def test_a_tiled_map_still_needs_its_tiles(self):
        import plotly.graph_objects as go

        from shared.geo_assets import inlined_traces

        fig = go.Figure(go.Scattermap(lat=[1], lon=[2]))
        assert inlined_traces(fig) == [] and remote_basemap_traces(fig) == ["scattermap"]


class TestTheTraceListIsHonest:
    def test_a_bar_chart_needs_nothing(self, tmp_path):
        import plotly.graph_objects as go

        fig = go.Figure(go.Bar(x=["a"], y=[1]))
        assert remote_basemap_traces(fig) == []

    def test_a_choropleth_is_named(self, tmp_path):
        import plotly.graph_objects as go

        fig = go.Figure(go.Choropleth(locations=["FRA"], z=[1]))
        assert remote_basemap_traces(fig) == ["choropleth"]

    def test_each_kind_is_listed_once(self):
        import plotly.graph_objects as go

        fig = go.Figure([go.Scattergeo(lat=[1], lon=[2]), go.Scattergeo(lat=[3], lon=[4])])
        assert remote_basemap_traces(fig) == ["scattergeo"]

    def test_the_table_covers_both_plotly_spellings(self):
        """plotly renamed mapbox traces to map; both spellings still appear."""
        for name in ("scattermapbox", "scattermap", "choroplethmapbox", "choroplethmap"):
            assert name in REMOTE_BASEMAP_TRACES, name
