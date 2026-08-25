"""The one chart that is not finished when it is written.

Every other page this server produces carries everything it draws. A map does
not: plotly fetches its country outlines from `https://cdn.plot.ly/un/` when the
page is opened, and a tiled map fetches raster tiles. Rendered with the network
blocked, a choropleth is a colour bar beside an empty white rectangle -- the
same silent blank the plotly sidecar used to produce for every chart, arriving
here from a different direction:

    generate_geo_map(...)         success: true
    open it offline               console: "unexpected error while fetching
                                  topojson file at cdn.plot.ly/un/world_110m.json"
                                  page: a legend, and nothing else

Carrying the geometry would mean vendoring a world dataset into the repo and
re-doing the country-name matching plotly.js does internally, and a map joined
to the wrong outlines is worse than no map. So the geometry stays remote and
what changes is that the response says so before the file is opened, in three
places: `self_contained: false`, `needs_network_for`, and a warn.
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


class TestAMapDeclaresItsRemoteBasemap:
    def test_a_choropleth_is_not_self_contained(self, countries, tmp_path):
        r = geo(countries, tmp_path, location_column="country", value_column="spends")
        assert r["success"] is True, r.get("error")
        assert r["self_contained"] is False
        assert r["needs_network_for"] == ["choropleth"]

    def test_the_hint_says_what_is_fetched_and_what_is_not(self, countries, tmp_path):
        r = geo(countries, tmp_path, location_column="country", value_column="spends")
        assert "cdn.plot.ly" in r["hint"]
        assert "self-contained" in r["hint"], "say which parts do travel with the file"

    def test_it_warns_rather_than_only_recording(self, countries, tmp_path):
        r = geo(countries, tmp_path, location_column="country", value_column="spends")
        warnings = [p for p in r["progress"] if p.get("status") == "warn"]
        assert any("network" in p["message"] for p in warnings), r["progress"]

    def test_a_point_map_says_the_same(self, points, tmp_path):
        r = geo(points, tmp_path, lat_column="lat", lon_column="lon", value_column="spends")
        assert r["success"] is True, r.get("error")
        assert r["self_contained"] is False
        assert r["needs_network_for"], "a scattergeo needs the same outlines"

    def test_the_file_is_still_written(self, countries, tmp_path):
        r = geo(countries, tmp_path, location_column="country", value_column="spends")
        assert Path(r["output_path"]).exists()
        assert Path(r["output_path"]).stat().st_size > 4_000_000, "the library still travels"


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
