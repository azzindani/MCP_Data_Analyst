"""generate_geo_map drew an empty world and called it a success.

    generate_geo_map(location_column="campaign_platform", value_column="spends")
      -> success: true, rows_plotted: 2, phase07_geo_map.html (12 KB)

Rendering that file shows a complete choropleth: title "Ad Spend by Campaign
Platform", a colour bar running 0.6M to 1.8M over the real spend range, every
coastline drawn -- and not one country shaded, because "Google Ads" and
"Facebook Ads" are not places. px.choropleth drops locations it cannot resolve
without a word, and `rows_plotted` counted the distinct values rather than the
ones that landed, so the response reported "2 locations" about a blank map. Two
sweeps in a row recorded it as PASS.

An earlier sweep saw this and left it, reasoning that validating country names
needs an embedded list and an incomplete list would refuse valid maps. That
conflated validating-to-refuse with validating-to-warn, and the list only has to
be good enough to tell "France" from "Google Ads". shared/geo_names.py holds it.

Found by looking at the picture, not the response.
"""

from __future__ import annotations

import csv as csvmod
from pathlib import Path

import pytest

from servers.data_visual.engine import generate_dashboard, generate_geo_map
from shared.geo_names import unrecognised_locations


def write_csv(path: Path, header: list[str], rows: list[list]) -> str:
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csvmod.writer(fh)
        w.writerow(header)
        w.writerows(rows)
    return str(path)


@pytest.fixture()
def categories_csv(tmp_path: Path) -> str:
    return write_csv(
        tmp_path / "ads.csv",
        ["campaign_platform", "spends"],
        [["Google Ads", 1939003], ["Facebook Ads", 564115]],
    )


@pytest.fixture()
def countries_csv(tmp_path: Path) -> str:
    return write_csv(
        tmp_path / "countries.csv",
        ["country", "spends"],
        [["France", 10], ["Japan", 20], ["United States", 30], ["Côte d'Ivoire", 5]],
    )


class TestACategoryColumnIsNotAMap:
    def test_it_refuses_rather_than_drawing_nothing(self, categories_csv: str, tmp_path: Path):
        r = generate_geo_map(
            categories_csv,
            location_column="campaign_platform",
            value_column="spends",
            output_path=str(tmp_path / "m.html"),
            open_after=False,
        )
        assert r["success"] is False

    def test_no_empty_map_is_written(self, categories_csv: str, tmp_path: Path):
        out = tmp_path / "m.html"
        generate_geo_map(
            categories_csv,
            location_column="campaign_platform",
            value_column="spends",
            output_path=str(out),
            open_after=False,
        )
        assert not out.exists()

    def test_the_error_names_the_column_and_the_values(self, categories_csv: str, tmp_path: Path):
        r = generate_geo_map(
            categories_csv,
            location_column="campaign_platform",
            value_column="spends",
            output_path=str(tmp_path / "m.html"),
            open_after=False,
        )
        assert "campaign_platform" in r["error"], r["error"]
        assert "Google Ads" in r["error"], r["error"]

    def test_the_error_reads_as_english(self, categories_csv: str, tmp_path: Path):
        """The plotly mode string is 'country names'; the message must not say
        'is a recognisable country names'."""
        r = generate_geo_map(
            categories_csv,
            location_column="campaign_platform",
            value_column="spends",
            output_path=str(tmp_path / "m.html"),
            open_after=False,
        )
        assert "country names" not in r["error"], r["error"]
        assert "country name" in r["error"], r["error"]

    def test_the_hint_points_at_a_chart_that_would_work(self, categories_csv: str, tmp_path: Path):
        r = generate_geo_map(
            categories_csv,
            location_column="campaign_platform",
            value_column="spends",
            output_path=str(tmp_path / "m.html"),
            open_after=False,
        )
        assert "generate_chart" in r["hint"] and "bar" in r["hint"], r["hint"]


class TestARealMapStillDraws:
    def test_countries_are_plotted(self, countries_csv: str, tmp_path: Path):
        out = tmp_path / "m.html"
        r = generate_geo_map(
            countries_csv,
            location_column="country",
            value_column="spends",
            output_path=str(out),
            open_after=False,
        )
        assert r["success"] is True, r.get("error")
        assert r["rows_plotted"] == 4
        assert out.exists()

    def test_an_accented_name_is_not_rejected(self, countries_csv: str, tmp_path: Path):
        r = generate_geo_map(
            countries_csv,
            location_column="country",
            value_column="spends",
            output_path=str(tmp_path / "m.html"),
            open_after=False,
        )
        assert r["success"] is True, r.get("error")
        assert [s for s in r["progress"] if s.get("status") == "warn"] == []

    def test_iso3_codes_still_work(self, tmp_path: Path):
        path = write_csv(tmp_path / "iso.csv", ["iso3", "spends"], [["FRA", 1], ["JPN", 2]])
        r = generate_geo_map(
            path,
            location_column="iso3",
            value_column="spends",
            output_path=str(tmp_path / "m.html"),
            open_after=False,
        )
        assert r["success"] is True, r.get("error")


class TestAPartlyPlaceableColumnWarnsAndDraws:
    @pytest.fixture()
    def mixed_csv(self, tmp_path: Path) -> str:
        return write_csv(
            tmp_path / "mixed.csv",
            ["country", "spends"],
            [["France", 10], ["Google Ads", 20], ["Japan", 30]],
        )

    def test_it_still_succeeds(self, mixed_csv: str, tmp_path: Path):
        r = generate_geo_map(
            mixed_csv,
            location_column="country",
            value_column="spends",
            output_path=str(tmp_path / "m.html"),
            open_after=False,
        )
        assert r["success"] is True, r.get("error")

    def test_it_says_what_was_dropped(self, mixed_csv: str, tmp_path: Path):
        r = generate_geo_map(
            mixed_csv,
            location_column="country",
            value_column="spends",
            output_path=str(tmp_path / "m.html"),
            open_after=False,
        )
        warns = [s for s in r["progress"] if s.get("status") == "warn"]
        assert warns, r["progress"]
        assert "Google Ads" in str(warns[0].get("detail", "")), warns


class TestTheDashboardLeavesOutABlankPanel:
    def test_a_non_place_country_column_gets_no_choropleth(self, tmp_path: Path):
        path = write_csv(
            tmp_path / "fake.csv",
            ["country", "spends", "clicks"],
            [["Domestic", 10, 1], ["International", 20, 2], ["Domestic", 5, 3]],
        )
        r = generate_dashboard(path, output_path=str(tmp_path / "d.html"), open_after=False, dry_run=True)
        assert "geo_choropleth" not in r["would_generate"]["charts"], r["would_generate"]

    def test_the_other_panels_are_still_built(self, tmp_path: Path):
        path = write_csv(
            tmp_path / "fake.csv",
            ["country", "spends", "clicks"],
            [["Domestic", 10, 1], ["International", 20, 2], ["Domestic", 5, 3]],
        )
        r = generate_dashboard(path, output_path=str(tmp_path / "d.html"), open_after=False, dry_run=True)
        assert "bar" in r["would_generate"]["charts"]

    def test_real_countries_still_get_one(self, tmp_path: Path):
        path = write_csv(
            tmp_path / "real.csv",
            ["country", "spends", "clicks"],
            [["France", 10, 1], ["Japan", 20, 2], ["Brazil", 5, 3]],
        )
        r = generate_dashboard(path, output_path=str(tmp_path / "d.html"), open_after=False, dry_run=True)
        assert "geo_choropleth" in r["would_generate"]["charts"], r["would_generate"]


class TestTheNameTable:
    @pytest.mark.parametrize(
        "name",
        ["France", "United States", "USA", "UK", "Russia", "South Korea", "Czechia", "Turkiye"],
    )
    def test_common_spellings_are_recognised(self, name: str):
        assert unrecognised_locations([name], "country names") == []

    @pytest.mark.parametrize("name", ["Côte d'Ivoire", "São Tomé and Príncipe", "Réunion", "Curaçao"])
    def test_accents_are_folded(self, name: str):
        assert unrecognised_locations([name], "country names") == []

    def test_padding_and_case_do_not_matter(self):
        assert unrecognised_locations(["  fRaNcE  "], "country names") == []

    @pytest.mark.parametrize("name", ["Google Ads", "Atlantis", "Q3", ""])
    def test_non_places_are_rejected(self, name: str):
        assert unrecognised_locations([name], "country names") == [name]

    def test_an_unknown_mode_never_reports_a_problem(self):
        """Refusing to guess beats reporting a valid map as broken."""
        assert unrecognised_locations(["anything"], "geojson-id") == []

    def test_us_states(self):
        assert unrecognised_locations(["CA", "NY"], "USA-states") == []
        assert unrecognised_locations(["ZZ"], "USA-states") == ["ZZ"]
