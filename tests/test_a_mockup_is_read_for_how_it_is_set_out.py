"""A mockup's design is more than its colours: how it is framed, how airy it is and how wide its cards are.

`dashboard_looks(source=mockup.html)` already read colours, type and radius. A mockup also says whether a
rail or a sidebar frames the page, how much space lies between cards, and the width each kind of card takes
on a twelve-column grid (a KPI a quarter of the row, a trend chart two thirds, a ranked list a third). Those
widths are `style.arrange`; a generated dashboard takes them, and its rows end flush. The mockup is parsed
as text -- nothing in it runs.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from servers.data_advanced._adv_dashboard import generate_dashboard
from servers.data_advanced._dash_looks import dashboard_looks
from shared.dashboard_spec import SpecError, validate_page_style
from shared.mockup_layout import arrange, digest_arrangement, validate_arrangement

MOCKUPS = Path(__file__).resolve().parents[1] / "design" / "mockups"


@pytest.fixture(autouse=True)
def _home(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    return tmp_path


def read(name: str) -> dict:
    return digest_arrangement((MOCKUPS / f"{name}.src.html").read_text(encoding="utf-8"))


class TestTheThreeMockupsWeSaved:
    def test_lagoon_is_a_rail_with_a_twelve_column_grid(self):
        found = read("lagoon")
        assert found["frame"] == "rail" and found["report"]["frame_from"].startswith("nav.rail")
        assert found["spans"]["kpi"] == [3, 3, 3, 3]
        assert found["spans"]["chart"][:2] == [8, 4]
        assert found["spans"]["ranking"] == [4]
        assert [[c["span"] for c in row] for row in found["rows"]][:3] == [[3, 3, 3, 3], [8, 4], [4, 4, 4]]

    def test_harbor_is_a_sidebar_and_its_wide_card_is_a_chart(self):
        found = read("harbor")
        assert found["frame"] == "sidebar"
        assert found["spans"]["kpi"] == [3, 3, 3, 3]
        assert found["spans"]["chart"] == [12, 7]
        assert found["spans"]["table"] == [5]

    def test_nocturne_is_a_banner_and_says_what_a_script_fills(self):
        found = read("nocturne")
        assert found["frame"] == "banner" and found["density"] == "airy"
        assert found["spans"]["chart"][:2] == [4, 8]
        assert any("stages" in note and "script" in note for note in found["report"]["notes"])

    def test_the_spans_are_a_valid_arrangement(self):
        for name in ("lagoon", "harbor", "nocturne"):
            assert validate_arrangement(read(name)["spans"])


class TestAGridOfCardsIsReadByWhatItDoes:
    def page(self, css: str, body: str) -> dict:
        return digest_arrangement(f"<style>{css}</style><body>{body}</body>")

    def test_equal_tracks_share_the_twelve_columns(self):
        found = self.page(
            ".row{display:grid;grid-template-columns:repeat(3,1fr)}",
            '<div class="row"><div class="card kpi"></div><div class="card kpi"></div><div class="card kpi"></div></div>',
        )
        assert found["spans"] == {"kpi": [4, 4, 4]}

    def test_a_fixed_track_takes_its_share_of_a_page(self):
        found = self.page(
            ".row{display:grid;grid-template-columns:1fr 300px}",
            '<div class="row"><div class="card chart"></div><div class="card list"></div></div>',
        )
        assert found["rows"][0][0]["span"] + found["rows"][0][1]["span"] == 12 and found["rows"][0][1]["span"] == 3

    def test_an_auto_fit_grid_is_read_from_its_minimum_width(self):
        found = self.page(
            ".row{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr))}",
            '<div class="row">' + '<div class="card kpi"></div>' * 4 + "</div>",
        )
        assert found["spans"]["kpi"] == [3, 3, 3, 3]

    def test_a_cards_own_span_beats_its_track(self):
        found = self.page(
            ".g{display:grid;grid-template-columns:repeat(12,1fr)}.w{grid-column:span 9}",
            '<div class="g"><div class="card w chart"></div><div class="card chart"></div></div>',
        )
        assert [c["span"] for c in found["rows"][0]] == [9, 1]

    def test_cards_that_do_not_fit_one_row_wrap_to_the_next(self):
        found = self.page(
            ".g{display:grid;grid-template-columns:repeat(12,1fr)}.a{grid-column:span 8}",
            '<div class="g"><div class="card a chart"></div><div class="card a chart"></div></div>',
        )
        assert [[c["span"] for c in row] for row in found["rows"]] == [[8], [8]]

    def test_what_a_card_holds_is_read_from_its_names_and_tags(self):
        found = self.page(
            ".g{display:grid;grid-template-columns:repeat(4,1fr)}",
            '<div class="g"><div class="card"><table></table></div>'
            '<div class="card"><ol class="rank"></ol></div>'
            '<div class="card"><div class="chart"></div></div>'
            '<div class="card" id="k-revenue"></div></div>',
        )
        assert [c["kind"] for c in found["rows"][0]] == ["table", "ranking", "chart", "kpi"]

    def test_a_script_in_the_mockup_is_text_and_never_runs(self):
        found = digest_arrangement(
            "<style>.g{display:grid;grid-template-columns:1fr 1fr}</style><body><div class='g'>"
            "<div class='card chart'></div><div class='card chart'></div></div>"
            "<script>throw new Error('ran')</script></body>"
        )
        assert found["spans"] == {"chart": [6, 6]}

    def test_a_page_with_no_grid_says_so(self):
        found = self.page("body{color:red}", "<p>hello</p>")
        assert found["spans"] == {} and any("no grid of cards" in n for n in found["report"]["notes"])

    def test_the_gap_between_cards_is_its_density(self):
        for gap, density in (("8px", "compact"), ("16px", "comfortable"), ("1.75rem", "airy")):
            found = self.page(f".g{{display:grid;grid-template-columns:1fr 1fr;gap:{gap}}}", "<div class='g'></div>")
            assert found["density"] == density


class TestAnArrangementIsChecked:
    @pytest.mark.parametrize(
        "bad",
        [
            {},
            [3, 4],
            {"kpi": []},
            {"kpi": [0]},
            {"kpi": [13]},
            {"kpi": [True]},
            {"kpi": ["4"]},
            {"banner": [4]},
            {"chart": [1] * 25},
        ],
    )
    def test_these_are_refused(self, bad):
        with pytest.raises(ValueError):
            validate_arrangement(bad)

    def test_the_page_style_refuses_one_and_names_the_key(self):
        with pytest.raises(SpecError, match="style.arrange"):
            validate_page_style({"arrange": {"kpi": [20]}})
        validate_page_style({"arrange": {"kpi": [3, 3], "chart": [8, 4]}})


def panel(chart: str, span: int) -> dict:
    return {"chart": chart, "title": chart, "place": {"span": span}}


class TestCardsTakeTheMockupsWidths:
    def test_each_kind_takes_its_widths_in_order(self):
        layout = [
            panel("kpi", 12),
            panel("kpi", 12),
            panel("kpi", 12),
            panel("kpi", 12),
            panel("line", 12),
            panel("bar", 12),
        ]
        moved = arrange(layout, None, {"kpi": [3], "chart": [8, 4]})
        assert [p["place"]["span"] for p in layout] == [3, 3, 3, 3, 8, 4]
        assert moved >= 6

    def test_a_kind_the_mockup_lacks_keeps_its_width(self):
        layout = [panel("markdown", 12), panel("line", 12)]
        arrange(layout, None, {"chart": [6]})
        assert [p["place"]["span"] for p in layout] == [12, 6 + 6]  # the lone chart's row is made flush

    def test_a_short_row_is_made_flush(self):
        layout = [panel("line", 12), panel("bar", 12), panel("pie", 12)]
        arrange(layout, None, {"chart": [8, 4, 4]})
        assert [p["place"]["span"] for p in layout] == [8, 4, 12]

    def test_a_row_that_overflows_closes_the_one_before(self):
        layout = [panel("line", 12), panel("bar", 12)]
        arrange(layout, None, {"chart": [8, 8]})
        assert [p["place"]["span"] for p in layout] == [12, 12]

    def test_every_tab_is_set_out_on_its_own(self):
        layout = [panel("kpi", 12), panel("line", 12), panel("kpi", 12), panel("line", 12)]
        arrange(layout, [{"name": "A", "slots": [0, 1]}, {"name": "B", "slots": [2, 3]}], {"kpi": [4], "chart": [8]})
        assert [p["place"]["span"] for p in layout] == [4, 8, 4, 8]

    def test_a_short_row_shares_what_is_left_among_its_cards(self):
        layout = [panel("kpi", 12), panel("kpi", 12), panel("line", 12)]
        arrange(layout, None, {"kpi": [3], "chart": [8]})
        assert [p["place"]["span"] for p in layout] == [6, 6, 12]

    def test_an_unplaced_panel_is_left_alone(self):
        layout = [{"chart": "line", "title": "x"}, panel("bar", 12)]
        arrange(layout, None, {"chart": [6]})
        assert "place" not in layout[0]


@pytest.fixture
def shop(_home) -> str:
    rng = np.random.default_rng(3)
    n = 400
    frame = pd.DataFrame(
        {
            "day": pd.date_range("2023-01-01", periods=n).strftime("%Y-%m-%d"),
            "region": rng.choice(["North", "South", "East", "West"], n),
            "channel": rng.choice(["web", "store", "phone"], n),
            "spend": rng.integers(50, 500, n),
            "orders": rng.integers(1, 40, n),
        }
    )
    path = _home / "shop.csv"
    frame.to_csv(path, index=False)
    return str(path)


def spans_in(html: str) -> list[int]:
    import re

    return [int(n) for n in re.findall(r'class="cc[^"]*" style="grid-column:span (\d+)', html)]


class TestAGeneratedPageIsSetOutLikeTheMockup:
    def test_the_story_takes_the_widths(self, shop, _home):
        spans = {"kpi": [3], "chart": [8, 4], "ranking": [4]}
        result = generate_dashboard(
            shop,
            output_path=str(_home / "p.html"),
            open_after=False,
            spec={"style": {"look": "lagoon", "arrange": spans}},
        )
        assert result["success"] is True, result.get("error")
        assert any("set out like the mockup" in p["message"].lower() for p in result["progress"])
        layout = result["spec"]["layout"]
        assert any(p["chart"] == "kpi" for p in layout)
        # Every row of every tab ends flush: the cards on a row sum to twelve columns.
        for tab in result["spec"]["tabs"]:
            row = 0
            for i in tab["slots"]:
                row += layout[i]["place"]["span"]
                assert row <= 12, f"{tab['name']}: a row of {row} columns"
                row %= 12
            assert row == 0, f"{tab['name']} ends on a short row"
        assert "arrange" not in result["spec"]["style"], "applied once: the widths now live in the layout"

    def test_a_layout_the_caller_wrote_is_placed_as_written(self, shop, _home):
        spec = {
            "layout": [
                {"slot": 0, "chart": "bar", "cols": {"category": "region", "value": "spend"}, "place": {"span": 7}}
            ],
            "style": {"arrange": {"chart": [4]}},
        }
        result = generate_dashboard(shop, output_path=str(_home / "q.html"), open_after=False, spec=spec)
        assert result["success"] is True, result.get("error")
        assert result["spec"]["layout"][0]["place"]["span"] == 7
        assert any("not applied" in p["message"] for p in result["progress"])

    def test_a_bad_arrangement_is_refused_before_anything_is_drawn(self, shop, _home):
        result = generate_dashboard(
            shop, output_path=str(_home / "r.html"), open_after=False, spec={"style": {"arrange": {"kpi": [99]}}}
        )
        assert result["success"] is False and "style.arrange" in result["error"]
        assert not (_home / "r.html").exists()


class TestDashboardLooksReadsTheArrangement:
    def test_a_mockup_gives_a_look_with_its_frame_and_the_widths(self, _home):
        copy = _home / "lagoon.html"
        copy.write_text((MOCKUPS / "lagoon.src.html").read_text(encoding="utf-8"), encoding="utf-8")
        result = dashboard_looks(source=str(copy))
        assert result["success"] is True, result.get("error")
        assert result["look"]["frame"] == "rail"
        assert result["arrangement"]["spans"]["kpi"] == [3, 3, 3, 3]
        assert result["use"]["style"]["arrange"] == result["arrangement"]["spans"]
        assert "frame" in result["report"] and "rail" in result["report"]["frame"]

    def test_what_it_hands_back_is_what_generate_takes(self, shop, _home):
        copy = _home / "harbor.html"
        copy.write_text((MOCKUPS / "harbor.src.html").read_text(encoding="utf-8"), encoding="utf-8")
        found = dashboard_looks(source=str(copy))
        result = generate_dashboard(
            shop, output_path=str(_home / "h.html"), open_after=False, spec={"style": found["use"]["style"]}
        )
        assert result["success"] is True, result.get("error")
        html = (_home / "h.html").read_text(encoding="utf-8")
        assert "sidebar" in html and spans_in(html)
