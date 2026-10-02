"""A generated dashboard reads like a person wrote it: the words, the type, the cards, the chart marks.

The numbers were right and the page still looked like a machine's: "Avg lead_time", "is_canceled rate" in
a system font, a column of tall tiles, a trend drawn from zero. These pin what turned it into something a
person would put on a slide, and each says what it protects.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from servers.data_advanced._adv_dashboard import generate_dashboard
from shared import dashboard_looks as looks
from shared.labels import humanize, relabel
from tests.dashboard_page import drawn

CLASSIC = {"style": {"look": "classic"}}


@pytest.fixture
def bookings(tmp_path, monkeypatch) -> Path:
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(31)
    n = 400  # 2024-01-01 to 2025-02-03: the last month is three days old
    frame = pd.DataFrame(
        {
            "booking_date": pd.date_range("2024-01-01", periods=n).strftime("%Y-%m-%d"),
            "is_canceled": rng.choice([0, 1], n, p=[0.7, 0.3]),
            "channel": rng.choice(["Direct", "Agent", "Online", "Corporate"], n),
            "lead_time": rng.integers(0, 200, n),
            "revenue": rng.normal(200, 15, n).round(2),  # level, so a month's total barely moves
        }
    )
    path = tmp_path / "bookings.csv"
    frame.to_csv(path, index=False)
    return path


def _make(path: Path, name: str = "page", **spec) -> tuple[dict, str]:
    out = path.parent / f"{name}.html"
    result = generate_dashboard(str(path), output_path=str(out), open_after=False, spec=spec or None)
    assert result["success"] is True, result.get("error")
    return result, out.read_text(encoding="utf-8")


class TestAColumnIsSaidAsAPersonWouldSayIt:
    @pytest.mark.parametrize(
        ("name", "said"),
        [
            ("lead_time", "Lead time"),
            ("PoliceReportFiled", "Police report filed"),
            ("is_canceled", "Canceled"),  # a flag's name is its outcome; its rate reads "Canceled rate"
            ("adr", "ADR"),
            ("total_of_special_requests", "Total of special requests"),
            ("store_id", "Store ID"),
            ("Hospital overall rating", "Hospital overall rating"),  # somebody's own wording is left alone
        ],
    )
    def test_a_header_row_name_becomes_a_label(self, name, said):
        assert humanize(name) == said

    def test_a_name_in_a_sentence_is_swapped_whole_and_keeps_the_sentences_capitals(self):
        names = ["lead_time", "campaign_platform", "id"]
        assert relabel("Avg lead_time by month", names) == "Avg lead time by month"
        assert relabel("Revenue by campaign_platform", names) == "Revenue by campaign platform"  # mid-sentence: lower
        assert relabel("campaign_platform is where it comes from", names) == "Campaign platform is where it comes from"

    def test_only_whole_names_are_swapped(self):
        # `id` is "ID", but "paid", "valid_id" and "identity" contain it and are other words.
        out = relabel("paid valid_id identity id", ["id"])
        assert out == "paid valid_id identity ID"

    def test_the_page_says_the_labels_not_the_headers(self, bookings):
        result, html = _make(bookings)
        words = " ".join(str(p.get(k, "")) for p in result["spec"]["layout"] for k in ("title", "text"))
        for header in ("lead_time", "is_canceled", "booking_date"):
            assert header not in words, header
        assert "Lead time" in words or "lead time" in words


class TestTheTypefaceTravelsWithThePage:
    def test_every_bundled_face_is_a_real_font_file(self):
        for key in looks.BUNDLED:
            assert (looks.FONT_DIR / looks.BUNDLED[key][1]).stat().st_size > 10_000, key
            assert looks._font_face(key).startswith("@font-face{font-family:")

    def test_a_look_carries_the_faces_it_uses_and_no_others(self):
        for name in looks.builtin_names():
            look = looks.resolve_look(name)
            css = looks.font_css(look)
            used = {k for k in (look.get("font"), look.get("display")) if k in looks.BUNDLED}
            assert css.count("@font-face") == len(used), name
            assert css.count("data:font/woff2;base64,") == len(used), name

    def test_a_look_in_a_system_font_embeds_nothing(self):
        look = looks.validate_look(
            {
                "mode": "light",
                "font": "Georgia, 'Times New Roman'",
                "light": {
                    "bg": "#ffffff",
                    "surface": "#ffffff",
                    "text": "#111111",
                    "text-muted": "#666666",
                    "accent": "#0072b2",
                    "border": "#dddddd",
                },
            }
        )
        assert looks.font_css(look) == ""

    def test_the_default_page_draws_in_its_own_face_not_the_readers(self, bookings):
        _, html = _make(bookings)
        look = looks.resolve_look(looks.DEFAULT_LOOK)
        assert looks.STACKS[look["font"]].split(",")[0] in html
        assert html.count("@font-face") == len(
            {k for k in (look.get("font"), look.get("display")) if k in looks.BUNDLED}
        )

    def test_a_mockup_is_read_into_a_face_the_page_can_carry(self):
        mockups = Path(__file__).resolve().parent.parent / "design" / "mockups"
        for src in sorted(mockups.glob("*.src.html")):
            look, _ = looks.digest_html(src.read_text(encoding="utf-8"), src.name.split(".")[0])
            assert look["font"] in looks.BUNDLED or look["font"] in looks.STACKS, src.name


class TestTheOverviewIsComposed:
    def test_it_opens_on_cards_then_the_trend_then_what_stands_out(self, bookings):
        result, _ = _make(bookings)
        layout = result["spec"]["layout"]
        kinds = [p["chart"] for p in layout]
        cards = kinds.count("kpi")
        assert 2 <= cards <= 6 and kinds[:cards] == ["kpi"] * cards, kinds
        trend = kinds.index("time_series")
        assert trend < kinds.index("markdown"), kinds
        assert layout[trend]["style"].get("fill") and layout[trend]["style"].get("peak")
        assert layout[kinds.index("markdown")]["title"] == "What stands out"

    def test_a_flag_is_shown_as_a_ring_of_its_share(self, bookings):
        result, _ = _make(bookings)
        ring = next(p for p in result["spec"]["layout"] if p["chart"] == "pie")
        assert ring["style"]["center"].startswith("share:") and ring["style"]["hole"] >= 70

    def test_no_title_is_a_header_row_spelling(self, bookings):
        result, _ = _make(bookings)
        for p in result["spec"]["layout"]:
            assert "_" not in str(p.get("title", "")), p["title"]

    def test_the_cards_share_one_row_and_fill_it(self, bookings):
        # Compact tiles side by side, each a share of the 12 columns: not a column of full-width tiles.
        result, _ = _make(bookings)
        spans = [p["place"]["span"] for p in result["spec"]["layout"] if p["chart"] == "kpi"]
        assert len(spans) >= 2 and sum(spans) == 12 and max(spans) < 12, spans

    def test_the_look_is_the_skin_not_the_composition(self, bookings):
        # Asking for `classic` takes the look's type and colour off the page; what the page says stays.
        dressed, dressed_html = _make(bookings, name="dressed")
        plain, plain_html = _make(bookings, name="plain", **CLASSIC)
        assert [p["chart"] for p in plain["spec"]["layout"]] == [p["chart"] for p in dressed["spec"]["layout"]]
        assert f"look-{looks.DEFAULT_LOOK}" in dressed_html and "look-" not in plain_html


class TestACardReadsAtAGlance:
    def test_a_dated_figure_carries_its_change_and_its_shape(self, bookings):
        _, html = _make(bookings)
        cards = [v for v in drawn(html)["html"].values() if "kpi-main" in v]
        assert cards
        first = cards[0]
        assert "kpi-big" in first and "kpi-foot" in first and "kpi-spark" in first
        assert "kpi-delta" in first and ("▲" in first or "▼" in first or "■" in first)
        assert re.search(r"\d{4}-\d{2} vs \d{4}-\d{2}", first)  # the change names what it is against

    def test_no_card_is_a_bare_number(self, bookings):
        _, html = _make(bookings)
        cards = [v for v in drawn(html)["html"].values() if "kpi-main" in v]
        assert cards
        for card in cards:
            assert "kpi-delta" in card or "kpi-sub" in card, card

    def test_an_undated_figure_says_what_it_is_made_of(self, bookings, tmp_path):
        undated = tmp_path / "undated.csv"
        pd.read_csv(bookings).drop(columns=["booking_date"]).to_csv(undated, index=False)
        layout = [
            {"chart": "kpi", "cols": {"value": "revenue"}},
            {"chart": "kpi", "cols": {"value": "lead_time"}, "agg": "mean"},
        ]
        _, html = _make(undated, name="undated", layout=layout)
        total, mean = (v for v in drawn(html)["html"].values() if "kpi-main" in v)
        assert " per row" in total and "range " in mean

    def test_a_context_line_can_be_set_and_is_escaped(self, bookings):
        layout = [{"chart": "kpi", "cols": {"value": "revenue"}, "style": {"sub": "<b>net</b> of tax"}}]
        _, html = _make(bookings, name="sub", layout=layout)
        card = next(v for v in drawn(html)["html"].values() if "kpi-big" in v)
        assert "&lt;b&gt;net&lt;/b&gt; of tax" in card and "<b>net</b>" not in card


class TestARingShowsOneShare:
    def test_the_middle_says_the_real_share_and_the_counts(self, bookings):
        frame = pd.read_csv(bookings)
        _, html = _make(bookings)
        figures = drawn(html)["figures"]
        ring = next(f for f in figures.values() if f["data"][0]["type"] == "pie" and f["data"][0].get("hole") == 0.74)
        text = ring["layout"]["annotations"][0]["text"]
        said = float(re.search(r"<b>([\d.]+)%</b>", text).group(1))
        yes, rows = int((frame["is_canceled"] == 1).sum()), len(frame)
        assert said == pytest.approx(yes / rows * 100, abs=0.06)
        assert f"{yes:,} of {rows:,}" in text

    def test_a_pie_needs_no_value_column_it_counts_rows(self, bookings):
        layout = [{"chart": "pie", "cols": {"category": "channel"}, "title": "Bookings by channel"}]
        _, html = _make(bookings, name="count", layout=layout)
        pie = next(f for f in drawn(html)["figures"].values() if f["data"][0]["type"] == "pie")
        frame = pd.read_csv(bookings)
        counts = frame["channel"].value_counts()
        assert dict(zip(pie["data"][0]["labels"], pie["data"][0]["values"], strict=True)) == {
            k: int(v) for k, v in counts.items()
        }


class TestALineIsDrawnToBeRead:
    def _trend(self, bookings, **spec) -> dict:
        _, html = _make(bookings, **spec)
        return next(f for cid, f in drawn(html)["figures"].items() if cid.endswith("time_series"))

    def test_a_trend_that_barely_moves_is_not_drawn_from_zero(self, bookings):
        figure = self._trend(bookings)
        low = figure["layout"]["yaxis"]["range"][0]
        assert low > 0, "the axis hugs the line so the little it does is visible"
        assert figure["data"][0].get("fill") == "tozeroy"

    def test_a_trend_that_asks_for_no_fill_is_still_the_plain_line_from_zero(self, bookings):
        # The fill is what asks for the tight axis; a panel without it draws as it always did.
        layout = [{"chart": "time_series", "cols": {"date": "booking_date", "value": "revenue"}}]
        _, html = _make(bookings, name="plainline", layout=layout)
        figure = next(f for cid, f in drawn(html)["figures"].items() if cid.endswith("time_series"))
        assert "range" not in figure["layout"].get("yaxis", {}) and "fill" not in figure["data"][0]

    def test_a_partial_period_far_off_the_axis_is_said_in_words_not_drawn_as_a_line_to_nowhere(self, bookings):
        figure = self._trend(bookings)
        notes = [a["text"] for a in figure["layout"].get("annotations", [])]
        assert "latest period incomplete" in notes
        assert not any(t.get("name") == "incomplete period" for t in figure["data"])

    def test_the_peak_is_called_out_when_it_stands_clear_of_the_rest(self, bookings, tmp_path):
        frame = pd.read_csv(bookings)
        frame.loc[frame["booking_date"].str.startswith("2024-06"), "revenue"] *= 3  # a June that stands out
        spiked = tmp_path / "spiked.csv"
        frame.to_csv(spiked, index=False)
        figure = self._trend(spiked)
        peaks = [a["text"] for a in figure["layout"].get("annotations", []) if str(a["text"]).startswith("Peak ")]
        assert len(peaks) == 1 and "2024-06" in peaks[0]

    def test_no_peak_is_claimed_on_a_line_that_is_level(self, bookings):
        figure = self._trend(bookings)
        assert not [a for a in figure["layout"].get("annotations", []) if str(a["text"]).startswith("Peak ")]


class TestTheNewStylesAreChecked:
    def _refused(self, bookings, style, chart="time_series", **extra) -> str:
        cols = {"date": "booking_date", "value": "revenue"} if chart == "time_series" else {"value": "revenue"}
        layout = [{"chart": chart, "title": "t", "cols": cols, **extra, "style": style}]
        result = generate_dashboard(
            str(bookings), output_path=str(bookings.parent / "r.html"), open_after=False, spec={"layout": layout}
        )
        assert result["success"] is False, result
        return result["error"]

    def test_fill_and_peak_are_true_or_false(self, bookings):
        assert "fill" in self._refused(bookings, {"fill": "yes"})
        assert "peak" in self._refused(bookings, {"peak": 1})

    def test_the_context_line_is_short_text(self, bookings):
        err = self._refused(bookings, {"sub": "x" * 61}, chart="kpi", agg="sum")
        assert "sub" in err and "60" in err

    def test_a_look_is_named_or_classic_nothing_else(self, bookings):
        result = generate_dashboard(
            str(bookings),
            output_path=str(bookings.parent / "l.html"),
            open_after=False,
            spec={"style": {"look": "no_such_look"}},
        )
        assert result["success"] is False and "look" in json.dumps(result["error"])
