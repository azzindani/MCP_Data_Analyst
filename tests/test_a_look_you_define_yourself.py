"""A look is something the caller defines: the marks, the type, the spacing, the tabs, the ground -- or a whole look
from an accent colour and a mood.

Four built-in looks were all a caller could pick. A look can now say how the lines and bars are drawn, how big and how
spaced the type is, how the page is held, what the tabs and the KPI tiles are, and `brand` makes all of it, legible in
light and dark, from an accent and a mood. Every value is one of a named set or a number in a range: none can carry a
rule, a URL or a script into the page. Each guard was run once with its fix switched off.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from servers.data_advanced._adv_dashboard import generate_dashboard
from servers.data_advanced._dash_looks import dashboard_looks
from shared import dashboard_looks as looks
from shared.look_maker import MOODS, OKABE_ITO, _hue_gap, contrast, look_from_brand
from tests.dashboard_page import NODE, drawn

needs_node = pytest.mark.skipif(NODE is None, reason="node is not installed")
STUDIO = looks.BUILTIN["studio"]


def _with(**extra) -> dict:
    return {**STUDIO, **extra}


class TestWhatALookMaySay:
    def test_a_look_that_says_none_of_it_is_what_it_was(self):
        for name in looks.BUILTIN:
            look = looks.validate_look(looks.BUILTIN[name])
            assert not {"chart", "type", "space", "tabs", "bg", "border_width", "kpi_spark", "kpi_delta"} & set(look), (
                name
            )

    def test_every_group_takes_its_values_and_gives_them_back(self):
        look = looks.validate_look(
            _with(
                chart={"line_width": 3, "line_shape": "spline", "markers": "ends", "area": 20, "bar_radius": 8, "bar_gap": 30,
                       "grid": "dotted", "axis_line": True, "font_size": 13, "hover": "dark"},
                type={"scale": 1.1, "title_weight": 700, "title_case": "upper", "tracking": 0.05, "numerals": "tabular"},
                space={"gap": 1.2, "max_width": 100},
                tabs="underline", bg="dots", border_width=2, kpi="outline", kpi_spark=False, kpi_delta="text",
            )
        )  # fmt: skip
        assert look["chart"]["bar_radius"] == 8 and look["type"]["scale"] == 1.1 and look["space"]["max_width"] == 100
        assert (look["tabs"], look["bg"], look["kpi"], look["kpi_spark"], look["kpi_delta"]) == (
            "underline",
            "dots",
            "outline",
            False,
            "text",
        )

    @pytest.mark.parametrize(
        ("bad", "names"),
        [
            ({"chart": {"grid": "wavy"}}, "look.chart.grid is one of none, soft, dotted, strong"),
            ({"chart": {"line_width": 9}}, "look.chart.line_width is a number from 1 to 4"),
            ({"chart": {"axis_line": "yes"}}, "look.chart.axis_line is true or false"),
            ({"chart": {"bar_gap": True}}, "look.chart.bar_gap is a number from 5 to 80"),
            ({"type": {"title_weight": 450}}, "look.type.title_weight is one of 400, 500, 600, 700, 800"),
            ({"type": {"scale": 2}}, "look.type.scale is a number from 0.9 to 1.15"),
            ({"space": {"max_width": 30}}, "look.space.max_width is 0 (the full window) or a number from 60 to 140"),
            ({"tabs": "tabs"}, "look.tabs is one of pills, underline, segmented"),
            ({"bg": "stripes"}, "look.bg is one of solid, gradient, dots, grid"),
            ({"kpi": "huge"}, "look.kpi is one of plain, tile, gradient, outline, bar, minimal"),
            ({"border_width": 9}, "look.border_width is a card border in pixels, 0 to 3"),
            ({"kpi_spark": "no"}, "look.kpi_spark is true or false"),
            ({"chart": {"colour": "red"}}, "look.chart has unknown key(s): colour"),
            ({"chart": "bold"}, "look.chart is a dict; it takes: line_width"),
        ],
    )
    def test_what_cannot_be_drawn_is_refused_by_name_and_what_is_allowed(self, bad, names):
        with pytest.raises(looks.LookError) as err:
            looks.validate_look(_with(**bad))
        assert names in str(err.value)

    def test_a_zero_width_is_the_full_window(self):
        assert looks.validate_look(_with(space={"max_width": 0}))["space"]["max_width"] == 0

    def test_nothing_a_caller_writes_reaches_the_stylesheet_as_text(self):
        for hostile in ("}body{display:none", "url(http://x/y)", "red;}</style><script>"):
            with pytest.raises(looks.LookError):
                looks.validate_look(_with(chart={"grid": hostile}))
            with pytest.raises(looks.LookError):
                looks.validate_look(_with(tabs=hostile))
            with pytest.raises(looks.LookError):
                looks.validate_look(_with(type={"scale": hostile}))

    def test_the_grammar_lists_every_key_with_what_it_takes(self):
        g = looks.grammar()
        for group, keys in looks.GRAMMAR.items():
            assert set(g[group]) == set(keys)
        assert g["tabs"] == list(looks.TABS) and "0 (the full window)" in g["space"]["max_width"]


class TestWhatItDrawsInThePage:
    def test_each_choice_is_a_rule_in_the_stylesheet(self):
        css = looks.look_css(
            looks.validate_look(
                _with(
                    type={"scale": 1.1, "title_case": "upper", "numerals": "tabular", "title_weight": 700},
                    space={"gap": 1.2, "max_width": 100},
                    tabs="underline", bg="dots", border_width=2, kpi="outline", kpi_spark=False, kpi_delta="text",
                )
            )
        )  # fmt: skip
        assert css.count("{") == css.count("}")
        for rule in ("html{font-size:110.0%}", "text-transform:uppercase", "font-variant-numeric:tabular-nums",
                     ".cgrid,.kpi-row{gap:1.2rem}", "body{max-width:100rem;margin-inline:auto}", "radial-gradient(",
                     ".tabs .tab-btn[aria-selected=true]{background:transparent;color:var(--accent);border-bottom-color",
                     ".cc,.kpi-card,.tbl-card,.mbox{border:2px solid var(--border)}", "border:1.5px solid var(--c,var(--accent))",
                     ".kpi-spark{display:none}", ".kpi-delta b{background:none;padding:0}"):  # fmt: skip
            assert rule in css, rule

    def test_a_rail_keeps_its_own_tabs(self):
        css = looks.look_css(looks.validate_look({**looks.BUILTIN["lagoon"], "tabs": "underline"}))
        assert "border-bottom-color:var(--accent)" not in css

    def test_a_gradient_tiles_title_reads_on_the_gradient(self):
        css = looks.look_css(looks.validate_look(_with(kpi="gradient")))
        assert ".cc-kpi .cc-hdr h3" in css  # the engine's own title rule is as specific: it must be named too

    def test_a_number_stays_on_one_line_in_any_typeface(self):
        from servers.data_advanced._dash_ext import EXT_CSS

        assert ".cc-kpi .kpi-big{white-space:nowrap;font-size:clamp(1.35rem,13cqi,2.25rem)}" in EXT_CSS


LAYOUT = [
    {"chart": "line", "cols": {"date": "day", "value": "revenue"}, "style": {"fill": True}},
    {"chart": "bar", "cols": {"category": "region", "value": "revenue"}},
]


@pytest.fixture
def sales(tmp_path, monkeypatch) -> Path:
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(3)
    n = 200
    path = tmp_path / "sales.csv"
    pd.DataFrame(
        {
            "day": pd.date_range("2024-01-01", periods=n).strftime("%Y-%m-%d"),
            "region": rng.choice(["North", "South", "East", "West"], n),
            "revenue": rng.normal(100, 10, n).round(2),
        }
    ).to_csv(path, index=False)
    return path


def _figures(path: Path, look, layout=LAYOUT, name="p") -> dict:
    out = path.parent / f"{name}.html"
    result = generate_dashboard(
        str(path), output_path=str(out), open_after=False, spec={"layout": layout, "style": {"look": look}}
    )
    assert result["success"] is True, result.get("error")
    return drawn(out.read_text(encoding="utf-8"))["figures"]


@needs_node
class TestTheMarksFollowTheLook:
    def _line(self, figures):
        return next(f for cid, f in figures.items() if "line" in cid or "time_series" in cid)

    def _bar(self, figures):
        return next(f for cid, f in figures.items() if cid.endswith("_bar"))

    def test_a_look_that_says_nothing_draws_as_before(self, sales):
        figures = _figures(sales, "studio")
        line, bar = self._line(figures), self._bar(figures)
        assert line["data"][0]["line"] == {"color": line["data"][0]["line"]["color"], "width": 2, "shape": "linear"}
        assert "cornerradius" not in bar["data"][0]["marker"] and bar["layout"]["bargap"] == 0.42

    def test_the_line_takes_its_width_curve_markers_and_area(self, sales):
        look = _with(chart={"line_width": 3, "line_shape": "spline", "markers": "none", "area": 25})
        main = self._line(_figures(sales, look))["data"][0]
        assert (main["line"]["width"], main["line"]["shape"], main["mode"]) == (3, "spline", "lines")
        assert main["fillcolor"].endswith(",0.25)")

    def test_markers_on_the_ends_only(self, sales):
        main = self._line(_figures(sales, _with(chart={"markers": "ends"})))["data"][0]
        sizes = main["marker"]["size"]
        assert sizes[0] == sizes[-1] == 7 and set(sizes[1:-1]) == {0}

    def test_bars_take_their_corners_and_their_gap(self, sales):
        bar = self._bar(_figures(sales, _with(chart={"bar_radius": 8, "bar_gap": 30})))
        assert bar["data"][0]["marker"]["cornerradius"] == 8 and bar["layout"]["bargap"] == 0.3

    def test_the_grid_the_axis_line_and_the_type_inside_a_chart(self, sales):
        dotted = self._line(_figures(sales, _with(chart={"grid": "dotted", "axis_line": True, "font_size": 13})))[
            "layout"
        ]
        assert dotted["xaxis"]["griddash"] == "dot" and dotted["yaxis"]["showline"] is True
        assert dotted["font"]["size"] == 13 and dotted["xaxis"]["tickfont"]["size"] == 13
        assert self._line(_figures(sales, _with(chart={"grid": "none"})))["layout"]["yaxis"]["showgrid"] is False

    def test_a_dark_tooltip_is_the_pages_text_colour_on_its_card(self, sales):
        layout = self._line(_figures(sales, _with(chart={"hover": "dark"})))["layout"]["hoverlabel"]
        theme = looks.chart_theme(looks.validate_look(_with(chart={"hover": "dark"})), "light")
        assert layout["bgcolor"] == theme["font"] and layout["font"]["color"] == theme["bg"]


class TestALookFromABrand:
    ACCENTS = ("#7c3aed", "#ffd60a", "#0b1d3a", "#888888", "#e11d48", "#00a86b", "#f97316", "#ffffff", "#000000")

    @pytest.mark.parametrize("mood", list(MOODS))
    def test_it_reads_in_light_and_in_dark_whatever_the_accent(self, mood):
        for accent in self.ACCENTS:
            look, report = look_from_brand(accent, mood)
            for mode in ("light", "dark"):
                t = look[mode]
                assert contrast(t["text"], t["surface"]) >= 12, (accent, mode)
                assert contrast(t["text-muted"], t["surface"]) >= 4.5, (accent, mode)
                assert contrast(t["accent"], t["surface"]) >= 4.5, (accent, mode)
                assert report["contrast"][mode]["accent"] == round(contrast(t["accent"], t["surface"]), 1)

    def test_the_accent_is_moved_along_its_lightness_never_its_hue(self):
        from shared.look_maker import _hsl

        look, report = look_from_brand("#ffd60a", "calm")
        assert look["light"]["accent"] != "#ffd60a" and "accent_moved" in report
        assert abs(_hsl(look["light"]["accent"])[0] - _hsl("#ffd60a")[0]) < 0.01

    def test_an_accent_that_already_reads_is_left_alone(self):
        look, report = look_from_brand("#7c3aed", "calm")
        assert look["light"]["accent"] == "#7c3aed" and "accent_moved" not in report

    @pytest.mark.parametrize("accent", ["#7c3aed", "#0f6fb5"])  # the second is a blue the safe set already has
    def test_the_series_colours_are_the_colour_blind_safe_set_with_the_accent_first(self, accent):
        look, _ = look_from_brand(accent, "bold")
        palette = look["palette"]
        assert palette[0] == look["light"]["accent"] and len(palette) <= 8
        assert all(_hue_gap(c, palette[0]) > 25 for c in palette[1:]) and set(palette[1:]) <= set(OKABE_ITO)

    def test_each_mood_is_a_different_look(self):
        made = {m: look_from_brand("#0e7490", m)[0] for m in MOODS}
        assert len({json.dumps(v, sort_keys=True) for v in made.values()}) == len(MOODS)
        css = {m: looks.look_css(v) for m, v in made.items()}
        assert all(c.count("{") == c.count("}") for c in css.values()) and len(set(css.values())) == len(MOODS)

    def test_a_mode_makes_only_the_tokens_it_needs(self):
        light, _ = look_from_brand("#0e7490", "calm", mode="light")
        dark, _ = look_from_brand("#0e7490", "calm", mode="dark")
        assert "dark" not in light and "light" not in dark and light["mode"] == "light" and dark["mode"] == "dark"

    def test_a_font_replaces_the_moods_body_face(self):
        assert look_from_brand("#0e7490", "calm", font="manrope")[0]["font"] == "manrope"

    def test_it_is_the_same_every_time(self):
        assert look_from_brand("#0e7490", "editorial") == look_from_brand("#0e7490", "editorial")

    @pytest.mark.parametrize(
        ("kwargs", "says"),
        [
            ({"accent": "teal-ish"}, "brand.accent is a colour as #rrggbb"),
            ({"accent": "#0e7490", "mood": "loud"}, "brand.mood is one of calm, bold, editorial, technical, playful"),
            ({"accent": "#0e7490", "mode": "sepia"}, "brand.mode is one of light, dark, device"),
        ],
    )
    def test_a_brand_it_cannot_make_is_refused_by_name(self, kwargs, says):
        with pytest.raises(looks.LookError, match=says):
            look_from_brand(**kwargs)


class TestTheLooksAction:
    def test_it_lists_the_grammar_and_the_moods_for_a_caller_defining_its_own(self):
        r = dashboard_looks()
        assert r["success"] and set(r["moods"]) == set(MOODS) and r["grammar"]["chart"]["grid"].startswith("one of")
        assert "brand=" in r["hint"]

    def test_a_brand_makes_a_look_the_spec_can_use(self):
        r = dashboard_looks(brand={"accent": "#7c3aed", "mood": "editorial"})
        assert r["success"] is True and r["look"]["tabs"] == "underline" and r["use"] == {"style": {"look": r["look"]}}

    def test_overrides_change_a_group_key_by_key_not_whole(self):
        r = dashboard_looks(
            brand={"accent": "#7c3aed", "mood": "calm"}, overrides={"chart": {"bar_radius": 0}, "tabs": "segmented"}
        )
        assert (
            r["look"]["chart"]["bar_radius"] == 0 and r["look"]["chart"]["line_shape"] == "spline"
        )  # the rest of chart stayed
        assert r["look"]["tabs"] == "segmented"

    def test_a_bad_override_is_refused_by_name(self):
        r = dashboard_looks(name="studio", overrides={"chart": {"grid": "wavy"}})
        assert r["success"] is False and "look.chart.grid is one of" in r["error"]

    @pytest.mark.parametrize(
        ("kwargs", "says"),
        [
            ({"name": "studio", "brand": {"accent": "#0e7490"}}, "Pass one of name, source or brand"),
            ({"brand": {"mood": "calm"}}, "brand needs an accent"),
            ({"brand": {"accent": "#0e7490", "tone": "x"}}, "unknown key(s): tone"),
            ({"brand": "teal"}, "brand is a dict"),
        ],
    )
    def test_what_it_cannot_do_is_refused_by_name(self, kwargs, says):
        r = dashboard_looks(**kwargs)
        assert r["success"] is False and says in r["error"]

    def test_a_saved_brand_look_is_named_by_its_path(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
        r = dashboard_looks(brand={"accent": "#0e7490", "mood": "technical"}, output_path=str(tmp_path / "mine.json"))
        assert r["success"] is True and looks.resolve_look(r["output_path"]) == r["look"]

    def test_a_page_wears_a_brand_look(self, sales):
        look = dashboard_looks(brand={"accent": "#9f1239", "mood": "editorial"})["look"]
        out = sales.parent / "wears.html"
        result = generate_dashboard(str(sales), output_path=str(out), open_after=False, spec={"style": {"look": look}})
        assert result["success"] is True, result.get("error")
        html = out.read_text(encoding="utf-8")
        assert "text-transform:uppercase" in html and "border-bottom-color:var(--accent)" in html
