"""A dashboard wears a look: any number of them, each data, none able to write into the page.

Every dashboard the engine drew was the same dashboard in one of three colour sets. A look is the rest
of a design: colour tokens per mode, type, a card's radius, shadow and density, how the page is framed
(a left rail, a console sidebar, a banner, a rounded app frame) and what a KPI tile looks like. It is
chosen in the page's style (`style.look`: a built-in name, a saved .json, or the look as a dict), it
travels in the embedded spec, and a look can be read out of an HTML mockup. Plotly still draws every
mark; the look decides what surrounds it and which colours it is handed.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from servers.data_advanced._adv_dashboard import customize_dashboard, generate_dashboard
from servers.data_advanced._dash_looks import dashboard_looks
from servers.data_domain.server import DOMAINS
from shared import dashboard_looks as looks

MOCKUPS = Path(__file__).resolve().parents[1] / "design" / "mockups"


@pytest.fixture(autouse=True)
def _home(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    return tmp_path


@pytest.fixture
def data(_home) -> str:
    rng = np.random.default_rng(2)
    n = 300
    frame = pd.DataFrame(
        {
            "day": pd.date_range("2024-01-01", periods=n).strftime("%Y-%m-%d"),
            "region": rng.choice(["North", "South", "East"], n),
            "spend": rng.integers(50, 500, n),
            "rating": rng.uniform(1, 5, n).round(2),
        }
    )
    path = _home / "shop.csv"
    frame.to_csv(path, index=False)
    return str(path)


def _page(data: str, home: Path, look, name: str = "p", **style) -> tuple[dict, str]:
    result = generate_dashboard(
        data, output_path=str(home / f"{name}.html"), open_after=False, spec={"style": {"look": look, **style}}
    )
    assert result["success"] is True, result.get("error")
    return result, (home / f"{name}.html").read_text(encoding="utf-8")


class TestTheBuiltInLooks:
    def test_there_are_several_and_each_is_valid_and_different(self):
        names = looks.builtin_names()
        assert len(names) >= 6
        sheets = {n: looks.look_css(looks.resolve_look(n)) for n in names}
        assert len(set(sheets.values())) == len(names)
        for css in sheets.values():
            assert css.count("{") == css.count("}")

    def test_every_look_names_the_modes_it_has(self):
        for n in looks.builtin_names():
            look = looks.resolve_look(n)
            assert look["mode"] in looks.MODES
            assert ("dark" in look) == (look["mode"] in ("dark", "device"))
            assert ("light" in look) == (look["mode"] in ("light", "device"))

    def test_the_font_names_are_the_ones_the_page_style_knows(self):
        from shared.dashboard_spec import PAGE_FONTS

        assert set(PAGE_FONTS) <= set(looks.STACKS)


class TestAPageWearsIt:
    @pytest.mark.parametrize("name", ["lagoon", "harbor", "nocturne", "ledger", "slate", "signal"])
    def test_each_look_draws_a_page(self, data, _home, name):
        result, html = _page(data, _home, name, name=name)
        assert "/* look */" in html and f"frame-{looks.resolve_look(name)['frame']}" in html
        assert f"look-{name}" in html
        assert result["spec"]["style"]["look"] == name

    def test_a_page_that_asks_for_classic_is_what_it_was(self, data, _home):
        # The engine's own page, as it was before looks: still there for whoever asks for it by name.
        result = generate_dashboard(
            data, output_path=str(_home / "plain.html"), open_after=False, spec={"style": {"look": "classic"}}
        )
        assert result["success"] is True, result.get("error")
        html = (_home / "plain.html").read_text(encoding="utf-8")
        assert "/* look */" not in html and "frame-" not in html and "look-" not in html

    def test_a_generated_page_wears_studio_unless_it_asks_otherwise(self, data, _home):
        result = generate_dashboard(data, output_path=str(_home / "dflt.html"), open_after=False)
        html = (_home / "dflt.html").read_text(encoding="utf-8")
        assert f"look-{looks.DEFAULT_LOOK}" in html and "/* look */" in html
        assert "@font-face" in html and "font/woff2" in html  # its typeface travels inside the page
        other = generate_dashboard(
            data, output_path=str(_home / "other.html"), open_after=False, spec={"style": {"look": "nocturne"}}
        )
        assert other["success"] is True and "look-nocturne" in (_home / "other.html").read_text(encoding="utf-8")
        assert result["success"] is True

    def test_a_dark_only_look_is_dark_whatever_was_asked(self, data, _home):
        _, html = _page(data, _home, "nocturne")
        assert (
            re.search(r'const _THEME=\{[^;]*"bg"', html) and '"device":true' not in html.split("const _THEME=")[1][:400]
        )

    def test_a_device_look_carries_both_modes_to_the_renderer(self, data, _home):
        _, html = _page(data, _home, "lagoon")
        theme = json.loads(re.search(r"const _THEME=(\{.*?\});\nconst _STYLE", html, flags=re.S).group(1))
        assert theme["device"] is True and theme["light"]["bg"] != theme["dark"]["bg"]
        assert theme["light"]["bg"] == looks.resolve_look("lagoon")["light"]["surface"]

    def test_the_looks_palette_colours_the_marks_unless_the_spec_names_one(self, data, _home):
        _, html = _page(data, _home, "lagoon")
        assert '"palette":["#11a9ba"' in html
        _, mine = _page(data, _home, "lagoon", name="mine", palette=["#111111", "#222222"])
        assert '"palette":["#111111","#222222"]' in mine

    def test_the_generic_device_relayout_does_not_repaint_a_look(self, data, _home):
        plain = generate_dashboard(
            data, output_path=str(_home / "a.html"), open_after=False, spec={"style": {"look": "classic"}}
        )
        assert plain["success"]
        _, with_look = _page(data, _home, "lagoon", name="b")
        marker = "function applyTheme"
        assert marker in (_home / "a.html").read_text(encoding="utf-8")
        assert marker not in with_look

    def test_kpi_tiles_are_tinted_in_the_order_they_are_shown(self, data, _home):
        _, html = _page(data, _home, "lagoon")
        tones = re.findall(r'class="cc cc-kpi kt(\d)"', html)
        assert tones[:2] == ["0", "1"]

    def test_the_rail_look_puts_the_tabs_in_a_rail(self, data, _home):
        _, html = _page(data, _home, "lagoon")
        assert "frame-rail" in html and ".tabs{position:absolute" in html and "padding-left:5rem" in html

    def test_the_sidebar_look_uses_the_engines_sidebar(self, data, _home):
        _, html = _page(data, _home, "harbor")
        assert 'class="' in html and "sidebar" in re.search(r"<body class=\"([^\"]*)\"", html).group(1)


class TestALookIsData:
    def _mine(self, **over) -> dict:
        base = {
            "mode": "light",
            "light": {
                "bg": "#fafafa", "surface": "#ffffff", "border": "#dddddd", "text": "#111111",
                "text-muted": "#666666", "accent": "#d6336c",
            },
        }  # fmt: skip
        return {**base, **over}

    def test_a_look_given_as_a_dict_is_embedded_whole(self, data, _home):
        result, _ = _page(data, _home, self._mine(radius=2, card="flat", frame="banner", header="gradient"))
        assert result["spec"]["style"]["look"]["radius"] == 2

    def test_a_saved_look_is_embedded_not_referenced(self, data, _home):
        saved = _home / "mine.json"
        saved.write_text(json.dumps(self._mine(radius=9)))
        result, html = _page(data, _home, str(saved))
        assert isinstance(result["spec"]["style"]["look"], dict) and "mine.json" not in html

    def test_customize_swaps_the_look_of_a_page_that_exists(self, data, _home):
        _page(data, _home, "lagoon", name="first")
        changed = customize_dashboard(
            str(_home / "first.html"),
            changes={"style": {"look": "ledger"}},
            output_path=str(_home / "second.html"),
            open_after=False,
        )
        assert changed["success"] is True, changed.get("error")
        html = (_home / "second.html").read_text(encoding="utf-8")
        assert "look-ledger" in html and "look-lagoon" not in html

    def test_a_look_with_one_mode_missing_is_refused_by_name(self):
        with pytest.raises(looks.LookError, match=r"look\.dark is required"):
            looks.validate_look({"mode": "device", "light": self._mine()["light"]})

    @pytest.mark.parametrize(
        "bad",
        [
            {"frame": "floating"},
            {"radius": 400},
            {"radius": True},
            {"shadow": "0 0 0 red; background:url(x)"},
            {"font": "Comic Sans; } body { display: none"},
            {"palette": ["#fff", "url(javascript:alert(1))"]},
            {"unknown": 1},
            {
                "light": {
                    "bg": "red; } body { display:none",
                    "surface": "#fff",
                    "border": "#ddd",
                    "text": "#000",
                    "text-muted": "#666",
                    "accent": "#00f",
                }
            },
        ],
    )
    def test_nothing_that_is_not_a_choice_a_colour_or_a_number_gets_in(self, bad):
        with pytest.raises(looks.LookError):
            looks.validate_look(self._mine(**bad))

    def test_an_unknown_look_name_lists_the_known_ones(self, data, _home):
        result = generate_dashboard(
            data, output_path=str(_home / "x.html"), open_after=False, spec={"style": {"look": "neon"}}
        )
        assert result["success"] is False and "lagoon" in result["error"] and "ledger" in result["error"]

    def test_a_bad_look_in_a_page_writes_no_page(self, data, _home):
        result = generate_dashboard(
            data,
            output_path=str(_home / "y.html"),
            open_after=False,
            spec={"style": {"look": self._mine(frame="floating")}},
        )
        assert result["success"] is False and not (_home / "y.html").exists()

    def test_the_page_css_carries_only_what_was_validated(self):
        look = looks.validate_look(self._mine(font="Georgia, 'Times New Roman'", radius=7.9))
        css = looks.look_css(look)
        assert "--r:7px" in css and "Georgia, 'Times New Roman'" in css and "url(" not in css


class TestALookFromAMockup:
    def test_lagoon_is_read_back(self):
        look, report = looks.digest_html((MOCKUPS / "lagoon.src.html").read_text(), "lagoon")
        assert look["mode"] == "device" and look["frame"] == "rail" and look["radius"] == 20
        assert look["light"]["accent"] == "#11a9ba" and look["light"]["surface"] == "#ffffff"
        assert look["light"]["rail"] == "#15b0bf" and look["dark"]["surface"] == "#15303a"
        assert look["font"] == "nunito" and report["dark_set"] is True and look["shadow"] == "soft"  # a bundled face

    def test_harbor_is_read_back_with_its_dark_set(self):
        look, _ = looks.digest_html((MOCKUPS / "harbor.src.html").read_text(), "harbor")
        assert look["mode"] == "device" and look["light"]["rail"] == "#0d1f3e" and look["dark"]["bg"] == "#0a111c"

    def test_a_dark_mockup_without_a_light_set_is_a_dark_look(self):
        look, report = looks.digest_html((MOCKUPS / "nocturne.src.html").read_text(), "nocturne")
        assert look["mode"] == "dark" and "dark" in look and "light" not in look
        assert any("dark only" in n for n in report["notes"])

    def test_what_is_in_the_mockup_but_not_a_token_is_reported(self):
        _, report = looks.digest_html((MOCKUPS / "nocturne.src.html").read_text(), "n")
        assert {"cyan", "magenta"} <= set(report["colours_not_mapped_to_a_token"])

    def test_a_mockup_that_styles_body_and_card_but_names_no_variables_is_still_read(self):
        html = (
            "<style>body{background:#101820;color:#e6edf3;font-family:Georgia,serif}"
            ".card{background:#1a2430;border:1px solid #2c3a4a;border-radius:14px;box-shadow:0 8px 30px rgba(0,0,0,.4)}"
            ":root{--accent:#ff8a00}</style>"
        )
        look, report = looks.digest_html(html)
        assert look["dark"]["bg"] == "#101820" and look["dark"]["surface"] == "#1a2430"
        assert look["dark"]["border"] == "#2c3a4a" and look["radius"] == 14 and look["shadow"] == "lift"
        assert look["font"] == "serif" and any("body background" in g for g in report["read_from_rules"])

    def test_a_mockup_with_no_colours_to_read_is_refused_saying_what_it_needs(self):
        with pytest.raises(looks.LookError, match="could not read these from the mockup"):
            looks.digest_html("<style>h1{margin:0}</style><h1>hi</h1>")

    def test_a_mockup_cannot_carry_a_rule_into_the_look(self):
        html = (
            "<style>:root{--bg:#fff;--card:#fff;--line:#ddd;--ink:#000;--muted:#666;"
            "--accent:#f00;} body{display:none} --accent2:url(javascript:alert(1));"
            "--font:'x'}</style>"
        )
        look, _ = looks.digest_html(html)
        assert "javascript" not in json.dumps(look) and "display" not in looks.look_css(look)


class TestTheAction:
    def test_it_lists_the_looks_with_a_swatch(self):
        result = dashboard_looks()
        assert result["success"] is True
        by_name = {entry["name"]: entry for entry in result["looks"]}
        assert {"lagoon", "harbor", "nocturne"} <= set(by_name)
        assert by_name["lagoon"]["swatch"]["accent"] == "#11a9ba" and by_name["lagoon"]["frame"] == "rail"

    def test_a_look_is_tweaked_and_saved_and_the_page_can_name_it(self, data, _home):
        out = _home / "tweaked.json"
        result = dashboard_looks(
            name="slate",
            overrides={"frame": "banner", "radius": 2, "light": {"accent": "#c2185b"}},
            output_path=str(out),
        )
        assert result["success"] is True, result.get("error")
        saved = json.loads(out.read_text())
        assert saved["frame"] == "banner" and saved["radius"] == 2 and saved["light"]["accent"] == "#c2185b"
        assert saved["light"]["bg"] == looks.resolve_look("slate")["light"]["bg"]
        assert result["use"] == {"style": {"look": str(out)}}
        page, _ = _page(data, _home, str(out))
        assert page["spec"]["style"]["look"]["frame"] == "banner"

    def test_a_mockup_is_digested_and_saved(self, _home):
        out = _home / "from_mockup.json"
        result = dashboard_looks(source=str(MOCKUPS / "harbor.src.html"), output_path=str(out))
        assert result["success"] is True, result.get("error")
        assert json.loads(out.read_text())["light"]["accent"] == "#1f7cf0" and "report" in result

    def test_an_existing_saved_look_is_snapshotted_not_lost(self, _home):
        out = _home / "keep.json"
        out.write_text('{"old": true}')
        result = dashboard_looks(name="ledger", output_path=str(out))
        assert result["success"] is True and Path(result["backup"]).read_text() == '{"old": true}'

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"name": "lagoon", "source": "x.html"},
            {"overrides": {"frame": "plain"}},
            {"name": "lagoon", "output_path": "x.txt"},
            {"name": "nope"},
            {"source": "/nonexistent/m.html"},
            {"name": "lagoon", "overrides": {"radius": 999}},
        ],
    )
    def test_refusals_name_what_to_do(self, kwargs):
        result = dashboard_looks(**kwargs)
        assert result["success"] is False and result["hint"]

    def test_it_is_an_action_of_the_report_tool(self):
        assert any(tool == "dashboard_looks" for _, tool in DOMAINS["data_report"][1])
