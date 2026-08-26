"""The axis label was in the file and never on the screen.

    customize_chart(ads_3d_scatter.html, x_label="Spend (USD)",
                    y_label="Impressions")
      -> changes_applied: ["x-axis label → 'Spend (USD)'",
                           "y-axis label → 'Impressions'", ...]

Both strings really were written, into layout."xaxis".title and
layout."yaxis".title. But the figure is a scatter3d, and plotly reads a 3D
figure's axis titles from layout."scene"."xaxis" -- never from the top level.
The scene block was there and had no title key at all, so the two labels were
inert: present on disk, absent from the picture.

That is the nastiest shape this round has turned up, because the obvious check
passes. Grep the customized HTML for "Spend (USD)" and it is there. Diff the
layouts and a new string appeared. Only asking *which object* it landed in --
and knowing plotly reads a different one for 3D -- shows the change did
nothing. A round-16 phase did exactly that and called it: "success-flag-but-
wrong-output".

The same mistake has a second form. A pie chart has no axes at all, so
x_label on one had nowhere correct to go either; the old code wrote a
top-level xaxis onto it and reported the change applied. Saying "x-axis label →
'Spend'" about a pie chart is the same lie told about a different figure, so
that is refused by name now rather than written somewhere inert.

2D cartesian charts are untouched: the top level is where their titles are
read from, and that is still where they go.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from servers.data_visual import engine as dv
from servers.data_visual._adv_customize import _split_newplot


def _layout(path: Path) -> dict:
    _, _, _, _, layout_str, _ = _split_newplot(path.read_text(encoding="utf-8"))
    return json.loads(layout_str.lstrip(", "))


def _traces(path: Path) -> list:
    _, _, data_str, _, _, _ = _split_newplot(path.read_text(encoding="utf-8"))
    return json.loads(data_str)


@pytest.fixture()
def numeric_csv(tmp_path: Path) -> Path:
    src = tmp_path / "d.csv"
    rows = "\n".join(f"{i},{i * 2},{i * 3}" for i in range(1, 40))
    src.write_text(f"a,b,c\n{rows}\n", encoding="utf-8")
    return src


@pytest.fixture()
def category_csv(tmp_path: Path) -> Path:
    src = tmp_path / "c.csv"
    src.write_text("cat,val\nA,3\nB,5\nC,2\n", encoding="utf-8")
    return src


@pytest.fixture()
def chart_3d(numeric_csv: Path, tmp_path: Path) -> Path:
    out = tmp_path / "c3d.html"
    r = dv.generate_3d_chart(
        str(numeric_csv),
        chart_type="scatter_3d",
        x_column="a",
        y_column="b",
        z_column="c",
        output_path=str(out),
        open_after=False,
    )
    assert r["success"] is True, r.get("error")
    return out


class TestA3DLabelLandsWherePlotlyReadsIt:
    @pytest.fixture()
    def customized(self, chart_3d: Path, tmp_path: Path) -> Path:
        out = tmp_path / "out.html"
        r = dv.customize_chart(str(chart_3d), x_label="Spend (USD)", y_label="Impressions", output_path=str(out))
        assert r["success"] is True, r.get("error")
        return out

    def test_it_really_is_a_3d_figure(self, customized: Path) -> None:
        """Otherwise the rest of this class proves nothing."""
        assert _traces(customized)[0]["type"] == "scatter3d"

    def test_the_x_label_is_in_the_scene(self, customized: Path) -> None:
        scene = _layout(customized)["scene"]
        assert scene["xaxis"]["title"] == {"text": "Spend (USD)"}, scene.get("xaxis")

    def test_the_y_label_is_in_the_scene(self, customized: Path) -> None:
        scene = _layout(customized)["scene"]
        assert scene["yaxis"]["title"] == {"text": "Impressions"}, scene.get("yaxis")

    def test_nothing_was_written_to_the_top_level(self, customized: Path) -> None:
        """The exact bug: a title here is read by nothing on a 3D figure."""
        layout = _layout(customized)
        for key in ("xaxis", "yaxis"):
            assert "title" not in (layout.get(key) or {}), f"top-level {key} still carries a title"

    def test_the_string_alone_would_have_fooled_a_grep(self, customized: Path) -> None:
        """Why the old check passed: the label is in the file either way."""
        assert "Spend (USD)" in customized.read_text(encoding="utf-8")


class TestTwoDimensionalChartsAreUnchanged:
    @pytest.fixture()
    def bar_customized(self, category_csv: Path, tmp_path: Path) -> Path:
        raw = tmp_path / "bar.html"
        made = dv.generate_chart(
            str(category_csv),
            chart_type="bar",
            value_column="val",
            category_column="cat",
            output_path=str(raw),
            open_after=False,
        )
        assert made["success"] is True, made.get("error")
        out = tmp_path / "bar_c.html"
        r = dv.customize_chart(str(raw), x_label="Category", y_label="Value", output_path=str(out))
        assert r["success"] is True, r.get("error")
        return out

    def test_the_label_stays_at_the_top_level(self, bar_customized: Path) -> None:
        layout = _layout(bar_customized)
        assert layout["xaxis"]["title"] == {"text": "Category"}
        assert layout["yaxis"]["title"] == {"text": "Value"}

    def test_no_scene_is_invented(self, bar_customized: Path) -> None:
        assert "scene" not in _layout(bar_customized)


class TestAChartWithNoAxesIsRefused:
    @pytest.fixture()
    def pie(self, category_csv: Path, tmp_path: Path) -> Path:
        out = tmp_path / "pie.html"
        r = dv.generate_chart(
            str(category_csv),
            chart_type="pie",
            value_column="val",
            category_column="cat",
            output_path=str(out),
            open_after=False,
        )
        assert r["success"] is True, r.get("error")
        return out

    def test_an_axis_label_on_a_pie_is_an_error(self, pie: Path, tmp_path: Path) -> None:
        r = dv.customize_chart(str(pie), x_label="Category", output_path=str(tmp_path / "p.html"))
        assert r["success"] is False, r
        assert "no axes to label" in r["error"], r["error"]

    def test_it_names_the_chart_type(self, pie: Path, tmp_path: Path) -> None:
        r = dv.customize_chart(str(pie), x_label="Category", output_path=str(tmp_path / "p.html"))
        assert "pie" in r["error"], r["error"]

    def test_the_hint_says_what_to_do_instead(self, pie: Path, tmp_path: Path) -> None:
        r = dv.customize_chart(str(pie), x_label="Category", output_path=str(tmp_path / "p.html"))
        assert "generate_chart()" in r["hint"], r["hint"]

    def test_other_customisations_still_work_on_a_pie(self, pie: Path, tmp_path: Path) -> None:
        """Only the impossible change is refused, not the whole call."""
        out = tmp_path / "pt.html"
        r = dv.customize_chart(str(pie), title="New Title", output_path=str(out))
        assert r["success"] is True, r.get("error")
        assert any("title" in c for c in r["changes_applied"]), r["changes_applied"]
        assert out.is_file()
