"""A chart's figure is edited by path, over an allow-list, and nothing outside it.

customize_chart took a fixed list of keywords -- title, labels, colours, sort,
highlight, annotations, value labels, size. A log axis, the legend on the
right, bars drawn as a line, a second y axis, a target line: none had a route
except rewriting the page. `ops` reach the figure itself:

    {"op": "set", "path": "layout.yaxis.type", "value": "log"}
    {"op": "set", "path": "data[*].line.width", "value": 3}
    {"op": "reference_line", "axis": "y", "value": 1000, "label": "Target"}

What has to hold, read off the figure the page carries:

- each path changes what it names, on the traces it names;
- a trace moved to y2 gets a right-hand axis over the first, not one Plotly
  invents across the whole plot;
- a reference line's label rides on the line. As an annotation it sat at
  y=value, which a log axis reads as log10: "Target" at 70000 was drawn at
  10^70000, off the chart, while the line sat where it should;
- a path, a value or a trace outside the list is refused by name, and a
  refused edit writes nothing;
- a dry run writes nothing.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from servers.data_visual.engine import customize_chart, generate_chart
from shared.chart_ops import MAX_CHART_OPS
from shared.plotly_payload import split_newplot


@pytest.fixture
def csv(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(41)
    n = 200
    path = tmp_path / "sales.csv"
    pd.DataFrame(
        {
            "day": pd.date_range("2024-01-01", periods=n).strftime("%Y-%m-%d"),
            "region": rng.choice(["North", "South", "East"], n),
            "channel": rng.choice(["web", "store"], n),
            "revenue": rng.normal(1000, 150, n).round(2),
        }
    ).to_csv(path, index=False)
    return path


def _chart(csv: Path, kind: str, **kw) -> Path:
    out = csv.parent / f"{kind}_{len(kw)}.html"
    r = generate_chart(str(csv), kind, "revenue", output_path=str(out), open_after=False, **kw)
    assert r["success"] is True, r
    return Path(r["output_path"])


@pytest.fixture
def bar(csv):
    return _chart(csv, "bar", category_column="region")


@pytest.fixture
def lines(csv):
    """A line chart with a trace per channel."""
    return _chart(csv, "line", category_column="day", color_column="channel")


def _figure(path: Path) -> tuple[list, dict]:
    _, _, data, _, layout, _ = split_newplot(path.read_text(encoding="utf-8"))
    return json.loads(data), json.loads(layout)


def _edit(chart: Path, ops: list, **kw) -> dict:
    out = chart.parent / f"{chart.stem}_edited.html"
    return customize_chart(str(chart), ops=ops, output_path=str(out), **kw)


def _edited(chart: Path, ops: list, **kw) -> tuple[list, dict, dict]:
    r = _edit(chart, ops, **kw)
    assert r["success"] is True, r
    return (*_figure(Path(r["output_path"])), r)


class TestEachPathChangesWhatItNames:
    def test_a_log_axis_and_a_legend_on_the_right(self, bar):
        ops = [
            {"op": "set", "path": "layout.yaxis.type", "value": "log"},
            {"op": "set", "path": "layout.legend.orientation", "value": "v"},
            {"op": "set", "path": "layout.yaxis.tickprefix", "value": "$"},
        ]
        _, layout, r = _edited(bar, ops)
        assert layout["yaxis"]["type"] == "log" and layout["yaxis"]["tickprefix"] == "$"
        assert layout["legend"]["orientation"] == "v"
        assert r["changes_applied"] == [
            "layout.yaxis.type → 'log'",
            "layout.legend.orientation → 'v'",
            "layout.yaxis.tickprefix → '$'",
        ]

    def test_bars_drawn_as_a_line_keep_their_data(self, bar):
        before, _ = _figure(bar)
        traces, _, _ = _edited(bar, [{"op": "set", "path": "data[0].type", "value": "scatter"}])
        assert traces[0]["type"] == "scatter" and traces[0]["mode"] == "lines+markers"
        assert (traces[0]["x"], traces[0]["y"]) == (before[0]["x"], before[0]["y"])

    def test_every_trace_or_one(self, lines):
        before, _ = _figure(lines)
        assert len(before) >= 2
        traces, _, _ = _edited(
            lines,
            [
                {"op": "set", "path": "data[*].line.width", "value": 4},
                {"op": "set", "path": "data[1].line.dash", "value": "dot"},
            ],
        )
        assert all(t["line"]["width"] == 4 for t in traces)
        assert traces[1]["line"]["dash"] == "dot" and traces[0].get("line", {}).get("dash") != "dot"

    def test_a_second_y_axis_is_the_right_hand_one(self, lines):
        traces, layout, _ = _edited(lines, [{"op": "set", "path": "data[1].yaxis", "value": "y2"}])
        assert traces[1]["yaxis"] == "y2" and traces[0].get("yaxis", "y") == "y"
        assert layout["yaxis2"]["overlaying"] == "y" and layout["yaxis2"]["side"] == "right"

    def test_ops_come_after_the_keywords(self, bar):
        _, layout, r = _edited(bar, [{"op": "set", "path": "layout.height", "value": 520}], height=300, title="T")
        assert layout["height"] == 520
        assert r["changes_applied"][-1] == "layout.height → 520"


class TestAReferenceLine:
    def test_is_a_line_across_the_plot_with_its_label_on_it(self, bar):
        _, layout, r = _edited(
            bar,
            [
                {"op": "set", "path": "layout.yaxis.type", "value": "log"},
                {"op": "reference_line", "axis": "y", "value": 70000, "label": "Target"},
            ],
        )
        shape = layout["shapes"][-1]
        assert (shape["yref"], shape["y0"], shape["y1"], shape["xref"], shape["x0"], shape["x1"]) == (
            "y",
            70000,
            70000,
            "paper",
            0,
            1,
        )
        assert shape["label"]["text"] == "Target"
        assert not any(a.get("y") == 70000 for a in layout.get("annotations") or [])
        assert r["changes_applied"][-1] == "reference line at y=70000 (Target)"

    def test_on_a_category_axis(self, bar):
        _, layout, _ = _edited(bar, [{"op": "reference_line", "axis": "x", "value": "North", "dash": "solid"}])
        shape = layout["shapes"][-1]
        assert (shape["xref"], shape["x0"], shape["line"]["dash"]) == ("x", "North", "solid") and "label" not in shape


class TestWhatIsOutsideTheListIsRefused:
    @pytest.mark.parametrize(
        ("ops", "says"),
        [
            ([{"op": "set", "path": "layout.font.family", "value": "x"}], "'layout.font.family' is not editable"),
            ([{"op": "set", "path": "layout.font.family", "value": "x"}], "layout.yaxis.{type,range,autorange"),
            ([{"op": "set", "path": "data[0].x", "value": [1]}], "'data[0].x' is not editable. Trace paths:"),
            ([{"op": "set", "path": "data[7].name", "value": "x"}], "names trace 7, and the chart has 1 (0 to 0)"),
            ([{"op": "set", "path": "traces.0", "value": 1}], "Paths start 'layout.' or 'data[N].'"),
            ([{"op": "set", "path": "layout.hovermode", "value": 0}], "layout.hovermode takes 'closest'"),
            ([{"op": "set", "path": "layout.yaxis.type", "value": "logarithmic"}], "'linear' | 'log'"),
            ([{"op": "set", "path": "data[0].marker.color", "value": "reddish"}], "takes a colour such as"),
            ([{"op": "set", "path": "layout.height", "value": 10}], "a whole number from 160 to 3000"),
            ([{"op": "set", "path": "data[0].type", "value": "pie"}], "'bar' | 'scatter'"),
            ([{"op": "set", "path": "layout.height"}], "set needs a value"),
            ([{"op": "set", "path": "layout.height", "value": 400, "where": 1}], "unknown key(s): where"),
            ([{"op": "delete", "path": "layout"}], "op='delete' is not an edit. Valid: set, reference_line"),
            ([{"op": "reference_line", "axis": "z", "value": 1}], "axis must be 'x' or 'y'"),
            ([{"op": "reference_line", "value": None}], "value is where the line sits on the y axis"),
            ([], "ops must be a list"),
            ([{"op": "set", "path": "layout.height", "value": 400}] * (MAX_CHART_OPS + 1), "at most 50"),
        ],
    )
    def test_by_name_and_nothing_is_written(self, bar, ops, says):
        r = _edit(bar, ops)
        assert r["success"] is False and says in r["error"], r.get("error")
        assert not (bar.parent / f"{bar.stem}_edited.html").exists()

    def test_a_pie_has_no_axes_and_its_trace_cannot_become_a_bar(self, csv):
        pie = _chart(csv, "pie", category_column="region")
        r = _edit(pie, [{"op": "set", "path": "layout.yaxis.type", "value": "log"}])
        assert r["success"] is False and "a pie chart has no yaxis to edit" in r["error"]
        r = _edit(pie, [{"op": "set", "path": "data[0].type", "value": "bar"}])
        assert r["success"] is False and "trace 0 is a pie" in r["error"]
        r = _edit(pie, [{"op": "reference_line", "value": 1}])
        assert r["success"] is False and "no axes to draw a reference line across" in r["error"]


class TestADryRun:
    def test_says_what_it_would_do_and_writes_nothing(self, bar):
        before = sorted(p.name for p in bar.parent.iterdir())
        r = customize_chart(str(bar), ops=[{"op": "set", "path": "layout.bargap", "value": 0.4}], dry_run=True)
        assert r["success"] is True and r["dry_run"] is True
        assert r["changes_applied"] == ["layout.bargap → 0.4"] and r["would_write"].endswith("_customized.html")
        assert sorted(p.name for p in bar.parent.iterdir()) == before
