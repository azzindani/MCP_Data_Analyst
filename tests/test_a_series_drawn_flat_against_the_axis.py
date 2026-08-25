"""The second series was a straight line at zero, and it was not flat.

    time_series_analysis(value_columns=["impressions", "link_clicks"],
                         period="W")

Weekly sums of the reference dataset. impressions peaks near 700,000 and
link_clicks near 4,400 -- a 30:1 spread, and both went onto one y-axis. So
impressions set the scale and link_clicks rendered across **2 pixels**, its
forecast across **0**: a flat red line along the bottom, for a series that rises
from nothing to 4,400 and falls back twice inside the window.

Measured from the rendered page. A flat line and a small line are the same
picture at that height, and the response could not tell them apart either --
every number in it was right.

Found by the same-day sibling rule, immediately after fixing the identical
defect in period_comparison: that one plotted metrics as grouped bars on a
shared axis, this one plots them as lines on a shared axis. Same cause, same
shape, different server.

The layout had already half-anticipated it: the height was being computed with
`calc_chart_height(len(value_columns), mode="subplot")`. The subplots were the
part that was missing. One row per series now, sharing the x-axis so the periods
still line up, and only the bottom row carries the date label -- a title under an
axis whose ticks are hidden is a title floating in the middle of the figure.

These assertions read the figure out of the written page, so they stay offline
like the rest of the suite; the pixel measurements live in the commit message.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from servers.data_medium import engine as dm
from shared.plotly_payload import load_figure

PAIR = ["impressions", "link_clicks"]


@pytest.fixture()
def mixed(ad_data_full_csv: Path, tmp_path: Path) -> Path:
    out = tmp_path / "mixed.html"
    r = dm.time_series_analysis(
        str(ad_data_full_csv),
        date_column="Date",
        value_columns=PAIR,
        period="W",
        output_path=str(out),
        open_after=False,
    )
    assert r["success"] is True, r.get("error")
    return out


@pytest.fixture()
def alone(ad_data_full_csv: Path, tmp_path: Path) -> Path:
    out = tmp_path / "alone.html"
    r = dm.time_series_analysis(
        str(ad_data_full_csv),
        date_column="Date",
        value_columns=["spends"],
        period="M",
        output_path=str(out),
        open_after=False,
    )
    assert r["success"] is True, r.get("error")
    return out


class TestEachSeriesGetsItsOwnScale:
    def test_there_is_a_row_per_series(self, mixed: Path) -> None:
        _, layout = load_figure(mixed.read_text(encoding="utf-8"))
        axes = {k for k in layout if k.startswith("yaxis")}
        assert len(axes) == len(PAIR), sorted(axes)

    def test_the_two_series_do_not_share_one(self, mixed: Path) -> None:
        data, _ = load_figure(mixed.read_text(encoding="utf-8"))
        history = [t for t in data if not t.get("name", "").endswith("(forecast)")]
        assert len({t.get("yaxis", "y") for t in history}) == len(PAIR)

    def test_the_smaller_series_is_not_flattened(self, mixed: Path) -> None:
        """Its axis has to be sized to it, not to impressions."""
        data, _ = load_figure(mixed.read_text(encoding="utf-8"))
        tops: dict[str, float] = {}
        for trace in data:
            values = [v for v in (trace.get("y") or []) if isinstance(v, (int, float))]
            if values:
                axis = trace.get("yaxis", "y")
                tops[axis] = max(tops.get(axis, 0), max(values))
        ordered = sorted(tops.values())
        assert len(ordered) == len(PAIR), tops
        assert ordered[0] < ordered[-1] / 10, tops

    def test_the_forecast_stays_with_its_series(self, mixed: Path) -> None:
        """A dashed continuation on a different panel is not a continuation."""
        data, _ = load_figure(mixed.read_text(encoding="utf-8"))
        by_name = {t.get("name", ""): t.get("yaxis", "y") for t in data}
        for col in PAIR:
            if f"{col} (forecast)" in by_name:
                assert by_name[f"{col} (forecast)"] == by_name[col], col


class TestTheSharedThingsStayShared:
    def test_the_periods_line_up(self, mixed: Path) -> None:
        data, _ = load_figure(mixed.read_text(encoding="utf-8"))
        history = [t for t in data if not t.get("name", "").endswith("(forecast)")]
        xs = {tuple(t.get("x") or []) for t in history}
        assert len(xs) == 1, "series drawn against different period axes"

    def test_the_date_label_appears_once(self, mixed: Path) -> None:
        _, layout = load_figure(mixed.read_text(encoding="utf-8"))
        titled = [k for k, v in layout.items() if k.startswith("xaxis") and (v or {}).get("title")]
        assert len(titled) == 1, titled


class TestOneSeriesIsStillOneChart:
    def test_it_draws(self, alone: Path) -> None:
        data, layout = load_figure(alone.read_text(encoding="utf-8"))
        assert len({k for k in layout if k.startswith("yaxis")}) == 1
        assert any(t.get("name") == "spends" for t in data), [t.get("name") for t in data]

    def test_the_page_still_stands_alone(self, alone: Path) -> None:
        assert not (alone.parent / "plotly.min.js").exists()
        assert "plotly-graph-div" in alone.read_text(encoding="utf-8")


class TestTheNumbersAreUnchanged:
    def test_the_plotted_history_is_the_real_resample(self, mixed: Path, ad_data_full_csv: Path) -> None:
        import pandas as pd

        df = pd.read_csv(ad_data_full_csv, low_memory=False)
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        weekly = df.dropna(subset=["Date"]).set_index("Date").resample("W")[PAIR].sum()

        data, _ = load_figure(mixed.read_text(encoding="utf-8"))
        for col in PAIR:
            trace = next(t for t in data if t.get("name") == col)
            drawn = [float(v) for v in (trace.get("y") or []) if v is not None]
            expected = [float(v) for v in weekly[col].tolist()]
            assert len(drawn) == len(expected), (col, len(drawn), len(expected))
            assert max(abs(a - b) for a, b in zip(drawn, expected)) < 1e-6, col
