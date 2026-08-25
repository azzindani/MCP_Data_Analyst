"""A metric the caller asked to compare was drawn four pixels tall.

    period_comparison(metrics=["impressions", "link_clicks", "clicks"],
                      period_unit="M", current_period="2019-12")

Real months of the reference dataset, a 170:1 spread between the largest metric
and the smallest. Every metric went onto one shared y-axis, so impressions
(407,634) set the scale and link_clicks (3,629) rendered at **4 pixels** beside
a 456-pixel bar. Its reference period rendered at **0**. The caller had named
link_clicks as a thing to compare, the chart showed nothing for it, and the
response said success with all the right numbers in it.

Measured off the rendered page with getBoundingClientRect, not judged by eye --
at that size "the bar is small" and "the bar is absent" look identical in a
screenshot, and only one of them is the defect.

The chart's own docstring had the reasoning right and the scope wrong: grouped
bars rather than percentage deltas, because "a +300% move on a base of 2 reads
very differently next to its bar". True -- while the metrics share a scale.
They are one panel per metric now, each with its own y-axis, so the magnitudes
still show and none of them collapses.

Re-rendering the fix caught a second thing, which is why that rule exists: a
period is named "2019-11", and the moment it became an x *value* rather than a
series name, plotly read it as a date and built a date axis running Oct 27 to
Dec 8. They are two labels, not two instants.

These assertions read the figure out of the written page rather than a
screenshot -- a subplot count and an axis type are structural, and the tests
here do not open a browser. The pixel measurement lives in the commit message
and in this docstring; what is pinned below is the property that made it true.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from servers.data_statistics import engine as ds
from shared.plotly_payload import load_figure

METRICS = ["impressions", "link_clicks", "clicks"]


def figure_of(path: Path) -> tuple[list, dict]:
    return load_figure(path.read_text(encoding="utf-8"))


@pytest.fixture()
def wide(ad_data_full_csv: Path, tmp_path: Path) -> Path:
    out = tmp_path / "wide.html"
    r = ds.period_comparison(
        str(ad_data_full_csv),
        date_column="Date",
        metrics=METRICS,
        period_unit="M",
        current_period="2019-12",
        output_path=str(out),
    )
    assert r["success"] is True, r.get("error")
    return out


@pytest.fixture()
def single(ad_data_full_csv: Path, tmp_path: Path) -> Path:
    out = tmp_path / "single.html"
    r = ds.period_comparison(
        str(ad_data_full_csv),
        date_column="Date",
        metrics=["spends"],
        period_unit="M",
        current_period="2020-07",
        output_path=str(out),
    )
    assert r["success"] is True, r.get("error")
    return out


class TestEachMetricGetsItsOwnScale:
    def test_there_is_a_panel_per_metric(self, wide: Path) -> None:
        _, layout = figure_of(wide)
        axes = {k for k in layout if k.startswith("yaxis")}
        assert len(axes) == len(METRICS), sorted(axes)

    def test_no_two_metrics_share_a_y_axis(self, wide: Path) -> None:
        """The defect itself: one axis meant the largest metric set it."""
        data, _ = figure_of(wide)
        used = {t.get("yaxis", "y") for t in data}
        assert len(used) == len(METRICS), used

    def test_every_metric_is_named_on_the_page(self, wide: Path) -> None:
        text = wide.read_text(encoding="utf-8")
        for metric in METRICS:
            assert metric in text, metric

    def test_the_small_metric_is_not_flattened(self, wide: Path) -> None:
        """Its panel's axis has to be sized to it, not to impressions.

        link_clicks tops out at 3,629 and impressions at 407,634. On a shared
        axis the link_clicks panel would have to reach the larger number.
        """
        data, _ = figure_of(wide)
        by_axis: dict[str, list[float]] = {}
        for trace in data:
            by_axis.setdefault(trace.get("yaxis", "y"), []).extend(
                v for v in (trace.get("y") or []) if isinstance(v, (int, float))
            )
        tops = sorted(max(vs) for vs in by_axis.values() if vs)
        assert tops[0] < tops[-1] / 10, tops


class TestThePeriodsAreLabelsNotDates:
    def test_the_x_axis_is_categorical(self, wide: Path) -> None:
        _, layout = figure_of(wide)
        xaxes = [v for k, v in layout.items() if k.startswith("xaxis")]
        assert xaxes, layout.keys()
        for axis in xaxes:
            assert axis.get("type") == "category", axis

    def test_the_bars_sit_on_the_period_names(self, wide: Path) -> None:
        data, _ = figure_of(wide)
        labels = {x for t in data for x in (t.get("x") or [])}
        assert labels == {"2019-11", "2019-12"}, labels


class TestOneMetricIsStillOneChart:
    def test_it_draws(self, single: Path) -> None:
        data, layout = figure_of(single)
        assert len(data) == 2  # one bar per period
        assert len({k for k in layout if k.startswith("yaxis")}) == 1

    def test_it_stays_responsive(self, single: Path) -> None:
        """A single row keeps the height every other chart here uses."""
        _, layout = figure_of(single)
        assert layout.get("autosize") is True
        assert not layout.get("height")

    def test_the_page_still_stands_alone(self, single: Path) -> None:
        text = single.read_text(encoding="utf-8")
        assert "plotly-graph-div" in text
        assert not (single.parent / "plotly.min.js").exists()


class TestTheNumbersAreUnchanged:
    """A chart fix must not move a value."""

    def test_the_response_still_carries_the_comparison(self, ad_data_full_csv: Path, tmp_path: Path) -> None:
        r = ds.period_comparison(
            str(ad_data_full_csv),
            date_column="Date",
            metrics=["clicks"],
            period_unit="M",
            current_period="2019-12",
            output_path=str(tmp_path / "c.html"),
        )
        assert r["success"] is True, r.get("error")
        body = json.dumps(r)
        assert "pct_change" in body and "delta" in body

    def test_the_plotted_values_are_the_real_sums(self, wide: Path, ad_data_full_csv: Path) -> None:
        import pandas as pd

        df = pd.read_csv(ad_data_full_csv, low_memory=False)
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        monthly = df.dropna(subset=["Date"]).set_index("Date").to_period("M")
        totals = monthly.groupby(monthly.index)[METRICS].sum()
        expected = {
            metric: float(totals.loc[[p for p in totals.index if str(p) == "2019-12"][0], metric])
            for metric in METRICS
        }
        data, _ = figure_of(wide)
        drawn = sorted(round(float(t["y"][0]), 2) for t in data if (t.get("x") or [None])[0] == "2019-12")
        assert len(drawn) == len(METRICS), drawn
        assert drawn == sorted(round(v, 2) for v in expected.values()), (drawn, expected)
