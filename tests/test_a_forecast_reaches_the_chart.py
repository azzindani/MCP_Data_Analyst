"""The chart showed the history and left the forecast out of it.

time_series_analysis computes a three-period exponential-smoothing forecast,
returns it in `forecast_values` and `forecast_dates`, writes an HTML chart, and
then says:

    "HTML chart saved — open output_path for the full visualization."

The chart plotted `resampled[col]` and nothing else. Round 11 read the file back
and found the ten months of history embedded in it and none of the forecast
values or dates -- so the tool's own hint pointed at a file that was missing the
part the caller asked the tool for.

The forecast trace now continues from the last observed point, dashed and in the
same colour as the series it extends, so it reads as a continuation rather than
a second unrelated line.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_medium")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from servers.data_medium import engine  # noqa: E402


@pytest.fixture
def series(tmp_path) -> Path:
    days = pd.date_range("2023-01-01", periods=400, freq="D")
    frame = pd.DataFrame(
        {
            "Date": days.astype(str),
            "a": [100 + i for i in range(400)],
            "b": [50 + (i % 30) for i in range(400)],
        }
    )
    p = tmp_path / "ts.csv"
    frame.to_csv(p, index=False)
    return p


@pytest.fixture
def rendered(series, tmp_path):
    out = tmp_path / "chart.html"
    r = engine.time_series_analysis(
        str(series),
        date_column="Date",
        value_columns=["a", "b"],
        period="M",
        output_path=str(out),
        open_after=False,
    )
    assert r["success"] is True, r.get("error")
    return r, Path(r["output_path"]).read_text(encoding="utf-8")


class TestTheForecastIsInTheFile:
    def test_every_forecast_value_appears(self, rendered):
        r, html = rendered
        assert r["forecast_values"], "fixture produced no forecast"
        for col, values in r["forecast_values"].items():
            assert str(values[0]) in html, f"{col}: {values[0]} missing from the chart"

    def test_every_forecast_date_appears(self, rendered):
        r, html = rendered
        for col, dates in r["forecast_dates"].items():
            assert dates[0] in html, f"{col}: {dates[0]} missing from the chart"

    def test_the_forecast_has_its_own_named_trace(self, rendered):
        r, html = rendered
        for col in r["value_columns"]:
            assert f"{col} (forecast)" in html, col

    def test_it_is_drawn_as_a_dashed_line(self, rendered):
        _, html = rendered
        assert "dash" in html


class TestTheHistoryIsStillThere:
    def test_the_observed_periods_survive(self, rendered):
        r, html = rendered
        assert r["total_periods"] > 0
        assert "lines+markers" in html

    def test_both_series_are_plotted(self, rendered):
        r, html = rendered
        for col in r["value_columns"]:
            assert f'"name":"{col}"' in html or f'"name": "{col}"' in html, col


class TestTheForecastJoinsTheLine:
    def test_it_starts_at_the_last_observed_point(self, series, tmp_path):
        # A forecast drawn from its own first period leaves a visible gap; this
        # one is prefixed with the last real point so the dashed segment
        # continues the solid one.
        out = tmp_path / "c.html"
        r = engine.time_series_analysis(
            str(series),
            date_column="Date",
            value_columns=["a"],
            period="M",
            output_path=str(out),
            open_after=False,
        )
        html = Path(r["output_path"]).read_text(encoding="utf-8")
        frame = pd.read_csv(series, parse_dates=["Date"]).set_index("Date")
        last = float(frame["a"].resample("ME").sum().dropna().iloc[-1])
        assert str(last) in html or str(int(last)) in html, last


class TestTheHintMatchesTheFile:
    def test_it_no_longer_promises_more_than_it_draws(self, rendered):
        r, _ = rendered
        assert "forecast" in r["hint"], r["hint"]
