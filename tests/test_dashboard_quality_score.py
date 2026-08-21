"""The dashboard's headline score contradicted the panel directly beneath it.

Rendered from the real 16k-row ad dataset, the dashboard read:

    QUALITY SCORE  100
    DATA QUALITY - 15 ALERTS, 2 SERIOUS

Both were computed from the same frame. The score was

    quality = max(0, round(100 - null_pct * 2 - dup_pct * 0.5))

so it saw only nulls and duplicates. Everything the alert panel had already
found -- constant columns, 89.6% zero-inflation, skewness of +17, outliers at
13% -- cost nothing. After a cleaning pass dropped the duplicates and imputed
the nulls, the number went to a flat 100 while fifteen real problems sat listed
under it. The headline is the part people read.

The score now prices in the same alerts the panel renders, so the two cannot
disagree. The alerts are computed once, before the KPI row, and passed to both.

Also here: the numeric filter placeholders carry each column's range and were
clipped mid-number ("Max (67,4") by a fixed 5.5rem input, which is a hint the
reader cannot finish.
"""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


def _quality_score(*a):
    """Imported lazily so the rendered-page tests below fail on their own
    assertions rather than on a collection error when the helper is absent."""
    from servers.data_advanced._adv_dashboard import _quality_score as f

    return f(*a)


def _compact_num(*a):
    from servers.data_advanced._adv_dashboard import _compact_num as f

    return f(*a)


class TestTheScoreSeesTheAlerts:
    def test_a_spotless_frame_still_scores_100(self):
        assert _quality_score(0.0, 0.0, []) == 100

    def test_one_warning_costs_something(self):
        assert _quality_score(0.0, 0.0, [{"sev": "warning"}]) < 100

    def test_a_serious_alert_costs_more_than_a_warning(self):
        warn = 100 - _quality_score(0.0, 0.0, [{"sev": "warning"}])
        err = 100 - _quality_score(0.0, 0.0, [{"sev": "error"}])
        assert err > warn

    def test_the_real_case_no_longer_reads_100(self):
        """15 alerts, 2 serious, on a frame with no nulls or duplicates."""
        alerts = [{"sev": "error"}] * 2 + [{"sev": "warning"}] * 13
        assert _quality_score(0.0, 0.0, alerts) < 80

    def test_nulls_and_duplicates_still_count(self):
        assert _quality_score(10.0, 0.0, []) < 100
        assert _quality_score(0.0, 20.0, []) < 100

    def test_it_never_goes_below_zero(self):
        assert _quality_score(100.0, 100.0, [{"sev": "error"}] * 50) == 0


class TestCompactNumbersFitTheirInput:
    @pytest.mark.parametrize(
        "value,expected",
        [
            (0.0, "0"),
            (0.5, "0.5"),
            (999, "999"),
            (1075, "1.1K"),
            (9222, "9.2K"),
            (67454, "67K"),
            (4847190, "4.8M"),
            (1.5e9, "1.5B"),
            (-67454, "-67K"),
        ],
    )
    def test_formatting(self, value: float, expected: str):
        assert _compact_num(value) == expected

    @pytest.mark.parametrize("value", [0.0, 0.5, 999, 9222, 67454, 2503115.19, 4847190, 1.5e9])
    def test_short_enough_to_render(self, value: float):
        """ "Max (67,454)" is 11 characters and was clipped. Keep the whole
        placeholder inside a narrow input."""
        assert len(f"Max ({_compact_num(value)})") <= 10


class TestTheRenderedDashboardAgrees:
    @pytest.fixture()
    def dashboard(self, tmp_path: Path) -> str:
        from servers.data_advanced.engine import generate_dashboard

        rng = np.random.default_rng(0)
        n = 200
        csv = tmp_path / "messy.csv"
        pd.DataFrame(
            {
                "Date": pd.date_range("2026-01-01", periods=n).astype(str),
                # constant column -> a serious alert, and no nulls or dupes
                "product": ["only"] * n,
                "spends": np.concatenate([rng.integers(1, 100, n - 4), [90000] * 4]).astype(float),
                "clicks": np.concatenate([np.zeros(int(n * 0.9)), rng.integers(1, 50, n - int(n * 0.9))]).astype(float),
                "cat": rng.choice(list("abc"), n),
            }
        ).to_csv(csv, index=False)
        out = tmp_path / "dash.html"
        result = generate_dashboard(str(csv), output_path=str(out), open_after=False)
        assert result["success"] is True, result.get("error")
        return out.read_text(encoding="utf-8")

    def _score(self, page: str) -> int:
        m = re.search(r'<div class="kpi-val"[^>]*>(\d+)</div><div class="kpi-lbl">Quality Score', page)
        assert m, "could not find the quality score in the rendered dashboard"
        return int(m.group(1))

    def _alert_count(self, page: str) -> int:
        m = re.search(r"Data quality — (\d+) alert", page)
        return int(m.group(1)) if m else 0

    def test_the_fixture_actually_raises_alerts(self, dashboard: str):
        """Otherwise the next assertion passes for the wrong reason."""
        assert self._alert_count(dashboard) > 0

    def test_a_flawed_frame_does_not_score_100(self, dashboard: str):
        assert self._score(dashboard) < 100

    def test_alerts_and_score_move_together(self, dashboard: str):
        alerts = self._alert_count(dashboard)
        assert self._score(dashboard) <= 100 - 3 * alerts + 1

    def test_the_placeholders_are_not_long_numbers(self, dashboard: str):
        for ph in re.findall(r'placeholder="M(?:in|ax) \(([^)]*)\)"', dashboard):
            assert len(ph) <= 6, f"placeholder {ph!r} will clip inside the input"


class TestTheInputCanUseItsColumn:
    def test_the_fixed_width_is_gone(self):
        from shared.html_layout import css_dashboard
        from shared.html_theme import css_vars

        css = css_dashboard(css_vars("light"))
        assert ".ninp{width:5.5rem" not in css
        assert ".ninp{flex:1 1 0;min-width:0" in css
