"""Two reports, one dataset, two verdicts 57 points apart.

Rendered from the real 16k-row ad file, the EDA report read:

    Alerts (16)
    ...
    QUALITY SCORE  98

while the dashboard, from the same frame and the same compute_alerts() call,
showed 41. The EDA score was

    100 - null_penalty - dup_penalty * 0.5 - outlier_penalty * 0.3

computed one line *before* the alerts existed, so constant columns, 89.7%
zero-inflation, skewness of +17 and imbalance cost it nothing. The dashboard had
already been fixed to price alerts in; the EDA report had not, and nothing tied
the two together, so they drifted.

The scorer now lives beside compute_alerts in shared/data_alerts.py and both
call it. Outliers are no longer charged separately -- they arrive as alerts, and
the old outlier_penalty term was double-counting them.

Also here: table cells wrapped inside words. The data-sample table squeezed
"Performance" into "Perfor / manc / e" and "2019-10-16" into "2019 / -10- / 16",
because the base CSS set word-break:break-word on everything. Cells now wrap
between words only; the table grows to its natural width and the overflow-x
wrapper it already sat inside does the scrolling.
"""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from shared.data_alerts import alerts_for_frame, quality_score


@pytest.fixture()
def flawed_csv(tmp_path: Path) -> Path:
    """No nulls and no duplicates, but plainly unhealthy: the case where a
    nulls-and-duplicates score reports a clean bill of health."""
    rng = np.random.default_rng(7)
    n = 900
    p = tmp_path / "flawed.csv"
    pd.DataFrame(
        {
            "product": ["Product 1"] * n,  # constant
            "phase": ["Performance"] * n,  # constant
            "channel": rng.choice(["Search", "Social"], n),
            "spends": np.concatenate([rng.gamma(1.2, 50, n - 30), rng.uniform(4e4, 9e4, 30)]),  # skew + outliers
            "link_clicks": np.where(rng.random(n) < 0.9, 0, rng.integers(1, 40, n)),  # zero-inflated
        }
    ).drop_duplicates().to_csv(p, index=False)
    return p


class TestTheScorerSeesWhatThePanelReports:
    def test_alerts_cost_something(self):
        clean = quality_score(0.0, 0.0, [])
        flagged = quality_score(0.0, 0.0, [{"sev": "error", "type": "CONSTANT", "msg": "x"}])
        assert flagged < clean

    def test_a_serious_alert_costs_more_than_a_warning(self):
        error = quality_score(0.0, 0.0, [{"sev": "error"}])
        warning = quality_score(0.0, 0.0, [{"sev": "warning"}])
        assert error < warning

    def test_a_spotless_frame_still_scores_100(self):
        assert quality_score(0.0, 0.0, []) == 100

    def test_the_score_never_goes_below_zero(self):
        assert quality_score(100.0, 100.0, [{"sev": "error"}] * 50) == 0

    def test_a_flawed_frame_is_not_called_healthy(self, flawed_csv: Path):
        df = pd.read_csv(flawed_csv)
        alerts = alerts_for_frame(df, ["spends", "link_clicks"], ["product", "phase", "channel"])
        assert alerts, "fixture is meant to trip alerts"
        # 80 is the boundary the UI paints green; a frame like this must not reach it.
        assert quality_score(0.0, 0.0, alerts) < 80


class TestBothReportsGiveTheSameVerdict:
    """The bug was not the formula, it was having two of them."""

    def test_the_eda_report_and_the_dashboard_agree(self, flawed_csv: Path, tmp_path: Path):
        from servers.data_advanced.engine import generate_dashboard, run_eda

        eda = run_eda(str(flawed_csv), output_path=str(tmp_path / "e.html"), open_after=False)
        assert eda["success"] is True, eda.get("error")

        dash_out = tmp_path / "d.html"
        dash = generate_dashboard(str(flawed_csv), output_path=str(dash_out), open_after=False)
        assert dash["success"] is True, dash.get("error")

        page = dash_out.read_text(encoding="utf-8")
        m = re.search(r'kpi-val[^>]*>(\d+)</div><div class="kpi-lbl">Quality Score', page)
        assert m, "dashboard did not render a quality score"
        assert int(m.group(1)) == eda["quality_score"]

    def test_the_eda_score_reflects_its_own_alert_count(self, flawed_csv: Path, tmp_path: Path):
        from servers.data_advanced.engine import run_eda

        out = tmp_path / "e.html"
        r = run_eda(str(flawed_csv), output_path=str(out), open_after=False)
        page = out.read_text(encoding="utf-8")
        n_alerts = int(re.search(r"Alerts \((\d+)\)", page).group(1))
        assert n_alerts > 0, "fixture is meant to trip alerts"
        assert r["quality_score"] < 100, f"{n_alerts} alerts but a perfect score"


class TestTableCellsDoNotBreakInsideWords:
    def test_the_base_css_keeps_words_whole_in_cells(self):
        from shared.html_layout import css_report

        css = css_report("")
        assert "word-break:normal" in css

    def test_a_long_category_value_is_not_hyphenated_into_the_markup(self, flawed_csv: Path, tmp_path: Path):
        """The break was visual, not textual, so assert on the rule that caused
        it rather than on the rendered glyphs."""
        from servers.data_advanced.engine import run_eda

        out = tmp_path / "e.html"
        run_eda(str(flawed_csv), output_path=str(out), open_after=False)
        page = out.read_text(encoding="utf-8")
        cell_rule = re.search(r"th,td\{[^}]*\}", page)
        assert cell_rule, "cell rule missing from the page"
        assert "word-break:normal" in cell_rule.group(0)
        assert "overflow-wrap:break-word" not in cell_rule.group(0)

    def test_wide_tables_still_have_something_to_scroll_in(self, flawed_csv: Path, tmp_path: Path):
        """Keeping words whole only works because the table can overflow a
        scrolling parent instead of being squeezed."""
        from servers.data_advanced.engine import run_eda

        out = tmp_path / "e.html"
        run_eda(str(flawed_csv), output_path=str(out), open_after=False)
        page = out.read_text(encoding="utf-8")
        i = page.find('id="sample"')
        assert "overflow-x:auto" in page[i : i + 400]
