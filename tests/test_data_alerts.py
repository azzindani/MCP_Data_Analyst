"""The alert panel is the most useful thing this repo produces — it says what is
wrong with the data rather than restating it. It lived inside the EDA page
builder, so the interactive dashboard, which is the artifact people actually
send to a colleague, showed 26 charts of a dataset without mentioning that two
of its columns were constant.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from shared.data_alerts import alerts_for_frame, alerts_html, compute_alerts

try:
    from servers.data_advanced.engine import generate_dashboard, run_eda

    HAS_ADVANCED = True
except ImportError:
    HAS_ADVANCED = False


@pytest.fixture()
def messy() -> pd.DataFrame:
    n = 200
    return pd.DataFrame(
        {
            "constant": ["only"] * n,
            "dominant": ["yes"] * (n - 3) + ["no"] * 3,
            "mostly_zero": [0] * (n - 10) + list(range(10)),
            "identifier": [f"id-{i}" for i in range(n)],
            "clean": list(range(n)),
        }
    )


def _types(alerts: list[dict]) -> set[str]:
    return {a["type"] for a in alerts}


class TestAlertEngine:
    def test_constant_column(self, messy):
        assert "CONSTANT" in _types(alerts_for_frame(messy, ["mostly_zero", "clean"], ["constant", "dominant"]))

    def test_imbalanced_category(self, messy):
        assert "IMBALANCED" in _types(alerts_for_frame(messy, ["mostly_zero"], ["dominant"]))

    def test_zero_inflated_numeric(self, messy):
        assert "ZEROS" in _types(alerts_for_frame(messy, ["mostly_zero"], []))

    def test_identifier_like_column(self, messy):
        assert "HIGH CARDINALITY" in _types(alerts_for_frame(messy, [], ["identifier"]))

    def test_duplicates_are_counted(self):
        df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
        assert "DUPLICATES" in _types(alerts_for_frame(df, ["a"], ["b"]))

    def test_high_correlation_is_found_without_a_prepared_table(self):
        """alerts_for_frame works out the correlation pairs itself, so a caller
        that has not built one still gets the multicollinearity alert."""
        df = pd.DataFrame({"a": list(range(50)), "b": [2 * i + 1 for i in range(50)]})
        assert "HIGH CORR" in _types(alerts_for_frame(df, ["a", "b"], []))

    def test_a_clean_frame_raises_nothing(self):
        df = pd.DataFrame({"a": [1, 2, 3, 4, 5, 6, 7, 8], "b": [8, 3, 5, 1, 9, 2, 7, 4]})
        assert alerts_for_frame(df, ["a", "b"], []) == []

    def test_severity_is_set_on_every_alert(self, messy):
        alerts = alerts_for_frame(messy, ["mostly_zero", "clean"], ["constant", "dominant", "identifier"])
        assert alerts
        assert all(a["sev"] in ("error", "warning", "info") for a in alerts)

    def test_compute_alerts_still_takes_a_prepared_table(self):
        df = pd.DataFrame({"a": [1, 1, 1, 1]})
        alerts = compute_alerts(df, ["a"], [], [], 4, 0)
        assert "CONSTANT" in _types(alerts)


class TestAlertMarkup:
    def test_empty_panel_says_so(self):
        assert "No data quality alerts" in alerts_html([])

    def test_badges_carry_the_severity_class(self):
        html = alerts_html([{"col": "a", "type": "CONSTANT", "sev": "error", "msg": "x"}])
        assert 'class="alert-item error"' in html
        assert 'class="alert-badge error"' in html

    def test_messages_are_escaped(self):
        html = alerts_html([{"col": None, "type": "X", "sev": "warning", "msg": "<script>bad()</script>"}])
        assert "<script>" not in html


@pytest.mark.skipif(not HAS_ADVANCED, reason="data_advanced deps unavailable")
class TestDashboardCarriesTheSameJudgement:
    @pytest.fixture()
    def messy_csv(self, tmp_path: Path, messy: pd.DataFrame) -> Path:
        csv = tmp_path / "messy.csv"
        messy.to_csv(csv, index=False)
        return csv

    def test_dashboard_shows_the_alerts(self, messy_csv, tmp_path):
        out = tmp_path / "dash.html"
        result = generate_dashboard(str(messy_csv), output_path=str(out), open_after=False)
        assert result["success"] is True
        page = out.read_text(encoding="utf-8")
        assert "Data quality" in page
        assert "alert-item" in page
        assert "CONSTANT" in page

    def test_the_same_wording_as_the_eda_report(self, messy_csv, tmp_path):
        """One implementation, so the two artifacts cannot say different things
        about the same dataset."""
        import re

        def messages(page: Path) -> set[str]:
            html = page.read_text(encoding="utf-8")
            return set(re.findall(r'<span class="alert-badge \w+">[^<]+</span> ([^<]+)', html))

        dash = tmp_path / "dash.html"
        eda = tmp_path / "eda.html"
        generate_dashboard(str(messy_csv), output_path=str(dash), open_after=False)
        run_eda(str(messy_csv), output_path=str(eda), open_after=False)

        from_dash, from_eda = messages(dash), messages(eda)
        assert from_dash
        assert from_dash <= from_eda

    def test_a_hostile_column_name_cannot_inject(self, tmp_path):
        """The old EDA path interpolated column names into the panel unescaped."""
        csv = tmp_path / "hostile.csv"
        pd.DataFrame({"<script>alert(1)</script>": ["same"] * 20, "n": range(20)}).to_csv(csv, index=False)
        out = tmp_path / "dash.html"
        generate_dashboard(str(csv), output_path=str(out), open_after=False)
        page = out.read_text(encoding="utf-8")
        assert "<script>alert(1)</script>" not in page

    def test_a_clean_dataset_gets_no_panel(self, tmp_path):
        csv = tmp_path / "clean.csv"
        pd.DataFrame({"a": range(40), "b": [i % 7 for i in range(40)]}).to_csv(csv, index=False)
        out = tmp_path / "dash.html"
        generate_dashboard(str(csv), output_path=str(out), open_after=False)
        assert "Data quality" not in out.read_text(encoding="utf-8")
