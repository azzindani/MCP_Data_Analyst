"""Two things a response said that were not true of the run it described.

**A p-value of zero.** Seventeen call sites did round(float(p), 6), so anything
below 5e-7 came back as 0.0. A sweep running a t-test on the reference dataset
got "p_value": 0.0 where the value is about 1.8e-58 -- a number the test cannot
produce, which reads as a failed computation as readily as an extreme result,
and which throws away the difference between p=1e-8 and p=1e-58.

**A hint about the wrong thing.** Every tool ends in `except Exception` with one
domain-specific hint. A PermissionError writing a chart into a scratch directory
was answered with "Check date_column is a datetime column and value_columns are
numeric", and cost a diagnostic detour. The hint now matches the exception and
falls back to the domain text, so nothing is lost where the guess was right.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.file_utils import hint_for_error  # noqa: E402
from shared.stats_format import format_p, round_p  # noqa: E402

FIXTURE = ROOT / "tests" / "fixtures" / "ad_data_full.csv"


@pytest.fixture
def csv(tmp_path: Path) -> str:
    dst = tmp_path / "data.csv"
    shutil.copy2(FIXTURE, dst)
    return str(dst)


class TestAPValueIsNeverFlattenedToZero:
    @pytest.mark.parametrize("p", [1.8e-58, 1e-8, 4.9e-7, 1e-300])
    def test_a_tiny_p_survives(self, p: float):
        assert round_p(p) != 0.0, p
        assert round_p(p) == pytest.approx(p, rel=1e-2)

    @pytest.mark.parametrize("p", [0.05, 0.032451, 0.5, 1.0])
    def test_an_ordinary_p_keeps_its_old_form(self, p: float):
        assert round_p(p) == round(p, 6)

    def test_a_true_zero_and_a_nan_are_handled(self):
        assert round_p(0.0) == 0.0
        assert round_p(float("nan")) is None
        assert round_p(None) is None

    def test_the_text_form_matches(self):
        assert format_p(1.8e-58) == "1.8e-58"
        assert format_p(0.05) == "0.0500"
        assert format_p(None) == "n/a"

    def test_a_real_test_reports_a_real_p(self, csv):
        from servers.data_statistics.engine import statistical_test

        r = statistical_test(csv, test="t_test", column_a="spends", column_b="impressions")
        assert r["success"] is True, r.get("error")
        p = r["p_value"]
        assert p is None or p > 0.0, f"p_value came back as {p}"

    def test_the_interpretation_does_not_print_zero(self, csv):
        from servers.data_statistics.engine import statistical_test

        r = statistical_test(csv, test="t_test", column_a="spends", column_b="impressions")
        text = " ".join(f"{m.get('msg', '')} {m.get('detail', '')}" for m in r.get("progress", []))
        assert "p=0.0000" not in text, text


class TestAHintMatchesWhatWentWrong:
    def test_a_permission_error_talks_about_permissions(self):
        hint = hint_for_error(PermissionError(13, "Permission denied"), "Check date_column is a datetime column.")
        assert "ermission" in hint
        assert "date_column" not in hint

    def test_a_missing_file_talks_about_the_path(self):
        hint = hint_for_error(FileNotFoundError(2, "No such file"), "Check date_column.")
        assert "path" in hint.lower()

    def test_an_unrecognised_error_keeps_the_domain_hint(self):
        fallback = "Check date_column is a datetime column and value_columns are numeric."
        assert hint_for_error(ValueError("something else"), fallback) == fallback

    def test_the_catch_alls_route_through_it(self):
        """Every rewritten site must sit inside a handler that binds `exc`."""
        import ast

        outside = []
        for p in sorted((ROOT / "servers").rglob("*.py")):
            src = p.read_text(encoding="utf-8")
            if "hint_for_error(exc" not in src:
                continue
            tree = ast.parse(src)
            bound = [
                (n.lineno, n.end_lineno) for n in ast.walk(tree) if isinstance(n, ast.ExceptHandler) and n.name == "exc"
            ]
            for i, line in enumerate(src.splitlines(), start=1):
                if "hint_for_error(exc" in line and not any(a <= i <= b for a, b in bound):
                    outside.append(f"{p}:{i}")
        assert not outside, outside

    def test_a_real_permission_failure_says_so(self, csv, tmp_path):
        from servers.data_medium.engine import time_series_analysis

        locked = tmp_path / "locked"
        locked.mkdir()
        locked.chmod(0o500)
        try:
            r = time_series_analysis(
                csv,
                date_column="Date",
                value_columns=["spends"],
                output_path=str(locked / "out.html"),
                open_after=False,
            )
            if r["success"] is False and "ermission" in str(r.get("error", "")):
                assert "ermission" in r["hint"], r["hint"]
                assert "date_column" not in r["hint"], r["hint"]
        finally:
            locked.chmod(0o700)
