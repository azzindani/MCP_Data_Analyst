"""Three tools reported success while doing nothing the caller asked for.

Found by a full-coverage sweep of every tool in this repo. All three returned
success:true, so nothing short of checking the filesystem or the resulting frame
would have caught them.

- `regression_analysis` and `period_comparison` both declared `output_path`,
  threaded it down into the engine, and never referenced it again. Neither
  module contained any file-writing code. The caller asked for a report, got
  full statistics back, and no file -- and since the response never echoed
  `output_path` either, nothing in the result said the artifact was missing.

- `fill_nulls` matched its strategy through an if/elif chain with no else, so an
  unrecognised strategy fell through every branch and returned {"filled": 0}.
  That reads as "nothing needed filling". `run_workspace_pipeline` validates
  against the same allow-list and rejects it up front, so the identical op dict
  was rejected in one pipeline and silently ignored in the other.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

_REPO = Path(__file__).resolve().parents[1]
for _p in (str(_REPO / "servers" / "data_statistics"), str(_REPO / "servers" / "data_medium")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from servers.data_basic._patch_ops import _op_fill_nulls  # noqa: E402


@pytest.fixture()
def numeric_csv(tmp_path: Path) -> Path:
    path = tmp_path / "n.csv"
    pd.DataFrame(
        {
            "y": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "a": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "b": [5, 3, 8, 1, 9, 2, 7, 4, 6, 0],
        }
    ).to_csv(path, index=False)
    return path


class TestRegressionWritesWhatItPromises:
    def test_output_path_produces_a_file(self, numeric_csv, tmp_path):
        from _stats_regression import regression_analysis  # type: ignore[import]

        out = tmp_path / "reg.html"
        result = regression_analysis(str(numeric_csv), "y", ["a", "b"], output_path=str(out))

        assert result["success"] is True
        assert out.exists(), "output_path was accepted and silently ignored"
        assert out.stat().st_size > 0

    def test_the_written_path_is_reported_back(self, numeric_csv, tmp_path):
        """Without this the caller cannot tell a written file from a dropped one."""
        from _stats_regression import regression_analysis  # type: ignore[import]

        out = tmp_path / "reg.html"
        result = regression_analysis(str(numeric_csv), "y", ["a", "b"], output_path=str(out))
        assert result.get("output_path")

    def test_no_output_path_still_writes_nothing(self, numeric_csv, tmp_path):
        from _stats_regression import regression_analysis  # type: ignore[import]

        result = regression_analysis(str(numeric_csv), "y", ["a", "b"])
        assert result["success"] is True
        assert "output_path" not in result
        assert list(tmp_path.glob("*.html")) == []

    def test_the_chart_carries_the_fitted_coefficients(self, numeric_csv, tmp_path):
        """The chart must not be able to disagree with the returned numbers."""
        from _stats_regression import regression_analysis  # type: ignore[import]

        out = tmp_path / "reg.html"
        result = regression_analysis(str(numeric_csv), "y", ["a", "b"], output_path=str(out))
        page = out.read_text(encoding="utf-8")
        for name in result["coefficients"]:
            assert name in page


class TestPeriodComparisonWritesWhatItPromises:
    @pytest.fixture()
    def dated_csv(self, tmp_path: Path) -> Path:
        path = tmp_path / "d.csv"
        dates = pd.date_range("2024-01-01", periods=120, freq="D")
        pd.DataFrame({"when": dates, "revenue": range(120), "units": range(120, 240)}).to_csv(path, index=False)
        return path

    def test_output_path_produces_a_file(self, dated_csv, tmp_path):
        from _stats_comparative import period_comparison  # type: ignore[import]

        out = tmp_path / "cmp.html"
        result = period_comparison(str(dated_csv), "when", ["revenue", "units"], "M", output_path=str(out))

        assert result["success"] is True
        assert out.exists(), "output_path was accepted and silently ignored"
        assert result.get("output_path")


class TestFillNullsRefusesWhatItCannotDo:
    def test_a_valid_strategy_still_fills(self):
        df = pd.DataFrame({"x": [1.0, None, 3.0]})
        _, report = _op_fill_nulls(df.copy(), {"column": "x", "strategy": "median"})
        assert report["filled"] == 1
        assert report["value_used"] == 2.0

    def test_an_unknown_strategy_raises_instead_of_reporting_zero_filled(self):
        df = pd.DataFrame({"x": [1.0, None, 3.0]})
        with pytest.raises(ValueError) as excinfo:
            _op_fill_nulls(df.copy(), {"column": "x", "strategy": "zero"})
        assert "zero" in str(excinfo.value)

    def test_the_error_lists_the_strategies_that_do_work(self):
        df = pd.DataFrame({"x": [1.0, None, 3.0]})
        with pytest.raises(ValueError) as excinfo:
            _op_fill_nulls(df.copy(), {"column": "x", "strategy": "nonsense"})
        message = str(excinfo.value)
        for strategy in ("mean", "median", "mode", "ffill", "bfill", "drop"):
            assert strategy in message

    def test_the_two_pipelines_now_agree(self):
        """The validator rejected 'zero' while the executor ignored it. Whatever
        one refuses, the other must refuse too."""
        from shared.patch_validator import validate_ops

        op = {"op": "fill_nulls", "column": "x", "strategy": "zero"}
        assert validate_ops([op]), "validator should reject it"
        with pytest.raises(ValueError):
            _op_fill_nulls(pd.DataFrame({"x": [1.0, None]}), op)
