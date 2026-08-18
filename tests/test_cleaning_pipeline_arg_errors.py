"""run_cleaning_pipeline reported a bare KeyError as its error message.

A coverage sweep drove every tool in the repo and hit this on the third of four
attempts at one call:

    {"success": false,
     "error": "Op 1 (fill_nulls): 'strategy'",
     "hint": "Restored from snapshot. Fix the op and retry.",
     "applied": 1}

The op name was validated against the handler map; its arguments were not. So a
missing required key travelled all the way into the handler, came out as
`KeyError('strategy')`, and was formatted with `str(exc)` -- which for a
KeyError is just the quoted key. Neither the error nor the hint says what is
missing or what a valid value would be, and the caller has to guess. Worse, by
the time it failed, op 0 had already been applied to the file and rolled back
from a snapshot.

`shared/patch_validator.validate_ops` already knew how to say this properly and
was already used by apply_patch and run_workspace_pipeline; this path just never
called it. Validation now happens before the file is opened.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from servers.data_medium.engine import run_cleaning_pipeline


@pytest.fixture()
def csv(tmp_path: Path) -> Path:
    p = tmp_path / "d.csv"
    pd.DataFrame({"a": [1.0, None, 3.0], "b": ["x", "y", None]}).to_csv(p, index=False)
    return p


def _original(path: Path) -> str:
    return path.read_text(encoding="utf-8")


class TestMissingRequiredArgs:
    def test_missing_strategy_names_the_key_not_a_keyerror(self, csv: Path):
        result = run_cleaning_pipeline(str(csv), [{"op": "fill_nulls", "column": "a"}])
        assert result["success"] is False
        assert "strategy" in result["error"]
        # The whole defect: the error used to be exactly "'strategy'".
        assert result["error"] != "Op 0 (fill_nulls): 'strategy'"
        assert "missing" in result["error"].lower()

    def test_the_error_lists_the_valid_strategies(self, csv: Path):
        result = run_cleaning_pipeline(str(csv), [{"op": "fill_nulls", "column": "a"}])
        for strategy in ("mean", "median", "mode", "ffill", "bfill", "drop"):
            assert strategy in result["error"]

    def test_missing_column_is_named_too(self, csv: Path):
        result = run_cleaning_pipeline(str(csv), [{"op": "fill_nulls", "strategy": "mean"}])
        assert result["success"] is False
        assert "column" in result["error"]

    def test_nothing_is_applied_or_rolled_back(self, csv: Path):
        """The bad op used to be caught only after an earlier op had been
        written and then restored from a snapshot."""
        before = _original(csv)
        result = run_cleaning_pipeline(
            str(csv),
            [
                {"op": "drop_duplicates"},
                {"op": "fill_nulls", "column": "a"},
            ],
        )
        assert result["success"] is False
        assert result["applied"] == 0
        assert _original(csv) == before

    def test_the_hint_says_the_file_is_untouched(self, csv: Path):
        result = run_cleaning_pipeline(str(csv), [{"op": "fill_nulls", "column": "a"}])
        assert "retry" in result["hint"].lower()

    def test_a_second_op_missing_args_is_reported_with_its_index(self, csv: Path):
        result = run_cleaning_pipeline(
            str(csv),
            [
                {"op": "fill_nulls", "column": "a", "strategy": "mean"},
                {"op": "cast_column", "column": "a"},
            ],
        )
        assert result["success"] is False
        assert "Op 1" in result["error"]


class TestInvalidValuesStillReportWell:
    def test_an_unknown_strategy_is_distinguished_from_a_missing_one(self, csv: Path):
        result = run_cleaning_pipeline(str(csv), [{"op": "fill_nulls", "column": "a", "strategy": "zero"}])
        assert result["success"] is False
        assert "zero" in result["error"]
        assert "missing" not in result["error"].lower()

    def test_unknown_op_names_still_report_first(self, csv: Path):
        result = run_cleaning_pipeline(str(csv), [{"op": "fillna", "column": "a"}])
        assert result["success"] is False
        assert "fillna" in result["error"]


class TestValidPipelinesStillRun:
    def test_a_correct_pipeline_applies(self, csv: Path):
        result = run_cleaning_pipeline(
            str(csv),
            [{"op": "fill_nulls", "column": "a", "strategy": "median"}],
        )
        assert result["success"] is True
        assert pd.read_csv(csv)["a"].isna().sum() == 0

    def test_dry_run_is_unaffected(self, csv: Path):
        before = _original(csv)
        result = run_cleaning_pipeline(
            str(csv),
            [{"op": "fill_nulls", "column": "a", "strategy": "median"}],
            dry_run=True,
        )
        assert result["success"] is True
        assert _original(csv) == before
