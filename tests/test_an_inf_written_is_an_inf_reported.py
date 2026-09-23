"""A formula that writes +/-inf says so.

The sweep's `column_math round(clicks/impressions*100, 2)` on Ad_Data.csv came
back `null_count: 0` with 4 rows of inf in the file, and `spends/clicks`
`null_count: 4104` with 426 inf unmentioned. inf is not missing -- it is a
value, and every mean, sum and chart built on the column inherits it -- so a
result that reports only nulls calls the column clean. read_column_stats
already counted non-finite values; the patch result did not.

Both tools that run patch ops (apply_patch and run_cleaning_pipeline) share
the handlers and now share the count. The advice in the warning is tested
too: following it must leave no inf.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_basic"), str(ROOT / "servers" / "data_medium")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import _med_transform  # noqa: E402

from servers.data_basic.engine import apply_patch  # noqa: E402

DIVIDE = {"op": "column_math", "formula": "spends / clicks", "target_column": "cpc"}


@pytest.fixture
def csv(tmp_path) -> str:
    # clicks: two zeros under a nonzero spend (inf), one 0/0 (null), the rest fine.
    frame = pd.DataFrame({"spends": [10.0, 5.0, 0.0, 8.0, 6.0], "clicks": [2, 0, 0, 0, 3]})
    path = tmp_path / "ads.csv"
    frame.to_csv(path, index=False)
    return str(path)


def _patch(path: str, *ops: dict) -> dict:
    return apply_patch(path, list(ops))


def _pipeline(path: str, *ops: dict) -> dict:
    return _med_transform.run_cleaning_pipeline(path, list(ops))


def _results(answer: dict) -> list[dict]:
    return answer.get("results") or answer.get("summary") or []


BOTH = [pytest.param(_patch, id="apply_patch"), pytest.param(_pipeline, id="run_cleaning_pipeline")]


class TestTheCountIsThere:
    @pytest.mark.parametrize("run", BOTH)
    def test_inf_is_counted_apart_from_nulls(self, csv, run):
        answer = run(csv, DIVIDE)
        assert answer["success"] is True, answer
        result = _results(answer)[0]
        assert result["null_count"] == 1
        assert result["non_finite_count"] == 2

    @pytest.mark.parametrize("run", BOTH)
    def test_the_warning_names_the_column(self, csv, run):
        result = _results(run(csv, DIVIDE))[0]
        assert "2 row(s) of 'cpc' are +/-inf" in result["warning"]

    @pytest.mark.parametrize("run", BOTH)
    def test_the_count_matches_the_file(self, csv, run):
        run(csv, DIVIDE)
        written = pd.read_csv(csv)["cpc"]
        assert int(np.isinf(written).sum()) == 2

    def test_a_clean_column_carries_zero_and_no_warning(self, csv):
        result = _results(_patch(csv, {"op": "column_math", "formula": "spends * 2", "target_column": "double"}))[0]
        assert result["non_finite_count"] == 0 and "warning" not in result


class TestTheAdviceWorks:
    def test_the_guard_the_warning_names_writes_a_null_not_inf(self, csv):
        guarded = {"op": "column_math", "formula": "spends / clicks if clicks != 0 else None", "target_column": "cpc"}
        result = _results(_patch(csv, guarded))[0]
        assert "warning" not in result
        written = pd.to_numeric(pd.read_csv(csv)["cpc"], errors="coerce")
        assert int(np.isinf(written).sum()) == 0 and int(written.isna().sum()) == 3
