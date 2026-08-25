"""The rest of the n=1 fixes, found by re-running the phases they touched.

Three tools were left behind by the first pass and one shared module carried the
same defect across a repo boundary:

  generate_correlation_heatmap  drew an all-NaN grid and reported success in
                                silence -- a blank matrix reads as measured
                                absence of correlation
  generate_pairwise_plot        same, for a scatter matrix of one point
  compute_alerts                raised a CONSTANT error for every column of a
                                one-row frame, 8 points each, so a clean file
                                with no nulls and no duplicates scored 0/100
                                under a panel reading "no issues"

generate_distribution_plot got its warning in the first pass and its two
siblings in the same file did not, which is the shape technique 5 exists to
catch. compute_alerts is ml-medium's check_data_quality defect, fixed there
earlier the same day, in the Data_Analyst module nobody thought to check.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_advanced")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _adv_charts import generate_correlation_heatmap, generate_pairwise_plot  # noqa: E402
from _adv_eda import run_eda  # noqa: E402

from shared.data_alerts import compute_alerts, quality_score  # noqa: E402


def _csv(tmp_path, n_rows: int) -> Path:
    f = tmp_path / f"rows_{n_rows}.csv"
    rows = "\n".join(f"West,{i * 10},{i * 3}" for i in range(1, n_rows + 1))
    f.write_text(f"region,spend,clicks\n{rows}\n", encoding="utf-8")
    return f


# --- the two chart siblings -------------------------------------------------


@pytest.mark.parametrize("fn", [generate_correlation_heatmap, generate_pairwise_plot])
def test_a_relationship_chart_of_one_row_says_so(tmp_path, fn):
    r = fn(str(_csv(tmp_path, 1)), output_path=str(tmp_path / "c.html"), open_after=False)
    assert r["success"] is True
    assert r["rows_used"] == 1
    assert "hint" in r, r
    assert "1 row" in r["hint"]
    warnings = [p for p in r["progress"] if p.get("status") == "warn"]
    assert any("Too few rows" in p["message"] for p in warnings), warnings


@pytest.mark.parametrize("fn", [generate_correlation_heatmap, generate_pairwise_plot])
def test_a_real_sample_draws_without_comment(tmp_path, fn):
    r = fn(str(_csv(tmp_path, 12)), output_path=str(tmp_path / "c.html"), open_after=False)
    assert r["success"] is True
    assert r["rows_used"] == 12
    assert "hint" not in r or "row(s)" not in r["hint"]
    assert not any("Too few rows" in p["message"] for p in r["progress"] if p.get("status") == "warn")


# --- the shared alert module ------------------------------------------------


def _alerts(df: pd.DataFrame) -> list[dict]:
    num = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    cat = [c for c in df.columns if c not in num]
    return compute_alerts(df, num, cat, [], len(df), 0)


def test_one_row_raises_no_constant_alerts():
    df = pd.DataFrame([{"a": "x", "b": 1, "c": 2.5}])
    types = {a["type"] for a in _alerts(df)}
    assert "CONSTANT" not in types
    assert "IMBALANCED" not in types


def test_a_genuinely_constant_column_is_still_an_error():
    df = pd.DataFrame({"flag": ["X"] * 6, "n": range(6)})
    constant = [a for a in _alerts(df) if a["type"] == "CONSTANT"]
    assert [a["col"] for a in constant] == ["flag"]
    assert constant[0]["sev"] == "error"


def test_an_all_null_column_is_flagged_at_any_row_count():
    """nunique == 0 is about the data, not the row count -- it survives n=1."""
    df = pd.DataFrame([{"a": "x", "empty": None}])
    types = {(a["type"], a["col"]) for a in _alerts(df)}
    assert ("ALL NULL", "empty") in types


def test_a_clean_single_row_does_not_score_zero(tmp_path):
    """The whole point: 0/100 for a file with nothing wrong with it."""
    r = run_eda(str(_csv(tmp_path, 1)), output_path=str(tmp_path / "eda.html"), open_after=False)
    assert r["success"] is True
    assert r["rows"] == 1
    assert r["null_summary"] == {}
    assert r["duplicate_rows"] == 0
    assert r["quality_score"] > 80, r["quality_score"]


def test_the_score_still_falls_for_real_problems():
    df = pd.DataFrame({"mostly_null": [None] * 9 + [1], "flat": ["X"] * 10, "ok": range(10)})
    alerts = _alerts(df)
    assert quality_score(null_pct=30.0, dup_pct=0.0, alerts=alerts) < 60
