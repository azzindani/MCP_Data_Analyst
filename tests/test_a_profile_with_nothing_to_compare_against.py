""" "EDA without target/comparison is a toy" — the review's own sentence.

It took the point from sweetviz and stated the fix in one line:

    Target-aware + compare, never vacuum: `analyze(df, target_feat)` +
    `compare(train,test)` side-by-side. MCP: every profiler takes
    `target_column="loan_status"` + `compare_to="chargedoff.csv"`.

A profile of 24 columns says what is in each one. It does not say which three
matter for the thing the caller came to predict, or which four have moved since
the model was trained -- and those are the two questions that make a profile
worth running rather than a set of histograms worth scrolling.

Two properties the tests below pin:

* **Every association names its measure.** An AUC of 0.75 and a Cramer's V of
  0.75 are not the same claim, and a ranked list is an invitation to compare
  them as though they were.
* **A column that could not be measured is listed with a reason, not dropped.**
  A column missing from a ranking reads as "unrelated", which is a claim nobody
  made.

`compare_to` also closes something that had been open since it was written:
`shared/quality.py` reports `drift: None` with "no baseline supplied; pass
compare_to to measure drift". There was no `compare_to` to pass. Now there is,
and the fourth component is a number.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_advanced")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _adv_eda import run_eda  # noqa: E402

from shared.association import MEASURES, compare_frames, target_association  # noqa: E402


@pytest.fixture
def loans(tmp_path, monkeypatch):
    """One strong predictor, one weak, one categorical, one useless."""
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(9)
    n = 600
    default = rng.random(n) < 0.3
    p = tmp_path / "loans.csv"
    pd.DataFrame(
        {
            "status": np.where(default, "default", "paid"),
            "score": np.where(default, rng.normal(560, 40, n), rng.normal(720, 40, n)),
            "noise": rng.normal(size=n),
            "grade": np.where(default, rng.choice(["D", "E"], n), rng.choice(["A", "B"], n)),
        }
    ).to_csv(p, index=False)
    return p


# ---------------------------------------------------------------------------
# target_column
# ---------------------------------------------------------------------------


def test_the_strong_predictor_ranks_above_the_noise(loans):
    r = run_eda(str(loans), open_after=False, mode="minimal", target_column="status")
    assert r["success"] is True
    ranked = [a["column"] for a in r["target_association"]]
    assert ranked.index("score") < ranked.index("noise")


def test_every_association_names_its_measure(loans):
    """0.75 under two statistics is two different claims."""
    r = run_eda(str(loans), open_after=False, mode="minimal", target_column="status")
    for a in r["target_association"]:
        assert a["measure"] in MEASURES or a["strength"] is None, a
        if a["strength"] is not None:
            assert a["measure_note"], a


def test_a_categorical_feature_gets_a_categorical_measure(loans):
    r = run_eda(str(loans), open_after=False, mode="minimal", target_column="status")
    grade = next(a for a in r["target_association"] if a["column"] == "grade")
    assert grade["measure"] == "cramers_v"
    assert grade["strength"] > 0.5, "grade is nearly determined by status in this fixture"


def test_a_numeric_feature_against_a_binary_target_uses_auc(loans):
    r = run_eda(str(loans), open_after=False, mode="minimal", target_column="status")
    score = next(a for a in r["target_association"] if a["column"] == "score")
    assert score["measure"] == "auc"
    assert 0.5 <= score["strength"] <= 1.0


def test_an_unmeasurable_column_is_listed_with_a_reason(tmp_path, monkeypatch):
    """Dropping it would read as "unrelated", which nobody measured."""
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    df = pd.DataFrame({"y": [0, 1] * 50, "constant": ["x"] * 100})
    rows = target_association(df, "y")
    const = next(r for r in rows if r["column"] == "constant")
    assert const["strength"] is None
    assert "not computable" in const["note"]


def test_unmeasured_columns_sort_last(loans):
    r = run_eda(str(loans), open_after=False, mode="minimal", target_column="status")
    strengths = [a["strength"] for a in r["target_association"]]
    seen_none = False
    for s in strengths:
        if s is None:
            seen_none = True
        else:
            assert not seen_none, "a measured column after an unmeasured one"


def test_a_missing_target_is_refused_with_the_column_list(loans):
    r = run_eda(str(loans), open_after=False, mode="minimal", target_column="nope")
    assert r["success"] is False
    assert "nope" in r["error"]
    assert "status" in r["hint"]


def test_no_target_means_no_field(loans):
    r = run_eda(str(loans), open_after=False, mode="minimal")
    assert "target_association" not in r, "an empty list reads as 'measured, found nothing'"


# ---------------------------------------------------------------------------
# compare_to
# ---------------------------------------------------------------------------


@pytest.fixture
def shifted(tmp_path, loans, monkeypatch):
    """The same schema, one column moved a long way."""
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    base = pd.read_csv(loans)
    moved = base.copy()
    moved["score"] = moved["score"] + 250  # a shift no one could miss
    p = tmp_path / "loans_later.csv"
    moved.to_csv(p, index=False)
    return p


def test_a_moved_column_is_named(loans, shifted):
    r = run_eda(str(shifted), open_after=False, mode="minimal", compare_to=str(loans))
    assert r["success"] is True
    top = r["comparison"]["drift"][0]
    assert top["column"] == "score"
    assert top["measure"] == "psi"
    assert top["drift"] >= 0.25
    assert "major shift" in top["reading"]


def test_a_schema_change_is_reported(loans, tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    base = pd.read_csv(loans)
    changed = base.drop(columns=["noise"]).assign(new_col=1.0)
    p = tmp_path / "changed.csv"
    changed.to_csv(p, index=False)
    r = run_eda(str(p), open_after=False, mode="minimal", compare_to=str(loans))
    assert r["comparison"]["columns_removed"] == ["noise"]
    assert r["comparison"]["columns_added"] == ["new_col"]


def test_comparing_a_file_to_itself_finds_no_drift(loans):
    r = run_eda(str(loans), open_after=False, mode="minimal", compare_to=str(loans))
    assert r["comparison"]["columns_drifted"] == 0
    assert r["comparison"]["drift_pct"] == 0.0


def test_a_missing_baseline_is_refused(loans, tmp_path):
    r = run_eda(str(loans), open_after=False, mode="minimal", compare_to=str(tmp_path / "nope.csv"))
    assert r["success"] is False
    assert "compare_to" in r["error"]


def test_the_fourth_quality_component_becomes_a_number(loans, shifted):
    """It has been `None` with "pass compare_to" since quality.py was written."""
    without = run_eda(str(shifted), open_after=False, mode="minimal")
    assert without["quality_breakdown"]["components"]["drift"] is None
    assert "drift_note" in without["quality_breakdown"]

    with_base = run_eda(str(shifted), open_after=False, mode="minimal", compare_to=str(loans))
    assert with_base["quality_breakdown"]["components"]["drift"] is not None


def test_no_baseline_means_no_comparison_field(loans):
    r = run_eda(str(loans), open_after=False, mode="minimal")
    assert "comparison" not in r


# ---------------------------------------------------------------------------
# the helper
# ---------------------------------------------------------------------------


def test_drift_pct_counts_only_what_was_measured():
    """A column that could not be compared is not quietly counted as stable."""
    rng = np.random.default_rng(2)
    base = pd.DataFrame({"a": rng.normal(size=200), "tiny": [1, 2] * 100})
    cur = pd.DataFrame({"a": rng.normal(size=200) + 5, "tiny": [1, 2] * 100})
    out = compare_frames(base, cur)
    measured = [d for d in out["drift"] if d["drift"] is not None]
    assert out["drift_pct"] == round(out["columns_drifted"] / len(measured) * 100, 2)
    assert "rather than counted as stable" in out["drift_note"]
