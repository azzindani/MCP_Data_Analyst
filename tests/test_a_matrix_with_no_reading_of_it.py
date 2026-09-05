"""576 correct numbers, and no answer.

The review's roadmap: *`insights.json` beside every matrix (correlation,
cross-tab, outliers, EDA)*. It sits under composability rather than truth, and
the distinction is the point -- nothing these tools return is wrong.

`id` and `member_id` correlated at 0.9936 in the review's file. The matrix said
so, correctly, among 576 other numbers. Nothing in the response said *these are
one column twice, drop one before modelling*, so the finding existed only for a
reader who already knew to look for it -- which is exactly the knowledge an
agent does not have and the reason it called the tool.

An insight here is a claim with its evidence attached, never a restatement: what
was measured, the threshold it crossed, and the call that acts on it. Severity
uses the same three levels as the data-quality alerts, so a reader does not have
to learn a second scale to combine them.

**The sidecar is deliberate, and does not contradict the stand-alone rule.**
That rule is about *deliverables*: an HTML page that renders as an empty box
without its sibling is broken, which is what the plotly-sidecar experiment
proved. `insights.json` is a second answer to the same call, not a part of the
first -- the insights are in the response too, so a caller who never opens the
file still has them.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_medium"), str(ROOT / "servers" / "data_advanced")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _adv_eda import run_eda  # noqa: E402
from _med_analysis import correlation_analysis  # noqa: E402
from _med_report import cross_tabulate  # noqa: E402

from shared.insights import (  # noqa: E402
    NOTABLE_R,
    REDUNDANT_R,
    from_correlations,
    from_crosstab,
    insight,
    insights_path,
    rank,
)


@pytest.fixture
def redundant(tmp_path, monkeypatch):
    """Two columns that are the same column twice, plus one that is not."""
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(4)
    n = 300
    base = rng.normal(size=n)
    p = tmp_path / "loans.csv"
    pd.DataFrame(
        {
            "id": range(n),
            "member_id": [i + 0.0001 * rng.normal() for i in range(n)],
            "amount": base * 100,
            "unrelated": rng.normal(size=n),
        }
    ).to_csv(p, index=False)
    return p


# ---------------------------------------------------------------------------
# the reading, not a restatement
# ---------------------------------------------------------------------------


def test_a_near_duplicate_pair_is_named_as_one(redundant):
    r = correlation_analysis(str(redundant), open_after=False)
    assert r["success"] is True
    kinds = {i["kind"] for i in r["insights"]}
    assert "redundant_pair" in kinds
    top = next(i for i in r["insights"] if i["kind"] == "redundant_pair")
    assert set(top["columns"]) == {"id", "member_id"}
    assert top["severity"] == "high"
    assert "one column twice" in top["headline"]


def test_the_insight_carries_the_number_it_was_derived_from(redundant):
    r = correlation_analysis(str(redundant), open_after=False)
    top = next(i for i in r["insights"] if i["kind"] == "redundant_pair")
    assert top["evidence"]["correlation"] >= REDUNDANT_R
    assert top["evidence"]["threshold"] == REDUNDANT_R


def test_the_insight_says_what_to_do_next(redundant):
    r = correlation_analysis(str(redundant), open_after=False)
    assert all(i.get("suggested_next") for i in r["insights"] if i["severity"] == "high")


def test_an_uncorrelated_frame_produces_no_findings(tmp_path, monkeypatch):
    """A tool that always finds something is a tool nobody reads."""
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(11)
    p = tmp_path / "noise.csv"
    pd.DataFrame({f"c{i}": rng.normal(size=400) for i in range(4)}).to_csv(p, index=False)
    r = correlation_analysis(str(p), open_after=False)
    assert r["insights"] == []


# ---------------------------------------------------------------------------
# the sidecar
# ---------------------------------------------------------------------------


def test_the_file_lands_beside_the_matrix(redundant):
    r = correlation_analysis(str(redundant), open_after=False)
    side = Path(r["insights_path"])
    assert side.exists()
    assert side.parent == Path(r["output_path"]).parent
    assert side.name.endswith("_insights.json")


def test_the_file_and_the_response_agree(redundant):
    """The response carries them too, so the file is never the only copy."""
    r = correlation_analysis(str(redundant), open_after=False)
    payload = json.loads(Path(r["insights_path"]).read_text(encoding="utf-8"))
    assert payload["insights"] == r["insights"]
    assert payload["op"] == "correlation_analysis"
    assert payload["insight_count"] == len(r["insights"])


def test_the_sidecar_counts_by_severity(redundant):
    r = correlation_analysis(str(redundant), open_after=False)
    payload = json.loads(Path(r["insights_path"]).read_text(encoding="utf-8"))
    assert payload["counts_by_severity"]["high"] >= 1


def test_the_path_is_derived_not_guessed():
    assert insights_path("/a/b/x_correlation.html").name == "x_correlation_insights.json"


# ---------------------------------------------------------------------------
# cross-tab: the arithmetic the table contains and does not do
# ---------------------------------------------------------------------------


def test_a_crosstab_cell_far_from_independence_is_named():
    table = {
        "A": {"paid": 900, "default": 100},
        "B": {"paid": 100, "default": 900},
    }
    found = from_crosstab(table, "grade", "status")
    assert found, "a 9:1 flip between rows is exactly what a reader wants pointed at"
    assert any("over-represented" in i["headline"] or "under-represented" in i["headline"] for i in found)
    assert all(i["columns"] == ["grade", "status"] for i in found)


def test_an_independent_table_says_nothing():
    table = {"A": {"x": 250, "y": 250}, "B": {"x": 250, "y": 250}}
    assert from_crosstab(table, "r", "c") == []


def test_cells_too_small_to_judge_are_left_alone():
    """An expected count under 5 makes the ratio noise."""
    table = {"A": {"x": 1, "y": 0}, "B": {"x": 0, "y": 1}}
    assert from_crosstab(table, "r", "c") == []


def test_crosstab_insights_reach_the_response(redundant, tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    p = tmp_path / "cat.csv"
    pd.DataFrame(
        {
            "grade": ["A"] * 1000 + ["B"] * 1000,
            "status": ["paid"] * 900 + ["default"] * 100 + ["paid"] * 100 + ["default"] * 900,
        }
    ).to_csv(p, index=False)
    r = cross_tabulate(str(p), row_column="grade", col_column="status", open_after=False)
    assert r["success"] is True
    assert r["insights"], r
    assert Path(r["insights_path"]).exists()


# ---------------------------------------------------------------------------
# EDA gathers all three onto one scale
# ---------------------------------------------------------------------------


def test_the_eda_report_carries_findings_from_every_section(redundant):
    r = run_eda(str(redundant), open_after=False)
    assert r["insights"], "an EDA with alerts and a 0.99 pair has findings"
    assert Path(r["insights_path"]).exists()
    severities = {i["severity"] for i in r["insights"]}
    assert severities <= {"high", "medium", "low"}, "one scale, not three"


def test_findings_are_ordered_worst_first(redundant):
    r = run_eda(str(redundant), open_after=False)
    order = {"high": 0, "medium": 1, "low": 2}
    got = [order[i["severity"]] for i in r["insights"]]
    assert got == sorted(got)


def test_a_minimal_run_writes_no_sidecar(redundant):
    """No page, no sidecar beside it -- and the insights are still returned."""
    r = run_eda(str(redundant), open_after=False, mode="minimal")
    assert r["insights_path"] == ""
    assert "insights" in r


# ---------------------------------------------------------------------------
# the shape itself
# ---------------------------------------------------------------------------


def test_an_unknown_severity_is_refused():
    with pytest.raises(ValueError) as exc:
        insight("k", "critical", "x")
    assert "high" in str(exc.value)


def test_rank_is_stable_within_a_severity():
    items = [insight("a", "low", "1"), insight("b", "high", "2"), insight("c", "low", "3")]
    assert [i["kind"] for i in rank(items)] == ["b", "a", "c"]


def test_a_pair_between_the_thresholds_is_medium_not_high():
    mid = (REDUNDANT_R + NOTABLE_R) / 2
    found = from_correlations([{"col_a": "a", "col_b": "b", "correlation": mid}])
    assert found and found[0]["severity"] == "medium"
    assert found[0]["kind"] == "strong_pair"
