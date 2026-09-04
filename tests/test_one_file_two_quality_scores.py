"""Two servers scored one file 77 and 53, and neither said what it measured.

A user review ran `run_eda` and `check_data_quality` over the same 38,576-row
credit file. They flagged the same four issues -- a constant column, a
28,000-value identifier, income skew of 31.07, a pair correlated at 0.9936 --
and reported 77 and 53. Neither response published a denominator, so an agent
handing one to the next step could not reconcile them.

Neither formula was wrong on its own terms; they were different terms. What
makes it a defect rather than a difference of opinion is that both had already
been fixed once for disagreeing with a sibling. This repo's docstring records
"the dashboard said 41, the EDA report said 98"; the ML repo's records "the
sibling report in MCP_Data_Analyst scored the same file 41". Each was repaired
locally, in its own repo, and the two went on disagreeing across the boundary.

`shared/quality.py` is byte-identical in both repos, and the score now arrives
with its parts. `drift` is reported as None rather than invented: it needs a
baseline, and a component scored 100 because nothing was measured would be the
same class of falsehood.
"""

from __future__ import annotations

import pytest

from shared.quality import COMPONENTS, WEIGHTS, quality_report, quality_score, severity_of

# --------------------------------------------------------------------------
# the two vocabularies
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "alert, expected",
    [
        ({"sev": "error"}, "high"),
        ({"severity": "high"}, "high"),
        ({"sev": "warning"}, "medium"),
        ({"severity": "medium"}, "medium"),
        ({"sev": "info"}, "low"),
        ({"severity": "low"}, "low"),
        ({}, "low"),
        ({"sev": "something new"}, "low"),
    ],
)
def test_both_repos_severity_spellings_are_understood(alert, expected):
    """One repo writes `sev`, the other `severity`, with different words."""
    assert severity_of(alert) == expected


def test_the_same_alerts_score_the_same_whichever_vocabulary_they_use():
    """The heart of it: the two servers must agree on identical findings."""
    as_data_analyst = [{"sev": "error"}, {"sev": "error"}, {"sev": "warning"}, {"sev": "warning"}]
    as_machine_learning = [{"severity": "high"}, {"severity": "high"}, {"severity": "medium"}, {"severity": "medium"}]
    assert quality_score(3.73, 0.0, as_data_analyst) == quality_score(3.73, 0.0, as_machine_learning)


# --------------------------------------------------------------------------
# the breakdown
# --------------------------------------------------------------------------


def test_the_score_arrives_with_its_parts():
    report = quality_report(10.0, 0.0, [{"sev": "error"}])
    assert set(report["components"]) == set(COMPONENTS)
    assert report["weights"] == WEIGHTS
    assert report["alert_counts"] == {"high": 1, "medium": 0, "low": 0}


def test_the_parts_say_which_problem_it_is():
    """53 from missing values and 53 from a constant column want opposite fixes."""
    missing = quality_report(40.0, 0.0, [])
    alerting = quality_report(0.0, 0.0, [{"sev": "error"}] * 5)

    assert missing["components"]["completeness"] < 50
    assert missing["components"]["validity"] == 100.0
    assert alerting["components"]["completeness"] == 100.0
    assert alerting["components"]["validity"] < 50


def test_drift_is_reported_as_unmeasured_not_as_perfect():
    report = quality_report(0.0, 0.0, [])
    assert report["components"]["drift"] is None
    assert "compare_to" in report["drift_note"]
    # And a component that was not measured must not lift the headline.
    assert report["quality_score"] == 100.0


def test_a_baseline_turns_drift_on():
    report = quality_report(0.0, 0.0, [], has_baseline=True, drift_pct=20.0)
    assert report["components"]["drift"] == 80.0
    assert "drift_note" not in report


def test_the_floor_is_zero_and_reachable():
    report = quality_report(100.0, 100.0, [{"sev": "error"}] * 20)
    assert report["quality_score"] == 0.0
    assert all(v == 0.0 for k, v in report["components"].items() if v is not None)


def test_a_clean_frame_scores_100():
    assert quality_report(0.0, 0.0, [])["quality_score"] == 100.0


def test_the_weights_are_published_and_sum_to_one():
    """A score whose weights are private is one a caller cannot argue with."""
    assert abs(sum(WEIGHTS.values()) - 1.0) < 1e-9


# --------------------------------------------------------------------------
# the repo boundary
# --------------------------------------------------------------------------


def test_the_module_is_byte_identical_in_both_repos():
    """The whole point. A local copy is how the two drifted the first time."""
    import hashlib
    import pathlib

    here = pathlib.Path(__file__).resolve().parents[1] / "shared/quality.py"
    sibling = pathlib.Path("/root/MCP_Machine_Learning/shared/quality.py")
    if not sibling.exists():
        pytest.skip("sibling repo not present in this checkout")
    assert hashlib.sha256(here.read_bytes()).hexdigest() == hashlib.sha256(sibling.read_bytes()).hexdigest()


def test_this_repos_public_name_still_works_and_delegates():
    """Eighteen call sites import `data_alerts.quality_score`; they must not break."""
    from shared.data_alerts import quality_score as legacy

    alerts = [{"sev": "error"}, {"sev": "warning"}]
    assert legacy(5.0, 1.0, alerts) == int(round(quality_score(5.0, 1.0, alerts)))
