"""A t-test says which groups it compared, their means, and which one is higher.

The sweep ran a t-test of clicks by campaign_platform on Ad_Data.csv through
both tools and got `statistic: -33.2, Means differ significantly` -- no group
names, no means, no direction. The sign followed whichever group the file
listed first, so it could not be read, and which platform gets more clicks
was the question. It was Student's equal-variance t on groups whose spreads
differ seven-fold and sizes nine-fold; Welch's t is the test for that, and
now the default, named in the answer. The statistics tier also compared the
first two of three groups without a word, and answered with no `op` and an
interpretation that read "Reject H0: Reject H0 (...)".

The data here has that shape: a small, spread-out, high-mean group listed
second, and a large, tight, low-mean group listed first.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from scipy import stats as scipy_stats

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_statistics"), str(ROOT / "servers" / "data_medium")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import _med_analysis  # noqa: E402
import _stats_tests  # noqa: E402

rng = np.random.default_rng(7)
LOW = rng.normal(8, 2, 300)  # "Google": many rows, tight, low
HIGH = rng.normal(40, 30, 30)  # "Facebook": few rows, wide, high


def _welch() -> float:
    return float(scipy_stats.ttest_ind(LOW, HIGH, equal_var=False).statistic)


def _student() -> float:
    return float(scipy_stats.ttest_ind(LOW, HIGH).statistic)


@pytest.fixture
def grouped(tmp_path) -> str:
    frame = pd.DataFrame(
        {
            "platform": ["Google"] * len(LOW) + ["Facebook"] * len(HIGH),
            "clicks": np.concatenate([LOW, HIGH]),
            "tier": (["x", "y", "z"] * 110),
        }
    )
    path = tmp_path / "ads.csv"
    frame.to_csv(path, index=False)
    return str(path)


@pytest.fixture
def two_columns(tmp_path) -> str:
    frame = pd.DataFrame({"before": LOW[:30], "after": HIGH})
    path = tmp_path / "pair.csv"
    frame.to_csv(path, index=False)
    return str(path)


def _statistics(path: str, **kw) -> dict:
    return _stats_tests.statistical_test(path, test="t_test", **kw)


def _medium(path: str, **kw) -> dict:
    return _med_analysis.statistical_tests(path, test_type="ttest", **kw)


BOTH = [pytest.param(_statistics, id="statistical_test"), pytest.param(_medium, id="statistical_tests")]


class TestTheGroupsAreNamed:
    @pytest.mark.parametrize("run", BOTH)
    def test_each_group_has_its_name_size_and_mean(self, grouped, run):
        result = run(grouped, column_a="clicks", group_column="platform")
        assert result["success"] is True, result
        by_name = {g["name"]: g for g in result["groups"]}
        assert set(by_name) == {"Google", "Facebook"}
        assert by_name["Google"]["n"] == 300 and by_name["Facebook"]["n"] == 30
        assert by_name["Facebook"]["mean"] == pytest.approx(HIGH.mean(), abs=1e-3)

    @pytest.mark.parametrize("run", BOTH)
    def test_the_answer_says_which_is_higher(self, grouped, run):
        result = run(grouped, column_a="clicks", group_column="platform")
        assert result["direction"].startswith("'Facebook' has the higher mean")
        assert "'Facebook' has the higher mean" in result["interpretation"]

    @pytest.mark.parametrize("run", BOTH)
    def test_the_sign_is_tied_to_the_first_group(self, grouped, run):
        result = run(grouped, column_a="clicks", group_column="platform")
        first = result["groups"][0]["name"]
        assert f"positive when '{first}', the first group, is higher" in result["direction"]
        assert (result["statistic"] > 0) == (result["groups"][0]["mean"] > result["groups"][1]["mean"])

    @pytest.mark.parametrize("run", BOTH)
    def test_two_columns_are_named_by_column(self, two_columns, run):
        result = run(two_columns, column_a="before", column_b="after")
        assert [g["name"] for g in result["groups"]] == ["before", "after"]
        assert result["direction"].startswith("'after' has the higher mean")


class TestItIsWelchAndSaysSo:
    @pytest.mark.parametrize("run", BOTH)
    def test_the_statistic_is_welchs_not_students(self, grouped, run):
        result = run(grouped, column_a="clicks", group_column="platform")
        assert result["statistic"] == pytest.approx(_welch(), abs=1e-3)
        assert abs(_welch() - _student()) > 1  # the data is chosen so the two disagree

    def test_the_statistics_tier_names_the_method(self, grouped):
        assert "Welch" in _statistics(grouped, column_a="clicks", group_column="platform")["method"]

    def test_the_medium_tier_names_the_test(self, grouped):
        assert "Welch" in _medium(grouped, column_a="clicks", group_column="platform")["test"]


class TestTwoGroupsOrRefuse:
    @pytest.mark.parametrize("run", BOTH)
    def test_three_groups_are_refused_by_name(self, grouped, run):
        result = run(grouped, column_a="clicks", group_column="tier")
        assert result["success"] is False
        assert "'tier' has 3: 'x', 'y', 'z'" in result["error"]
        assert "anova" in result["hint"]


class TestTheContract:
    def test_statistical_test_answers_with_op(self, grouped):
        assert _statistics(grouped, column_a="clicks", group_column="platform")["op"] == "statistical_test"

    def test_the_verdict_is_said_once(self, grouped):
        text = _statistics(grouped, column_a="clicks", group_column="platform")["interpretation"]
        assert text.startswith("Reject H0 (p=") and text.count("H0") == 1

    def test_a_test_is_not_sent_to_the_cleaning_tools(self, grouped):
        hint = _medium(grouped, column_a="clicks", group_column="platform")["hint"]
        assert "apply_patch" not in hint and "cleaning" not in hint
