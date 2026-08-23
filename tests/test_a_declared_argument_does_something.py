"""Three parameters the schema declared and the code never read.

Round 11 found five patch ops whose catalog entry advertised a field the
handler ignored. The tools outside the op arrays have the same failure mode
with a different shape: a parameter declared on the tool, forwarded by the
wrapper, and never referenced in the engine's body. `strict_args` guarantees a
declared name is *accepted*; it says nothing about whether the value goes
anywhere.

An AST walk over every @mcp.tool in the fleet found six, of which two were
internal helpers and one was in a sibling repo. The three here:

    statistical_test(correction)      never read
    statistical_test(posthoc)         read only by the ANOVA branch, and there
                                      it returned a stub
    compare_datasets(key_columns)     never read
    generate_dashboard(geo_file_path) never read

`posthoc=True` after a significant ANOVA returned:

    {"method": "Tukey HSD",
     "note": "Use scipy.stats.tukey_hsd for full pairwise comparisons."}

A method name implying a test had been run, and a note pointing at a function
the caller has no way to call -- on a server that exists because the caller
cannot run scipy. After a Kruskal-Wallis it did nothing at all.

`key_columns` is why "value changes" in compare_datasets' own docstring meant
column means: round 11 changed three cells in a 16,834-row copy and saw only
the two numeric means that moved. The edit to a text column was invisible.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_statistics"), str(ROOT / "servers" / "data_medium")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from servers.data_medium import engine as med  # noqa: E402
from servers.data_statistics import engine as stats  # noqa: E402


@pytest.fixture
def groups(tmp_path) -> str:
    rng = np.random.default_rng(3)
    rows = []
    for label, mu in (("a", 10), ("b", 12), ("c", 18), ("d", 18.2)):
        rows += [(label, float(v)) for v in rng.normal(mu, 2, 60)]
    p = tmp_path / "g.csv"
    pd.DataFrame(rows, columns=["grp", "val"]).to_csv(p, index=False)
    return str(p)


class TestPosthocRunsARealTest:
    def test_anova_returns_pairwise_comparisons(self, groups):
        r = stats.statistical_test(groups, test="anova", column_a="val", group_column="grp", posthoc=True)
        assert r["success"] is True, r.get("error")
        ph = r["posthoc"]
        assert ph["n_comparisons"] == 6, ph  # four groups -> 4C2
        assert len(ph["comparisons"]) == 6

    def test_it_no_longer_tells_the_caller_to_run_scipy(self, groups):
        r = stats.statistical_test(groups, test="anova", column_a="val", group_column="grp", posthoc=True)
        assert "note" not in r["posthoc"], r["posthoc"]

    def test_each_comparison_carries_a_p_value(self, groups):
        r = stats.statistical_test(groups, test="anova", column_a="val", group_column="grp", posthoc=True)
        for c in r["posthoc"]["comparisons"]:
            assert "p_value" in c and "significant" in c, c
            assert {"group_a", "group_b"} <= set(c)

    def test_the_pairs_match_scipy(self, groups):
        from scipy import stats as sp

        r = stats.statistical_test(groups, test="anova", column_a="val", group_column="grp", posthoc=True)
        frame = pd.read_csv(groups)
        labels = list(dict.fromkeys(frame["grp"]))
        data = [frame.loc[frame["grp"] == g, "val"].to_numpy() for g in labels]
        expected = sp.tukey_hsd(*data)
        for c in r["posthoc"]["comparisons"]:
            i, j = labels.index(c["group_a"]), labels.index(c["group_b"])
            assert c["p_value"] == pytest.approx(float(expected.pvalue[i][j]), abs=1e-6)

    def test_kruskal_gets_posthoc_too(self, groups):
        # The ANOVA branch was the only one reading posthoc at all.
        r = stats.statistical_test(groups, test="kruskal", column_a="val", group_column="grp", posthoc=True)
        assert r["success"] is True, r.get("error")
        assert r["posthoc"]["n_comparisons"] == 6, r["posthoc"]

    def test_without_posthoc_there_are_no_comparisons(self, groups):
        r = stats.statistical_test(groups, test="anova", column_a="val", group_column="grp")
        assert "posthoc" not in r


class TestCorrectionIsApplied:
    def test_bonferroni_multiplies_by_the_comparison_count(self, groups):
        raw = stats.statistical_test(
            groups, test="kruskal", column_a="val", group_column="grp", posthoc=True, correction="none"
        )["posthoc"]
        adj = stats.statistical_test(
            groups, test="kruskal", column_a="val", group_column="grp", posthoc=True, correction="bonferroni"
        )["posthoc"]
        m = adj["n_comparisons"]
        assert m == 6
        scaled = 0
        for a, b in zip(raw["comparisons"], adj["comparisons"]):
            # min(1, p*m), not p*m: one of these six pairs sits at p=0.513, and
            # 0.513*6 is 3.08. A p-value cannot exceed 1, and the clamp is why
            # a plain ratio assertion fails on that pair alone.
            expected = min(1.0, b["p_value_raw"] * m)
            # round_p keeps three significant figures below 1e-4, so a value
            # rounded before multiplication and one rounded after differ in the
            # last digit.
            assert b["p_value"] == pytest.approx(expected, rel=2e-2), (a, b)
            if expected < 1.0:
                scaled += 1
        assert scaled >= 5, "the fixture stopped exercising the un-clamped path"

    def test_a_large_p_value_is_clamped_at_one(self, groups):
        adj = stats.statistical_test(
            groups, test="kruskal", column_a="val", group_column="grp", posthoc=True, correction="bonferroni"
        )["posthoc"]
        assert all(c["p_value"] <= 1.0 for c in adj["comparisons"]), adj["comparisons"]
        assert any(c["p_value"] == 1.0 for c in adj["comparisons"]), "fixture no longer reaches the clamp"

    def test_holm_is_never_stricter_than_bonferroni(self, groups):
        holm = stats.statistical_test(
            groups, test="kruskal", column_a="val", group_column="grp", posthoc=True, correction="holm"
        )["posthoc"]
        bonf = stats.statistical_test(
            groups, test="kruskal", column_a="val", group_column="grp", posthoc=True, correction="bonferroni"
        )["posthoc"]
        for h, b in zip(holm["comparisons"], bonf["comparisons"]):
            assert h["p_value"] <= b["p_value"] + 1e-12, (h, b)

    def test_the_raw_p_value_is_kept_alongside(self, groups):
        r = stats.statistical_test(
            groups, test="kruskal", column_a="val", group_column="grp", posthoc=True, correction="bonferroni"
        )
        for c in r["posthoc"]["comparisons"]:
            assert c["p_value_raw"] <= c["p_value"] + 1e-12

    def test_the_correction_used_is_reported(self, groups):
        r = stats.statistical_test(
            groups, test="kruskal", column_a="val", group_column="grp", posthoc=True, correction="holm"
        )
        assert r["posthoc"]["correction"] == "holm"

    def test_tukey_says_it_corrects_itself(self, groups):
        # Stacking bonferroni on top of Tukey would be wrong, not redundant.
        r = stats.statistical_test(
            groups, test="anova", column_a="val", group_column="grp", posthoc=True, correction="bonferroni"
        )
        assert "tukey" in r["posthoc"]["correction"], r["posthoc"]["correction"]

    def test_an_unknown_correction_is_refused(self, groups):
        r = stats.statistical_test(
            groups, test="kruskal", column_a="val", group_column="grp", posthoc=True, correction="fdr"
        )
        assert r["success"] is False
        assert "bonferroni" in r["hint"], r["hint"]


class TestKeyColumnsFindsChangedRows:
    @pytest.fixture
    def pair(self, tmp_path):
        a = pd.DataFrame({"id": [1, 2, 3, 4], "name": ["a", "b", "c", "d"], "n": [10, 20, 30, 40]})
        b = a.copy()
        b.loc[1, "name"] = "B"
        b.loc[2, "n"] = 33
        b = b[b.id != 4]
        b = pd.concat([b, pd.DataFrame({"id": [5], "name": ["e"], "n": [50]})], ignore_index=True)
        fa, fb = tmp_path / "a.csv", tmp_path / "b.csv"
        a.to_csv(fa, index=False)
        b.to_csv(fb, index=False)
        return str(fa), str(fb)

    def test_a_text_edit_is_no_longer_invisible(self, pair):
        r = med.compare_datasets(*pair, key_columns=["id"])
        assert r["success"] is True, r.get("error")
        assert r["keyed_diff"]["changed_by_column"]["name"] == 1

    def test_added_and_removed_rows_are_counted(self, pair):
        k = med.compare_datasets(*pair, key_columns=["id"])["keyed_diff"]
        assert (k["rows_added"], k["rows_removed"], k["rows_changed"]) == (1, 1, 2)
        assert k["added_keys"] == [5] and k["removed_keys"] == [4]

    def test_the_before_and_after_values_are_reported(self, pair):
        k = med.compare_datasets(*pair, key_columns=["id"])["keyed_diff"]
        changes = {row["key"]: row["changes"] for row in k["changed_sample"]}
        assert changes[2]["name"] == {"a": "b", "b": "B"}
        assert changes[3]["n"] == {"a": 30, "b": 33}

    def test_numbers_stay_numbers(self, pair):
        # numpy scalars are not int/float, so a naive isinstance check turns 30
        # into "30" and a reader cannot tell a numeric change from a text one.
        k = med.compare_datasets(*pair, key_columns=["id"])["keyed_diff"]
        changes = {row["key"]: row["changes"] for row in k["changed_sample"]}
        assert isinstance(changes[3]["n"]["a"], int)

    def test_a_missing_key_column_is_refused(self, pair):
        r = med.compare_datasets(*pair, key_columns=["nope"])
        assert r["success"] is False
        assert "nope" in r["error"]

    def test_a_non_unique_key_is_refused(self, tmp_path):
        a = pd.DataFrame({"k": ["x", "x"], "v": [1, 2]})
        fa, fb = tmp_path / "a.csv", tmp_path / "b.csv"
        a.to_csv(fa, index=False)
        a.to_csv(fb, index=False)
        r = med.compare_datasets(str(fa), str(fb), key_columns=["k"])
        assert r["success"] is False
        assert "unique" in r["error"], r["error"]

    def test_identical_files_report_no_changes(self, tmp_path):
        a = pd.DataFrame({"id": [1, 2], "v": [1, 2]})
        fa, fb = tmp_path / "a.csv", tmp_path / "b.csv"
        a.to_csv(fa, index=False)
        a.to_csv(fb, index=False)
        k = med.compare_datasets(str(fa), str(fb), key_columns=["id"])["keyed_diff"]
        assert (k["rows_changed"], k["rows_added"], k["rows_removed"]) == (0, 0, 0)

    def test_a_null_on_both_sides_is_not_a_change(self, tmp_path):
        # NaN != NaN, so comparing without a null mask makes every null row a
        # change.
        a = pd.DataFrame({"id": [1, 2], "v": [None, 5.0]})
        fa, fb = tmp_path / "a.csv", tmp_path / "b.csv"
        a.to_csv(fa, index=False)
        a.to_csv(fb, index=False)
        k = med.compare_datasets(str(fa), str(fb), key_columns=["id"])["keyed_diff"]
        assert k["rows_changed"] == 0, k

    def test_without_key_columns_it_says_what_it_is_missing(self, pair):
        r = med.compare_datasets(*pair)
        assert "keyed_diff" not in r
        assert "key_columns" in r["hint"], r["hint"]


class TestAnUnreadArgumentIsRefused:
    def test_the_dashboard_refuses_an_external_geo_file(self, tmp_path):
        from servers.data_advanced import engine as adv

        csv = tmp_path / "d.csv"
        pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).to_csv(csv, index=False)
        r = adv.generate_dashboard(str(csv), geo_file_path=str(tmp_path / "regions.geojson"))
        assert r["success"] is False
        assert "enrich_with_geo" in r["hint"], r["hint"]

    def test_without_it_the_dashboard_still_builds(self, tmp_path):
        from servers.data_advanced import engine as adv

        csv = tmp_path / "d.csv"
        pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]}).to_csv(csv, index=False)
        r = adv.generate_dashboard(str(csv), output_path=str(tmp_path / "d.html"), open_after=False)
        assert r["success"] is True, r.get("error")
