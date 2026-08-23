"""Two wrong numbers, both deterministic, both under success: true.

**The correlation.** A paired test compares row i of one column against row i
of the other, so both series have to come out of the same rows. The code
dropped each column's nulls separately and then cut both to the shorter
length:

    a = to_numeric(df[column_a]).dropna()
    b = to_numeric(df[column_b]).dropna()
    n = min(len(a), len(b))
    pearsonr(a.iloc[:n], b.iloc[:n])

That looks equivalent and is not. After the first null every pair is offset by
one, and the offset grows with each null after it. On the reference dataset,
clicks against link_clicks -- 546 nulls in 16,834 rows, the first at row 2,011:

    as written           r = 0.0015   p = 0.847257   "not significant"
    pairwise deletion    r = 0.9256   p < 1e-300     n = 16,288

A near-perfect correlation reported as no correlation at all. The p-value is
the tell: 0.847 is what r=0.0015 gives at n=16,288, so the number is internally
consistent and gives a reader nothing to be suspicious of.

Three sites had it -- the correlation test, the wilcoxon test, and the
auto-scan's top_correlations, which is worse again because it truncated each
series to the *other* column's length.

**The percentiles.** extended_stats built its keys as `f"p{int(p)}"` and
divided by 100, so the argument had to be whole percentages. Asked for
pandas' spelling, [0.25, 0.5, 0.75], int() made all three keys "p0" -- one
surviving entry holding the 0.75th percentile -- while percentiles_computed
echoed the request back as though all three had been honoured.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from scipy import stats as scipy_stats

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_medium")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from servers.data_medium import engine  # noqa: E402
from shared.column_utils import paired_numeric  # noqa: E402


@pytest.fixture
def gapped(tmp_path) -> Path:
    """b tracks a closely, but 200 of its values are missing.

    The nulls start a fifth of the way in, so a positional cut misaligns the
    tail exactly the way the reference dataset does.
    """
    rng = np.random.default_rng(11)
    n = 1200
    a = rng.normal(50, 12, n)
    b = a * 1.8 + rng.normal(0, 2, n)
    b[300:500] = np.nan
    p = tmp_path / "g.csv"
    pd.DataFrame({"a": a, "b": b}).to_csv(p, index=False)
    return p


def expected_r(path: Path) -> float:
    frame = pd.read_csv(path)[["a", "b"]].dropna()
    return float(scipy_stats.pearsonr(frame["a"], frame["b"])[0])


class TestTheCorrelationUsesMatchingRows:
    def test_it_matches_a_pairwise_recompute(self, gapped):
        r = engine.statistical_tests(str(gapped), test_type="correlation", column_a="a", column_b="b")
        assert r["success"] is True, r.get("error")
        assert r["statistic"] == pytest.approx(expected_r(gapped), abs=5e-4)

    def test_the_correlation_is_still_found(self, gapped):
        # The whole failure mode is a strong correlation reading as none.
        r = engine.statistical_tests(str(gapped), test_type="correlation", column_a="a", column_b="b")
        assert r["statistic"] > 0.9, r["statistic"]
        assert r["significant"] is True

    def test_it_says_how_many_pairs_it_used(self, gapped):
        r = engine.statistical_tests(str(gapped), test_type="correlation", column_a="a", column_b="b")
        assert r["n"] == 1000, r
        assert r["rows_dropped"] == 200, r

    def test_a_column_with_no_nulls_is_unaffected(self, tmp_path):
        rng = np.random.default_rng(5)
        a = rng.normal(0, 1, 400)
        b = a * 3 + rng.normal(0, 0.5, 400)
        p = tmp_path / "clean.csv"
        pd.DataFrame({"a": a, "b": b}).to_csv(p, index=False)
        r = engine.statistical_tests(str(p), test_type="correlation", column_a="a", column_b="b")
        assert r["statistic"] == pytest.approx(expected_r(p), abs=5e-4)
        assert r["rows_dropped"] == 0


class TestTheAutoScanUsesMatchingRows:
    def test_the_scanned_correlation_matches(self, gapped):
        r = engine.statistical_tests(str(gapped))
        assert r["success"] is True, r.get("error")
        pair = next(c for c in r["top_correlations"] if {c["col_a"], c["col_b"]} == {"a", "b"})
        assert pair["r"] == pytest.approx(expected_r(gapped), abs=5e-3)

    def test_the_scan_reports_its_pair_count(self, gapped):
        r = engine.statistical_tests(str(gapped))
        pair = next(c for c in r["top_correlations"] if {c["col_a"], c["col_b"]} == {"a", "b"})
        assert pair["n"] == 1000, pair


class TestTheWilcoxonUsesMatchingRows:
    def test_it_matches_a_pairwise_recompute(self, gapped):
        r = engine.statistical_tests(str(gapped), test_type="wilcoxon", column_a="a", column_b="b")
        assert r["success"] is True, r.get("error")
        frame = pd.read_csv(gapped)[["a", "b"]].dropna()
        stat, _ = scipy_stats.wilcoxon(frame["a"], frame["b"])
        assert r["statistic"] == pytest.approx(float(stat), rel=1e-6)


class TestTheHelperPairsByRow:
    def test_it_drops_a_row_either_column_is_null_in(self):
        df = pd.DataFrame({"a": [1, 2, None, 4], "b": [10, None, 30, 40]})
        a, b = paired_numeric(df, "a", "b")
        assert list(a) == [1.0, 4.0]
        assert list(b) == [10.0, 40.0]

    def test_the_two_series_stay_the_same_length(self):
        df = pd.DataFrame({"a": [1, 2, 3, 4, 5], "b": [None, 2, None, 4, None]})
        a, b = paired_numeric(df, "a", "b")
        assert len(a) == len(b) == 2

    def test_non_numeric_text_is_dropped_as_a_pair(self):
        df = pd.DataFrame({"a": ["1", "oops", "3"], "b": ["10", "20", "30"]})
        a, b = paired_numeric(df, "a", "b")
        assert list(a) == [1.0, 3.0]
        assert list(b) == [10.0, 30.0]


class TestPercentilesAreHonoured:
    @pytest.fixture
    def numbers(self, tmp_path) -> Path:
        p = tmp_path / "n.csv"
        pd.DataFrame({"v": list(range(1, 101))}).to_csv(p, index=False)
        return p

    def test_fractions_are_read_as_fractions(self, numbers):
        r = engine.extended_stats(str(numbers), columns=["v"], percentiles=[0.25, 0.5, 0.75])
        assert r["success"] is True, r.get("error")
        assert sorted(r["stats"]["v"]["percentiles"]) == ["p25", "p50", "p75"]

    def test_percentages_still_work(self, numbers):
        r = engine.extended_stats(str(numbers), columns=["v"], percentiles=[25, 50, 75])
        assert sorted(r["stats"]["v"]["percentiles"]) == ["p25", "p50", "p75"]

    def test_both_spellings_give_the_same_numbers(self, numbers):
        a = engine.extended_stats(str(numbers), columns=["v"], percentiles=[0.25, 0.5, 0.75])
        b = engine.extended_stats(str(numbers), columns=["v"], percentiles=[25, 50, 75])
        assert a["stats"]["v"]["percentiles"] == b["stats"]["v"]["percentiles"]

    def test_the_values_match_pandas(self, numbers):
        r = engine.extended_stats(str(numbers), columns=["v"], percentiles=[0.25, 0.5, 0.75])
        s = pd.read_csv(numbers)["v"]
        got = r["stats"]["v"]["percentiles"]
        assert got["p25"] == pytest.approx(float(s.quantile(0.25)), abs=1e-4)
        assert got["p50"] == pytest.approx(float(s.quantile(0.50)), abs=1e-4)
        assert got["p75"] == pytest.approx(float(s.quantile(0.75)), abs=1e-4)

    def test_what_it_echoes_back_is_what_it_computed(self, numbers):
        r = engine.extended_stats(str(numbers), columns=["v"], percentiles=[0.25, 0.5, 0.75])
        keys = set(r["stats"]["v"]["percentiles"])
        assert {f"p{p:g}" for p in r["percentiles_computed"]} == keys

    def test_a_fractional_percentage_keeps_its_own_key(self, numbers):
        # 2.5 and 25 collapsed to the same name under int().
        r = engine.extended_stats(str(numbers), columns=["v"], percentiles=[2.5, 25])
        assert sorted(r["stats"]["v"]["percentiles"]) == ["p2.5", "p25"]

    def test_the_default_is_unchanged(self, numbers):
        r = engine.extended_stats(str(numbers), columns=["v"])
        assert sorted(r["stats"]["v"]["percentiles"]) == [
            "p10",
            "p25",
            "p5",
            "p50",
            "p75",
            "p90",
            "p95",
            "p99",
        ]

    def test_an_out_of_range_percentile_is_refused(self, numbers):
        r = engine.extended_stats(str(numbers), columns=["v"], percentiles=[150])
        assert r["success"] is False
        assert "150" in r["error"], r["error"]
