"""`method="zscore"` found no outliers in a column holding 2,178.

Round 27 shipped `enforce_known_arguments`, which refuses an argument NAME the
tool does not declare. It cannot see a wrong VALUE on a parameter whose entire
job is to select behaviour. Round 28 sent all 55 such parameters in the fleet a
value they cannot mean; fifty refused and named the legal set, and five did not.

The worst was `check_outliers`:

    if method in ("iqr", "both"):
        ...
    if method in ("std", "both"):
        ...

No `else`. An unrecognised method ran neither branch, so the per-column record
stayed `{"n": 16834}`, `cols_with_outliers` stayed 0, and the tool returned
`success: True` with `columns_with_outliers: 0`. Not a crash -- an answer.

    method="iqr"     -> 4 columns, spends: 2178 outliers
    method="zscore"  -> 0 columns, success: true

And `zscore` is not a random string: it is what `detect_anomalies`, in this same
repo, calls the identical statistic. A caller who learned the vocabulary at one
tool and carried it to the other was told the data was clean. So both spellings
resolve at both tools now, which is `value_alias.py`'s answer to the same
problem one layer up, and `shared/choice.py` holds the tables.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_medium"), str(ROOT / "servers" / "data_statistics")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _med_inspect import check_outliers  # noqa: E402
from _med_report import cross_tabulate, pivot_table  # noqa: E402


@pytest.fixture
def csv(tmp_path):
    """A column with a real outlier in it, and a categorical pair to tabulate."""
    path = tmp_path / "d.csv"
    values = [1.0] * 40 + [5000.0]
    pd.DataFrame(
        {
            "spend": values,
            "platform": ["Google"] * 20 + ["Facebook"] * 21,
            "kind": ["Search"] * 20 + ["Conversions"] * 21,
        }
    ).to_csv(path, index=False)
    return str(path)


class TestAnUnrecognisedMethodIsNeverAnAnswer:
    def test_the_outlier_is_found_by_the_documented_method(self, csv):
        r = check_outliers(csv, method="iqr", open_after=False)
        assert r["success"] is True
        assert r["columns_with_outliers"] == 1, r["results"]

    def test_a_typo_is_refused_rather_than_answered(self, csv):
        r = check_outliers(csv, method="definitely_not_a_method", open_after=False)
        assert r["success"] is False
        assert "columns_with_outliers" not in r

    def test_the_refusal_names_every_legal_value(self, csv):
        r = check_outliers(csv, method="definitely_not_a_method", open_after=False)
        blob = f"{r['error']} {r['hint']}"
        for legal in ("iqr", "std", "both"):
            assert legal in blob, blob

    def test_it_does_not_report_zero_outliers_on_a_column_that_has_one(self, csv):
        """The whole defect in one assertion."""
        r = check_outliers(csv, method="zscore", open_after=False)
        assert r.get("columns_with_outliers") != 0, (
            "an unrecognised method returned a clean bill of health for data with an outlier"
        )


class TestTheTwoSiblingsShareAVocabulary:
    def test_zscore_reaches_check_outliers(self, csv):
        """`zscore` is detect_anomalies' spelling for the same 3-sigma scan."""
        r = check_outliers(csv, method="zscore", open_after=False)
        assert r["success"] is True
        assert r["method"] == "std", r["method"]

    def test_std_still_means_std(self, csv):
        r = check_outliers(csv, method="std", open_after=False)
        assert r["success"] is True
        assert r["method"] == "std"

    def test_std_reaches_detect_anomalies(self, csv):
        """And the other direction: check_outliers' spelling, at the other tool."""
        from _med_analysis import detect_anomalies

        r = detect_anomalies(csv, method="std")
        assert r["success"] is True
        assert r["method"] == "zscore", r["method"]


class TestCrossTabulateReportsWhatItDid:
    def test_a_normalize_it_cannot_read_is_refused(self, csv):
        r = cross_tabulate(csv, "platform", "kind", normalize="sideways", open_after=False)
        assert r["success"] is False
        for legal in ("index", "columns", "all"):
            assert legal in f"{r['error']} {r['hint']}"

    def test_the_response_echoes_the_normalize_that_was_used(self, csv):
        """It used to echo the caller's word, so an unnormalised table claimed it was."""
        r = cross_tabulate(csv, "platform", "kind", normalize="rows", open_after=False)
        assert r["success"] is True
        assert r["normalize"] == "index", r["normalize"]

    def test_no_normalisation_says_so(self, csv):
        r = cross_tabulate(csv, "platform", "kind", open_after=False)
        assert r["normalize"] is False

    def test_an_agg_func_that_cannot_apply_is_not_silently_dropped(self, csv):
        """Without values_column pandas counts, and agg_func has no effect at all."""
        r = cross_tabulate(csv, "platform", "kind", agg_func="mean", open_after=False)
        assert r["success"] is True
        assert r["agg_func"] == "count"
        warned = " ".join(str(p) for p in r["progress"])
        assert "agg_func" in warned and "values_column" in warned, warned


class TestTheHintNamesTheArgumentThatWasWrong:
    def test_pivot_table_blames_agg_func_and_not_the_file(self, csv):
        """It used to pass pandas' exception through under "Check file_path and column names"."""
        r = pivot_table(csv, index=["platform"], values=["spend"], agg_func="definitely_not_a_func")
        assert r["success"] is False
        assert "agg_func" in r["error"], r["error"]
        assert "file_path" not in r["hint"], r["hint"]

    def test_the_siblings_accept_the_same_words(self, csv):
        """compute_aggregations took five functions; pivot_table took whatever pandas did."""
        from _med_transform import compute_aggregations

        for func in ("median", "std", "var", "nunique"):
            a = compute_aggregations(csv, group_by=["platform"], agg_column="spend", agg_func=func)
            b = pivot_table(csv, index=["platform"], values=["spend"], agg_func=func)
            assert a["success"] is True, (func, a.get("error"))
            assert b["success"] is True, (func, b.get("error"))
