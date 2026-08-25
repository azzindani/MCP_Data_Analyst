"""check_outliers counted anomalies and never named one.

    Scan for outliers + anomalies. method: iqr std both. Flags anomalous rows.

The last clause was the one that never happened. The tool returned per-column
counts, per-column fences and a box plot; nothing in the response or on disk was
row-level. The round-14 sweep caught it the way a caller would -- by asking for
the rows:

    check_outliers(..., output_path="outliers_both.csv")
      -> warn: "Output extension changed ... this tool writes HTML"
      -> outliers_both.html, 335 KB of box plots
      -> zero flagged rows, anywhere

The masks already existed inside the scan loop; both branches threw them away
after calling .sum() on them. Keeping them is what makes the sentence true.

Bounded like every other listing here: `flagged_rows` carries at most
get_max_results() entries, `flagged_rows_total` says how many there were, and
the hint names the tool that can page through the rest.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_medium")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from servers.data_statistics.engine import check_outliers  # noqa: E402


@pytest.fixture
def spiky(tmp_path) -> Path:
    """Two columns whose last row is far outside the fence, one that is flat."""
    f = tmp_path / "spiky.csv"
    pd.DataFrame(
        {
            "a": [*range(1, 13), 900],
            "b": [1] * 12 + [500],
            "c": list(range(13)),
        }
    ).to_csv(f, index=False)
    return f


def scan(path, tmp_path, **kw):
    return check_outliers(str(path), open_after=False, output_path=str(tmp_path / "o.html"), **kw)


def test_the_flagged_row_is_named(spiky, tmp_path):
    r = scan(spiky, tmp_path)
    assert r["success"] is True, r.get("error")
    assert r["flagged_rows"] == [{"row": 12, "columns": ["a", "b"]}]


def test_the_row_says_which_columns_flagged_it(spiky, tmp_path):
    r = scan(spiky, tmp_path)
    assert "c" not in r["flagged_rows"][0]["columns"], "c has no outliers"


def test_the_total_matches_the_listing_when_it_fits(spiky, tmp_path):
    r = scan(spiky, tmp_path)
    assert r["flagged_rows_total"] == 1
    assert r["flagged_rows_truncated"] is False


def test_a_clean_dataset_flags_nothing(tmp_path):
    f = tmp_path / "flat.csv"
    pd.DataFrame({"a": list(range(30))}).to_csv(f, index=False)
    r = scan(f, tmp_path)
    assert r["success"] is True, r.get("error")
    assert r["flagged_rows"] == []
    assert r["flagged_rows_total"] == 0


def test_each_method_flags_on_its_own(spiky, tmp_path):
    for method in ("iqr", "std", "both"):
        r = scan(spiky, tmp_path, method=method)
        assert r["flagged_rows_total"] == 1, method


def test_the_counts_and_the_rows_agree(spiky, tmp_path):
    """A per-column count of 1 and no row named was the whole defect."""
    r = scan(spiky, tmp_path)
    flagged_cols = {c for row in r["flagged_rows"] for c in row["columns"]}
    counted = {
        col
        for col, res in r["results"].items()
        if (res.get("outlier_count_iqr") or 0) or (res.get("outlier_count_std") or 0)
    }
    assert flagged_cols == counted


def test_a_long_listing_is_bounded_and_says_so(tmp_path, monkeypatch):
    import _med_inspect

    monkeypatch.setattr(_med_inspect, "get_max_results", lambda: 5)
    f = tmp_path / "many.csv"
    # 200 identical rows, so q1 == q3 and the fence has no width, and ten rows
    # that are not that value.
    pd.DataFrame({"a": [*([1.0] * 200), *([1_000_000.0] * 10)]}).to_csv(f, index=False)
    r = scan(f, tmp_path)
    assert r["flagged_rows_total"] == 10
    assert len(r["flagged_rows"]) == 5
    assert r["flagged_rows_truncated"] is True
    assert "10 row(s) were flagged" in r["hint"]
    assert "filter_rows()" in r["hint"]


def test_nulls_do_not_shift_the_row_numbers(tmp_path):
    """The masks are built on dropna()'d values and reported against the file."""
    f = tmp_path / "gappy.csv"
    pd.DataFrame({"a": [1, None, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 900]}).to_csv(f, index=False)
    r = scan(f, tmp_path)
    assert r["flagged_rows_total"] == 1
    assert r["flagged_rows"][0]["row"] == 12, "the 900 is the thirteenth row of the file"


def test_the_chart_is_still_written(spiky, tmp_path):
    r = scan(spiky, tmp_path)
    assert Path(r["output_path"]).exists()
