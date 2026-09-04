"""Three DD-MM-YYYY columns came back datetime and a fourth came back text.

From a user review of a 38,576-row credit file: `last_credit_pull_date`,
`last_payment_date` and `next_payment_date` were typed datetime with a cast
suggestion, and `issue_date` -- same format, 65 unique values -- was typed
text with none.

The cause was two different rules in one `if` block:

    pd.to_datetime(s.dropna().head(50), errors="raise")   # all or nothing
    pd.to_numeric(s.dropna().head(50), errors="coerce").notna().mean() > 0.9

The datetime branch demanded every one of the first fifty values parse, the
numeric branch beside it accepted ninety percent, and neither looked past the
head of the column. One stray value near the top of a file decided a column's
type, and the response said nothing about the near miss.

The repo already had the right rule and this tool was not using it:
`shared.column_utils.parse_dates` is what every other date-aware tool here
calls, with `format="mixed"` and an orientation chosen from the data. Both
branches now read one threshold and one spread sample, so they cannot come
apart again.
"""

from __future__ import annotations

import logging

import pandas as pd
import pytest

from servers.data_medium._med_inspect import _TYPE_MATCH_THRESHOLD, _type_sample, auto_detect_schema

logging.disable(logging.CRITICAL)


def _columns(result):
    cols = result["columns"]
    return cols if isinstance(cols, dict) else {c["column"]: c for c in cols}


@pytest.fixture()
def credit(tmp_path):
    """Four DD-MM-YYYY columns, one of them low-cardinality like `issue_date`."""
    n = 300
    src = tmp_path / "credit.csv"
    pd.DataFrame(
        {
            "last_credit_pull_date": pd.date_range("2015-01-01", periods=n).strftime("%d-%m-%Y"),
            "last_payment_date": pd.date_range("2016-03-05", periods=n).strftime("%d-%m-%Y"),
            "next_payment_date": pd.date_range("2017-06-11", periods=n).strftime("%d-%m-%Y"),
            "issue_date": [f"{(i % 28) + 1:02d}-{(i % 12) + 1:02d}-201{i % 5}" for i in range(n)],
            "grade": ["A", "B", "C", "D"] * (n // 4),
        }
    ).to_csv(src, index=False)
    return src


def test_columns_of_one_format_get_one_verdict(credit):
    """The review's finding, exactly."""
    cols = _columns(auto_detect_schema(str(credit)))
    dated = ["last_credit_pull_date", "last_payment_date", "next_payment_date", "issue_date"]
    verdicts = {c: cols[c]["inferred_type"] for c in dated}
    assert set(verdicts.values()) == {"datetime"}, verdicts


def test_each_of_them_also_gets_the_cast_suggestion(credit):
    """Being typed datetime and told nothing is only half an answer."""
    cols = _columns(auto_detect_schema(str(credit)))
    for c in ["last_credit_pull_date", "last_payment_date", "next_payment_date", "issue_date"]:
        assert cols[c]["suggestion"], f"{c} was typed datetime with no cast suggestion"


def test_the_verdict_shows_how_close_it_was(credit):
    """A near miss must be visible, not silently rounded to a type."""
    cols = _columns(auto_detect_schema(str(credit)))
    assert cols["issue_date"]["match_rate"] >= _TYPE_MATCH_THRESHOLD


def test_one_stray_value_no_longer_flips_the_type(tmp_path):
    """The mechanism behind the finding: all-or-nothing on the first fifty."""
    n = 300
    src = tmp_path / "one_bad.csv"
    values = list(pd.date_range("2015-01-01", periods=n).strftime("%d-%m-%Y"))
    values[3] = "not a date"  # inside the old head(50) window
    pd.DataFrame({"when": values}).to_csv(src, index=False)

    col = _columns(auto_detect_schema(str(src)))["when"]
    assert col["inferred_type"] == "datetime"
    assert col["match_rate"] < 1.0


def test_a_column_that_is_mostly_not_dates_is_still_not_a_date(tmp_path):
    """The threshold has to refuse as well as accept, or it means nothing."""
    src = tmp_path / "prose.csv"
    pd.DataFrame({"note": [f"row {i} free text" for i in range(200)]}).to_csv(src, index=False)
    col = _columns(auto_detect_schema(str(src)))["note"]
    assert col["inferred_type"] != "datetime"


def test_both_branches_read_the_same_threshold():
    """The defect was two constants. There is one, and it is shared."""
    import inspect

    import servers.data_medium._med_inspect as mod

    # Comments are excluded: this file's own explanation of the defect quotes
    # the old call, and a check that trips on the record of a fix is useless.
    code = "\n".join(
        line for line in inspect.getsource(mod.auto_detect_schema).splitlines() if not line.lstrip().startswith("#")
    )
    assert code.count("_TYPE_MATCH_THRESHOLD") == 2
    assert 'errors="raise"' not in code


def test_the_sample_is_spread_not_taken_from_the_head():
    """`head(50)` sees whatever the file opens with."""
    s = pd.Series(["x"] * 500 + ["y"] * 500)
    sample = _type_sample(s)
    assert len(sample) <= 200
    assert set(sample) == {"x", "y"}, "a head-only sample would never reach the y values"


def test_an_ambiguous_orientation_is_surfaced(tmp_path):
    """The repo's own rule: never swallow `ambiguous`."""
    n = 120
    src = tmp_path / "ambiguous.csv"
    # Every day and month <= 12, so nothing in the data settles the order.
    pd.DataFrame({"d": [f"{(i % 12) + 1:02d}-{(i % 12) + 1:02d}-2020" for i in range(n)]}).to_csv(src, index=False)
    col = _columns(auto_detect_schema(str(src)))["d"]
    if col["inferred_type"] == "datetime":
        assert "dayfirst" in col
