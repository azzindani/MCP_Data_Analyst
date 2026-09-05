"""8.5 MB to say which 2,793 rows of 38,576 were anomalous.

A user review ran `detect_anomalies` over one file and got back a CSV holding
every row with boolean flag columns appended. 2,793 rows were flagged. The
other 35,783 were in the file because that is how the tool writes: the answer
to "which rows are anomalous" arrived as a file that is 93% rows that are not.

And the reason a flagged row was flagged existed only as a `True` under a
column name. `income_iqr_flag: True` says a rule fired. It does not say which
value, or how far past what limit, so a caller who wanted to act on it had to
call another tool to find the number the detector had just computed.

The review's prescription: *anomalies-only CSV with plain-language `reason` +
`suggested_fix`, plus a full scored file*. Both, because they answer different
questions -- the small one is what an agent reads, the big one is what a later
pass re-scores against. The scored file is unchanged, at the same path, so
nothing that already reads it moves.

One deliberate refusal in the wording: `suggested_fix` never says "drop the
row". An outlier is a value that is unusual, not a value that is wrong, and
this tool cannot tell which. It names what to check and the two ordinary
outcomes instead.
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

from _med_analysis import detect_anomalies  # noqa: E402

from shared.anomaly_reasons import row_fix, row_reason  # noqa: E402


@pytest.fixture
def spiky(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    # 200 ordinary rows and 4 obvious outliers, so the flagged set is a small
    # minority exactly as it was in the review.
    values = [100 + (i % 7) for i in range(200)] + [100_000, 99_000, -50_000, 98_500]
    other = [1.0] * 204
    p = tmp_path / "money.csv"
    pd.DataFrame({"income": values, "flat": other}).to_csv(p, index=False)
    return p


def test_the_scored_file_still_holds_every_row(spiky):
    """The big file is unchanged: something later re-scores against it."""
    r = detect_anomalies(str(spiky))
    assert r["success"] is True
    full = pd.read_csv(r["output_path"])
    assert len(full) == 204


def test_the_anomalies_file_holds_only_the_anomalies(spiky):
    r = detect_anomalies(str(spiky))
    assert r["anomalies_only_path"], "the short answer needs somewhere to live"
    only = pd.read_csv(r["anomalies_only_path"])
    assert len(only) == r["anomaly_count"]
    assert len(only) < 204, "it is only worth writing because it is smaller"
    assert (only["_anomaly_score"] > 0).all()


def test_it_is_materially_smaller_on_disk(spiky):
    r = detect_anomalies(str(spiky))
    full_kb = Path(r["output_path"]).stat().st_size
    only_kb = Path(r["anomalies_only_path"]).stat().st_size
    assert only_kb < full_kb / 2, f"{only_kb} vs {full_kb}"


def test_every_flagged_row_says_why(spiky):
    r = detect_anomalies(str(spiky))
    only = pd.read_csv(r["anomalies_only_path"])
    assert "anomaly_reason" in only.columns
    assert only["anomaly_reason"].str.len().gt(0).all(), "a flagged row with no reason is the old defect"
    # The number the detector already computed, in the row, rather than behind
    # another call.
    assert only["anomaly_reason"].str.contains("income").any()
    assert only["anomaly_reason"].str.contains("limit|deviations", regex=True).all()


def test_every_flagged_row_says_what_to_do(spiky):
    r = detect_anomalies(str(spiky))
    only = pd.read_csv(r["anomalies_only_path"])
    assert "suggested_fix" in only.columns
    assert only["suggested_fix"].str.len().gt(0).all()


def test_the_fix_never_tells_a_caller_to_delete_data():
    """An outlier is unusual, not wrong, and this tool cannot tell the difference."""
    hits = [{"column": "income", "method": "iqr", "value": 100_000, "limit": 130, "side": "above"}]
    fix = row_fix(hits)
    assert "drop" not in fix.lower()
    assert "delete" not in fix.lower()
    assert "check" in fix.lower()


def test_a_clean_file_writes_no_anomalies_file(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    p = tmp_path / "flat.csv"
    pd.DataFrame({"a": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]}).to_csv(p, index=False)
    r = detect_anomalies(str(p))
    assert r["success"] is True
    assert r["anomalies_only_path"] == "", "an empty file is a worse answer than no file"
    assert r["anomalies_only_rows"] == 0


# ---------------------------------------------------------------------------
# the wording itself
# ---------------------------------------------------------------------------


def test_a_reason_names_the_value_the_limit_and_the_side():
    hits = [{"column": "income", "method": "iqr", "value": 250_000, "limit": 125_000, "side": "above"}]
    text = row_reason(hits)
    assert "income" in text
    assert "250,000" in text
    assert "125,000" in text
    assert "above" in text


def test_a_zscore_reason_names_the_threshold():
    hits = [{"column": "dti", "method": "zscore", "value": 42.0, "limit": 3.0, "side": ""}]
    text = row_reason(hits)
    assert "dti" in text and "3" in text and "standard deviations" in text


def test_several_hits_are_all_named():
    hits = [
        {"column": "a", "method": "iqr", "value": 1, "limit": 0, "side": "above"},
        {"column": "b", "method": "zscore", "value": 2, "limit": 3.0, "side": ""},
    ]
    text = row_reason(hits)
    assert "a " in text and "b " in text


def test_a_row_flagged_on_many_columns_is_advised_differently():
    """One odd value and a row that is odd all over are different findings."""
    one = row_fix([{"column": "a", "method": "iqr", "value": 1, "limit": 0, "side": "above"}])
    many = row_fix(
        [
            {"column": "a", "method": "iqr", "value": 1, "limit": 0, "side": "above"},
            {"column": "b", "method": "iqr", "value": 1, "limit": 0, "side": "above"},
        ]
    )
    assert one != many
    assert "row itself is unusual" in many


def test_no_hits_means_no_words():
    assert row_reason([]) == ""
    assert row_fix([]) == ""
