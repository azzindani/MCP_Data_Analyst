"""pivot_table summed text columns, and returned 330,195 tokens saying success.

Called the way its schema documents -- `values` is optional -- against the real
16,834-row Ad_Data.csv:

    pivot_table(file_path=..., index=["campaign_platform"])

    {"success": true, "rows": 2, "returned": 2, "truncated": false,
     "token_estimate": 330189}

1.32 MB in one response, from a server whose whole reason to exist is feeding a
local model a 10,000-12,000 token context. 27 times the entire budget, in a
single call, with nothing in the response suggesting anything was wrong.

The size was the symptom. Without `values`, pandas pivots *every* remaining
column, and aggfunc="sum" over an object column concatenates it:

    clicks         77569                                    <- a real answer
    spends         564115.51                                <- a real answer
    campaign_type  'ConversionsConversionsConversions...'   <- 19,063 chars
    subchannel     'Facebook AdsFacebook AdsFacebook...'    <- 20,796 chars

Twelve of the sixteen columns were that. Not a sum of anything -- one word
repeated once per row -- presented as a pivot result. The two numbers a caller
actually wanted were buried inside 1.3 MB of it.

A pivot's values are its measures, so an unspecified `values` now means the
numeric columns, and the skipped text columns are named in progress.
compute_aggregations, the neighbour that does the same job, has always required
an explicit `agg_column`; pivot_table was the outlier.

The second defect was in the same three lines. The cap and the flag were two
different numbers --

    truncated = len(pt) > get_max_rows()   # 100
    records   = pt.head(10)

-- so a 20-row pivot returned 10 rows under `truncated: false`, dropping half
the answer silently, and a 257-row pivot warned "Showing first 100 rows" having
returned 10. One cap now drives both, and the warning counts what it returned.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "servers" / "data_medium"))

from _med_report import pivot_table  # noqa: E402

from shared.platform_utils import get_max_rows  # noqa: E402

# The context these servers are built for; one call must stay well inside it.
CONTEXT_BUDGET = 12_000

ROWS = 40
PLATFORMS = ["Google Ads", "Facebook Ads"]


@pytest.fixture()
def csv(tmp_path: Path) -> str:
    frame = pd.DataFrame(
        {
            "campaign_platform": [PLATFORMS[i % 2] for i in range(ROWS)],
            "campaign_type": ["Conversions"] * ROWS,
            "subchannel": ["Search Keywords"] * ROWS,
            "spends": [10.0] * ROWS,
            "clicks": list(range(ROWS)),
        }
    )
    dst = tmp_path / "ad.csv"
    frame.to_csv(dst, index=False)
    return str(dst)


@pytest.fixture()
def wide_csv(tmp_path: Path) -> str:
    """More distinct index values than the row cap, to exercise truncation."""
    n = get_max_rows() * 2
    frame = pd.DataFrame({"day": [f"2019-{i:04d}" for i in range(n)], "spends": [1.0] * n})
    dst = tmp_path / "wide.csv"
    frame.to_csv(dst, index=False)
    return str(dst)


def size_in_tokens(result: dict) -> int:
    return len(json.dumps(result, default=str)) // 4


class TestTextColumnsAreNotSummed:
    def test_an_unspecified_values_picks_the_numeric_columns(self, csv: str):
        r = pivot_table(csv, index=["campaign_platform"])
        assert r["success"] is True, r.get("error")
        assert sorted(r["values"]) == ["clicks", "spends"]

    def test_no_text_column_appears_in_the_result(self, csv: str):
        r = pivot_table(csv, index=["campaign_platform"])
        for row in r["result"]:
            assert "campaign_type" not in row, row
            assert "subchannel" not in row, row

    def test_no_cell_is_a_concatenated_string(self, csv: str):
        """The tell: a cell far longer than any single value in the file."""
        r = pivot_table(csv, index=["campaign_platform"])
        for row in r["result"]:
            for key, value in row.items():
                assert len(str(value)) < 100, (key, str(value)[:60])

    def test_the_numbers_are_still_right(self, csv: str):
        r = pivot_table(csv, index=["campaign_platform"])
        by_platform = {row["campaign_platform"]: row for row in r["result"]}
        # 20 rows each, spends 10.0 apiece
        assert by_platform["Google Ads"]["spends"] == pytest.approx(200.0)
        assert by_platform["Facebook Ads"]["spends"] == pytest.approx(200.0)

    def test_it_says_which_columns_it_skipped(self, csv: str):
        r = pivot_table(csv, index=["campaign_platform"])
        msgs = " ".join(f"{p.get('msg', '')} {p.get('detail', '')}" for p in r["progress"])
        assert "campaign_type" in msgs and "subchannel" in msgs

    def test_an_explicit_values_is_still_obeyed(self, csv: str):
        """Naming a text column is the caller's choice to make."""
        r = pivot_table(csv, index=["campaign_platform"], values=["clicks"])
        assert r["values"] == ["clicks"]
        assert set(r["result"][0]) == {"campaign_platform", "clicks"}

    def test_a_file_with_no_numeric_column_is_refused(self, tmp_path: Path):
        frame = pd.DataFrame({"a": ["x", "y"], "b": ["p", "q"]})
        dst = tmp_path / "text.csv"
        frame.to_csv(dst, index=False)
        r = pivot_table(str(dst), index=["a"])
        assert r["success"] is False
        assert "values=" in r["hint"] and "b" in r["hint"]


class TestTheResponseFitsTheContextItWasBuiltFor:
    def test_the_default_pivot_is_small(self, csv: str):
        assert size_in_tokens(pivot_table(csv, index=["campaign_platform"])) < CONTEXT_BUDGET // 4

    def test_token_estimate_matches_what_was_actually_sent(self, csv: str):
        r = pivot_table(csv, index=["campaign_platform"])
        assert r["token_estimate"] == pytest.approx(size_in_tokens(r), rel=0.25)

    def test_a_truncated_pivot_is_bounded_too(self, wide_csv: str):
        assert size_in_tokens(pivot_table(wide_csv, index=["day"])) < CONTEXT_BUDGET


class TestTruncationTellsTheTruth:
    def test_it_returns_up_to_the_row_cap(self, wide_csv: str):
        r = pivot_table(wide_csv, index=["day"])
        assert r["returned"] == get_max_rows(), r["returned"]

    def test_it_flags_truncation_when_it_truncates(self, wide_csv: str):
        r = pivot_table(wide_csv, index=["day"])
        assert r["truncated"] is True
        assert r["rows"] > r["returned"]

    def test_the_warning_counts_what_it_actually_returned(self, wide_csv: str):
        r = pivot_table(wide_csv, index=["day"])
        detail = " ".join(str(p.get("detail", "")) for p in r["progress"])
        assert f"{r['returned']}" in detail and f"{r['rows']}" in detail

    def test_nothing_is_dropped_without_the_flag(self, csv: str):
        """The 20-row case: 10 returned, truncated false, half the answer gone."""
        r = pivot_table(csv, index=["campaign_platform"])
        if not r["truncated"]:
            assert r["returned"] == r["rows"], r

    @pytest.mark.parametrize("fraction", [0.25, 0.6, 1.0])
    def test_a_pivot_under_the_cap_returns_every_row(self, tmp_path: Path, fraction: float):
        """Sized from get_max_rows(), because constrained mode moves it to 20 --
        a literal 25 here passes normally and fails under MCP_CONSTRAINED_MODE=1,
        which is a property of the test, not of the tool."""
        n = max(1, int(get_max_rows() * fraction))
        frame = pd.DataFrame({"k": [f"k{i}" for i in range(n)], "v": [1.0] * n})
        dst = tmp_path / f"n{n}.csv"
        frame.to_csv(dst, index=False)
        r = pivot_table(str(dst), index=["k"])
        assert r["rows"] == n
        assert r["returned"] == n, f"{n} rows in, {r['returned']} out, truncated={r['truncated']}"
        assert r["truncated"] is False


class TestTheCapIsOneNumber:
    def test_the_source_does_not_carry_a_second_hardcoded_cap(self):
        """`_response_cap = 10` beside get_max_rows() is what caused this."""
        src = (Path(__file__).parent.parent / "servers" / "data_medium" / "_med_report.py").read_text()
        body = src.split("def pivot_table", 1)[1].split("\ndef ", 1)[0]
        assert "_response_cap" not in body, "a second cap is back"
        assert "head(max_r)" in body, body[:0]
