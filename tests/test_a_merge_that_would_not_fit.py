"""A merge is sized in bytes before it is built, and a dry run never builds it.

One ordinary call OOM-killed the whole Data_Analyst server -- every tier, every
concurrent session: merge_datasets(Ad_Data.csv, ad_clean.csv, how="inner",
dry_run=True). Auto-detect took the first shared column, Date, as the key; the
guard counted 1,804,677 + 33,463 rows, under its 2,000,000-row cap; and the dry
run performed the real merge. 1.8M rows of 31 mostly-text columns against a
1 GB container. Found by driving the deployed tools directly.
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

from servers.data_medium.engine import merge_datasets  # noqa: E402
from shared.platform_utils import get_max_merge_bytes  # noqa: E402


def _write(path: Path, frame: pd.DataFrame) -> str:
    frame.to_csv(path, index=False)
    return str(path)


@pytest.fixture
def wide(tmp_path) -> tuple[str, str]:
    """Two tables sharing a repeated key: 200 x 200 rows per day, 20 days, text-heavy."""
    days = [f"2024-01-{d:02d}" for d in range(1, 21)]
    text = "a fairly long creative name that repeats " * 3
    left = pd.DataFrame({"day": days * 200, "creative": [text + str(i) for i in range(4000)]})
    right = pd.DataFrame({"day": days * 200, "headline": [text + str(i) for i in range(4000)]})
    return _write(tmp_path / "left.csv", left), _write(tmp_path / "right.csv", right)


class TestTheGuardCountsBytes:
    def test_rows_under_the_row_cap_are_refused_when_they_would_not_fit(self, wide, monkeypatch, tmp_path):
        monkeypatch.setenv("MCP_MAX_MERGE_MB", "50")
        left, right = wide
        r = merge_datasets(left, right, left_on="day", right_on="day", how="inner", open_after=False)
        assert r["success"] is False, r
        assert r["estimated_rows"] == 800_000  # 20 days x 200 x 200: far under 2,000,000
        assert r["estimated_mb"] > 50
        assert "MB" in r["error"]
        assert not (tmp_path / "left_merged.csv").exists()

    def test_the_same_join_fits_a_bigger_budget(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MCP_MAX_MERGE_MB", "4096")
        days = ["2024-01-01", "2024-01-02"]
        left = _write(tmp_path / "l.csv", pd.DataFrame({"day": days * 20, "v": range(40)}))
        right = _write(tmp_path / "r.csv", pd.DataFrame({"day": days * 20, "w": range(40)}))
        r = merge_datasets(left, right, left_on="day", right_on="day", how="inner", open_after=False)
        assert r["success"] is True, r
        assert r["result_rows"] == 800

    def test_the_budget_has_a_default_and_survives_a_bad_value(self, monkeypatch):
        monkeypatch.delenv("MCP_MAX_MERGE_MB", raising=False)
        assert get_max_merge_bytes() == 256 * 2**20
        monkeypatch.setenv("MCP_MAX_MERGE_MB", "lots")
        assert get_max_merge_bytes() == 256 * 2**20
        monkeypatch.setenv("MCP_MAX_MERGE_MB", "1024")
        assert get_max_merge_bytes() == 1024 * 2**20


class TestADryRunNeverBuildsTheTable:
    def test_no_merge_of_the_tables_runs(self, wide, monkeypatch):
        built: list[int] = []
        real_merge = pd.DataFrame.merge

        def spy(self, *a, **kw):
            if kw.get("indicator"):
                built.append(len(self))
            return real_merge(self, *a, **kw)

        monkeypatch.setattr(pd.DataFrame, "merge", spy)
        monkeypatch.setenv("MCP_MAX_MERGE_MB", "4096")
        left, right = wide
        r = merge_datasets(left, right, left_on="day", right_on="day", how="inner", dry_run=True, open_after=False)
        assert r["success"] is True, r
        assert r["dry_run"] is True
        assert r["result_rows"] == 800_000
        assert built == [], "a dry run must answer from the count, not by building the join"

    @pytest.mark.parametrize("how", ["inner", "left", "right", "outer"])
    def test_the_count_is_the_row_count_the_merge_writes(self, tmp_path, how):
        left = pd.DataFrame({"k": [1, 1, 2, 3, None, None], "a": range(6)})
        right = pd.DataFrame({"k": [1.0, 2.0, 2.0, 4.0, None], "b": range(5)})
        lp, rp = _write(tmp_path / "l.csv", left), _write(tmp_path / "r.csv", right)
        dry = merge_datasets(lp, rp, left_on="k", right_on="k", how=how, dry_run=True, open_after=False)
        real = merge_datasets(lp, rp, left_on="k", right_on="k", how=how, open_after=False)
        assert dry["success"] is True and real["success"] is True, (dry, real)
        assert dry["result_rows"] == real["result_rows"] == len(pd.read_csv(real["output_path"]))
        assert dry["matched"] == real["matched"]


class TestAKeyPickedUnasked:
    def test_a_date_is_passed_over_for_the_column_that_identifies_rows(self, tmp_path):
        left = pd.DataFrame({"Date": ["2024-01-01", "2024-01-02"] * 3, "id": [1, 2, 3, 4, 5, 6], "x": range(6)})
        right = pd.DataFrame(
            {"Date": ["2024-01-01", "2024-01-02", "2024-01-01"], "id": [1, 2, 3], "label": list("abc")}
        )
        lp, rp = _write(tmp_path / "l.csv", left), _write(tmp_path / "r.csv", right)
        r = merge_datasets(lp, rp, how="inner", open_after=False)
        assert r["success"] is True, r
        assert r["left_on"] == r["right_on"] == "id"
        assert r["result_rows"] == 3

    def test_no_key_is_picked_when_every_shared_column_repeats(self, tmp_path):
        left = pd.DataFrame({"Date": ["2024-01-01", "2024-01-02"] * 3, "product": ["a", "b", "c"] * 2})
        right = pd.DataFrame({"Date": ["2024-01-01", "2024-01-02"] * 2, "product": ["a", "b"] * 2})
        lp, rp = _write(tmp_path / "l.csv", left), _write(tmp_path / "r.csv", right)
        r = merge_datasets(lp, rp, how="inner", open_after=False)
        assert r["success"] is False, r
        assert "Date (a date)" in r["hint"]
        assert "product (repeats on both sides)" in r["hint"]
        assert "left_on" in r["hint"]
        assert not (tmp_path / "l_merged.csv").exists()

    def test_a_key_named_on_one_side_is_used_on_both(self, tmp_path):
        left = pd.DataFrame({"Date": ["2024-01-01"] * 3, "id": [1, 2, 3]})
        right = pd.DataFrame({"Date": ["2024-01-01"] * 2, "id": [2, 3], "label": ["b", "c"]})
        lp, rp = _write(tmp_path / "l.csv", left), _write(tmp_path / "r.csv", right)
        r = merge_datasets(lp, rp, left_on="id", how="inner", open_after=False)
        assert r["success"] is True, r
        assert r["left_on"] == r["right_on"] == "id"
        assert r["result_rows"] == 2
