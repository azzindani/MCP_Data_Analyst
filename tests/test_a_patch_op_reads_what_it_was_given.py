"""apply_patch must read the fields it was handed, and do what it was asked.

Both defects here came from a coverage sweep told to verify each result rather
than read the success flag.

**The catalog taught the shape the tool rejected.** list_patch_ops describes
every op as {"op": "clean_text", "params": "scope: headers|values|both"}, so a
caller writes the same shape back -- {"op": ..., "params": {...}} -- and every
field inside was dropped. The refusal then named fields the caller had supplied:

    Op 0 (add_column): missing 'name'; Op 0 (add_column): math mode requires 'expr'

**clean_text did one thing and said so nowhere.** It applied strip+title
unconditionally, reading no vocabulary at all, so asking for lowercase headers
returned `Campaign_Platform`. It also counted every column it looked at, so
headers that were already clean still reported columns_affected: 16.

Two claims from the same report did *not* survive checking and are pinned here
so they are not re-investigated: a partial failure does not mutate the file, and
add_column's math mode does accept a real column name.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from servers.data_basic.engine import apply_patch  # noqa: E402

FIXTURE = ROOT / "tests" / "fixtures" / "ad_data_full.csv"


@pytest.fixture
def csv(tmp_path: Path) -> str:
    dst = tmp_path / "data.csv"
    shutil.copy2(FIXTURE, dst)
    return str(dst)


def header(path: str) -> list[str]:
    return Path(path).read_text(encoding="utf-8").splitlines()[0].split(",")


class TestTheNestedParamsShapeIsRead:
    def test_an_op_written_the_way_the_catalog_prints_it_works(self, csv):
        r = apply_patch(
            csv,
            [{"op": "add_column", "params": {"name": "total", "mode": "math", "expr": "clicks + impressions"}}],
        )
        assert r["success"] is True, r.get("error")
        assert "total" in header(csv)

    def test_flat_keys_still_work(self, csv):
        r = apply_patch(csv, [{"op": "add_column", "name": "total", "mode": "math", "expr": "clicks + impressions"}])
        assert r["success"] is True, r.get("error")
        assert "total" in header(csv)

    def test_a_flat_key_wins_over_the_nested_one(self, csv):
        r = apply_patch(
            csv,
            [
                {
                    "op": "add_column",
                    "name": "flat",
                    "params": {"name": "nested", "mode": "math", "expr": "clicks + impressions"},
                }
            ],
        )
        assert r["success"] is True, r.get("error")
        cols = header(csv)
        assert "flat" in cols and "nested" not in cols

    def test_a_genuinely_missing_field_is_still_refused(self, csv):
        r = apply_patch(csv, [{"op": "add_column", "params": {"mode": "math", "expr": "clicks + 1"}}])
        assert r["success"] is False
        assert "name" in r["error"]


class TestCleanTextDoesWhatItIsAsked:
    def test_lower_means_lower(self, csv):
        r = apply_patch(csv, [{"op": "clean_text", "scope": "headers", "operations": ["lower"]}])
        assert r["success"] is True, r.get("error")
        assert "campaign_platform" in header(csv)
        assert "Campaign_Platform" not in header(csv)

    def test_upper_means_upper(self, csv):
        r = apply_patch(csv, [{"op": "clean_text", "scope": "headers", "operations": ["upper"]}])
        assert r["success"] is True, r.get("error")
        assert "CAMPAIGN_PLATFORM" in header(csv)

    def test_the_default_is_unchanged(self, csv):
        """Existing callers must see exactly what they saw before."""
        r = apply_patch(csv, [{"op": "clean_text", "scope": "headers"}])
        assert r["success"] is True, r.get("error")
        assert "Campaign_Platform" in header(csv)

    def test_an_unknown_operation_is_refused_with_the_list(self, csv):
        r = apply_patch(csv, [{"op": "clean_text", "scope": "headers", "operations": ["snake_case"]}])
        assert r["success"] is False
        assert "snake_case" in r["error"]
        for valid in ("lower", "upper", "title", "strip"):
            assert valid in r["error"]

    def test_the_operations_it_used_are_reported(self, csv):
        r = apply_patch(csv, [{"op": "clean_text", "scope": "headers", "operations": ["lower"]}])
        assert r["results"][0]["operations"] == ["lower"]

    def test_nothing_changed_counts_as_nothing_affected(self, csv):
        apply_patch(csv, [{"op": "clean_text", "scope": "headers"}])
        again = apply_patch(csv, [{"op": "clean_text", "scope": "headers"}])
        assert again["success"] is True, again.get("error")
        assert again["results"][0]["columns_affected"] == 0

    def test_the_catalog_advertises_the_operations(self):
        from servers.data_basic.engine import list_patch_ops

        entry = next(o for ops in list_patch_ops()["ops"].values() for o in ops if o["op"] == "clean_text")
        assert "operations" in entry["params"]
        assert "lower" in entry["params"]


class TestTwoClaimsThatDidNotHold:
    """Checked and false. Recorded so they are not chased again."""

    def test_a_partial_failure_leaves_the_file_alone(self, csv):
        before = Path(csv).read_bytes()
        r = apply_patch(
            csv,
            [
                {"op": "clean_text", "scope": "headers"},
                {"op": "add_column", "name": "t", "mode": "math", "expr": "nosuchcolumn * 2"},
            ],
        )
        assert r["success"] is False
        assert Path(csv).read_bytes() == before, "a failed batch must not rewrite the file"

    def test_add_column_math_takes_a_real_column_name(self, csv):
        r = apply_patch(csv, [{"op": "add_column", "name": "t", "mode": "math", "expr": "clicks + impressions"}])
        assert r["success"] is True, r.get("error")
