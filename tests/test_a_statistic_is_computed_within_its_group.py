"""Per-group arithmetic had no route through the tools, and one path faked it.

Two things here, both about the same gap.

**The defect.** `aggregate_dataset(mode="window")` takes `group_by` in its
schema and dropped it on the floor -- the rolling ran over the whole frame.
On a file holding two platforms interleaved by date:

    Date        platform  spends   whole-frame roll   per-group roll
    2019-10-18  Google     300.0        167.33            200.0

167.33 is two Google rows averaged with a Meta row. The caller asked for
group_by and was given a number computed across the groups it named, under
success: true. Rolling now happens inside each group.

**The gap.** Nothing could compute a statistic *within* a group and keep the
rows. `aggregate_dataset(mode="groupby")` reduces -- 16,834 rows become one per
platform -- and all 51 apply_patch ops work on the whole frame. So the ordinary
question about campaign data, "what share of its own platform's spend is this
row", had no answer. `group_transform` is pandas' groupby().transform(), which
had no route through the tools.

It is one op, not a family of them, because the aggregation is a parameter:
sum mean median max min std count nunique (the group's number, broadcast to its
rows) and share rank cumsum zscore diff_from_mean pct_of_max (each row's value
relative to its group).

The vocabulary is defined once, in shared/patch_validator, and imported by the
handler -- the validator and the handler disagreeing about what is legal is how
an op gets listed and then rejected, which this repo has shipped before.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from servers.data_basic.engine import apply_patch, list_patch_ops
from servers.data_transform.engine import aggregate_dataset
from shared.patch_validator import GROUP_AGGS, VALID_OPS

# Two groups with different totals, so a whole-frame answer cannot pass by luck.
ROWS = {
    "platform": ["Google", "Google", "Google", "Meta", "Meta"],
    "creative": ["a", "b", "c", "d", "e"],
    "spends": [100.0, 200.0, 300.0, 10.0, 30.0],
}


@pytest.fixture()
def csv(tmp_path: Path) -> Path:
    dst = tmp_path / "ad.csv"
    pd.DataFrame(ROWS).to_csv(dst, index=False)
    return dst


def run(csv: Path, **op) -> tuple[dict, pd.DataFrame]:
    result = apply_patch(str(csv), [{"op": "group_transform", "new_column": "v", **op}])
    return result, pd.read_csv(csv)


class TestEachAggComputesWithinTheGroup:
    def test_share_is_of_the_rows_own_group_total(self, csv: Path):
        _, out = run(csv, group_by=["platform"], column="spends", agg="share")
        # Google totals 600, Meta totals 40 -- not 640.
        assert out["v"].round(4).tolist() == [0.1667, 0.3333, 0.5, 0.25, 0.75]

    def test_each_group_of_shares_sums_to_one(self, csv: Path):
        _, out = run(csv, group_by=["platform"], column="spends", agg="share")
        for _, group in out.groupby("platform"):
            assert group["v"].sum() == pytest.approx(1.0)

    def test_rank_restarts_in_each_group(self, csv: Path):
        _, out = run(csv, group_by=["platform"], column="spends", agg="rank")
        assert out["v"].tolist() == [1.0, 2.0, 3.0, 1.0, 2.0]

    def test_rank_can_be_descending(self, csv: Path):
        _, out = run(csv, group_by=["platform"], column="spends", agg="rank", descending=True)
        assert out["v"].tolist() == [3.0, 2.0, 1.0, 2.0, 1.0]

    def test_cumsum_restarts_in_each_group(self, csv: Path):
        _, out = run(csv, group_by=["platform"], column="spends", agg="cumsum")
        assert out["v"].tolist() == [100.0, 300.0, 600.0, 10.0, 40.0]

    def test_a_reducer_is_broadcast_to_every_row_of_its_group(self, csv: Path):
        _, out = run(csv, group_by=["platform"], column="spends", agg="mean")
        assert out["v"].tolist() == [200.0, 200.0, 200.0, 20.0, 20.0]

    def test_diff_from_mean_is_relative_to_the_group(self, csv: Path):
        _, out = run(csv, group_by=["platform"], column="spends", agg="diff_from_mean")
        assert out["v"].tolist() == [-100.0, 0.0, 100.0, -10.0, 10.0]

    def test_pct_of_max_is_relative_to_the_group(self, csv: Path):
        _, out = run(csv, group_by=["platform"], column="spends", agg="pct_of_max")
        assert out["v"].round(4).tolist() == [0.3333, 0.6667, 1.0, 0.3333, 1.0]

    def test_zscore_is_relative_to_the_group(self, csv: Path):
        _, out = run(csv, group_by=["platform"], column="spends", agg="zscore")
        assert out["v"].round(4).tolist() == [-1.0, 0.0, 1.0, -0.7071, 0.7071]

    def test_count_counts_the_group_not_the_file(self, csv: Path):
        _, out = run(csv, group_by=["platform"], column="spends", agg="count")
        assert out["v"].tolist() == [3, 3, 3, 2, 2]

    def test_no_row_is_lost(self, csv: Path):
        _, out = run(csv, group_by=["platform"], column="spends", agg="share")
        assert len(out) == len(ROWS["platform"])

    def test_it_groups_by_more_than_one_column(self, csv: Path):
        _, out = run(csv, group_by=["platform", "creative"], column="spends", agg="share")
        # Every group holds one row, so every share is the whole of it.
        assert out["v"].tolist() == [1.0, 1.0, 1.0, 1.0, 1.0]

    def test_a_bare_string_group_by_is_accepted(self, csv: Path):
        _, out = run(csv, group_by="platform", column="spends", agg="mean")
        assert out["v"].tolist() == [200.0, 200.0, 200.0, 20.0, 20.0]

    def test_it_reports_how_many_groups_it_found(self, csv: Path):
        result, _ = run(csv, group_by=["platform"], column="spends", agg="mean")
        assert result["results"][0]["groups"] == 2


class TestDivisionByAnEmptyGroupSaysSoRatherThanInf:
    @pytest.fixture()
    def zeroed(self, tmp_path: Path) -> Path:
        dst = tmp_path / "zero.csv"
        pd.DataFrame({"g": ["a", "a", "b"], "v0": [0.0, 0.0, 5.0]}).to_csv(dst, index=False)
        return dst

    @pytest.mark.parametrize("agg", ["share", "pct_of_max"])
    def test_a_group_summing_to_zero_gives_null_not_infinity(self, zeroed: Path, agg: str):
        apply_patch(
            str(zeroed), [{"op": "group_transform", "group_by": ["g"], "column": "v0", "agg": agg, "new_column": "v"}]
        )
        out = pd.read_csv(zeroed)
        assert out["v"].iloc[:2].isna().all(), out["v"].tolist()
        assert out["v"].iloc[2] == 1.0

    def test_a_single_row_group_has_no_spread_to_divide_by(self, zeroed: Path):
        apply_patch(
            str(zeroed),
            [{"op": "group_transform", "group_by": ["g"], "column": "v0", "agg": "zscore", "new_column": "v"}],
        )
        out = pd.read_csv(zeroed)
        assert pd.isna(out["v"].iloc[2])


class TestTheOpIsDiscoverableAndValidated:
    def test_the_validator_knows_it(self):
        assert "group_transform" in VALID_OPS

    def test_the_catalog_lists_it(self):
        cat = list_patch_ops("")["ops"]
        listed = {o["op"] for group in cat.values() for o in group}
        assert "group_transform" in listed

    def test_the_catalog_names_every_agg_the_handler_accepts(self):
        """An op listed with a value it then rejects is this repo's own history."""
        entry = next(o for group in list_patch_ops("")["ops"].values() for o in group if o["op"] == "group_transform")
        for agg in GROUP_AGGS:
            assert agg in entry["params"], agg

    @pytest.mark.parametrize("agg", sorted(GROUP_AGGS))
    def test_every_advertised_agg_actually_runs(self, csv: Path, agg: str):
        result, out = run(csv, group_by=["platform"], column="spends", agg=agg)
        assert result["success"] is True, result.get("op_errors")
        assert "v" in out.columns

    def test_a_missing_group_by_is_refused_by_name(self, csv: Path):
        r = apply_patch(str(csv), [{"op": "group_transform", "column": "spends", "agg": "share"}], dry_run=True)
        assert r["success"] is False
        assert "group_by" in r["error"]

    def test_an_unknown_agg_lists_the_valid_ones(self, csv: Path):
        r = apply_patch(
            str(csv),
            [{"op": "group_transform", "group_by": ["platform"], "column": "spends", "agg": "nope"}],
            dry_run=True,
        )
        assert "nope" in r["error"] and "share" in r["error"]

    def test_a_missing_column_names_the_available_ones(self, csv: Path):
        r = apply_patch(str(csv), [{"op": "group_transform", "group_by": ["nosuch"], "column": "spends"}])
        assert r["success"] is False
        assert "nosuch" in r["op_errors"][0]["error"] and "platform" in r["op_errors"][0]["error"]

    def test_text_columns_are_refused_for_numeric_aggs(self, csv: Path):
        r = apply_patch(
            str(csv), [{"op": "group_transform", "group_by": ["platform"], "column": "creative", "agg": "mean"}]
        )
        assert r["success"] is False and "numeric" in r["op_errors"][0]["error"]

    def test_but_counting_them_is_fine(self, csv: Path):
        _, out = run(csv, group_by=["platform"], column="creative", agg="count")
        assert out["v"].tolist() == [3, 3, 3, 2, 2]

    def test_the_vocabulary_has_exactly_one_definition(self):
        """The handler imports GROUP_AGGS; it must not redefine it."""
        src = (Path(__file__).parent.parent / "servers" / "data_basic" / "_patch_ops.py").read_text()
        assert "GROUP_AGGS = " not in src, "the handler is redefining the validator's vocabulary"
        assert "GROUP_AGGS" in src


class TestARollingWindowStaysInsideItsGroup:
    """The defect: group_by was accepted here and ignored."""

    @pytest.fixture()
    def interleaved(self, tmp_path: Path) -> Path:
        dst = tmp_path / "w.csv"
        pd.DataFrame(
            {
                "Date": ["2019-10-16"] * 2 + ["2019-10-17"] * 2 + ["2019-10-18"] * 2,
                "platform": ["Google", "Meta"] * 3,
                "spends": [100.0, 1.0, 200.0, 2.0, 300.0, 3.0],
            }
        ).to_csv(dst, index=False)
        return dst

    def rolled(self, path: Path, out: Path, **kw) -> pd.DataFrame:
        r = aggregate_dataset(
            str(path), mode="window", order_by="Date", window=3, window_agg="mean", output_path=str(out), **kw
        )
        assert r["success"] is True, r.get("error")
        return pd.read_csv(out)

    def test_each_group_rolls_over_its_own_rows(self, interleaved: Path, tmp_path: Path):
        out = self.rolled(interleaved, tmp_path / "o.csv", group_by=["platform"])
        google = out[out.platform == "Google"]["spends_window_mean3"].iloc[-1]
        assert google == pytest.approx(200.0), "whole-frame rolling gives 167.33"

    def test_the_other_group_is_right_too(self, interleaved: Path, tmp_path: Path):
        out = self.rolled(interleaved, tmp_path / "o.csv", group_by=["platform"])
        meta = out[out.platform == "Meta"]["spends_window_mean3"].iloc[-1]
        assert meta == pytest.approx(2.0)

    def test_no_group_by_still_rolls_the_whole_frame(self, interleaved: Path, tmp_path: Path):
        """Ungrouped is a legitimate request; it must not change."""
        out = self.rolled(interleaved, tmp_path / "o.csv")
        assert out["spends_window_mean3"].iloc[-1] == pytest.approx(101.666667)

    def test_the_response_says_what_it_grouped_by(self, interleaved: Path, tmp_path: Path):
        r = aggregate_dataset(
            str(interleaved),
            mode="window",
            order_by="Date",
            group_by=["platform"],
            window=3,
            output_path=str(tmp_path / "o.csv"),
        )
        assert r["data"]["group_by"] == ["platform"]

    def test_an_unknown_group_column_is_refused(self, interleaved: Path, tmp_path: Path):
        r = aggregate_dataset(str(interleaved), mode="window", order_by="Date", group_by=["nosuch"], window=3)
        assert r["success"] is False
        assert "nosuch" in r["error"] and "platform" in r["hint"]

    def test_an_unknown_window_agg_is_refused(self, interleaved: Path):
        r = aggregate_dataset(str(interleaved), mode="window", order_by="Date", window=3, window_agg="nope")
        assert r["success"] is False and "nope" in r["error"]
