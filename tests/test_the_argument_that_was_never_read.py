"""Round 27: what a competent caller breaks that a probe never touches.

Four findings, one session, one real task. Every one came from *doing* the work
rather than probing it, and three of the four share a root.

**The root.** The move to the official `mcp` SDK put every server on the bundled
FastMCP, whose argument model is pydantic's default `extra="ignore"`. An
argument no tool declares is dropped and the call succeeds. Two repos had
already written `enforce_known_arguments` for their own reasons and kept it;
five lost the check without a line changing. `shared/arg_errors.py` in this repo
names that guard in its own docstring and assumes it is installed. It was not.

    aggregate_dataset(..., agg_func="mean")  -> success: true, SUMS returned
    aggregate_dataset(..., banana="yes")     -> success: true
    apply_patch(..., output_path="new.csv")  -> success: true, SOURCE overwritten

The third is why this is not cosmetic. A caller asking for the result to be
written somewhere else had that instruction discarded in silence, and the input
file -- the fleet's own test corpus -- lost a column. Only the snapshot
`apply_patch` takes first made it recoverable.

**The infinities.** `ctr = clicks / impressions` on four rows with zero
impressions gives four `inf` in 16,834. `inf` is not null: it survives dropna,
counts toward `n`, and poisons every statistic that sums or squares, which the
response sanitiser then writes as `null` -- the same token used for "not
computed" and "empty column". Eleven figures went null beside `null_count: 0`
with nothing to explain them.

Worse, `shapiro_p` stripped NaN with `~np.isnan` and kept inf. scipy does not
raise on inf; it returns `W=nan, p=1.0`. So `p > 0.05` was true and the tool
announced

    "distribution_hint": "likely normal (Shapiro p>1.00)"

-- a p-value that cannot exist, asserting the reverse of the truth, decided by
four rows in sixteen thousand. With the infinities dropped: p = 5.359e-65.

**The grammar.** `derive` is typed `list[dict]` with `additionalProperties`, so
its shape is invisible. Each error named the single next missing key, and the
"needs a key" message listed the keys the *caller* had sent, which reads as
confirmation they were right. Five round trips to write one ratio.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pandas as pd
import pytest
from mcp.types import CallToolResult

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_basic"), str(ROOT / "servers" / "data_transform")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from shared.derive_ops import DeriveError, apply_derivations, grammar_for  # noqa: E402
from shared.small_sample import finite_split, shapiro_p  # noqa: E402


def _call(server, name: str, arguments: dict) -> dict:
    """Dispatch the way the server does, result conversion included.

    Through `call_tool`, never the wrapper's `.fn`: the guard under test lives
    in `call_tool`, so calling the function directly would test nothing and
    pass.
    """
    result = asyncio.run(server.mcp._tool_manager.call_tool(name, arguments, convert_result=True))
    CallToolResult(content=list(result))
    return json.loads(result[0].text)


@pytest.fixture(scope="module")
def basic():
    import servers.data_basic.server as mod

    return mod


@pytest.fixture
def csv(tmp_path: Path) -> str:
    path = tmp_path / "ads.csv"
    pd.DataFrame(
        {
            "platform": ["g", "f"] * 30,
            "clicks": list(range(60)),
            "impressions": [0, 0] + list(range(2, 60)),
        }
    ).to_csv(path, index=False)
    return str(path)


class TestAnArgumentNoToolDeclaresIsRefused:
    def test_a_wholly_invented_name(self, basic, csv):
        out = _call(basic, "inspect_dataset", {"file_path": csv, "banana": "yes"})
        assert out["success"] is False
        assert "banana" in out["error"]

    def test_the_refusal_lists_what_the_tool_does_take(self, basic, csv):
        out = _call(basic, "inspect_dataset", {"file_path": csv, "banana": "yes"})
        assert "file_path" in out["hint"] and "include_sample" in out["hint"]

    def test_a_near_miss_gets_a_suggestion(self, basic, csv):
        out = _call(basic, "read_column_stats", {"file_path": csv, "columns": "clicks"})
        assert out["success"] is False
        assert "column" in out["hint"]

    def test_apply_patch_refuses_output_path_rather_than_overwriting(self, basic, csv, tmp_path):
        """The finding, exactly: this call used to edit the source and say success."""
        before = Path(csv).read_text(encoding="utf-8")
        out = _call(
            basic,
            "apply_patch",
            {
                "file_path": csv,
                "ops": [{"op": "drop_column", "columns": ["impressions"]}],
                "output_path": str(tmp_path / "elsewhere.csv"),
            },
        )
        assert out["success"] is False, "output_path was accepted; the source is being edited"
        assert "output_path" in out["error"]
        assert Path(csv).read_text(encoding="utf-8") == before, "the source file was modified anyway"

    def test_and_the_docstring_now_says_it_edits_in_place(self, basic):
        doc = basic.apply_patch.__doc__ or ""
        assert "in place" in doc.lower()

    def test_a_correct_call_is_untouched(self, basic, csv):
        out = _call(basic, "inspect_dataset", {"file_path": csv})
        assert out["success"] is True


class TestAnInfinityIsNotANull:
    @pytest.fixture
    def rates(self, tmp_path: Path) -> str:
        path = tmp_path / "rates.csv"
        # clicks starts at 1 so the two zero-impression rows give inf, not the
        # 0/0 NaN that would make this a test about nulls instead.
        frame = pd.DataFrame({"clicks": list(range(1, 61)), "impressions": [0, 0] + list(range(2, 60))})
        frame["ctr"] = frame["clicks"] / frame["impressions"]
        frame.to_csv(path, index=False)
        return str(path)

    def test_read_column_stats_counts_them(self, basic, rates):
        out = _call(basic, "read_column_stats", {"file_path": rates, "column": "ctr"})
        assert out["success"] is True
        assert out["non_finite_count"] == 2
        assert out["null_count"] == 0

    def test_and_says_why_the_statistics_are_null(self, basic, rates):
        out = _call(basic, "read_column_stats", {"file_path": rates, "column": "ctr"})
        assert out["mean"] is None, "precondition: the mean is not computable here"
        assert "infinite" in out["not_computed"]

    def test_a_clean_column_says_nothing(self, basic, rates):
        out = _call(basic, "read_column_stats", {"file_path": rates, "column": "clicks"})
        assert out["non_finite_count"] == 0
        assert out["not_computed"] == ""

    def test_testable_sample_splits_finite_from_the_rest(self):
        series = pd.Series([1.0, 2.0, float("inf"), float("nan"), -float("inf")])
        assert finite_split(series) == (2, 3)


class TestFourRowsCannotDecideANormalityVerdict:
    @pytest.fixture
    def skewed_with_infinities(self):
        import numpy as np

        rng = np.random.default_rng(0)
        values = list(rng.exponential(scale=1.0, size=4000))
        return pd.Series(values + [float("inf")] * 4)

    def test_shapiro_ignores_the_infinities(self, skewed_with_infinities):
        from scipy import stats

        p = shapiro_p(skewed_with_infinities.to_numpy(), stats)
        assert p is not None
        assert p < 0.05, f"an exponential sample was called normal (p={p})"

    def test_a_p_value_above_one_is_impossible_and_must_not_appear(self, skewed_with_infinities):
        from scipy import stats

        p = shapiro_p(skewed_with_infinities.to_numpy(), stats)
        assert 0.0 <= p <= 1.0

    def test_the_finite_sample_gives_the_same_answer(self, skewed_with_infinities):
        """Dropping 4 infinities must not change the verdict the data supports."""
        import numpy as np
        from scipy import stats

        array = skewed_with_infinities.to_numpy()
        with_inf = shapiro_p(array, stats)
        without = shapiro_p(array[np.isfinite(array)], stats)
        assert (with_inf < 0.05) == (without < 0.05)

    def test_too_few_values_still_returns_none(self):
        from scipy import stats

        assert shapiro_p(pd.Series([1.0, 2.0]).to_numpy(), stats) is None

    def test_a_column_that_is_all_infinite_is_undetermined_not_normal(self):
        from scipy import stats

        assert shapiro_p(pd.Series([float("inf")] * 50).to_numpy(), stats) is None


class TestTheDeriveGrammarIsTaughtAtOnce:
    FRAME = pd.DataFrame({"clicks": [1, 2, 3], "impressions": [10, 20, 30]})

    def _error(self, spec: dict) -> str:
        with pytest.raises(DeriveError) as exc:
            apply_derivations(self.FRAME.copy(), [spec])
        return str(exc.value)

    def test_a_missing_key_names_the_ops_whole_spec(self):
        message = self._error({"name": "ctr", "op": "arith", "column": "clicks", "other_column": "impressions"})
        # The key the caller actually needs -- and could not previously see,
        # because the message listed the keys they had already sent.
        assert "'other'" in message
        assert "add|sub|mul|div|floordiv|mod" in message

    def test_it_does_not_echo_the_callers_own_keys_back(self):
        message = self._error({"name": "ctr", "op": "arith", "column": "clicks", "other_column": "impressions"})
        assert "other_column" not in message, "the caller's wrong key was quoted back as if it were right"

    def test_a_bad_how_value_also_carries_the_grammar(self):
        message = self._error({"name": "c", "op": "arith", "how": "divide", "column": "clicks", "other": "impressions"})
        assert "Valid: add, div, floordiv, mod, mul, sub" in message
        assert "'other'" in message

    def test_an_unknown_op_names_every_grammar(self):
        message = self._error({"name": "c", "expr": "a/b"})
        for op in ("arith", "compare", "date_part", "parse_date", "text"):
            assert op in message

    def test_the_documented_spec_actually_works(self):
        """A grammar line that documents a key the dispatch ignores is the bug one level up."""
        frame = self.FRAME.copy()
        added, _ = apply_derivations(
            frame, [{"name": "ctr", "op": "arith", "column": "clicks", "how": "div", "other": "impressions"}]
        )
        assert added == ["ctr"]
        assert frame["ctr"].tolist() == [0.1, 0.1, 0.1]

    @pytest.mark.parametrize("op", ["parse_date", "date_part", "arith", "compare", "text"])
    def test_every_op_has_a_grammar_line(self, op):
        assert op in grammar_for(op)

    def test_list_derive_ops_returns_them_all(self):
        import servers.data_transform.engine as transform_engine

        out = transform_engine.list_derive_ops()
        assert out["success"] is True
        assert {entry["op"] for entry in out["ops"]} == {
            "parse_date",
            "date_part",
            "arith",
            "compare",
            "text",
        }

    def test_and_its_example_is_a_spec_that_runs(self):
        import servers.data_transform.engine as transform_engine

        frame = self.FRAME.copy()
        added, _ = apply_derivations(frame, [transform_engine.list_derive_ops()["example"]])
        assert added == ["ctr"]
