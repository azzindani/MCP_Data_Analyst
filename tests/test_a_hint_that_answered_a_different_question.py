"""The error named the column. The hint recited the ops.

Round 18's axis was: make each tool fail the way a careful caller plausibly
would, then do EXACTLY what the hint says -- because a model chaining tools has
nothing else to go on. Three tools in this repo failed it the same way.

    filter_dataset(conditions=[{"column": "nonexistent", ...}])
      error: Column 'nonexistent' not found. Available: ['Date', 'product', ...]
      hint : Valid filter ops: between, contains, date_range, ...

    run_workspace_pipeline(pipeline_name="nonexistent_pipeline")
      error: Pipeline 'nonexistent_pipeline' not found in workspace 'w'.
             Available: ['test_drop_column', 'test_drop_phase']
      hint : Use list_workspace_files() to check registered aliases.

    apply_patch(ops=[{"op": "drop_column", "colum": "product"}])
      error: Op 0 (drop_column): unknown field(s) colum -- did you mean
             columns? drop_column accepts: columns, op, params
      hint : Valid ops: abs_values, add_column, bin_column, ...

(The original example here was `column`, the singular. Round 28 found that
ml-medium's run_preprocessing runs a `drop_column` op too and spells it
`column`, so each server refused the other's spelling with a confident
correction. Both spellings work at all three tools now, and this example uses
a field name that is genuinely wrong.)

In all three the error is exactly right and the hint answers a question nobody
asked -- ops for a column, aliases for a pipeline, the op vocabulary for a
field name. The op the caller passed was already valid in two of them.

The sweep obeyed each one literally and each retry failed, which is the whole
point: a hint that names a specific WRONG fix is worse than a vague one,
because the caller acts on it. `run_workspace_pipeline`'s is the sharpest --
`list_workspace_files()` lists FILES, so the pipeline name it sends you to look
up is not in there at all, while the error had already printed both pipelines.

The shared cause is that the error is computed from what went wrong and the
hint is a constant picked by the call site. So the fix is one rule in
`hint_for_error` -- when the message already enumerates the alternatives, say
to use one of them -- plus routing the two constant hints through it, and one
narrowing in apply_patch so the op list appears only for an unknown op.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for p in (str(ROOT), str(ROOT / "shared"), str(ROOT / "servers")):
    if p not in sys.path:
        sys.path.insert(0, p)

from shared.file_utils import hint_for_error  # noqa: E402


@pytest.fixture
def csv(tmp_path) -> str:
    p = tmp_path / "d.csv"
    p.write_text("name,qty\na,1\nb,2\na,3\n")
    return str(p)


class TestTheRuleItself:
    def test_a_message_listing_alternatives_points_back_at_it(self):
        e = ValueError("Column 'nope' not found. Available: ['a', 'b']")
        h = hint_for_error(e, "Valid filter ops: between, contains")
        assert "error above" in h.lower(), h
        # The fallback must NOT win here -- that was the defect.
        assert "between" not in h, h

    def test_valid_colon_counts_too(self):
        e = ValueError("Unknown filter op 'nope'. Valid: between, contains")
        assert "error above" in hint_for_error(e, "some fallback").lower()

    def test_a_message_without_a_list_still_gets_the_fallback(self):
        # The rule must not swallow every error. With nothing enumerated, the
        # domain fallback is still the best answer available.
        e = RuntimeError("something else went wrong")
        assert hint_for_error(e, "Check mode and required parameters.") == "Check mode and required parameters."

    def test_the_other_branches_still_win_where_they_apply(self, tmp_path):
        assert "does not exist" in hint_for_error(FileNotFoundError(2, "nope"), "fallback")
        assert "Permission denied" in hint_for_error(PermissionError(13, "denied"), "fallback")


class TestFilterDataset:
    def test_a_bad_column_is_not_answered_with_the_op_list(self, csv):
        from data_transform import engine

        r = engine.filter_dataset(csv, [{"column": "nonexistent", "op": "equals", "value": "x"}])
        assert r["success"] is False
        assert "Valid filter ops" not in r["hint"], r["hint"]

    def test_the_error_still_lists_the_real_columns(self, csv):
        from data_transform import engine

        r = engine.filter_dataset(csv, [{"column": "nonexistent", "op": "equals", "value": "x"}])
        assert "name" in r["error"] and "qty" in r["error"]

    def test_a_valid_filter_still_runs(self, csv):
        from data_transform import engine

        r = engine.filter_dataset(csv, [{"column": "name", "op": "equals", "value": "a"}])
        assert r["success"] is True
        assert r["after_rows"] == 2


class TestApplyPatch:
    def test_a_bad_field_name_is_not_answered_with_the_op_list(self, csv):
        from data_basic import engine

        r = engine.apply_patch(csv, [{"op": "drop_column", "colum": "name"}])
        assert r["success"] is False
        assert not r["hint"].startswith("Valid ops:"), r["hint"]

    def test_the_error_still_suggests_the_right_field(self, csv):
        from data_basic import engine

        r = engine.apply_patch(csv, [{"op": "drop_column", "colum": "name"}])
        assert "columns" in r["error"]

    def test_the_ml_spelling_of_drop_column_is_accepted(self, csv):
        """`column` is what ml-medium's run_preprocessing calls this field.

        Refusing it here was the other half of a disagreement in which each
        server corrected the caller toward its own spelling and neither
        mentioned that the other existed.
        """
        from data_basic import engine

        r = engine.apply_patch(csv, [{"op": "drop_column", "column": "name"}])
        assert r["success"] is True, r.get("error")

    def test_the_documented_spelling_still_works(self, csv):
        from data_basic import engine

        r = engine.apply_patch(csv, [{"op": "drop_column", "columns": ["name"]}])
        assert r["success"] is True, r.get("error")

    def test_an_unknown_op_DOES_still_get_the_op_list(self, csv):
        # The narrowing must not go too far: when the op itself is wrong, the
        # vocabulary is exactly what the caller needs.
        from data_basic import engine

        r = engine.apply_patch(csv, [{"op": "not_a_real_op"}])
        assert r["success"] is False
        assert r["hint"].startswith("Valid ops:"), r["hint"]

    def test_a_valid_patch_still_applies(self, csv):
        from data_basic import engine

        r = engine.apply_patch(csv, [{"op": "drop_column", "columns": ["name"]}])
        assert r["success"] is True
