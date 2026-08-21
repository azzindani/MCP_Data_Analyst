"""run_cleaning_pipeline was the only transform tool that could not redirect output.

Nine of the ten tools on the transform server take `output_path`; this one did
not, so it could only rewrite the file it was given. A coverage sweep passed
`output_path` -- the reasonable assumption, having just used it on the nine
siblings -- and got a hard "unexpected keyword argument" error.

The in-place *default* is deliberate and unchanged: this tool snapshots first
and is meant to advance the file it is handed. What was missing was the choice.

Two traps this pins down:

  * the MCP wrapper called engine.run_cleaning_pipeline(file_path, ops, dry_run)
    positionally, so inserting output_path in the sibling position would have
    silently bound dry_run to output_path -- a dry run would have written a file
    named "True"
  * the failure path called _restore(path, backup) unconditionally, and backup
    is None when the source is not the target
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from servers.data_medium.engine import run_cleaning_pipeline


@pytest.fixture()
def csv(tmp_path: Path) -> Path:
    p = tmp_path / "dirty.csv"
    pd.DataFrame(
        {
            "Region": [" north ", "SOUTH", " north ", "east"],
            "Revenue": [100.0, None, 100.0, 250.0],
        }
    ).to_csv(p, index=False)
    return p


# clean_text with scope "both" normalises headers as well as values, so the
# column names here are already in the form it produces -- otherwise fill_nulls
# would be looking for a column clean_text had just renamed.
CLEAN = [
    {"op": "clean_text", "scope": "both"},
    {"op": "fill_nulls", "column": "Revenue", "strategy": "median"},
]


class TestItCanWriteSomewhereElse:
    def test_output_path_is_accepted_at_all(self, csv: Path, tmp_path: Path):
        out = tmp_path / "clean.csv"
        result = run_cleaning_pipeline(str(csv), CLEAN, output_path=str(out))
        assert result["success"] is True, result.get("error")

    def test_the_new_file_is_written(self, csv: Path, tmp_path: Path):
        out = tmp_path / "clean.csv"
        run_cleaning_pipeline(str(csv), CLEAN, output_path=str(out))
        assert out.is_file()
        assert pd.read_csv(out)["Revenue"].isna().sum() == 0

    def test_the_source_is_left_alone(self, csv: Path, tmp_path: Path):
        before = csv.read_bytes()
        run_cleaning_pipeline(str(csv), CLEAN, output_path=str(tmp_path / "clean.csv"))
        assert csv.read_bytes() == before, "redirecting output must not rewrite the input"

    def test_no_snapshot_is_taken_when_the_source_is_untouched(self, csv: Path, tmp_path: Path):
        result = run_cleaning_pipeline(str(csv), CLEAN, output_path=str(tmp_path / "clean.csv"))
        assert result["backup"] is None

    def test_the_result_names_where_it_went(self, csv: Path, tmp_path: Path):
        out = tmp_path / "clean.csv"
        result = run_cleaning_pipeline(str(csv), CLEAN, output_path=str(out))
        assert result["output_path"] == str(out)
        assert result["output_file"] == "clean.csv"


class TestTheInPlaceDefaultIsUnchanged:
    """Existing callers rely on this; the fix is additive, not a behaviour change."""

    def test_it_still_cleans_in_place(self, csv: Path):
        result = run_cleaning_pipeline(str(csv), CLEAN)
        assert result["success"] is True
        assert pd.read_csv(csv)["Revenue"].isna().sum() == 0

    def test_it_still_snapshots_first(self, csv: Path):
        result = run_cleaning_pipeline(str(csv), CLEAN)
        assert result["backup"], "an in-place clean must remain recoverable"

    def test_output_path_reports_the_source(self, csv: Path):
        result = run_cleaning_pipeline(str(csv), CLEAN)
        assert result["output_path"] == str(csv)


class TestDryRunSaysWhereItWouldWrite:
    def test_in_place_dry_run(self, csv: Path):
        result = run_cleaning_pipeline(str(csv), CLEAN, dry_run=True)
        assert result["dry_run"] is True
        assert result["output_path"] == str(csv)

    def test_redirected_dry_run(self, csv: Path, tmp_path: Path):
        out = tmp_path / "clean.csv"
        result = run_cleaning_pipeline(str(csv), CLEAN, output_path=str(out), dry_run=True)
        assert result["dry_run"] is True
        assert result["output_path"] == str(out)

    def test_a_dry_run_writes_nothing(self, csv: Path, tmp_path: Path):
        out = tmp_path / "clean.csv"
        before = csv.read_bytes()
        run_cleaning_pipeline(str(csv), CLEAN, output_path=str(out), dry_run=True)
        assert not out.exists()
        assert csv.read_bytes() == before


class TestTheWrapperDoesNotMisbindArguments:
    """engine.run_cleaning_pipeline(file_path, ops, dry_run) passed dry_run
    third. With output_path inserted in the sibling position that bool would
    land in output_path, and `resolve_path(True)` would write a file called
    "True" instead of doing a dry run."""

    def test_the_wrapper_passes_output_path_before_dry_run(self):
        import inspect

        from servers.data_transform import server

        # The decorated name is a FunctionTool; .fn is the function itself.
        params = list(inspect.signature(server.run_cleaning_pipeline.fn).parameters)
        assert params.index("output_path") < params.index("dry_run")

    def test_a_wrapper_dry_run_is_still_a_dry_run(self, csv: Path):
        from servers.data_transform import server

        before = csv.read_bytes()
        result = server.run_cleaning_pipeline.fn(str(csv), CLEAN, dry_run=True)
        assert result["dry_run"] is True
        assert csv.read_bytes() == before

    def test_the_signatures_agree(self):
        import inspect

        from servers.data_transform import server

        wrapper = list(inspect.signature(server.run_cleaning_pipeline.fn).parameters)
        engine_fn = list(inspect.signature(run_cleaning_pipeline).parameters)
        assert wrapper == engine_fn


class TestAFailedOpDoesNotCrashOnRollback:
    """_restore(path, backup) ran unconditionally, and backup is None when the
    source is not the target."""

    # Valid arguments, so validate_ops lets it through; the column simply does
    # not exist, so it raises inside the handler -- the path that used to call
    # _restore(path, None). (cast_column would not do: it coerces to NaN rather
    # than raising, so it never reaches the rollback at all.)
    BAD = [
        {"op": "clean_text", "scope": "both"},
        {"op": "fill_nulls", "column": "NoSuchColumn", "strategy": "median"},
    ]

    def test_redirected_failure_returns_cleanly(self, csv: Path, tmp_path: Path):
        result = run_cleaning_pipeline(str(csv), self.BAD, output_path=str(tmp_path / "clean.csv"))
        assert result["success"] is False
        assert "NoSuchColumn" in result["error"]

    def test_it_says_the_source_was_not_modified(self, csv: Path, tmp_path: Path):
        result = run_cleaning_pipeline(str(csv), self.BAD, output_path=str(tmp_path / "clean.csv"))
        assert "not modified" in result["hint"]
        assert "Restored from snapshot" not in result["hint"]

    def test_nothing_is_written_on_failure(self, csv: Path, tmp_path: Path):
        out = tmp_path / "clean.csv"
        before = csv.read_bytes()
        run_cleaning_pipeline(str(csv), self.BAD, output_path=str(out))
        assert not out.exists()
        assert csv.read_bytes() == before

    def test_in_place_failure_still_restores(self, csv: Path):
        result = run_cleaning_pipeline(str(csv), self.BAD)
        assert result["success"] is False
        assert "Restored from snapshot" in result["hint"]


class TestEveryTransformToolNowOffersTheSameChoice:
    """The gap was discoverable only by calling all ten. Pin it."""

    def test_no_transform_tool_lacks_output_path(self):
        import inspect

        from servers.data_transform import server

        missing = []
        for name in dir(server):
            tool = getattr(server, name)
            fn = getattr(tool, "fn", None)
            if fn is None or not callable(fn) or name.startswith("_"):
                continue
            params = inspect.signature(fn).parameters
            if "file_path" in params or "file_paths" in params:
                if "output_path" not in params:
                    missing.append(name)
        assert not missing, f"these write a dataset but cannot redirect it: {missing}"
