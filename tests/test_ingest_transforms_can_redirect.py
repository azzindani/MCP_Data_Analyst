"""The three ingest cleaners could only rewrite the file they were given.

normalize_headers, trim_empty and promote_header each read a CSV, changed it,
and wrote the result back over the caller's source. That is snapshotted and
recoverable, so it was never data loss -- but it made "clean this messy export
into a tidy copy and leave the original alone" impossible, which is the whole
job of an ingest server. Every one of the other seven data-ingest tools already
took a destination, as does every transform on data_transform.

The rule matches run_cleaning_pipeline and the ml_medium label writers, so all
four now behave the same way:

    output_path=""      rewritten in place, snapshot first   (unchanged default)
    output_path=X       written to X, source untouched, no snapshot

No snapshot when a destination is named: only the source needs saving from
itself, and a stray backup of a file the caller did not own is noise.

The wrappers passed dry_run positionally in the slot output_path now occupies.
Inserting the parameter without updating them would have bound the dry_run bool
to output_path, so a dry run would have written a file literally named "True" --
the same trap that came with the same fix on run_cleaning_pipeline, run_clustering
and anomaly_detection. TestTheWrappersDoNotMisbindArguments is what catches it.
"""

from __future__ import annotations

import inspect
from pathlib import Path

import pandas as pd
import pytest

from servers.data_ingest import engine

MESSY = "  Total Spends , Campaign Type ,Clicks\n10,Awareness,3\n20,Performance,4\n"


@pytest.fixture()
def messy(tmp_path: Path) -> Path:
    p = tmp_path / "export.csv"
    p.write_text(MESSY, encoding="utf-8")
    return p


def _tool(name: str):
    """DA's @mcp.tool() returns a FunctionTool; the plain function is on .fn."""
    from servers.data_ingest import server

    tool = getattr(server, name)
    return getattr(tool, "fn", tool)


class TestNormalizeHeadersCanRedirect:
    def test_it_writes_the_clean_copy_elsewhere(self, messy: Path, tmp_path: Path):
        out = tmp_path / "clean.csv"
        r = engine.normalize_headers(str(messy), output_path=str(out))
        assert r["success"] is True, r.get("error")
        assert list(pd.read_csv(out).columns) == ["total_spends", "campaign_type", "clicks"]

    def test_the_source_is_untouched(self, messy: Path, tmp_path: Path):
        engine.normalize_headers(str(messy), output_path=str(tmp_path / "clean.csv"))
        assert messy.read_text(encoding="utf-8") == MESSY

    def test_no_snapshot_when_the_source_is_not_the_target(self, messy: Path, tmp_path: Path):
        r = engine.normalize_headers(str(messy), output_path=str(tmp_path / "clean.csv"))
        assert r["backup"] == ""

    def test_it_reports_where_the_output_went(self, messy: Path, tmp_path: Path):
        out = tmp_path / "clean.csv"
        r = engine.normalize_headers(str(messy), output_path=str(out))
        assert r["output_path"] == str(out)

    def test_in_place_is_still_the_default(self, messy: Path):
        r = engine.normalize_headers(str(messy))
        assert r["success"] is True, r.get("error")
        assert list(pd.read_csv(messy).columns) == ["total_spends", "campaign_type", "clicks"]

    def test_an_in_place_rewrite_still_snapshots(self, messy: Path):
        assert engine.normalize_headers(str(messy))["backup"], "an in-place rewrite must stay recoverable"


class TestTrimEmptyCanRedirect:
    @pytest.fixture()
    def padded(self, tmp_path: Path) -> Path:
        p = tmp_path / "padded.csv"
        p.write_text("a,b,c\n1,,3\n,,\n2,,4\n", encoding="utf-8")
        return p

    def test_it_writes_the_trimmed_copy_elsewhere(self, padded: Path, tmp_path: Path):
        out = tmp_path / "trimmed.csv"
        r = engine.trim_empty(str(padded), output_path=str(out))
        assert r["success"] is True, r.get("error")
        assert out.is_file()
        assert r["rows_dropped"] == 1

    def test_the_source_is_untouched(self, padded: Path, tmp_path: Path):
        before = padded.read_bytes()
        engine.trim_empty(str(padded), output_path=str(tmp_path / "trimmed.csv"))
        assert padded.read_bytes() == before

    def test_no_snapshot_when_the_source_is_not_the_target(self, padded: Path, tmp_path: Path):
        assert engine.trim_empty(str(padded), output_path=str(tmp_path / "t.csv"))["backup"] == ""

    def test_in_place_is_still_the_default(self, padded: Path):
        r = engine.trim_empty(str(padded))
        assert r["success"] is True, r.get("error")
        assert len(pd.read_csv(padded)) == 2
        assert r["backup"]


class TestPromoteHeaderCanRedirect:
    @pytest.fixture()
    def preamble(self, tmp_path: Path) -> Path:
        p = tmp_path / "preamble.csv"
        p.write_text("Quarterly export,,\nspends,clicks,device\n10,3,mobile\n20,4,desktop\n", encoding="utf-8")
        return p

    def test_it_writes_the_promoted_copy_elsewhere(self, preamble: Path, tmp_path: Path):
        out = tmp_path / "promoted.csv"
        r = engine.promote_header(str(preamble), row_index=1, output_path=str(out))
        assert r["success"] is True, r.get("error")
        assert list(pd.read_csv(out).columns) == ["spends", "clicks", "device"]

    def test_the_source_is_untouched(self, preamble: Path, tmp_path: Path):
        before = preamble.read_bytes()
        engine.promote_header(str(preamble), row_index=1, output_path=str(tmp_path / "p.csv"))
        assert preamble.read_bytes() == before

    def test_no_snapshot_when_the_source_is_not_the_target(self, preamble: Path, tmp_path: Path):
        r = engine.promote_header(str(preamble), row_index=1, output_path=str(tmp_path / "p.csv"))
        assert r["backup"] == ""

    def test_in_place_is_still_the_default(self, preamble: Path):
        r = engine.promote_header(str(preamble), row_index=1)
        assert r["success"] is True, r.get("error")
        assert list(pd.read_csv(preamble).columns) == ["spends", "clicks", "device"]
        assert r["backup"]


class TestTheWrappersDoNotMisbindArguments:
    NAMES = ["normalize_headers", "trim_empty", "promote_header"]

    @pytest.mark.parametrize("name", NAMES)
    def test_output_path_precedes_dry_run(self, name: str):
        params = list(inspect.signature(_tool(name)).parameters)
        assert params.index("output_path") < params.index("dry_run")

    @pytest.mark.parametrize("name", NAMES)
    def test_the_wrapper_and_engine_agree(self, name: str):
        wrapper = list(inspect.signature(_tool(name)).parameters)
        assert wrapper == list(inspect.signature(getattr(engine, name)).parameters)

    @pytest.mark.parametrize("name", NAMES)
    def test_a_wrapper_dry_run_writes_nothing(self, name: str, messy: Path):
        before = messy.read_bytes()
        r = _tool(name)(str(messy), dry_run=True)
        assert r.get("dry_run") is True
        assert messy.read_bytes() == before

    @pytest.mark.parametrize("name", NAMES)
    def test_a_dry_run_says_where_it_would_have_written(self, name: str, messy: Path, tmp_path: Path):
        out = tmp_path / "would.csv"
        assert _tool(name)(str(messy), output_path=str(out), dry_run=True)["would_write"] == str(out)


class TestNoIngestToolRewritesWithoutAsking:
    """These three were the gap. Fail if a fourth ever appears without a
    destination -- this is the fourth time the same omission has been fixed
    (run_cleaning_pipeline, run_clustering, anomaly_detection, then these)."""

    def test_every_writer_offers_a_destination(self):
        from servers.data_ingest import server

        missing = []
        for name in dir(server):
            if name.startswith("_"):
                continue
            fn = getattr(getattr(server, name), "fn", None)
            if fn is None or not callable(fn):
                continue
            params = inspect.signature(fn).parameters
            # dry_run marks a tool that writes; a writer must let the caller
            # choose where, or it can only ever overwrite its own input.
            if "dry_run" in params and "output_path" not in params and "output_dir" not in params:
                missing.append(name)
        assert not missing, f"these rewrite the caller's file with no way to redirect: {missing}"
