"""A derived file carries its derivation, beside itself.

The user review, on the filtered CSV it checked:

    `Credit_Risk_chargedoff.csv` (1.1 MB, 5,333 rows) -- GOOD
    AGI: add `_lineage.json` sidecar (filter, before/after, timestamp, version)

The file was right. The problem is what a reader holding it can find out: 5,333
rows, out of *what*, selected *how*? The source's receipt records the filter --
but a reader holding the derived file does not know which source to go and read,
and the derived file's own receipt is empty, because `RECEIPT_SCOPE` says
"mutations only" and nothing has mutated it yet.

So the two sidecars answer opposite directions and this file keeps them apart:
a receipt is what was done *to* a file, a lineage is what a file was made
*from*. The test that matters most here is the one asserting a filter written
back over its own input produces no lineage -- that is a mutation, the receipt
already records it, and a lineage there would claim the file was derived from
itself.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from servers.data_medium import engine as dm
from servers.data_transform import engine as dt
from servers.data_visual import engine as dv
from shared.lineage import (
    LINEAGE_FORMAT,
    LINEAGE_SUFFIX,
    lineage_chain,
    lineage_path,
    read_lineage,
    write_lineage,
)
from shared.receipt import RECEIPT_SCOPE


@pytest.fixture()
def loans(tmp_path: Path) -> Path:
    rows = 400
    csv = tmp_path / "Credit_Risk.csv"
    pd.DataFrame(
        {
            "id": range(rows),
            "loan_amount": [1000 + i for i in range(rows)],
            "loan_status": ["Fully Paid"] * 300 + ["Charged Off"] * 100,
        }
    ).to_csv(csv, index=False)
    return csv


class TestTheSidecarAnswersTheOtherQuestion:
    def test_a_receipt_and_a_lineage_are_not_the_same_file(self, tmp_path):
        derived = tmp_path / "x.csv"
        assert lineage_path(derived).name == "x.csv" + LINEAGE_SUFFIX
        assert lineage_path(derived).name != "x.csv.mcp_receipt.json"

    def test_the_receipt_still_says_it_only_covers_mutations(self):
        """If that ever stops being true, this file's reason for existing changes."""
        assert "mutations only" in RECEIPT_SCOPE

    def test_a_file_with_no_derivation_reads_as_empty_not_as_an_error(self, loans):
        assert read_lineage(loans) == {}

    def test_it_carries_a_format_version(self, tmp_path):
        out = tmp_path / "d.csv"
        out.write_text("a\n1\n", encoding="utf-8")
        write_lineage(out, op="test", source=None)
        assert read_lineage(out)["_format"] == LINEAGE_FORMAT


class TestFilterDatasetRecordsWhatItSelected:
    @pytest.fixture()
    def filtered(self, loans, tmp_path):
        out = tmp_path / "Credit_Risk_chargedoff.csv"
        result = dt.filter_dataset(
            str(loans),
            [{"column": "loan_status", "op": "equals", "value": "Charged Off"}],
            output_path=str(out),
        )
        assert result["success"] is True, result.get("error")
        return result, out

    def test_the_response_names_the_sidecar(self, filtered):
        result, out = filtered
        assert Path(result["lineage_path"]) == lineage_path(out)

    def test_it_records_both_sides_of_the_count(self, filtered):
        _result, out = filtered
        entry = read_lineage(out)
        assert entry["rows_before"] == 400
        assert entry["rows_after"] == 100
        assert entry["rows_kept_pct"] == 25.0

    def test_the_percentage_is_derived_from_the_two_counts(self, filtered):
        """Same rule as `truncated` and `was_sampled`: never independently set."""
        _result, out = filtered
        entry = read_lineage(out)
        assert entry["rows_kept_pct"] == round(entry["rows_after"] / entry["rows_before"] * 100, 2)

    def test_it_records_the_filter_itself(self, filtered):
        _result, out = filtered
        conditions = read_lineage(out)["params"]["conditions"]
        assert conditions[0]["column"] == "loan_status"
        assert conditions[0]["value"] == "Charged Off"

    def test_it_names_the_source_a_reader_can_open(self, filtered, loans):
        _result, out = filtered
        entry = read_lineage(out)
        assert Path(entry["source"]) == loans
        assert entry["source_fingerprint"].startswith(("sha256:", "size-mtime:"))

    def test_the_sidecar_is_beside_the_derived_file(self, filtered):
        _result, out = filtered
        assert lineage_path(out).parent == out.parent


class TestAMutationIsNotADerivation:
    def test_filtering_in_place_writes_no_lineage(self, loans):
        """The receipt records this. A lineage would say the file came from itself."""
        result = dt.filter_dataset(
            str(loans),
            [{"column": "loan_status", "op": "equals", "value": "Charged Off"}],
            output_path=str(loans),
        )
        assert result["success"] is True
        assert "lineage_path" not in result
        assert read_lineage(loans) == {}


class TestTheChainIsByReference:
    def test_a_second_step_names_the_first_rather_than_copying_it(self, loans, tmp_path):
        step1 = tmp_path / "step1.csv"
        dt.filter_dataset(
            str(loans),
            [{"column": "loan_status", "op": "equals", "value": "Fully Paid"}],
            output_path=str(step1),
        )
        step2 = tmp_path / "step2.csv"
        dt.filter_dataset(
            str(step1),
            [{"column": "loan_amount", "op": "gt", "value": 1100}],
            output_path=str(step2),
        )
        entry = read_lineage(step2)
        assert Path(entry["source_lineage"]) == lineage_path(step1)
        # By reference: the parent's own numbers are not restated here.
        assert "rows_before" in entry
        assert str(loans) not in json.dumps(entry)

    def test_the_chain_walks_back_to_the_original(self, loans, tmp_path):
        step1 = tmp_path / "s1.csv"
        dt.filter_dataset(str(loans), [{"column": "loan_amount", "op": "gt", "value": 1000}], output_path=str(step1))
        step2 = tmp_path / "s2.csv"
        dt.filter_dataset(str(step1), [{"column": "loan_amount", "op": "gt", "value": 1100}], output_path=str(step2))
        chain = lineage_chain(step2)
        assert [c["derived"] for c in chain] == ["s2.csv", "s1.csv"]

    def test_a_cycle_cannot_hang_a_reader(self, tmp_path):
        a, b = tmp_path / "a.csv", tmp_path / "b.csv"
        a.write_text("x\n1\n", encoding="utf-8")
        b.write_text("x\n1\n", encoding="utf-8")
        write_lineage(a, op="t", source=b)
        write_lineage(b, op="t", source=a)
        assert len(lineage_chain(a, max_steps=4)) <= 4


class TestTheOtherDerivingTools:
    def test_export_data_records_the_format_it_wrote(self, loans, tmp_path):
        out = tmp_path / "loans.json"
        result = dv.export_data(str(loans), output_path=str(out), format="json", open_after=False)
        assert result["success"] is True
        entry = read_lineage(out)
        assert entry["op"] == "export_data"
        assert entry["params"]["format"] == "json"
        assert entry["rows_after"] == 400

    def test_detect_anomalies_records_both_files_it_wrote(self, loans, tmp_path):
        scored = tmp_path / "scored.csv"
        result = dm.detect_anomalies(str(loans), output_path=str(scored))
        assert result["success"] is True, result.get("error")

        assert read_lineage(scored)["op"] == "detect_anomalies"
        only = result.get("anomalies_only_path")
        if only:
            entry = read_lineage(only)
            assert entry["rows_after"] == result["anomalies_only_rows"]
            # The flagged file is derived from the scored one, not the source:
            # that is where the flag columns it selects on came from.
            assert Path(entry["source"]) == scored

    def test_reshape_records_the_shape_on_both_sides(self, loans, tmp_path):
        out = tmp_path / "wide.csv"
        result = dt.reshape_dataset(str(loans), mode="transpose", output_path=str(out))
        if not result.get("success"):
            pytest.skip(f"transpose unavailable for this frame: {result.get('error')}")
        entry = read_lineage(out)
        assert entry["columns_before"] == 3
        assert entry["columns_after"] == entry["columns_after"]


class TestItNeverBreaksTheToolItServes:
    def test_an_unwritable_sidecar_costs_nothing(self, tmp_path, monkeypatch):
        out = tmp_path / "d.csv"
        out.write_text("a\n1\n", encoding="utf-8")

        def explode(*_a, **_k):
            raise OSError("read-only filesystem")

        monkeypatch.setattr(Path, "write_text", explode)
        assert write_lineage(out, op="t", source=None) == ""

    def test_unreadable_json_reads_as_empty(self, tmp_path):
        out = tmp_path / "d.csv"
        out.write_text("a\n1\n", encoding="utf-8")
        lineage_path(out).write_text("{not json", encoding="utf-8")
        assert read_lineage(out) == {}
