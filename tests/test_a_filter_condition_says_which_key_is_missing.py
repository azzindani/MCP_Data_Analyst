"""A filter condition with the wrong key name was blamed on the data.

    filter_dataset(conditions=[{"variable": "spends", "op": "gt", "value": 0}])
      error: "Column '' not found. Available: ['Date', 'product', 'phase', ...]"

The caller never asked for a column called "". That empty string is the tool's
own default for a key it did not find, quoted back as though the caller had
named it — the same shape as delete_paragraph answering with an index nobody
sent, and as a line range reported with clamped bounds. Then it listed all
sixteen real column names, which reads as "yours is not among these" when the
column was right there and only the key was wrong.

`conditions` is a bare list[dict] in every schema that takes one, so the key
names live nowhere a caller can read them. `op` had always accepted `operator`
as an alias; the column key accepted exactly one spelling. Now it accepts the
obvious ones, and a condition naming no column at all says so and lists the keys
it did carry.

Three sites: data_transform.filter_dataset and both condition paths in
data_medium.filter_rows.

Found in the notes column of a round-8 sweep report that recorded the tool PASS.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from servers.data_medium import engine as dm
from servers.data_transform import engine as dt
from shared.column_utils import COLUMN_KEYS, condition_column, missing_column_error

ALIASES = ["column", "col", "field", "variable", "name", "column_name"]


@pytest.fixture()
def csv_path(ad_data_full_csv: Path) -> str:
    return str(ad_data_full_csv)


@pytest.fixture()
def out_path(tmp_path: Path) -> str:
    return str(tmp_path / "filtered.csv")


class TestTheSweepsCondition:
    def test_variable_is_accepted_by_filter_dataset(self, csv_path: str, out_path: str):
        r = dt.filter_dataset(csv_path, [{"variable": "spends", "op": "gt", "value": 0}], output_path=out_path)
        assert r["success"] is True, r.get("error")

    def test_variable_is_accepted_by_filter_rows(self, csv_path: str, out_path: str):
        r = dm.filter_rows(csv_path, [{"variable": "spends", "op": "gt", "value": 0}], output_path=out_path)
        assert r["success"] is True, r.get("error")

    def test_it_filters_the_same_rows_as_the_canonical_key(self, csv_path: str, tmp_path: Path):
        a = dt.filter_dataset(
            csv_path, [{"column": "spends", "op": "gt", "value": 0}], output_path=str(tmp_path / "a.csv")
        )
        b = dt.filter_dataset(
            csv_path, [{"variable": "spends", "op": "gt", "value": 0}], output_path=str(tmp_path / "b.csv")
        )
        assert a["success"] and b["success"]
        assert Path(tmp_path / "a.csv").read_bytes() == Path(tmp_path / "b.csv").read_bytes()


class TestEverySpellingWorks:
    @pytest.mark.parametrize("key", ALIASES)
    def test_filter_dataset_accepts_it(self, csv_path: str, out_path: str, key: str):
        r = dt.filter_dataset(csv_path, [{key: "spends", "op": "gt", "value": 0}], output_path=out_path)
        assert r["success"] is True, (key, r.get("error"))

    @pytest.mark.parametrize("key", ALIASES)
    def test_filter_rows_accepts_it(self, csv_path: str, out_path: str, key: str):
        r = dm.filter_rows(csv_path, [{key: "spends", "op": "gt", "value": 0}], output_path=out_path)
        assert r["success"] is True, (key, r.get("error"))

    def test_the_documented_list_and_the_code_agree(self):
        assert list(COLUMN_KEYS) == ALIASES


class TestAConditionWithNoColumnSaysSo:
    CONDITION = [{"op": "gt", "value": 0}]

    def test_filter_dataset_refuses(self, csv_path: str, out_path: str):
        r = dt.filter_dataset(csv_path, self.CONDITION, output_path=out_path)
        assert r["success"] is False

    def test_it_does_not_quote_back_an_empty_column(self, csv_path: str, out_path: str):
        r = dt.filter_dataset(csv_path, self.CONDITION, output_path=out_path)
        assert "Column ''" not in r["error"], r["error"]

    def test_it_lists_the_keys_the_caller_did_send(self, csv_path: str, out_path: str):
        r = dt.filter_dataset(csv_path, self.CONDITION, output_path=out_path)
        blob = f"{r['error']} {r.get('hint', '')}"
        assert "op" in blob and "value" in blob, blob

    def test_filter_rows_says_the_same_thing(self, csv_path: str, out_path: str):
        r = dm.filter_rows(csv_path, self.CONDITION, output_path=out_path)
        assert r["success"] is False
        assert "Column ''" not in r["error"], r["error"]
        assert "names no column" in r["error"], r["error"]

    def test_the_hint_shows_a_working_condition(self, csv_path: str, out_path: str):
        r = dm.filter_rows(csv_path, self.CONDITION, output_path=out_path)
        assert '"column"' in r["hint"] and '"op"' in r["hint"], r["hint"]


class TestARealColumnMistakeStillReadsAsOne:
    def test_filter_dataset_names_the_column_and_lists_the_real_ones(self, csv_path: str, out_path: str):
        r = dt.filter_dataset(csv_path, [{"column": "nope", "op": "gt", "value": 0}], output_path=out_path)
        assert r["success"] is False
        assert "nope" in r["error"], r["error"]
        assert "spends" in r["error"] or "spends" in r.get("hint", ""), r

    def test_filter_rows_does_too(self, csv_path: str, out_path: str):
        r = dm.filter_rows(csv_path, [{"column": "nope", "op": "gt", "value": 0}], output_path=out_path)
        assert r["success"] is False
        assert "nope" in r["error"], r["error"]


class TestTheHelperItself:
    def test_the_first_spelling_present_wins(self):
        assert condition_column({"column": "a", "variable": "b"}) == "a"

    def test_a_blank_value_is_not_a_column(self):
        assert condition_column({"column": "  ", "field": "spends"}) == "spends"

    def test_a_non_string_value_is_not_a_column(self):
        assert condition_column({"column": 3, "field": "spends"}) == "spends"

    def test_an_empty_condition_reports_none(self):
        error, _hint = missing_column_error({})
        assert "none" in error, error
