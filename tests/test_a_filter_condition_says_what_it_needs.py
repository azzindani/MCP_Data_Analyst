"""Both filter tools failed the documented `between` op with a one-word error.

filter_dataset's docstring is "Filter rows by conditions + sort. ops: equals
isin between regex date_range." It names the ops; nothing anywhere names the
*keys* each op reads, and they are not the same key. Most read "value"; isin
reads "values"; between reads "min" and "max"; regex reads "pattern". A call
with the documented op and the obvious key came back as:

    filter_dataset  {"column": "spends", "op": "between", "value": [0, 10000]}
      error: 'min'
      hint:  Valid filter ops: between, contains, date_range, ...

The whole error was the name of a dict key, from a bare KeyError, and the hint
listed the ops -- which were already right. filter_rows in data-medium, doing
the same job, failed the same call with

    float() argument must be a string or a real number, not 'list'

filter_rows already fell back to "value" for isin, between and regex, with
comments saying so; filter_dataset did not fall back at all. Both now accept
every spelling a caller reasonably reaches for, and when the bounds really are
absent they say which key is missing and show the shape of the condition.
"""

from __future__ import annotations

import csv as csvmod
from pathlib import Path

import pytest

from servers.data_medium.engine import filter_rows
from servers.data_transform.engine import filter_dataset

FILTERS = [pytest.param(filter_dataset, id="filter_dataset"), pytest.param(filter_rows, id="filter_rows")]


@pytest.fixture()
def csv_path(tmp_path: Path) -> str:
    path = tmp_path / "campaigns.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csvmod.writer(fh)
        w.writerow(["campaign_platform", "spends"])
        w.writerow(["Google Ads", 100])
        w.writerow(["Google Ads", 5000])
        w.writerow(["Facebook Ads", 20000])
    return str(path)


def run(fn, csv_path: str, out: Path, conditions: list[dict]) -> dict:
    kwargs = {"output_path": str(out)}
    if fn is filter_rows:
        kwargs["open_after"] = False
    return fn(csv_path, conditions, **kwargs)


def rows(out: Path) -> list[list[str]]:
    with out.open(newline="", encoding="utf-8") as fh:
        return list(csvmod.reader(fh))[1:]


class TestBetweenTakesARangeHoweverItIsWritten:
    @pytest.mark.parametrize("fn", FILTERS)
    def test_a_two_item_value_list(self, fn, csv_path: str, tmp_path: Path):
        out = tmp_path / "o.csv"
        r = run(fn, csv_path, out, [{"column": "spends", "op": "between", "value": [0, 10000]}])
        assert r["success"] is True, r.get("error")
        assert len(rows(out)) == 2, rows(out)

    @pytest.mark.parametrize("fn", FILTERS)
    def test_explicit_min_and_max(self, fn, csv_path: str, tmp_path: Path):
        out = tmp_path / "o.csv"
        r = run(fn, csv_path, out, [{"column": "spends", "op": "between", "min": 0, "max": 10000}])
        assert r["success"] is True, r.get("error")
        assert len(rows(out)) == 2, rows(out)

    @pytest.mark.parametrize("fn", FILTERS)
    def test_the_two_spellings_agree(self, fn, csv_path: str, tmp_path: Path):
        a, b = tmp_path / "a.csv", tmp_path / "b.csv"
        run(fn, csv_path, a, [{"column": "spends", "op": "between", "value": [0, 10000]}])
        run(fn, csv_path, b, [{"column": "spends", "op": "between", "min": 0, "max": 10000}])
        assert rows(a) == rows(b)


class TestIsinAndRegexTakeValueToo:
    @pytest.mark.parametrize("fn", FILTERS)
    def test_isin_with_value(self, fn, csv_path: str, tmp_path: Path):
        out = tmp_path / "o.csv"
        r = run(fn, csv_path, out, [{"column": "campaign_platform", "op": "isin", "value": ["Google Ads"]}])
        assert r["success"] is True, r.get("error")
        assert len(rows(out)) == 2, rows(out)

    @pytest.mark.parametrize("fn", FILTERS)
    def test_isin_with_values(self, fn, csv_path: str, tmp_path: Path):
        out = tmp_path / "o.csv"
        r = run(fn, csv_path, out, [{"column": "campaign_platform", "op": "isin", "values": ["Facebook Ads"]}])
        assert r["success"] is True, r.get("error")
        assert len(rows(out)) == 1, rows(out)

    @pytest.mark.parametrize("fn", FILTERS)
    def test_regex_with_value(self, fn, csv_path: str, tmp_path: Path):
        out = tmp_path / "o.csv"
        r = run(fn, csv_path, out, [{"column": "campaign_platform", "op": "regex", "value": "Google.*"}])
        assert r["success"] is True, r.get("error")
        assert len(rows(out)) == 2, rows(out)


class TestWhenTheKeyIsGenuinelyMissing:
    @pytest.mark.parametrize("fn", FILTERS)
    def test_it_fails(self, fn, csv_path: str, tmp_path: Path):
        r = run(fn, csv_path, tmp_path / "o.csv", [{"column": "spends", "op": "between"}])
        assert r["success"] is False

    @pytest.mark.parametrize("fn", FILTERS)
    def test_the_error_is_a_sentence_not_a_key_name(self, fn, csv_path: str, tmp_path: Path):
        error = run(fn, csv_path, tmp_path / "o.csv", [{"column": "spends", "op": "between"}])["error"]
        assert error.strip() not in ("'min'", "'max'", "'value'", "'values'", "'pattern'")
        assert len(error.split()) > 5, error

    @pytest.mark.parametrize("fn", FILTERS)
    def test_the_error_names_the_missing_keys(self, fn, csv_path: str, tmp_path: Path):
        error = run(fn, csv_path, tmp_path / "o.csv", [{"column": "spends", "op": "between"}])["error"]
        assert "min" in error and "max" in error, error

    @pytest.mark.parametrize("fn", FILTERS)
    def test_no_python_type_error_reaches_the_caller(self, fn, csv_path: str, tmp_path: Path):
        error = run(fn, csv_path, tmp_path / "o.csv", [{"column": "spends", "op": "between"}])["error"]
        assert "float()" not in error and "argument must be" not in error, error


class TestTheOtherOpsAreUnchanged:
    @pytest.mark.parametrize("fn", FILTERS)
    def test_equals_still_works(self, fn, csv_path: str, tmp_path: Path):
        out = tmp_path / "o.csv"
        r = run(fn, csv_path, out, [{"column": "campaign_platform", "op": "equals", "value": "Facebook Ads"}])
        assert r["success"] is True, r.get("error")
        assert len(rows(out)) == 1

    @pytest.mark.parametrize("fn", FILTERS)
    def test_an_unknown_op_is_still_rejected(self, fn, csv_path: str, tmp_path: Path):
        r = run(fn, csv_path, tmp_path / "o.csv", [{"column": "spends", "op": "roughly", "value": 1}])
        assert r["success"] is False
        assert "roughly" in r["error"], r["error"]
