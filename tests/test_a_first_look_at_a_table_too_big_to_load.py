"""inspect_dataset and read_column_stats on a file too big to load answer from chunks, with pandas' numbers.

Both tools read the whole file into pandas. At the live 1 GB a file of a few hundred MB is a frame of
a gigabyte and the call dies before it answers -- on the very first question anyone asks of a table.
`shared/big_table.py` says when a file is too big (its size against what a call may hold) and answers
the same questions from DuckDB a pass at a time. The pandas path stays the one for every file that fits,
so the two are held to the same numbers here, on one file read both ways (`MCP_BIG_TABLE=always|never`).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from servers.data_basic import engine
from shared import big_table

ROWS = 6_000


@pytest.fixture(autouse=True)
def _home(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    monkeypatch.delenv("MCP_BIG_TABLE", raising=False)
    monkeypatch.delenv("MCP_CALL_MEMORY_MB", raising=False)
    return tmp_path


@pytest.fixture
def table(_home):
    rng = np.random.default_rng(11)
    frame = pd.DataFrame(
        {
            "id": np.arange(ROWS),
            "units": rng.integers(0, 40, ROWS),
            "price": rng.normal(20, 4, ROWS).round(2),
            "gappy": np.where(rng.random(ROWS) < 0.1, np.nan, rng.integers(1, 9, ROWS)),
            "region": rng.choice(["north", "south", "east", "west", "centre"], ROWS, p=[0.4, 0.25, 0.2, 0.1, 0.05]),
            "when": (pd.Timestamp("2024-01-01") + pd.to_timedelta(rng.integers(0, 365, ROWS), unit="D")).strftime(
                "%Y-%m-%d"
            ),
            "flag": rng.random(ROWS) < 0.3,
            "same": 7,
            "empty": np.nan,
            " padded ": rng.integers(0, 5, ROWS),
        }
    )
    frame.loc[::50, "price"] = 0.0  # zeros are counted
    frame.loc[3, "price"] = 900.0  # an outlier on both fences
    frame["region"] = frame["region"].astype(object)
    frame.loc[::97, "region"] = "NA"  # pandas reads these as empty cells; so must the chunked read
    frame.loc[::89, "units"] = np.nan
    path = _home / "t.csv"
    frame.to_csv(path, index=False)
    return path


def both(monkeypatch, call, *args, **kwargs):
    monkeypatch.setenv("MCP_BIG_TABLE", "never")
    loaded = call(*args, **kwargs)
    monkeypatch.setenv("MCP_BIG_TABLE", "always")
    chunked = call(*args, **kwargs)
    return loaded, chunked


class TestInspectDatasetIsTheSameAnswer:
    FIELDS = (
        "rows",
        "columns",
        "column_names",
        "dtypes",
        "null_counts",
        "null_pct",
        "unique_counts",
        "numeric_columns",
        "categorical_columns",
        "datetime_columns",
    )

    def test_every_field(self, table, monkeypatch):
        loaded, chunked = both(monkeypatch, engine.inspect_dataset, str(table))
        assert loaded["success"] is True and chunked["success"] is True, chunked.get("error")
        for field in self.FIELDS:
            assert chunked[field] == loaded[field], field
        assert "chunked" not in loaded
        assert chunked["chunked"]["engine"] == "duckdb" and "MCP_BIG_TABLE" in chunked["chunked"]["why"]

    def test_a_cell_pandas_reads_as_empty_is_empty_here_too(self, table, monkeypatch):
        _, chunked = both(monkeypatch, engine.inspect_dataset, str(table))
        assert chunked["null_counts"]["region"] == len(range(0, ROWS, 97))

    def test_the_sample_rows(self, table, monkeypatch):
        loaded, chunked = both(monkeypatch, engine.inspect_dataset, str(table), include_sample=True)
        assert len(chunked["sample"]) == 2
        for a, b in zip(loaded["sample"], chunked["sample"], strict=True):
            assert a["id"] == b["id"] and a["region"] == b["region"] and a["when"] == str(b["when"])[:10]

    def test_the_file_is_never_loaded_whole(self, table, monkeypatch):
        monkeypatch.setenv("MCP_BIG_TABLE", "always")

        def refuse(*_a, **_k):
            raise AssertionError("loaded the whole file")

        monkeypatch.setattr(engine, "_read_csv", refuse)
        assert engine.inspect_dataset(str(table))["success"] is True
        assert engine.read_column_stats(str(table), "price")["success"] is True


class TestLoadDatasetIsTheSameAnswer:
    def test_every_field(self, table, monkeypatch):
        loaded, chunked = both(monkeypatch, engine.load_dataset, str(table))
        assert loaded["success"] is True and chunked["success"] is True, chunked.get("error")
        for field in ("rows", "total_rows", "counted_from_sample", "columns", "dtypes", "null_counts", "unique_counts"):
            assert chunked[field] == loaded[field], field
        assert [r["id"] for r in chunked["sample"]] == [r["id"] for r in loaded["sample"]]
        assert chunked["chunked"]["engine"] == "duckdb"

    def test_a_sample_the_caller_asked_for_is_still_read_whole(self, table, monkeypatch):
        monkeypatch.setenv("MCP_BIG_TABLE", "always")
        result = engine.load_dataset(str(table), max_rows=100)
        assert result["rows"] == 100 and "chunked" not in result


class TestAColumnsStatisticsAreTheSameAnswer:
    @pytest.mark.parametrize("column", ["price", "units", "gappy", "id", "same", "padded", "empty"])
    def test_a_numeric_column(self, table, monkeypatch, column):
        name = " padded " if column == "padded" else column
        loaded, chunked = both(monkeypatch, engine.read_column_stats, str(table), name.strip())
        assert loaded["success"] is True and chunked["success"] is True, chunked.get("error")
        for key in (
            "dtype",
            "count",
            "null_count",
            "null_pct",
            "zero_count",
            "non_finite_count",
            "mean",
            "median",
            "std",
            "min",
            "max",
            "q1",
            "q3",
            "iqr",
            "outlier_count_iqr",
            "outlier_count_std",
            "unique_count",
        ):
            expected = pytest.approx(loaded[key], abs=1e-3) if isinstance(loaded[key], float) else loaded[key]
            assert chunked[key] == expected, key
        assert chunked["chunked"]["engine"] == "duckdb"

    def test_the_outlier_is_found_on_both_fences(self, table, monkeypatch):
        _, chunked = both(monkeypatch, engine.read_column_stats, str(table), "price")
        assert chunked["outlier_count_iqr"] >= 1 and chunked["max"] == 900.0

    @pytest.mark.parametrize("column", ["region", "when", "flag"])
    def test_a_text_column(self, table, monkeypatch, column):
        loaded, chunked = both(monkeypatch, engine.read_column_stats, str(table), column)
        for key in ("dtype", "count", "null_count", "null_pct", "unique_count"):
            assert chunked[key] == loaded[key], key
        assert chunked["top_values"] == loaded["top_values"]

    def test_a_boolean_column_has_no_quartiles_on_either_path(self, table, monkeypatch):
        """read_column_stats asked a True/False column for its quartiles and failed on numpy's boolean subtract."""
        monkeypatch.setenv("MCP_BIG_TABLE", "never")
        result = engine.read_column_stats(str(table), "flag")
        assert result["success"] is True and set(result["top_values"]) == {"True", "False"}

    def test_a_column_that_is_not_there_is_named_with_the_ones_that_are(self, table, monkeypatch):
        loaded, chunked = both(monkeypatch, engine.read_column_stats, str(table), "nope")
        assert chunked["success"] is False and chunked["error"] == loaded["error"]
        assert "region" in chunked["hint"]


class TestWhenItIsChunked:
    def test_a_file_that_fits_is_loaded_as_it_always_was(self, table, monkeypatch):
        monkeypatch.setenv("MCP_CALL_MEMORY_MB", "4096")
        assert big_table.reason(table) == ""
        assert "chunked" not in engine.inspect_dataset(str(table))

    def test_a_file_that_does_not_fit_in_what_a_call_may_hold_is_chunked(self, table, monkeypatch):
        monkeypatch.setenv("MCP_CALL_MEMORY_MB", "64")
        monkeypatch.setattr(big_table, "LOAD_FACTORS", {".csv": 6000})  # this file stands in for a big one
        why = big_table.reason(table)
        assert "needs about" in why and "may hold 64 MB" in why
        result = engine.inspect_dataset(str(table))
        assert result["success"] is True and result["chunked"]["why"] == why

    def test_with_no_limit_known_nothing_says_it_will_not_fit(self, table, monkeypatch):
        monkeypatch.setattr(big_table, "memory_budget_mb", lambda default=1024: 0 if default == 0 else default)
        assert big_table.reason(table) == ""

    def test_never_means_never(self, table, monkeypatch):
        monkeypatch.setenv("MCP_CALL_MEMORY_MB", "64")
        monkeypatch.setattr(big_table, "LOAD_FACTORS", {".csv": 6000})
        monkeypatch.setenv("MCP_BIG_TABLE", "never")
        assert big_table.reason(table) == ""

    def test_a_file_the_tool_does_not_read_is_left_to_it(self, _home, monkeypatch):
        monkeypatch.setenv("MCP_BIG_TABLE", "always")
        parquet = _home / "t.parquet"
        pd.DataFrame({"a": [1, 2]}).to_parquet(parquet)
        assert big_table.reason(parquet) == ""
        assert engine.inspect_dataset(str(parquet))["success"] is False  # "is a Parquet file, not CSV text", as before
