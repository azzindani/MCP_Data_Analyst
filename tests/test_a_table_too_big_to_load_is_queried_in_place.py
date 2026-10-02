"""A table too big to load is narrowed with SQL where it lies, and the result is what pandas would say.

The 2026-10-02 sweep ran the data server at the live 1 GB limit: a 16 MB CSV exported to Excel was
killed by the kernel with the server, and nothing in the tool set could answer "what does this
50 GB table say" without first loading it. `query_data` runs one read-only SELECT over the files a
call names (DuckDB reads them in chunks and spills what it cannot hold) or a SQLite / DuckDB
database file opened read-only, and returns a preview or writes the whole result to a file.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from servers.data_domain.server import DOMAINS
from servers.data_ingest.engine import query_data
from shared import sql_query

ROWS = 20_000


@pytest.fixture(autouse=True)
def _home(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    return tmp_path


@pytest.fixture
def sales(_home) -> Path:
    rng = np.random.default_rng(7)
    frame = pd.DataFrame(
        {
            "region": rng.choice(["north", "south", "east", "west"], ROWS),
            "product": rng.choice([f"p{i}" for i in range(30)], ROWS),
            "units": rng.integers(1, 50, ROWS),
            "price": rng.normal(20, 4, ROWS).round(2),
            "day": pd.Timestamp("2024-01-01") + pd.to_timedelta(rng.integers(0, 365, ROWS), unit="D"),
        }
    )
    path = _home / "sales.csv"
    frame.to_csv(path, index=False)
    return path


class TestTheAnswerIsPandas:
    def test_a_grouped_sum(self, sales):
        result = query_data(
            "SELECT region, sum(units * price) AS revenue, count(*) AS n FROM data GROUP BY region ORDER BY region",
            file_path=str(sales),
        )
        assert result["success"] is True, result.get("error")
        frame = pd.read_csv(sales)
        expected = (
            frame.assign(r=frame.units * frame.price).groupby("region").agg(revenue=("r", "sum"), n=("r", "size"))
        )
        got = pd.DataFrame(result["rows"]).set_index("region")
        assert got["n"].tolist() == expected["n"].tolist()
        assert got["revenue"].tolist() == pytest.approx(expected["revenue"].tolist())
        assert result["engine"] == "duckdb" and result["rows_total"] == 4 and result["is_preview"] is False

    def test_a_join_across_two_files(self, sales, _home):
        pd.DataFrame({"region": ["north", "south", "east", "west"], "manager": list("ABCD")}).to_csv(
            _home / "mgr.csv", index=False
        )
        result = query_data(
            "SELECT m.manager, count(*) n FROM sales s JOIN mgr m USING (region) GROUP BY 1 ORDER BY 1",
            tables={"sales": str(sales), "mgr": str(_home / "mgr.csv")},
        )
        assert result["success"] is True, result.get("error")
        counts = pd.read_csv(sales).region.value_counts()
        by_region = dict(zip(["north", "south", "east", "west"], "ABCD", strict=True))
        expected = sorted((by_region[r], int(n)) for r, n in counts.items())
        assert [(r["manager"], r["n"]) for r in result["rows"]] == expected

    def test_dates_come_back_as_text_a_client_can_read(self, sales):
        result = query_data("SELECT min(day) first_day FROM data", file_path=str(sales))
        assert isinstance(result["rows"][0]["first_day"], str) and result["rows"][0]["first_day"].startswith("2024-")

    def test_a_query_with_no_rows_still_has_its_columns(self, sales, _home):
        out = _home / "none.csv"
        result = query_data(
            "SELECT region, units FROM data WHERE units < 0", file_path=str(sales), output_path=str(out)
        )
        assert result["success"] is True, result.get("error")
        assert result["rows_total"] == 0 and [c["name"] for c in result["columns"]] == ["region", "units"]
        assert pd.read_csv(out).shape == (0, 2)


class TestPreviewAndOutput:
    def test_a_big_result_is_a_preview_that_says_so(self, sales):
        result = query_data("SELECT * FROM data", file_path=str(sales), max_rows=5)
        assert len(result["rows"]) == 5 and result["is_preview"] is True and result["rows_total"] is None
        assert "output_path" in result["hint"]

    def test_the_preview_is_capped(self, sales):
        result = query_data("SELECT * FROM data", file_path=str(sales), max_rows=10**6)
        assert len(result["rows"]) == sql_query.MAX_PREVIEW

    @pytest.mark.parametrize("suffix", [".csv", ".parquet"])
    def test_the_whole_result_is_written(self, sales, _home, suffix):
        out = _home / f"all{suffix}"
        result = query_data(
            "SELECT * FROM data WHERE units > 25", file_path=str(sales), output_path=str(out), max_rows=3
        )
        assert result["success"] is True, result.get("error")
        written = pd.read_parquet(out) if suffix == ".parquet" else pd.read_csv(out)
        expected = pd.read_csv(sales).query("units > 25")
        assert len(written) == len(expected) == result["rows_total"]
        assert written["units"].tolist() == expected["units"].tolist()
        assert result["output_path"] == str(out) and len(result["rows"]) == 3

    def test_an_existing_output_is_snapshotted_not_lost(self, sales, _home):
        out = _home / "keep.csv"
        out.write_text("old\n1\n")
        result = query_data("SELECT 1 AS x FROM data LIMIT 1", file_path=str(sales), output_path=str(out))
        assert result["success"] is True and result.get("backup")
        assert Path(result["backup"]).read_text() == "old\n1\n"

    def test_only_csv_and_parquet_are_written(self, sales, _home):
        result = query_data("SELECT 1 FROM data", file_path=str(sales), output_path=str(_home / "x.xlsx"))
        assert result["success"] is False and "csv" in result["error"] and "parquet" in result["error"]


class TestTheQueryIsLockedDown:
    SECRET = "classified\n"

    @pytest.mark.parametrize(
        "sql",
        [
            "DROP TABLE data",
            "DROP VIEW data",
            "CREATE TABLE t AS SELECT 1",
            "COPY data TO 'out.csv'",
            "ATTACH 'x.db' AS x",
            "INSTALL httpfs",
            "SET enable_external_access=true",
            "SELECT 1; SELECT 2",
            "",
        ],
    )
    def test_only_one_select_runs(self, sales, sql):
        result = query_data(sql, file_path=str(sales))
        assert result["success"] is False and result["op"] == "query_data"

    def test_a_file_the_call_did_not_name_cannot_be_read(self, sales, _home):
        other = _home / "secret.csv"
        other.write_text("password\n" + self.SECRET)
        for sql in (
            f"SELECT * FROM read_csv('{other}')",
            f"SELECT * FROM read_text('{other}')",
            "SELECT * FROM glob('/*')",
        ):
            result = query_data(sql, file_path=str(sales))
            assert result["success"] is False, sql
            assert "classified" not in str(result), sql

    def test_the_named_file_is_not_changed(self, sales):
        before = sales.read_bytes()
        query_data("SELECT * FROM data", file_path=str(sales))
        assert sales.read_bytes() == before

    def test_a_wrong_column_is_answered_with_the_columns_there_are(self, sales):
        result = query_data("SELECT nothere FROM data", file_path=str(sales))
        assert result["success"] is False
        assert "Table data: region, product, units, price, day" in result["error"]

    def test_an_excel_workbook_is_pointed_at_convert_file(self, _home):
        book = _home / "b.xlsx"
        pd.DataFrame({"a": [1]}).to_excel(book, index=False)
        result = query_data("SELECT * FROM data", file_path=str(book))
        assert result["success"] is False and "convert_file" in result["error"]

    def test_a_missing_file_is_named(self, _home):
        result = query_data("SELECT 1", file_path=str(_home / "nope.csv"))
        assert result["success"] is False and "nope.csv" in result["error"]

    def test_a_table_name_used_twice_is_refused(self, sales):
        result = query_data("SELECT 1", file_path=str(sales), tables={"data": str(sales)})
        assert result["success"] is False and "twice" in result["error"]


class TestTheMemoryLimitIsKept:
    def test_a_query_that_cannot_fit_is_refused_not_killed(self, sales):
        result = query_data(
            "SELECT a.units, b.x FROM data a CROSS JOIN range(2000000) b(x) ORDER BY b.x * random()",
            file_path=str(sales),
            memory_mb=64,
        )
        assert result["success"] is False
        assert "memory_mb" in result["error"] and "64" in result["error"]

    def test_a_sort_larger_than_the_limit_spills_and_finishes(self, sales, _home):
        out = _home / "sorted.parquet"
        result = query_data(
            "SELECT * FROM data ORDER BY price DESC, units", file_path=str(sales), output_path=str(out), memory_mb=256
        )
        assert result["success"] is True, result.get("error")
        assert pd.read_parquet(out)["price"].is_monotonic_decreasing


class TestADatabaseFileIsReadOnly:
    @pytest.fixture
    def lite(self, _home) -> Path:
        path = _home / "shop.sqlite"
        con = sqlite3.connect(path)
        con.execute("create table orders(id integer, customer text, total real)")
        con.executemany("insert into orders values (?, ?, ?)", [(i, f"c{i % 3}", i * 1.5) for i in range(300)])
        con.commit()
        con.close()
        return path

    def test_sqlite_is_queried(self, lite):
        result = query_data(
            "SELECT customer, count(*) n, sum(total) t FROM orders GROUP BY 1 ORDER BY 1", database=str(lite)
        )
        assert result["success"] is True, result.get("error")
        assert result["engine"] == "sqlite"
        assert [r["n"] for r in result["rows"]] == [100, 100, 100]
        assert result["rows"][0]["t"] == pytest.approx(sum(i * 1.5 for i in range(0, 300, 3)))

    @pytest.mark.parametrize(
        "sql", ["DELETE FROM orders", "DROP TABLE orders", "PRAGMA table_info(orders)", "SELECT 1; DROP TABLE orders"]
    )
    def test_nothing_writes(self, lite, sql):
        assert query_data(sql, database=str(lite))["success"] is False
        con = sqlite3.connect(lite)
        assert con.execute("select count(*) from orders").fetchone()[0] == 300
        con.close()

    def test_a_duckdb_file_is_queried_read_only(self, _home):
        import duckdb

        path = _home / "w.duckdb"
        con = duckdb.connect(str(path))
        con.execute("create table t as select range as id, range % 4 as g from range(1000)")
        con.close()
        result = query_data("SELECT g, count(*) n FROM t GROUP BY g ORDER BY g", database=str(path))
        assert result["success"] is True, result.get("error")
        assert [r["n"] for r in result["rows"]] == [250] * 4
        assert query_data("DELETE FROM t", database=str(path))["success"] is False

    def test_a_database_of_another_kind_is_named(self, _home):
        path = _home / "x.mdb"
        path.write_bytes(b"x")
        result = query_data("SELECT 1", database=str(path))
        assert result["success"] is False and "SQLite" in result["error"] and "DuckDB" in result["error"]


class TestItIsAnActionOfTheIngestTool:
    def test_registered_on_the_domain_a_client_sees(self):
        assert "query_data" in {tool for _, members in DOMAINS.values() for _, tool in members}
        assert any(tool == "query_data" for _, tool in DOMAINS["data_ingest"][1])

    @pytest.mark.skipif(not sys.platform.startswith("linux"), reason="process isolation is Linux only")
    def test_it_runs_through_the_dispatcher(self, sales):
        import asyncio

        from servers.data_domain.server import mcp as domain

        result = asyncio.run(
            domain._tool_manager._tools["data_ingest"].run(
                {"action": "query_data", "args": {"sql": "SELECT count(*) n FROM data", "file_path": str(sales)}}
            )
        )
        assert result["success"] is True and result["rows"] == [{"n": ROWS}]


class TestWhatTheOtherToolsCanRead:
    """They read CSV, not Parquet: a hint that sent a caller to a .parquet result was a dead end."""

    def test_a_csv_result_is_handed_on(self, sales, _home):
        result = query_data("SELECT * FROM data", file_path=str(sales), output_path=str(_home / "r.csv"))
        assert "reads it from there" in result["hint"]

    def test_a_parquet_result_says_the_tools_read_csv(self, sales, _home):
        result = query_data("SELECT * FROM data", file_path=str(sales), output_path=str(_home / "r.parquet"))
        assert "CSV, not Parquet" in result["hint"] and "reads it from there" not in result["hint"]


class TestACellPandasReadsAsEmptyIsEmptyHere:
    """Every other tool loads a CSV through pandas, so a table read here must have the nulls those tools see."""

    def test_na_in_a_number_column_is_a_gap_not_text(self, _home):
        path = _home / "gaps.csv"
        path.write_text("n,label\n1,a\nNA,b\n3,NULL\n,d\nnull,e\n")
        result = query_data(
            "SELECT count(n) AS n_seen, sum(n) AS total, count(label) AS labels FROM data", file_path=str(path)
        )
        assert result["success"] is True, result.get("error")
        frame = pd.read_csv(path)
        assert result["rows"] == [
            {"n_seen": int(frame.n.count()), "total": float(frame.n.sum()), "labels": int(frame.label.count())}
        ]
