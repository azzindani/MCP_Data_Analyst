"""A database server is named in a call, never located: its URL, and the password in it, stay in the operator's environment.

A connection string anyone passes in an argument is read by the model and kept in transcripts, so
`query_data(database="warehouse")` names a profile the operator configured (`MCP_DB_WAREHOUSE_URL`) and
nothing else about the server appears in a call, a response or a receipt. The session is read-only twice
over: one SELECT is checked here, and the server is told to refuse a write itself.

pytest stays offline here (STANDARDS.md): the drivers are replaced by fakes that record what they are
asked. Against a real PostgreSQL or MySQL, set `MCP_TEST_PG_URL` / `MCP_TEST_MYSQL_URL` (a role that may
create a table) and the live class runs too.
"""

from __future__ import annotations

import os
import sys
import types
from pathlib import Path

import pandas as pd
import pytest

from servers.data_ingest.engine import query_data
from shared import sql_remote

PASSWORD = "s3cr3t-pw-do-not-print"


@pytest.fixture(autouse=True)
def _home(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    for key in [k for k in os.environ if k.startswith(sql_remote.PREFIX)]:
        monkeypatch.delenv(key)
    return tmp_path


@pytest.fixture
def warehouse(monkeypatch):
    monkeypatch.setenv("MCP_DB_WAREHOUSE_URL", f"postgresql://analyst:{PASSWORD}@db.internal:5432/shop")


class TestAProfileIsAName:
    def test_names_come_from_the_environment(self, monkeypatch):
        monkeypatch.setenv("MCP_DB_WAREHOUSE_URL", "postgresql://u:p@h/d")
        monkeypatch.setenv("MCP_DB_Crm_URL", "mysql://u:p@h/d")
        monkeypatch.setenv("MCP_DB_EMPTY_URL", "  ")
        assert sql_remote.names() == ["crm", "warehouse"]

    def test_only_a_bare_configured_name_is_one(self, monkeypatch):
        monkeypatch.setenv("MCP_DB_WAREHOUSE_URL", "postgresql://u:p@h/d")
        assert sql_remote.is_profile("warehouse") and sql_remote.is_profile("Warehouse")
        for not_one in ("other", "warehouse.db", "/data/warehouse", "../warehouse", "postgresql://u:p@h/d", ""):
            assert not sql_remote.is_profile(not_one), not_one

    def test_an_unknown_name_lists_the_ones_there_are_and_no_url(self, warehouse, monkeypatch):
        monkeypatch.setenv("MCP_DB_OTHER_URL", "mysql://u:p@h/d")
        result = query_data("SELECT 1", database="nope")
        assert result["success"] is False and "other, warehouse" in result["hint"]
        assert PASSWORD not in str(result) and "db.internal" not in str(result)

    def test_a_url_in_the_argument_is_not_a_database(self, warehouse):
        result = query_data("SELECT 1", database=f"postgresql://analyst:{PASSWORD}@db.internal/shop")
        assert result["success"] is False
        assert PASSWORD not in str(result)


class TestOneSelectAndNothingElse:
    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT 1",
            "  select 1;  ",
            "-- the daily total\nSELECT sum(x) FROM t",
            "/* note */ WITH a AS (SELECT 1) SELECT * FROM a",
            "SELECT ';' AS semicolon, 'it''s; fine' AS quote",
            'SELECT "a;b" FROM t',
            "VALUES (1), (2)",
            "TABLE orders",
        ],
    )
    def test_these_run(self, sql):
        sql_remote.check_select(sql)

    @pytest.mark.parametrize(
        "sql",
        [
            "",
            "DELETE FROM orders",
            "DROP TABLE orders",
            "INSERT INTO orders VALUES (1)",
            "UPDATE orders SET amount = 0",
            "TRUNCATE orders",
            "CALL cleanup()",
            "SELECT 1; SELECT 2",
            "SELECT 1; DROP TABLE orders",
            "/* x */ DELETE FROM orders",
            "-- x\nUPDATE t SET a = 1",
            "COPY orders TO '/tmp/x'",
        ],
    )
    def test_these_do_not(self, sql):
        with pytest.raises(sql_remote.RemoteRefused):
            sql_remote.check_select(sql)

    def test_a_refused_statement_never_reaches_a_driver(self, warehouse, monkeypatch):
        monkeypatch.setitem(sys.modules, "psycopg", None)  # an import would fail: it must not be reached
        result = query_data("DELETE FROM orders", database="warehouse")
        assert result["success"] is False and "Only one SELECT" in result["error"]


class FakePostgres:
    """Just enough of psycopg to see how it is asked."""

    def __init__(self, batches, fail_with=None):
        self.batches, self.fail_with = list(batches), fail_with
        self.connects: list[dict] = []
        self.executed: list[str] = []
        self.cursors: list[dict] = []
        self.closed = False
        self.read_only = None

    def module(self):
        outer = self

        class Error(Exception):
            def __init__(self, message="", primary=None):
                super().__init__(message)
                self.diag = types.SimpleNamespace(message_primary=primary, message_hint=None)

        class Cursor:
            description = [types.SimpleNamespace(name="id"), types.SimpleNamespace(name="label")]
            itersize = 0

            def __init__(self, name):
                outer.cursors.append({"name": name})

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, sql):
                outer.executed.append(sql)

            def fetchmany(self, n):
                return outer.batches.pop(0) if outer.batches else []

        class Connection:
            def __init__(self):
                self.read_only = None

            def cursor(self, name=None):
                outer.read_only = self.read_only
                return Cursor(name)

            def rollback(self):
                pass

            def close(self):
                outer.closed = True

        def connect(url, **kwargs):
            outer.connects.append({"url": url, **kwargs})
            if outer.fail_with:
                raise Error(outer.fail_with)
            return Connection()

        return types.SimpleNamespace(connect=connect, Error=Error)


@pytest.fixture
def fake_pg(monkeypatch):
    def install(batches, fail_with=None):
        fake = FakePostgres(batches, fail_with)
        monkeypatch.setitem(sys.modules, "psycopg", fake.module())
        return fake

    return install


class TestPostgres:
    def test_the_rows_come_back_and_the_session_is_read_only(self, warehouse, fake_pg):
        fake = fake_pg([[(1, "a"), (2, "b")], [(3, "c")]])
        result = query_data("SELECT id, label FROM t", database="warehouse")
        assert result["success"] is True, result.get("error")
        assert result["engine"] == "postgresql" and result["rows"] == [
            {"id": 1, "label": "a"},
            {"id": 2, "label": "b"},
            {"id": 3, "label": "c"},
        ]
        assert fake.read_only is True
        assert "default_transaction_read_only=on" in fake.connects[0]["options"]
        assert "statement_timeout=" in fake.connects[0]["options"]
        assert fake.cursors == [{"name": "mcp_query"}], "a server-side cursor: the table is not held whole"
        assert fake.executed == ["SELECT id, label FROM t"] and fake.closed

    def test_the_password_is_in_no_part_of_the_answer(self, warehouse, fake_pg, _home):
        fake_pg([[(1, "a")]])
        out = _home / "r.csv"
        result = query_data("SELECT id, label FROM t", database="warehouse", output_path=str(out))
        assert PASSWORD not in str(result) and "db.internal" not in str(result)
        assert result["engine"] == "postgresql" and pd.read_csv(out).shape == (1, 2)
        receipt = Path(str(out) + ".mcp_receipt.json")
        assert PASSWORD not in receipt.read_text() and "warehouse" in receipt.read_text()

    def test_a_failure_to_connect_has_the_password_taken_out(self, warehouse, fake_pg):
        fake_pg(
            [],
            fail_with=f"connection failed: password {PASSWORD} refused for postgresql://analyst:{PASSWORD}@db.internal",
        )
        result = query_data("SELECT 1", database="warehouse")
        assert result["success"] is False and "Could not connect" in result["error"]
        assert PASSWORD not in str(result)

    def test_a_result_is_written_in_batches_to_a_file(self, warehouse, fake_pg, _home):
        fake_pg([[(i, "x") for i in range(1000)], [(i, "y") for i in range(1000, 1500)]])
        out = _home / "all.csv"
        result = query_data("SELECT id, label FROM t", database="warehouse", output_path=str(out), max_rows=5)
        assert result["rows_total"] == 1500 and len(result["rows"]) == 5
        assert len(pd.read_csv(out)) == 1500

    def test_a_column_that_changes_type_is_refused_with_the_fix(self, warehouse, fake_pg):
        fake_pg([[(1, "a")], [("two", "b")]])
        result = query_data("SELECT id, label FROM t", database="warehouse")
        assert result["success"] is False and "CAST" in result["error"]

    def test_values_arrow_cannot_hold_arrive_as_text(self, warehouse, fake_pg):
        import uuid

        key = uuid.uuid4()
        fake_pg([[(1, key), (2, {"a": [1, 2]})]])
        result = query_data("SELECT id, label FROM t", database="warehouse")
        assert result["success"] is True, result.get("error")
        assert result["rows"][0]["label"] == str(key) and result["rows"][1]["label"] == '{"a": [1, 2]}'

    def test_files_and_a_server_are_not_queried_together(self, warehouse, _home):
        (_home / "a.csv").write_text("x\n1\n")
        result = query_data("SELECT 1", database="warehouse", tables={"a": str(_home / "a.csv")})
        assert result["success"] is False and "cannot be queried together" in result["error"]


class FakeMysql:
    def __init__(self, rows):
        self.rows = list(rows)
        self.statements: list[str] = []
        self.connect_kwargs: dict = {}
        self.cursor_closed = False
        self.conn_closed = False

    def module(self):
        outer = self

        class MySQLError(Exception):
            pass

        class SSCursor:
            pass

        class Cursor:
            description = [("id",), ("label",)]

            def __init__(self, streaming):
                self.streaming = streaming

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, sql):
                outer.statements.append(sql)

            def fetchmany(self, n):
                taken, outer.rows = outer.rows[:n], outer.rows[n:]
                return taken

            def close(self):
                outer.cursor_closed = True

        class Connection:
            def cursor(self):
                return Cursor(outer.connect_kwargs["cursorclass"] is SSCursor)

            def close(self):
                outer.conn_closed = True

        def connect(**kwargs):
            outer.connect_kwargs = kwargs
            return Connection()

        cursors = types.SimpleNamespace(SSCursor=SSCursor)
        module = types.ModuleType("pymysql")
        module.connect, module.MySQLError, module.cursors = connect, MySQLError, cursors  # type: ignore[attr-defined]
        return module, cursors


class TestMysql:
    def test_it_streams_read_only_and_hangs_up_without_draining(self, monkeypatch):
        monkeypatch.setenv("MCP_DB_CRM_URL", f"mysql://reader:{PASSWORD}@mysql.internal:3307/crm")
        fake = FakeMysql([(i, "x") for i in range(10)])
        module, cursors = fake.module()
        monkeypatch.setitem(sys.modules, "pymysql", module)
        monkeypatch.setitem(sys.modules, "pymysql.cursors", cursors)
        result = query_data("SELECT id, label FROM t", database="crm", max_rows=3)
        assert result["success"] is True, result.get("error")
        assert result["engine"] == "mysql" and len(result["rows"]) == 3
        assert fake.statements[0] == "SET SESSION TRANSACTION READ ONLY"
        assert fake.statements[-1] == "SELECT id, label FROM t"
        kw = fake.connect_kwargs
        assert kw["host"] == "mysql.internal" and kw["port"] == 3307 and kw["database"] == "crm"
        assert kw["cursorclass"] is cursors.SSCursor
        assert fake.conn_closed and not fake.cursor_closed, "closing a streaming cursor reads out the rest of the table"
        assert PASSWORD not in str(result)


class TestTheUrl:
    def test_the_scheme_must_be_one_the_drivers_speak(self, monkeypatch):
        monkeypatch.setenv("MCP_DB_ODD_URL", f"oracle://u:{PASSWORD}@h/d")
        result = query_data("SELECT 1", database="odd")
        assert result["success"] is False and "postgresql:// or mysql://" in result["error"]
        assert PASSWORD not in str(result)

    def test_a_scrubbed_message_has_neither_the_password_nor_the_url(self):
        url = f"postgresql://u:{PASSWORD}@h/d"
        text = sql_remote._scrub(f"could not connect to {url}: bad password {PASSWORD}", url)
        assert PASSWORD not in text and url not in text


LIVE_PG = os.environ.get("MCP_TEST_PG_URL", "")
LIVE_MYSQL = os.environ.get("MCP_TEST_MYSQL_URL", "")


@pytest.mark.skipif(not LIVE_PG, reason="MCP_TEST_PG_URL is not set (pytest stays offline by default)")
class TestAgainstARealPostgres:
    @pytest.fixture
    def shop(self, monkeypatch):
        import psycopg

        monkeypatch.setenv("MCP_DB_SHOP_URL", LIVE_PG)
        with psycopg.connect(LIVE_PG, autocommit=True) as conn:
            conn.execute("DROP TABLE IF EXISTS mcp_test_orders")
            conn.execute(
                "CREATE TABLE mcp_test_orders AS SELECT g AS id, g % 7 AS grp, g * 1.5 AS amount FROM generate_series(1, 20000) g"
            )
        yield
        with psycopg.connect(LIVE_PG, autocommit=True) as conn:
            conn.execute("DROP TABLE IF EXISTS mcp_test_orders")

    def test_the_answer_is_the_tables(self, shop):
        result = query_data(
            "SELECT grp, count(*) AS n, sum(amount) AS total FROM mcp_test_orders GROUP BY grp ORDER BY grp",
            database="shop",
        )
        assert result["success"] is True, result.get("error")
        frame = pd.DataFrame({"id": range(1, 20001)}).assign(grp=lambda f: f.id % 7, amount=lambda f: f.id * 1.5)
        expected = frame.groupby("grp").agg(n=("id", "size"), total=("amount", "sum"))
        got = pd.DataFrame(result["rows"]).set_index("grp")
        assert got["n"].tolist() == expected["n"].tolist()
        assert got["total"].tolist() == pytest.approx(expected["total"].tolist())

    def test_the_whole_table_is_written(self, shop, _home):
        out = _home / "orders.csv"
        result = query_data("SELECT * FROM mcp_test_orders", database="shop", output_path=str(out))
        assert result["rows_total"] == 20000 and len(pd.read_csv(out)) == 20000

    @pytest.mark.parametrize("sql", ["DELETE FROM mcp_test_orders", "UPDATE mcp_test_orders SET amount = 0"])
    def test_nothing_writes(self, shop, sql):
        assert query_data(sql, database="shop")["success"] is False
        count = query_data("SELECT count(*) AS n FROM mcp_test_orders", database="shop")["rows"][0]["n"]
        assert count == 20000

    def test_the_server_refuses_a_write_the_check_missed(self, shop):
        with (
            pytest.raises(sql_remote.RemoteRefused),
            sql_remote._postgres(LIVE_PG, "INSERT INTO mcp_test_orders (id) VALUES (1)") as (_, batches),
        ):
            list(batches)


@pytest.mark.skipif(not LIVE_MYSQL, reason="MCP_TEST_MYSQL_URL is not set (pytest stays offline by default)")
class TestAgainstARealMysql:
    @pytest.fixture
    def shop(self, monkeypatch):
        from urllib.parse import unquote, urlsplit

        import pymysql

        monkeypatch.setenv("MCP_DB_SHOP_URL", LIVE_MYSQL)
        parts = urlsplit(LIVE_MYSQL)
        conn = pymysql.connect(
            host=parts.hostname,
            port=parts.port or 3306,
            user=unquote(parts.username or ""),
            password=unquote(parts.password or ""),
            database=parts.path.lstrip("/"),
            autocommit=True,
        )
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS mcp_test_orders")
            cur.execute("CREATE TABLE mcp_test_orders (id INT, grp INT, amount DOUBLE)")
            cur.executemany(
                "INSERT INTO mcp_test_orders VALUES (%s, %s, %s)", [(i, i % 7, i * 1.5) for i in range(1, 5001)]
            )
        yield
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS mcp_test_orders")
        conn.close()

    def test_the_answer_is_the_tables(self, shop):
        result = query_data("SELECT grp, count(*) AS n FROM mcp_test_orders GROUP BY grp ORDER BY grp", database="shop")
        assert result["success"] is True, result.get("error")
        assert sum(r["n"] for r in result["rows"]) == 5000 and result["engine"] == "mysql"

    def test_nothing_writes(self, shop):
        assert query_data("DELETE FROM mcp_test_orders", database="shop")["success"] is False
        assert query_data("SELECT count(*) AS n FROM mcp_test_orders", database="shop")["rows"][0]["n"] == 5000

    def test_a_preview_hangs_up_without_reading_the_table_out(self, shop):
        result = query_data("SELECT * FROM mcp_test_orders", database="shop", max_rows=3)
        assert result["is_preview"] is True and len(result["rows"]) == 3
