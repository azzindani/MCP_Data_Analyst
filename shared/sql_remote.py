"""Read-only SQL against a database server (PostgreSQL, MySQL, MariaDB), by a profile the operator set up.

A connection string holds a password, and anything a caller passes is read by the model and kept in
transcripts. So a call never carries one: the operator names a database in the server's environment
(`MCP_DB_WAREHOUSE_URL=postgresql://user:password@host:5432/dbname`) and a call says `database="warehouse"`.
The name is all that ever appears in an argument, a response or a receipt, and an error has the password
taken out of it.

Nothing here writes. The statement is checked to be one SELECT, and the session is read-only at the
server too (`default_transaction_read_only` for PostgreSQL, `SET SESSION TRANSACTION READ ONLY` for MySQL),
so a statement the check missed is still refused by the database. Rows stream through a server-side cursor
in batches, so a table of any size costs one batch of memory; the caller narrows with SQL and gets a
preview, or the whole result written to a file.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import os
import re
from collections.abc import Iterator
from decimal import Decimal
from typing import Any
from urllib.parse import unquote, urlsplit

PREFIX = "MCP_DB_"
SUFFIX = "_URL"
BATCH_ROWS = 50_000
_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_POSTGRES = ("postgres", "postgresql")
_MYSQL = ("mysql", "mariadb")


class RemoteRefused(ValueError):
    """The request cannot be run as asked; the message says why and what to do."""


def profiles() -> dict[str, str]:
    """{profile name: URL} for every MCP_DB_<NAME>_URL set; names are lower case."""
    found = {}
    for key, value in os.environ.items():
        if key.startswith(PREFIX) and key.endswith(SUFFIX) and value.strip():
            name = key[len(PREFIX) : -len(SUFFIX)].lower()
            if name:
                found[name] = value.strip()
    return found


def is_profile(name: str) -> bool:
    """True when `name` is the name of a configured database rather than the path of a file."""
    return bool(_NAME.match(name)) and name.lower() in profiles()


def names() -> list[str]:
    return sorted(profiles())


def _scrub(text: str, url: str) -> str:
    """The message with the URL's password, and the URL itself, taken out."""
    secret = unquote(urlsplit(url).password or "")
    text = text.replace(url, "<url>")
    return text.replace(secret, "***") if secret else text


def _pg_message(exc: Exception, url: str) -> str:
    """What PostgreSQL said, without the cursor statement it was wrapped in or the caret that pointed into it."""
    diag = getattr(exc, "diag", None)
    primary = getattr(diag, "message_primary", None)
    if primary:
        hint = getattr(diag, "message_hint", None)
        return _scrub(f"{primary}{'. ' + hint if hint else ''}", url)
    return _scrub(str(exc).strip(), url)


def _first_word(sql: str) -> str:
    """The statement's first keyword, past leading whitespace and comments."""
    rest = sql.lstrip()
    while True:
        if rest.startswith("--"):
            rest = rest.partition("\n")[2].lstrip()
        elif rest.startswith("/*"):
            rest = rest.partition("*/")[2].lstrip()
        else:
            break
    return re.split(r"[\s(]", rest, maxsplit=1)[0].lower()


def _statements(sql: str) -> int:
    """How many statements `sql` holds: semicolons outside quotes and comments, ignoring a trailing one."""
    count, quote, i = 0, "", 0
    body = sql.strip()
    while i < len(body):
        c = body[i]
        if quote:
            if c == quote:
                if body[i + 1 : i + 2] == quote:  # '' or "" inside a string
                    i += 1
                else:
                    quote = ""
            elif c == "\\" and quote == "'":
                i += 1
        elif c in ("'", '"', "`"):
            quote = c
        elif body.startswith("--", i):
            i = body.find("\n", i)
            if i < 0:
                break
        elif body.startswith("/*", i):
            i = body.find("*/", i)
            if i < 0:
                break
            i += 1
        elif c == ";" and body[i + 1 :].strip():
            count += 1
        i += 1
    return count + 1


def check_select(sql: str) -> None:
    """Refuse anything but one SELECT (WITH ... SELECT is fine); the server's read-only session is the second lock."""
    if _first_word(sql) not in ("select", "with", "values", "table") or _statements(sql) != 1:
        raise RemoteRefused(
            "Only one SELECT statement is run (WITH ... SELECT is fine). Nothing here writes; the session "
            "is read-only on the server as well."
        )


def _cell(value: Any) -> Any:
    """A value Arrow can hold: scalars as they are, anything structured (arrays, JSON, UUIDs) as text."""
    if value is None or isinstance(value, (str, int, float, bool, Decimal, bytes, dt.date, dt.time, dt.timedelta)):
        return value
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, default=str)
    return str(value)


def _batches(names_: list[str], fetch) -> Iterator:
    """Arrow record batches of one schema, fixed by the first batch, from a `fetch(n) -> list of row tuples`."""
    import pyarrow as pa

    schema = None
    while True:
        rows = fetch(BATCH_ROWS)
        if not rows:
            break
        columns = [[_cell(v) for v in column] for column in zip(*rows, strict=True)]
        try:
            if schema is None:
                batch = pa.RecordBatch.from_arrays([pa.array(c) for c in columns], names=names_)
                schema = batch.schema
            else:
                batch = pa.RecordBatch.from_arrays([pa.array(c, type=f.type) for c, f in zip(columns, schema)], schema=schema)
        except (pa.ArrowInvalid, pa.ArrowTypeError) as exc:
            raise RemoteRefused(
                "A column changes type partway through the result. CAST it in the query "
                f"(CAST(col AS TEXT), or ::text on PostgreSQL). ({exc})"
            ) from exc
        yield batch
    if schema is None:
        yield pa.RecordBatch.from_pylist([], schema=pa.schema([(n, pa.string()) for n in names_]))


def _timeout_s() -> float:
    try:
        return float(os.environ.get("MCP_CALL_TIMEOUT_S", "1800"))
    except ValueError:
        return 1800.0


@contextlib.contextmanager
def _postgres(url: str, sql: str) -> Iterator[tuple[str, Iterator]]:
    import psycopg

    millis = int(_timeout_s() * 1000)
    try:
        conn = psycopg.connect(
            url, connect_timeout=15, options=f"-c default_transaction_read_only=on -c statement_timeout={millis}"
        )
    except psycopg.Error as exc:
        raise RemoteRefused(f"Could not connect: {_scrub(str(exc).strip(), url)}") from None
    try:
        conn.read_only = True
        with conn.cursor(name="mcp_query") as cur:
            cur.itersize = BATCH_ROWS
            try:
                cur.execute(sql)  # pyright: ignore[reportArgumentType]
            except psycopg.Error as exc:
                raise RemoteRefused(_pg_message(exc, url)) from None
            names_ = [d.name for d in cur.description or []]
            yield "postgresql", _batches(names_, cur.fetchmany)
    except psycopg.Error as exc:
        raise RemoteRefused(_pg_message(exc, url)) from None
    finally:
        with contextlib.suppress(Exception):
            conn.rollback()
        conn.close()


@contextlib.contextmanager
def _mysql(url: str, sql: str) -> Iterator[tuple[str, Iterator]]:
    import pymysql
    import pymysql.cursors

    parts = urlsplit(url)
    seconds = max(1, int(_timeout_s()))
    try:
        conn = pymysql.connect(
            host=parts.hostname or "localhost",
            port=parts.port or 3306,
            user=unquote(parts.username or ""),
            password=unquote(parts.password or ""),
            database=parts.path.lstrip("/") or None,
            connect_timeout=15,
            read_timeout=seconds,
            cursorclass=pymysql.cursors.SSCursor,  # streams: one batch in memory, not the table
            charset="utf8mb4",
        )
    except pymysql.MySQLError as exc:
        raise RemoteRefused(f"Could not connect: {_scrub(str(exc).strip(), url)}") from None
    try:
        with conn.cursor() as setup:
            setup.execute("SET SESSION TRANSACTION READ ONLY")
            for limit in (f"SET SESSION max_execution_time={seconds * 1000}", f"SET SESSION max_statement_time={seconds}"):
                with contextlib.suppress(pymysql.MySQLError):  # one of the two is the server's own name for it
                    setup.execute(limit)
        # No `with` on this cursor: closing a streaming cursor reads out every row it has not delivered, which
        # for a preview of a table of millions is the whole table. conn.close() just hangs up.
        cur = conn.cursor()
        try:
            cur.execute(sql)
        except pymysql.MySQLError as exc:
            raise RemoteRefused(_scrub(str(exc).strip(), url)) from None
        names_ = [d[0] for d in cur.description or []]
        yield "mysql", _batches(names_, cur.fetchmany)
    except pymysql.MySQLError as exc:
        raise RemoteRefused(_scrub(str(exc).strip(), url)) from None
    finally:
        conn.close()  # read-only, so there is nothing to roll back, and a rollback would first drain the stream


@contextlib.contextmanager
def query(profile: str, sql: str) -> Iterator[tuple[str, Iterator]]:
    """Run one SELECT on the profile's database; yields (engine, record batches)."""
    url = profiles().get(profile.lower())
    if url is None:
        configured = ", ".join(names()) or "none"
        raise RemoteRefused(
            f"No database named {profile!r} is configured (MCP_DB_<NAME>_URL). Configured: {configured}."
        )
    check_select(sql)
    scheme = urlsplit(url).scheme.lower()
    if scheme in _POSTGRES:
        opener = _postgres
    elif scheme in _MYSQL:
        opener = _mysql
    else:
        raise RemoteRefused(f"Database {profile!r}: the URL scheme must be postgresql:// or mysql:// (mariadb://).")
    with opener(url, sql) as opened:
        yield opened
