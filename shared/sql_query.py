"""Read-only SQL over files and database files, without holding the table in memory.

A 483,053-row file read whole into pandas is a few hundred MB; a 50 GB one is not a thing a 1 GB
container can answer however it is asked. DuckDB reads CSV, Parquet and JSON where they lie, in
chunks, spills a sort or a join it cannot hold to disk, and stops at a memory limit it is given
instead of at the kernel's. The caller narrows the table with SQL (filter, aggregate, sample, join)
and gets a preview, or the whole result written to a file the other tools read.

The query is the caller's, so the engine is locked down before it runs: one SELECT statement, only
the files and the database the call names (DuckDB's `allowed_paths`), no network, no extension
install, configuration frozen. A SQLite or DuckDB database file is opened read-only.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import os
import shutil
import sqlite3
import tempfile
import time
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd

BATCH_ROWS = 50_000
DEFAULT_PREVIEW = 50
MAX_PREVIEW = 1_000

CSV_SUFFIXES = {".csv", ".tsv", ".txt"}
JSON_SUFFIXES = {".json", ".jsonl", ".ndjson"}
TABLE_SUFFIXES = CSV_SUFFIXES | JSON_SUFFIXES | {".parquet"}
# A folder of tables is read by these: a stray .txt (a schema note, a readme) is not one.
FOLDER_SUFFIXES = TABLE_SUFFIXES - {".txt"}
DUCKDB_SUFFIXES = {".duckdb"}
SQLITE_SUFFIXES = {".sqlite", ".sqlite3", ".db"}
OUTPUT_SUFFIXES = {".csv", ".parquet"}


class QueryRefused(ValueError):
    """The request cannot be run as asked; the message says why and what to do."""


def _quote(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _plain(value: Any) -> Any:
    """A JSON-safe cell: dates as ISO text, decimals as floats, bytes as hex, the rest as is."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if value == value and value not in (float("inf"), float("-inf")) else None
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    if isinstance(value, dt.timedelta):
        return str(value)
    return str(value)


def _rows(batch) -> list[dict[str, Any]]:
    return [{k: _plain(v) for k, v in row.items()} for row in batch.to_pylist()]


def _view_sql(name: str, path: Path) -> str:
    suffix = path.suffix.lower()
    target = _quote(str(path))
    if suffix in CSV_SUFFIXES:
        # A sample of 100,000 rows settles a column's type; the default 20,480 mistyped a column
        # whose first non-integer sits further down.
        source = f"read_csv({target}, sample_size=100000)"
    elif suffix == ".parquet":
        source = f"read_parquet({target})"
    elif suffix in JSON_SUFFIXES:
        source = f"read_json_auto({target})"
    else:
        raise QueryRefused(
            f"Table {name!r}: {path.name} is not a file this reads in place (csv, tsv, parquet, json, jsonl). "
            "An Excel workbook is converted first (convert_file to csv or parquet)."
        )
    return f"CREATE VIEW {_identifier(name)} AS SELECT * FROM {source}"


def _cap_message(megabytes: int) -> str:
    return (
        f"The query needed more than the {megabytes:,} MB it was given, even after spilling to disk. "
        "Filter or aggregate earlier (WHERE before JOIN, GROUP BY on fewer columns), select only the "
        "columns you need, or pass a larger memory_mb."
    )


def _writer(output: Path, schema):
    """A batch writer for the output file's format, and the temp file it writes to."""
    import pyarrow.csv as pacsv
    import pyarrow.parquet as pq

    handle, tmp_name = tempfile.mkstemp(dir=output.parent, suffix=output.suffix)
    os.close(handle)
    tmp = Path(tmp_name)
    stream = tmp.open("wb")
    if output.suffix.lower() == ".parquet":
        sink = pq.ParquetWriter(stream, schema)
    else:
        sink = pacsv.CSVWriter(stream, schema)  # pyright: ignore[reportPrivateImportUsage]
    return sink, stream, tmp


def _finish(sink, stream, tmp: Path, output: Path) -> None:
    sink.close()
    stream.close()
    from shared.file_utils import apply_default_mode

    apply_default_mode(str(tmp))
    shutil.move(str(tmp), str(output))


def _drain(batches: Iterator, output: Path | None, preview_rows: int, schema=None) -> dict[str, Any]:
    """Walk the result once: keep the preview, and write every row to `output` when there is one.

    `schema` is passed when the engine knows it before the first row, so a query that returns no rows
    still has columns and still writes its (empty) file.
    """
    preview: list[dict[str, Any]] = []
    total = 0
    sink = stream = tmp = None
    try:
        if schema is not None and output is not None:
            sink, stream, tmp = _writer(output, schema)
        for batch in batches:
            if schema is None:
                schema = batch.schema
                if output is not None:
                    sink, stream, tmp = _writer(output, schema)
            if len(preview) <= preview_rows:
                preview.extend(_rows(batch.slice(0, preview_rows + 1 - len(preview))))
            total += batch.num_rows
            if sink is not None:
                sink.write_batch(batch)
            elif len(preview) > preview_rows:
                break  # the preview is full and nothing is being written: do not scan the rest
        if schema is None:
            raise QueryRefused("The query returned no columns.")
        if sink is not None:
            _finish(sink, stream, tmp, output)  # type: ignore[arg-type]
            sink = None
    finally:
        if sink is not None:
            with contextlib.suppress(Exception):
                sink.close()
        if stream is not None and not stream.closed:
            stream.close()
        if tmp is not None:
            tmp.unlink(missing_ok=True)
    truncated = len(preview) > preview_rows
    return {
        "columns": [{"name": f.name, "type": str(f.type)} for f in schema],
        "rows": preview[:preview_rows],
        "rows_returned": min(len(preview), preview_rows),
        "rows_total": total if (output is not None or not truncated) else None,
        "is_preview": truncated,
    }


def _describe(con, tables: dict[str, Path]) -> str:
    """The tables a failed query could have meant, with their columns: what a typo is corrected from."""
    if not tables:
        return "Tables in this query: those of the database."
    lines = []
    for name in tables:
        try:
            columns = [row[0] for row in con.execute(f"DESCRIBE {_identifier(name)}").fetchall()]
        except Exception:
            columns = []
        lines.append(f"Table {name}: {', '.join(columns[:40])}{' ...' if len(columns) > 40 else ''}")
    return "\n".join(lines)


def _run_duckdb(
    sql: str,
    tables: dict[str, Path],
    database: Path | None,
    output: Path | None,
    preview_rows: int,
    memory_mb: int,
    threads: int,
) -> dict[str, Any]:
    import duckdb

    spill = tempfile.mkdtemp(prefix="query-spill-")
    con = duckdb.connect(":memory:")
    try:
        con.execute(f"SET threads={int(threads)}")
        con.execute(f"SET memory_limit={_quote(f'{memory_mb}MB')}")
        con.execute(f"SET temp_directory={_quote(spill)}")
        allowed = [str(p) for p in tables.values()]
        for name, path in tables.items():
            con.execute(_view_sql(name, path))
        if database is not None:
            con.execute(f"ATTACH {_quote(str(database))} AS source_db (READ_ONLY)")
            con.execute("USE source_db")
            allowed.append(str(database))
        con.execute(f"SET temp_directory={_quote(spill)}")
        allowed.append(spill)
        con.execute("SET allowed_directories=[" + ", ".join(_quote(a) for a in [spill]) + "]")
        con.execute("SET allowed_paths=[" + ", ".join(_quote(a) for a in allowed if a != spill) + "]")
        con.execute("SET enable_external_access=false")
        con.execute("SET lock_configuration=true")

        statements = con.extract_statements(sql)
        if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
            raise QueryRefused(
                "Only one SELECT statement is run (WITH ... SELECT is fine). Nothing here writes: to keep a "
                "result, pass output_path."
            )
        started = time.monotonic()
        try:
            reader = con.execute(sql).to_arrow_reader(BATCH_ROWS)
            answer = _drain(iter(reader), output, preview_rows, reader.schema)
        except duckdb.OutOfMemoryException as exc:
            raise QueryRefused(_cap_message(memory_mb)) from exc
        except duckdb.PermissionException as exc:
            raise QueryRefused(
                "The query reached for a file it was not given. Name every file in `tables` (or `file_path`) "
                f"and query it by that name. ({str(exc).splitlines()[0]})"
            ) from exc
        except duckdb.Error as exc:
            raise QueryRefused(f"{str(exc).strip()}\n{_describe(con, tables)}") from exc
        answer["seconds"] = round(time.monotonic() - started, 2)
        answer["engine"] = "duckdb"
        return answer
    finally:
        con.close()
        shutil.rmtree(spill, ignore_errors=True)


_SQLITE_OK = {
    sqlite3.SQLITE_SELECT,
    sqlite3.SQLITE_READ,
    sqlite3.SQLITE_FUNCTION,
    sqlite3.SQLITE_RECURSIVE,
}


def _sqlite_authorizer(action: int, *_args: Any) -> int:
    return sqlite3.SQLITE_OK if action in _SQLITE_OK else sqlite3.SQLITE_DENY


def _sqlite_batches(con: sqlite3.Connection, sql: str) -> Iterator:
    """The result of `sql` as Arrow record batches of one schema, fixed by the first batch."""
    import pyarrow as pa

    schema = None
    for chunk in pd.read_sql_query(sql, con, chunksize=BATCH_ROWS):
        if schema is None:
            schema = pa.Schema.from_pandas(chunk, preserve_index=False)
        try:
            yield pa.RecordBatch.from_pandas(chunk, schema=schema, preserve_index=False)
        except (pa.ArrowInvalid, pa.ArrowTypeError) as exc:
            raise QueryRefused(
                "A column changes type partway through the result (SQLite lets one column hold text and "
                f"numbers). CAST it in the query, e.g. CAST(col AS TEXT). ({exc})"
            ) from exc
    if schema is None:
        cursor = con.execute(sql)
        names = [d[0] for d in cursor.description or []]
        yield pa.RecordBatch.from_pylist([], schema=pa.schema([(n, pa.string()) for n in names]))


def _run_sqlite(sql: str, database: Path, output: Path | None, preview_rows: int) -> dict[str, Any]:
    first = sql.lstrip().lower()
    if not first.startswith(("select", "with")):
        raise QueryRefused("Only one SELECT statement is run (WITH ... SELECT is fine); this database is read-only.")
    con = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True)
    started = time.monotonic()
    try:
        con.execute("PRAGMA query_only=ON")
        con.set_authorizer(_sqlite_authorizer)
        try:
            answer = _drain(_sqlite_batches(con, sql), output, preview_rows)
        except (sqlite3.Error, pd.errors.DatabaseError) as exc:
            raise QueryRefused(f"SQLite refused the query: {exc}") from exc
    finally:
        con.close()
    answer["seconds"] = round(time.monotonic() - started, 2)
    answer["engine"] = "sqlite"
    return answer


def run_query(
    sql: str,
    *,
    tables: dict[str, Path] | None = None,
    database: Path | None = None,
    output: Path | None = None,
    preview_rows: int = DEFAULT_PREVIEW,
    memory_mb: int = 1024,
    threads: int = 1,
) -> dict[str, Any]:
    """Run one read-only SELECT. Raises QueryRefused for anything the caller can fix by asking differently."""
    if not sql or not sql.strip():
        raise QueryRefused("sql is empty.")
    preview_rows = max(0, min(int(preview_rows), MAX_PREVIEW))
    tables = tables or {}
    if output is not None and output.suffix.lower() not in OUTPUT_SUFFIXES:
        raise QueryRefused(f"output_path writes {', '.join(sorted(OUTPUT_SUFFIXES))}; got {output.suffix or 'none'!r}.")
    if database is not None and database.suffix.lower() in SQLITE_SUFFIXES:
        if tables:
            raise QueryRefused("A SQLite database and files cannot be queried together; export the table you need.")
        return _run_sqlite(sql, database, output, preview_rows)
    if database is not None and database.suffix.lower() not in DUCKDB_SUFFIXES:
        raise QueryRefused(
            f"{database.name}: databases read are SQLite ({', '.join(sorted(SQLITE_SUFFIXES))}) and "
            f"DuckDB ({', '.join(sorted(DUCKDB_SUFFIXES))}) files."
        )
    if not tables and database is None:
        raise QueryRefused("Name what to query: file_path (one file, called `data`), tables, or database.")
    return _run_duckdb(sql, tables, database, output, preview_rows, memory_mb, threads)
