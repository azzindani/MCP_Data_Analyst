"""Reading a big file for a chain lazily: filter as it streams, and hold only the columns used.

A chain read its files whole before the first filter ran, so a job that keeps
a thousand rows of a ten-million-row file held all ten million, with every
column, in memory first. For a file over MCP_CHAIN_LAZY_MB, a load read by one
step is streamed in chunks instead:

- the filters that step opens with run on each chunk as it arrives, so only
  the rows they keep are ever held;
- when the table then feeds one group_by, scalar, pivot or resample, only the
  columns those steps name are kept.

The answer has to be the one the whole-file read gives, and it is the same
code on the same parser: pandas reads each chunk, and the chain's own filter
runs on it. What could differ is a column's type -- pandas infers it per chunk
here and over the whole file there -- zero-padded ids, which the whole read
keeps as text, and a malformed line, which the whole read skips. So a column
the chunks typed differently is read again with the type the whole read
gives it, padded ids are found the way the whole read finds them, and a
malformed line, a type that cannot be settled, or anything else at all going
wrong hands the file back to the whole read, as it always was.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd

from shared.expr import substitute
from shared.file_utils import _ENCODING_FALLBACKS, _PADDED_NUMBER

LAZY_MB = "MCP_CHAIN_LAZY_MB"
CHUNK_ROWS = "MCP_CHAIN_CHUNK_ROWS"
# Ops whose columns are all named in their fields. After one of these, a
# table read with only the named columns holds everything the op reads; an op
# like drop_duplicates with no subset reads every column, so it stops pruning.
EXPLICIT_OPS = frozenset(
    {
        "filter",
        "derive",
        "drop_column",
        "filter_isin",
        "filter_not_isin",
        "filter_between",
        "clip_values",
        "round_values",
        "abs_values",
    }
)
# Steps that read only the columns their fields name.
NAMING_STEPS = frozenset({"group_by", "scalar", "pivot", "resample"})
# The types read_csv gives: _NUMBERS mix to float64 over the whole file.
_NUMBERS = frozenset({"int64", "float64"})
_NUMBER_KINDS = frozenset({"int64", "float64", "uint64", "bool"})
# A dry run's sample of a load: the chain's _SAMPLE_ROWS.
HEAD_ROWS = 3


def _threshold() -> float:
    try:
        return float(os.environ.get(LAZY_MB, "64"))
    except ValueError:
        return 64.0


def _chunk_rows() -> int:
    try:
        return max(1, int(os.environ.get(CHUNK_ROWS, "200000")))
    except ValueError:
        return 200_000


def _texts(value: Any) -> list[str]:
    if isinstance(value, dict):
        return [t for v in value.values() for t in _texts(v)]
    if isinstance(value, (list, tuple)):
        return [t for v in value for t in _texts(v)]
    return [value] if isinstance(value, str) else []


def plan(planned: list[dict[str, Any]], until: str, dry_run: bool = False) -> dict[str, dict[str, Any]]:
    """The loads worth streaming: big, read by one ops step that opens with filters.

    Returns {load id: {"reader": ops step id, "filters": how many leading
    filters, "texts": every string the reader and its one consumer hold}} --
    the texts decide, once the header is known, which columns are kept. A dry
    run keeps them all: its samples show every column.
    """
    limit = _threshold() * 2**20
    order = {s["id"]: k for k, s in enumerate(planned)}
    stop = order.get(until, len(planned)) if until else len(planned)
    readers: dict[str, list[dict[str, Any]]] = {}
    for s in planned:
        for ref in s["reads"]:
            readers.setdefault(ref, []).append(s)
    lazy: dict[str, dict[str, Any]] = {}
    for s in planned:
        path = s.get("path")
        if s["kind"] != "load" or path is None or not path.is_file() or path.stat().st_size < limit:
            continue
        reads = readers.get(s["id"], [])
        if len(reads) != 1 or reads[0]["kind"] != "ops" or "fallback" in reads[0]["raw"]:
            continue
        reader = reads[0]
        if order[reader["id"]] > stop:
            continue
        ops = [op for _, op in reader["run_ops"]["ops"]]
        filters = next((i for i, op in enumerate(ops) if op.get("op") != "filter"), len(ops))
        if not filters:
            continue
        after = readers.get(reader["id"], [])
        texts: list[str] | None = None
        explicit = all(op.get("op") in EXPLICIT_OPS for op in ops)
        if explicit and len(after) == 1 and after[0]["kind"] in NAMING_STEPS and reader["id"] != until and not dry_run:
            texts = _texts(reader["run_ops"]) + _texts(after[0]["raw"])
        lazy[s["id"]] = {"reader": reader["id"], "filters": filters, "texts": texts}
    return lazy


def _encodings(encoding: str = "utf-8") -> list[str]:
    """The order read_csv tries: the given one, its fallbacks, then latin-1, which never fails."""
    return [encoding, *[e for e in _ENCODING_FALLBACKS if e != encoding], "latin-1"]


def _unified(kinds: set[str]) -> Any:
    """The type the whole read gives a column its chunks typed differently; None if it cannot be said.

    Whole, pandas makes a column of whole numbers with one gap float64, and a
    column with any text str, text or not. A chunk sees only its own rows: whole
    numbers in one, the gap in the next, an empty stretch that reads as float64.
    """
    if kinds <= _NUMBERS:
        return "float64"
    if "str" in kinds and kinds <= {"str", "object", "bool", *_NUMBERS}:
        return str
    return None


class _Pass:
    """One streaming pass: the rows the filters kept, the first rows, and every chunk's column types."""

    def __init__(self) -> None:
        self.kept: list[pd.DataFrame] = []
        self.empty: pd.DataFrame | None = None
        self.head: pd.DataFrame | None = None
        self.dtypes: dict[str, set[str]] = {}
        self.rows = 0
        self.counts: list[int] = []


def _stream(
    path: Path, enc: str, names: list[str], keep: list[str] | None, dtype: dict[str, Any], run_filters
) -> _Pass | None:
    """Read `path` in chunks, filtering each. None when a line has more fields than the header.

    The header is skipped and named here, with one spare name past it: a line
    with an extra field fills the spare. pandas, whole, refuses such a line (or
    indexes the file by its first column); in chunks it may drop the field in
    silence -- so a filled spare hands the file back to the whole read.
    """
    spare = next(f"_spare{i}" for i in range(len(names) + 1) if f"_spare{i}" not in names)
    state = _Pass()
    stripped = [n.strip() for n in names]
    with pd.read_csv(
        path,
        sep=",",
        encoding=enc,
        header=None,
        skiprows=1,
        names=[*names, spare],
        index_col=False,
        dtype=dtype or None,
        chunksize=_chunk_rows(),
        low_memory=False,
    ) as reader:
        for chunk in reader:
            if chunk[spare].notna().any():
                return None
            chunk = chunk.drop(columns=spare)
            for raw in names:
                state.dtypes.setdefault(raw, set()).add(str(chunk[raw].dtype))
            chunk.columns = stripped
            if state.head is None or len(state.head) < HEAD_ROWS:
                top = chunk.head(HEAD_ROWS)
                state.head = top if state.head is None else pd.concat([state.head, top]).head(HEAD_ROWS)
            state.rows += len(chunk)
            kept, counts = run_filters(chunk)
            if keep is not None:
                kept = kept[keep]
            state.counts = [a + b for a, b in zip(state.counts, counts, strict=True)] if state.counts else counts
            if len(kept):
                state.kept.append(kept)
            elif state.empty is None:
                state.empty = kept
    return state


def _numeric(kinds: set[str]) -> bool:
    """Whether the whole read's type for the column is a number, as padded_id_columns asks."""
    return (len(kinds) == 1 and next(iter(kinds)) in _NUMBER_KINDS) or kinds <= _NUMBERS


def _padded(path: Path, numeric: list[str]) -> list[str]:
    """read_csv_preserving_ids's padded_id_columns, read in chunks: the numeric columns written with a leading zero.

    The same read in every way but the chunks -- utf-8 whatever the file was
    read as, the stripped names as usecols -- so it fails where that one fails,
    and a failure keeps every column as parsed there too.
    """
    if not numeric:
        return []
    found: set[str] = set()
    try:
        with pd.read_csv(
            path,
            encoding="utf-8",
            sep=",",
            usecols=numeric,
            dtype=str,
            keep_default_na=False,
            low_memory=False,
            chunksize=_chunk_rows(),
        ) as reader:
            for chunk in reader:
                chunk.columns = chunk.columns.str.strip()
                found |= {c for c in numeric if c in chunk.columns and chunk[c].str.match(_PADDED_NUMBER).any()}
    except Exception:
        return []
    return [c for c in numeric if c in found]


def _first_line_blank(path: Path) -> bool:
    with path.open("rb") as f:
        first = f.readline()
    return not first.replace(b"\xef\xbb\xbf", b"").strip()


def read(
    path: Path, entry: dict[str, Any], filters: list[tuple[str, dict]], values: dict[str, Any], apply_filter
) -> dict[str, Any] | None:
    """Stream `path`, keeping what `filters` keep. None means read the file whole instead."""
    if _first_line_blank(path):
        return None  # the whole read looks past blank lines for its header
    for enc in _encodings():
        try:
            return _read_as(path, enc, entry, filters, values, apply_filter)
        except UnicodeDecodeError:
            continue
    return None


def _read_as(
    path: Path, enc: str, entry: dict[str, Any], filters: list[tuple[str, dict]], values: dict[str, Any], apply_filter
) -> dict[str, Any] | None:
    names = [str(c) for c in pd.read_csv(path, sep=",", encoding=enc, nrows=0).columns]
    stripped = [n.strip() for n in names]
    keep = None
    if entry["texts"] is not None and len(set(stripped)) == len(stripped):
        wanted = [name for name in stripped if any(name in t for t in entry["texts"])]
        if wanted and len(wanted) < len(names):
            keep = wanted

    def run_filters(chunk: pd.DataFrame) -> tuple[pd.DataFrame, list[int]]:
        counts = []
        for _, op in filters:
            chunk, _note = apply_filter(chunk, substitute(op["where"], values))
            counts.append(len(chunk))
        return chunk, counts

    state = _stream(path, enc, names, keep, {}, run_filters)
    if state is None or state.head is None:
        return None
    # Where the chunks typed a column differently, the whole read's type is
    # known, so read again with it set.
    forced: dict[str, Any] = {}
    kinds = dict(state.dtypes)
    for raw, seen in state.dtypes.items():
        if len(seen) > 1:
            forced[raw] = _unified(seen)
            if forced[raw] is None:
                return None
            kinds[raw] = {"float64" if forced[raw] == "float64" else "str"}
    numeric = [name for raw, name in zip(names, stripped, strict=True) if _numeric(kinds[raw])]
    for name in _padded(path, numeric):
        forced[name] = str  # padded only where every numeric name was already stripped, so this is its raw name
    if forced:
        state = _stream(path, enc, names, keep, forced, run_filters)
        if state is None or state.head is None or any(len(seen) != 1 for seen in state.dtypes.values()):
            return None
    if state.kept:
        df = pd.concat(state.kept, ignore_index=True)
    elif state.empty is not None:
        df = state.empty.reset_index(drop=True)
    else:
        return None
    return {
        "table": df,
        "rows": state.rows,
        "columns": len(names),
        "columns_read": list(keep) if keep is not None else stripped,
        "counts": state.counts,
        "head": state.head.reset_index(drop=True),
    }
