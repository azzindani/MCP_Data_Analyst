"""Ring-2 infrastructure utility — writes/reads the per-file receipt log (I/O).
NOT part of the pure innermost ring. Engine.py calls these as lateral peers.

**What this log records, and why the file says so.**

A user review drove roughly twenty calls against one dataset -- loads,
inspections, correlations, aggregations, a filter, a model comparison -- and
then read the receipt. It held two entries. Both were true, and the log was not
broken: `append_receipt` is called by the tools that CHANGE a file, and reads
change nothing.

The defect was that nothing said so. A file named `.mcp_receipt.json` invites
exactly one reading -- a record of what happened to this data -- and an agent
that trusts it as an audit log concludes eighteen operations never ran. The
review's verdict was that the receipt could not be trusted as an audit log, and
that is the correct conclusion from a log which silently holds a subset.

So the scope is declared in the file itself, in `read_receipt_log`'s output,
and in `RECEIPT_SCOPE` for any tool that wants to say it in prose. And entries
now carry what makes them lineage rather than a note: a hash of the arguments,
a fingerprint of the file before and after, and how long it took. That is the
difference between "filter_dataset ran" and "filter_dataset turned exactly
this file into exactly that one".

Hashing is capped. A fingerprint of a 200 MB CSV on every write would cost more
than the write; above the cap the file is fingerprinted by size and mtime and
the entry says which kind it is, rather than pretending to a content hash.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# What the log holds. Stated here once, so a tool never has to guess.
RECEIPT_SCOPE = (
    "mutations only: operations that wrote to this file. Reads, inspections, "
    "correlations and chart generation are not recorded here."
)

# Above this, a content hash costs more than the operation it describes.
_MAX_HASH_BYTES = 64 * 1024 * 1024


def _receipt_path(file_path: str) -> Path:
    p = Path(file_path)
    return p.parent / (p.name + ".mcp_receipt.json")


def _hash_args(args: dict) -> str:
    """Stable hash of the arguments, so two calls can be told apart."""
    try:
        blob = json.dumps(args, sort_keys=True, default=str)
    except Exception:
        blob = repr(sorted(args.items()))
    return "sha256:" + hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def fingerprint(file_path: str | Path) -> str:
    """Identify a file's contents, or say honestly that this is not a hash.

    Returns `sha256:<16 hex>` for a file small enough to read, and
    `size-mtime:<...>` for one that is not. The prefix is the point: a caller
    comparing two fingerprints must be able to tell a content hash from a
    cheaper stand-in, because only one of them proves the bytes are the same.
    """
    p = Path(file_path)
    try:
        stat = p.stat()
    except OSError:
        return ""
    if stat.st_size > _MAX_HASH_BYTES:
        return f"size-mtime:{stat.st_size}-{int(stat.st_mtime)}"
    try:
        digest = hashlib.sha256()
        with p.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return "sha256:" + digest.hexdigest()[:16]
    except OSError:
        return ""


def append_receipt(
    file_path: str,
    tool: str,
    args: dict,
    result: str,
    backup: str = "",
    input_fingerprint: str = "",
    duration_ms: float | None = None,
) -> None:
    """Append one entry to the receipt log. Never raises.

    `input_fingerprint` is what `fingerprint()` returned BEFORE the write; the
    output side is measured here, after it. Pass it and the entry says what the
    operation turned into what. Omit it and the entry is still valid -- one
    side of a lineage is better than none, and no call site is obliged to
    change.
    """
    try:
        rpath = _receipt_path(file_path)
        entries: list[Any] = []
        scope_header: dict[str, Any] | None = None
        if rpath.exists():
            try:
                loaded = json.loads(rpath.read_text(encoding="utf-8"))
            except json.JSONDecodeError, OSError:
                loaded = []
            entries, scope_header = _split_header(loaded)

        entry: dict[str, Any] = {
            "ts": datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ"),
            "tool": tool,
            "args": args,
            "args_hash": _hash_args(args),
            "result": result,
            "backup": backup,
        }
        if input_fingerprint:
            entry["input"] = input_fingerprint
        after = fingerprint(file_path)
        if after:
            entry["output"] = after
        if duration_ms is not None:
            entry["duration_ms"] = round(float(duration_ms), 1)
        entries.append(entry)

        header = scope_header or {"_scope": RECEIPT_SCOPE, "_format": 2}
        rpath.write_text(json.dumps([header, *entries], indent=2), encoding="utf-8")
    except Exception as exc:
        logger.warning("append_receipt failed silently: %s", exc)


def _split_header(loaded: Any) -> tuple[list[dict], dict | None]:
    """Separate the scope header from the entries, for either file format.

    Version 1 files are a bare list of entries and are still read exactly as
    they were written -- an existing receipt does not become unreadable because
    the format grew a header.
    """
    if isinstance(loaded, dict):
        # MCP_Microsoft_Office wrote `{"file": ..., "entries": [...]}` until the
        # formats were converged. Files in that shape still exist on disk, and
        # every reader in the fleet returned [] for them.
        entries = loaded.get("entries", [])
        return [e for e in entries if isinstance(e, dict)], None
    if not isinstance(loaded, list) or not loaded:
        return [], None
    first = loaded[0]
    if isinstance(first, dict) and "_scope" in first:
        return [e for e in loaded[1:] if isinstance(e, dict)], first
    return [e for e in loaded if isinstance(e, dict)], None


def read_receipt_log(file_path: str, last_n: int = 10) -> list[dict]:
    """Return receipt entries, newest first. Empty list if no receipt exists."""
    entries, _ = read_receipt(file_path, last_n)
    return entries


def read_receipt(file_path: str, last_n: int = 10) -> tuple[list[dict], str]:
    """Entries newest first, and the scope sentence that belongs beside them.

    Two return values rather than one because the count alone is what misled a
    caller: twenty operations, two entries, and no way to learn from the log
    that eighteen of them were never eligible for it.
    """
    rpath = _receipt_path(file_path)
    if not rpath.exists():
        return [], RECEIPT_SCOPE
    try:
        loaded = json.loads(rpath.read_text(encoding="utf-8"))
    except json.JSONDecodeError, OSError:
        return [], RECEIPT_SCOPE
    entries, header = _split_header(loaded)
    scope = str(header.get("_scope")) if header else RECEIPT_SCOPE
    entries = list(reversed(entries))
    if last_n > 0:
        entries = entries[:last_n]
    return entries, scope
