"""Snapshot / restore / list_versions — the safety net behind every write.

A snapshot is named for the file it came from, and until now that name dropped
the extension: `report.csv` and `report.docx` sitting in the same directory both
snapshotted to `.mcp_versions/report_{timestamp}.bak`, and `list_versions` found
them with `glob(f"{stem}_*.bak")`. So one file's history was another file's
history. Against the live endpoints, with a 12-byte CSV and a Word document
beside it:

    restore_version(file_path="report.csv")
    -> success: true, restored_from ".mcp_versions/report_2026-...-485477Z.bak"

    report.csv   12 bytes of CSV  ->  37,117 bytes of .docx

The tool offered the wrong file's timestamps as valid choices and then restored
one, reporting success and suggesting `inspect_dataset()` to confirm. The same
glob had a second collision in it: `Ad_Data_*` matches a snapshot of
`Ad_Data_test.csv`, so two datasets whose names differ by a suffix shared a
history too.

File_System already writes `{stem}_{ts}{ext}.bak`; this brings its three
siblings in line. Reading stays more forgiving than writing so that snapshots
taken before this change are not stranded -- but an extension-less legacy name
is only accepted when nothing else in the directory shares the stem, which is
exactly the case where it cannot be ambiguous.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path

VERSIONS_DIRNAME = ".mcp_versions"

# A snapshot name is the stem, an underscore, then a UTC timestamp that always
# begins with a four-digit year. Globbing `{stem}_*` alone lets a snapshot of
# `Ad_Data_test.csv` answer a query about `Ad_Data.csv`.
_TS_GLOB = "[0-9][0-9][0-9][0-9]-*"


def _versions_dir(path: Path) -> Path:
    return path.parent / VERSIONS_DIRNAME


def _legacy_is_unambiguous(path: Path) -> bool:
    """True when no other file beside this one shares its stem.

    `report_{ts}.bak` could be a snapshot of report.csv or of report.docx. When
    only one `report.*` exists there is nothing to confuse it with, so the old
    name is still safe to offer.
    """
    try:
        siblings = list(path.parent.iterdir())
    except OSError:
        return False
    return not any(p.is_file() and p.stem == path.stem and p.suffix != path.suffix for p in siblings)


def snapshot(file_path: str) -> str:
    """Copy file into .mcp_versions/ atomically; return backup path string."""
    path = Path(file_path)
    versions_dir = _versions_dir(path)
    versions_dir.mkdir(exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%S-%fZ")
    # The extension is part of the name: without it this file's history is
    # indistinguishable from that of any namesake with a different extension.
    backup_path = versions_dir / f"{path.stem}_{timestamp}{path.suffix}.bak"
    # On Windows datetime resolution can be coarser than microseconds, so two
    # rapid snapshots may collide on the same timestamp.  Append a counter
    # suffix until we find an unused name.
    counter = 1
    while backup_path.exists():
        backup_path = versions_dir / f"{path.stem}_{timestamp}_{counter}{path.suffix}.bak"
        counter += 1
    # Write to a temp file in the same directory, then atomic rename so a
    # mid-copy crash cannot leave a partial .bak file.
    fd, tmp = tempfile.mkstemp(dir=versions_dir)
    try:
        os.close(fd)
        shutil.copy2(str(path), tmp)
        shutil.move(tmp, str(backup_path))
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return str(backup_path)


def restore(file_path: str, backup_path: str) -> None:
    """Overwrite file_path with contents of backup_path atomically."""
    path = Path(file_path)
    # Write to temp in same directory, then atomic rename onto the target so
    # a mid-copy crash cannot corrupt the live file.
    fd, tmp = tempfile.mkstemp(dir=path.parent)
    try:
        os.close(fd)
        shutil.copy2(backup_path, tmp)
        shutil.move(tmp, str(path))
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def list_versions(file_path: str) -> list[str]:
    """Return backup filenames (newest first) for the given file."""
    path = Path(file_path)
    versions_dir = _versions_dir(path)
    if not versions_dir.exists():
        return []
    names = {p.name for p in versions_dir.glob(f"{path.stem}_{_TS_GLOB}{path.suffix}.bak")}
    if _legacy_is_unambiguous(path):
        names |= {p.name for p in versions_dir.glob(f"{path.stem}_{_TS_GLOB}.bak")}
    return sorted(names, reverse=True)


def snapshot_if_exists(path: str | Path) -> str:
    """Snapshot a path that is about to be written over. "" if nothing is there.

    Tools that write to an `output_path` had no safety net: they overwrote
    whatever was already there and recorded nothing, while every tool that edits
    a dataset in place snapshots first. Against the live endpoints:

        export_data(file_path="d.csv", output_path="precious.csv")
        -> success: true, precious.csv now holds d.csv, .mcp_versions empty

    That is the retry case as well as the typo case -- a client re-sending a
    call whose first attempt timed out discards anything written in between.
    Never raises: a snapshot failure must not stop the write the caller asked
    for.
    """
    p = Path(path)
    if not p.is_file():
        return ""
    try:
        return snapshot(str(p))
    except Exception:
        return ""


def discard_snapshot_if_unchanged(backup: str, live: str | Path) -> str:
    """Drop a snapshot whose file the operation turned out not to change.

    A snapshot has to be taken before the write, because nothing knows yet
    whether the write will change anything. Round 11 measured what that costs
    when the answer is "nothing": every call kept a full copy regardless.

        apply_patch     regex_replace, second identical call, changed=0 rows
                        -> 1,938,840 B .bak written anyway
        restore_version to a timestamp the file already equals
                        -> another full-size .bak

    Four calls on one 1.9 MB CSV left ~7.5 MB in .mcp_versions, and a client
    retrying on timeout grows that without limit. Comparing after the fact is
    exact rather than a guess: a backup byte-identical to the file now on disk
    cannot restore anything the file does not already hold, so deleting it
    loses nothing. Returns the backup path, or "" if it was discarded.
    """
    if not backup:
        return ""
    b, live_path = Path(backup), Path(live)
    try:
        if not (b.is_file() and live_path.is_file()):
            return backup
        if b.stat().st_size != live_path.stat().st_size:
            return backup
        if b.read_bytes() != live_path.read_bytes():
            return backup
        b.unlink()
        return ""
    except Exception:
        # Never let tidying up fail an operation that already succeeded.
        return backup
