"""Where a derived file came from, written beside the derived file.

The user review asked for this on the filtered CSV it checked:

    `Credit_Risk_chargedoff.csv` (1.1 MB, 5,333 rows) -- GOOD
    AGI: add `_lineage.json` sidecar (filter, before/after, timestamp,
    version); keep unique `{source}_{filter}_{date}.csv` naming.

**This is not the receipt, and the difference is the whole point.** A receipt is
keyed on a file and records what was done *to* it -- `shared/receipt.py` says so
in `RECEIPT_SCOPE`: "mutations only". A file that has just been created has no
mutations, so `Credit_Risk_chargedoff.csv.mcp_receipt.json` would be an empty
log forever, while the one question a reader has about that file -- *5,333 rows
out of what, selected how?* -- is answered nowhere. The source's receipt records
the filter, but a reader holding the derived file does not know which source to
go and read.

So lineage is keyed on the **derived** file and answers the other direction:
what this is, what it came from, how many rows went in and came out, and when.

**The chain is by reference, not by copy.** When the source has a lineage of its
own, `source_lineage` names it rather than inlining it. A three-step derivation
then costs three small files instead of one that grows quadratically, and no
step can disagree with the step before it because no step restates it.

**`rows_after` and `rows_before` are both recorded, and the percentage is
derived from them.** Same rule as `truncated` in `shared/counts.py` and
`was_sampled` in `shared/provenance.py`: a number a reader can check against the
two beside it can never drift away from them.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from shared.receipt import fingerprint

logger = logging.getLogger(__name__)

# Same shape as `.mcp_receipt.json`, so the two sidecars sort together beside
# the file they describe and neither looks like the dataset itself.
LINEAGE_SUFFIX = ".mcp_lineage.json"

# Bumped when the shape changes in a way a reader must notice. Readers here
# already handle a receipt written in three different shapes; starting this one
# with a version means they will never have to.
LINEAGE_FORMAT = 1

LINEAGE_SCOPE = (
    "how this file was derived: the source it came from, the operation that "
    "made it, and the row counts on both sides. Later edits to this file are "
    "recorded in its .mcp_receipt.json, not here."
)


def lineage_path(derived_path: str | Path) -> Path:
    """The sidecar that describes `derived_path`."""
    p = Path(derived_path)
    return p.parent / (p.name + LINEAGE_SUFFIX)


def write_lineage(
    derived_path: str | Path,
    *,
    op: str,
    source: str | Path | None,
    rows_before: int | None = None,
    rows_after: int | None = None,
    columns_before: int | None = None,
    columns_after: int | None = None,
    params: dict[str, Any] | None = None,
    note: str = "",
) -> str:
    """Write the sidecar. Returns its path, or "" if it could not be written.

    Never raises. A derivation that succeeded must not be reported as failed
    because its provenance file could not be written -- the file the caller
    asked for is on disk either way, and the response says whether the sidecar
    made it.
    """
    try:
        derived = Path(derived_path)
        entry: dict[str, Any] = {
            "_format": LINEAGE_FORMAT,
            "_scope": LINEAGE_SCOPE,
            "op": op,
            "derived": derived.name,
            "derived_fingerprint": fingerprint(derived),
            "ts": datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ"),
        }

        if source is not None:
            src = Path(source)
            entry["source"] = str(src)
            entry["source_name"] = src.name
            entry["source_fingerprint"] = fingerprint(src)
            # By reference. Inlining the parent would make a three-step chain
            # restate step one three times, and restatements drift.
            parent = lineage_path(src)
            if parent.exists():
                entry["source_lineage"] = str(parent)

        if rows_before is not None:
            entry["rows_before"] = int(rows_before)
        if rows_after is not None:
            entry["rows_after"] = int(rows_after)
        if rows_before is not None and rows_after is not None and int(rows_before) > 0:
            entry["rows_kept_pct"] = round(int(rows_after) / int(rows_before) * 100, 2)
        if columns_before is not None:
            entry["columns_before"] = int(columns_before)
        if columns_after is not None:
            entry["columns_after"] = int(columns_after)
        if params:
            entry["params"] = params
        if note:
            entry["note"] = note

        out = lineage_path(derived)
        out.write_text(json.dumps(entry, indent=2, default=str), encoding="utf-8")
        return str(out)
    except Exception as exc:
        logger.warning("write_lineage failed silently: %s", exc)
        return ""


def note_lineage(result: dict[str, Any], derived_path: str | Path, **fields: Any) -> str:
    """Write the sidecar and tell the caller where it went, in one call.

    Every derivation tool spells the response field the same way because they
    all go through here. A sidecar nobody is told about is a file on disk that
    no agent will ever open.
    """
    written = write_lineage(derived_path, **fields)
    if written:
        result["lineage_path"] = written
    return written


def read_lineage(derived_path: str | Path) -> dict[str, Any]:
    """Read the sidecar for a derived file. `{}` when there is none.

    A missing sidecar is not an error: files that arrived from outside this
    fleet have no derivation to record, and saying so with an empty dict is
    more useful than an exception a caller has to catch.
    """
    try:
        p = lineage_path(derived_path)
        if not p.exists():
            return {}
        loaded = json.loads(p.read_text(encoding="utf-8"))
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


def lineage_chain(derived_path: str | Path, max_steps: int = 10) -> list[dict[str, Any]]:
    """Walk `source_lineage` back to the original, newest step first.

    `max_steps` is a cycle guard rather than a policy: two files that somehow
    name each other as source would otherwise loop forever inside a tool that
    was only trying to answer where a CSV came from.
    """
    chain: list[dict[str, Any]] = []
    seen: set[str] = set()
    current = Path(derived_path)
    for _ in range(max_steps):
        entry = read_lineage(current)
        if not entry:
            break
        chain.append(entry)
        src = entry.get("source")
        if not src or str(src) in seen:
            break
        seen.add(str(src))
        current = Path(str(src))
    return chain
