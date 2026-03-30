from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


def _receipt_path(file_path: str) -> Path:
    p = Path(file_path)
    return p.parent / (p.name + ".mcp_receipt.json")


def append_receipt(
    file_path: str,
    tool: str,
    args: dict,
    result: str,
    backup: str = "",
) -> None:
    """Append one entry to the receipt log. Never raises."""
    try:
        rpath = _receipt_path(file_path)
        entries: list[dict] = []
        if rpath.exists():
            try:
                entries = json.loads(rpath.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                entries = []
        entries.append({
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ"),
            "tool": tool,
            "args": args,
            "result": result,
            "backup": backup,
        })
        rpath.write_text(json.dumps(entries, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.warning("append_receipt failed silently: %s", exc)


def read_receipt_log(file_path: str, last_n: int = 10) -> list[dict]:
    """Return receipt entries, newest first. Empty list if no receipt exists."""
    rpath = _receipt_path(file_path)
    if not rpath.exists():
        return []
    try:
        entries: list[dict] = json.loads(rpath.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []
    entries = list(reversed(entries))
    if last_n > 0:
        entries = entries[:last_n]
    return entries
