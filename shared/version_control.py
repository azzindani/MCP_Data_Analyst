from __future__ import annotations

import os
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path


def snapshot(file_path: str) -> str:
    """Copy file into .mcp_versions/ atomically; return backup path string."""
    path = Path(file_path)
    versions_dir = path.parent / ".mcp_versions"
    versions_dir.mkdir(exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%S-%fZ")
    backup_name = f"{path.stem}_{timestamp}.bak"
    backup_path = versions_dir / backup_name
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
    versions_dir = path.parent / ".mcp_versions"
    if not versions_dir.exists():
        return []
    pattern = f"{path.stem}_*.bak"
    backups = sorted(versions_dir.glob(pattern), reverse=True)
    return [b.name for b in backups]
