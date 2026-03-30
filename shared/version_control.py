from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path


def snapshot(file_path: str) -> str:
    """Copy file into .mcp_versions/; return backup path string."""
    path = Path(file_path)
    versions_dir = path.parent / ".mcp_versions"
    versions_dir.mkdir(exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S-%fZ")
    backup_name = f"{path.stem}_{timestamp}.bak"
    backup_path = versions_dir / backup_name
    shutil.copy2(str(path), str(backup_path))
    return str(backup_path)


def restore(file_path: str, backup_path: str) -> None:
    """Overwrite file_path with contents of backup_path."""
    shutil.copy2(backup_path, str(file_path))


def list_versions(file_path: str) -> list[str]:
    """Return backup filenames (newest first) for the given file."""
    path = Path(file_path)
    versions_dir = path.parent / ".mcp_versions"
    if not versions_dir.exists():
        return []
    pattern = f"{path.stem}_*.bak"
    backups = sorted(versions_dir.glob(pattern), reverse=True)
    return [b.name for b in backups]
