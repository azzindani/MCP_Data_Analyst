from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path


def resolve_path(file_path: str) -> Path:
    """Return absolute Path; raises FileNotFoundError if not found."""
    path = Path(file_path)
    if not path.is_absolute():
        path = Path.cwd() / path
    return path


def atomic_write(target: Path, content: bytes) -> None:
    """Write bytes to target atomically via temp file + move."""
    fd, tmp_path = tempfile.mkstemp(dir=target.parent)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(content)
        shutil.move(tmp_path, str(target))
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def atomic_write_text(target: Path, content: str, encoding: str = "utf-8") -> None:
    """Write text to target atomically."""
    atomic_write(target, content.encode(encoding))
