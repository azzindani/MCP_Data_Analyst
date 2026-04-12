from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path


def resolve_path(file_path: str, allowed_extensions: tuple[str, ...] = ()) -> Path:
    """Return resolved absolute Path; raises ValueError for bad extension."""
    path = Path(file_path).resolve()
    if allowed_extensions and path.suffix.lower() not in allowed_extensions:
        raise ValueError(f"Extension {path.suffix!r} not allowed. Allowed: {allowed_extensions}")
    return path


def get_default_output_dir(input_path: str | None = None) -> Path:
    """Return default output dir: input file's parent if provided, else ~/Downloads."""
    if input_path:
        p = Path(input_path).resolve()
        if p.parent.exists():
            return p.parent
    return Path.home() / "Downloads"


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
