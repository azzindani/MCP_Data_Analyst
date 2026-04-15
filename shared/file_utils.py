"""Ring-2 infrastructure utility — performs file I/O (read_csv, atomic_write).
NOT part of the pure innermost ring. Engine.py calls these as lateral peers,
not as inner-layer dependencies.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

import pandas as pd


def resolve_path(file_path: str, allowed_extensions: tuple[str, ...] = ()) -> Path:
    """Return resolved absolute Path; handles project:name/alias syntax.

    If file_path starts with 'project:', delegates to project_utils.resolve_alias
    to translate the alias into an absolute path before resolving.
    """
    # Lazy import to avoid circular dependency — project_utils imports file_utils
    if file_path.startswith("project:"):
        try:
            from shared.project_utils import resolve_alias

            path = resolve_alias(file_path)
        except Exception as exc:
            raise ValueError(f"Cannot resolve project alias '{file_path}': {exc}") from exc
    else:
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


_ENCODING_FALLBACKS = ("utf-8-sig", "cp1252", "latin-1")


def read_csv(
    file_path: str,
    encoding: str = "utf-8",
    separator: str = ",",
    max_rows: int = 0,
) -> pd.DataFrame:
    """Read CSV with automatic encoding and bad-line fallback.

    Tries the specified encoding first. On UnicodeDecodeError walks through
    utf-8-sig (BOM), cp1252 (Windows/Excel), then latin-1 (never fails).
    On tokenization errors (mismatched field counts) retries with
    on_bad_lines='skip' to drop malformed rows.
    """
    kwargs: dict = {"sep": separator, "low_memory": False}
    if max_rows > 0:
        kwargs["nrows"] = max_rows

    def _try_encs(extra: dict) -> pd.DataFrame:
        kw = {**kwargs, **extra}
        try:
            return pd.read_csv(file_path, encoding=encoding, **kw)
        except UnicodeDecodeError:
            pass
        for enc in _ENCODING_FALLBACKS:
            if enc == encoding:
                continue
            try:
                return pd.read_csv(file_path, encoding=enc, **kw)
            except UnicodeDecodeError:
                continue
        return pd.read_csv(file_path, encoding="latin-1", **kw)

    try:
        df = _try_encs({})
    except Exception as exc:
        if "tokeniz" in str(exc).lower() or "field" in str(exc).lower():
            df = _try_encs({"on_bad_lines": "skip"})
        else:
            raise

    df.columns = df.columns.str.strip()
    return df


def atomic_write(target: Path | str, content: bytes) -> None:
    """Write bytes to target atomically via temp file + move."""
    target = Path(target)
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


def atomic_write_text(target: Path | str, content: str, encoding: str = "utf-8") -> None:
    """Write text to target atomically."""
    atomic_write(target, content.encode(encoding))
