"""Ring-2 infrastructure utility — performs file I/O (read_csv, atomic_write).
NOT part of the pure innermost ring. Engine.py calls these as lateral peers,
not as inner-layer dependencies.
"""

from __future__ import annotations

import base64
import mimetypes
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd

from shared.exchange import (
    attach_public_url,
    fetch_url,
    get_inbox_dir,
    get_output_dir,
    is_url,
    public_url_for,
    url_fetch_enabled,
)

__all__ = [
    "atomic_write",
    "atomic_write_text",
    "attach_public_url",
    "embed_content",
    "fetch_url",
    "get_default_output_dir",
    "get_inbox_dir",
    "get_output_dir",
    "is_url",
    "public_url_for",
    "read_csv",
    "resolve_path",
    "url_fetch_enabled",
]


def resolve_path(file_path: str, allowed_extensions: tuple[str, ...] = ()) -> Path:
    """Return resolved absolute Path; handles workspace:name/alias and project:name/alias.

    Delegates alias resolution to workspace_utils.resolve_alias which supports
    both workspace: (new) and project: (legacy) prefix formats.

    An http(s) URL is downloaded into the inbox dir first and its local path
    returned, so every tool that takes a file path also takes a link once the
    server runs with MCP_FETCH_URLS=1 (off by default — see shared/exchange.py).
    """
    if is_url(file_path):
        path = fetch_url(file_path)
        if allowed_extensions and path.suffix.lower() not in allowed_extensions:
            raise ValueError(f"Extension {path.suffix!r} not allowed. Allowed: {allowed_extensions}")
        return path
    if file_path.startswith("workspace:") or file_path.startswith("project:"):
        try:
            from shared.workspace_utils import resolve_alias

            path = resolve_alias(file_path)
        except Exception as exc:
            raise ValueError(f"Cannot resolve project alias '{file_path}': {exc}") from exc
    else:
        path = Path(file_path).resolve()
    if allowed_extensions and path.suffix.lower() not in allowed_extensions:
        raise ValueError(f"Extension {path.suffix!r} not allowed. Allowed: {allowed_extensions}")
    return path


def get_default_output_dir(input_path: str | None = None) -> Path:
    """Return default output dir: MCP_OUTPUT_DIR, else input's parent, else ~/Downloads.

    MCP_OUTPUT_DIR outranks the input file's directory: a remote deployment
    sets it precisely so generated files land somewhere the caller can reach,
    which an input file's own directory is not guaranteed to be.
    """
    if os.environ.get("MCP_OUTPUT_DIR", "").strip():
        return get_output_dir()
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


# mimetypes.guess_type() depends on the OS's registered MIME db (registry on
# Windows, /etc/mime.types on Linux/macOS) and doesn't reliably resolve every
# extension on every platform — verified missing common Office types on
# windows-latest CI runners specifically. Known extensions are checked first.
_KNOWN_MIME_TYPES = {
    ".html": "text/html",
    ".csv": "text/csv",
    ".json": "application/json",
    ".pdf": "application/pdf",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}


def embed_content(result: dict[str, Any], path: Path, return_content: bool) -> dict[str, Any]:
    """Attach `public_url`, and base64 file bytes when return_content is set.

    In remote/HTTP deployments the caller has no filesystem in common with this
    server, so a server-local output path is useless to it. `public_url` (set
    whenever the file lands under a publicly served MCP_OUTPUT_DIR) gives it a
    link; return_content gives it the bytes themselves. A read failure here
    doesn't fail the whole tool call.
    """
    if not result.get("success"):
        return result
    attach_public_url(result, path)
    if not return_content:
        return result
    try:
        data = path.read_bytes()
    except OSError:
        return result
    result["content_base64"] = base64.b64encode(data).decode("ascii")
    mime = _KNOWN_MIME_TYPES.get(path.suffix.lower())
    if mime is None:
        mime = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
    result["content_mime_type"] = mime
    return result
