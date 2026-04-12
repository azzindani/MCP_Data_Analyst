"""Verify output path priority in shared/html_layout.py and shared/file_utils.py.

CI gate: exits non-zero if the priority order is wrong.

Correct priority (§26 of STANDARDS.md):
  1. Explicit output_path argument  -> use it as-is
  2. Input file provided            -> save beside the input file
  3. No input file                  -> ~/Downloads
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ERRORS: list[str] = []


def fail(msg: str) -> None:
    ERRORS.append(f"FAIL — {msg}")


def ok(msg: str) -> None:
    print(f"  ok  {msg}")


# ---------------------------------------------------------------------------
# Test get_output_path (html_layout.py)
# ---------------------------------------------------------------------------

def check_html_layout() -> None:
    from shared.html_layout import get_output_path

    print("shared/html_layout.py :: get_output_path")

    with tempfile.TemporaryDirectory() as tmp:
        input_file = Path(tmp) / "data.csv"
        input_file.touch()

        # 1. Explicit output_path -> always wins
        explicit = str(Path(tmp) / "explicit_out.html")
        result = get_output_path(explicit, input_file, "chart", "html")
        if result != Path(explicit).resolve():
            fail(f"explicit output_path not honoured: got {result}")
        else:
            ok("explicit output_path wins")

        # 2. input_path provided, no explicit -> save beside input file
        result = get_output_path("", input_file, "chart", "html")
        if result.parent != input_file.parent:
            fail(
                f"with input file, output should be beside input "
                f"(expected parent={input_file.parent}, got parent={result.parent})"
            )
        else:
            ok("input file provided -> saved beside input file")

        # 3. No input path, no explicit -> ~/Downloads (or similar home subdir)
        result = get_output_path("", None, "chart", "html")
        home = Path.home()
        try:
            result.relative_to(home)
        except ValueError:
            fail(
                f"no input file -> expected path under home dir, got {result}"
            )
        else:
            ok("no input file -> path under home dir (Downloads or similar)")

        # 4. Ensure input-file parent is used even when ~/Downloads exists
        downloads = Path.home() / "Downloads"
        if downloads.is_dir():
            result = get_output_path("", input_file, "chart", "html")
            if result.parent == downloads:
                fail(
                    "Downloads folder exists but input file was provided — "
                    "output should be beside input file, not in Downloads"
                )
            else:
                ok("Downloads exists but input file takes priority")


# ---------------------------------------------------------------------------
# Test get_default_output_dir (file_utils.py)
# ---------------------------------------------------------------------------

def check_file_utils() -> None:
    from shared.file_utils import get_default_output_dir

    print("shared/file_utils.py :: get_default_output_dir")

    with tempfile.TemporaryDirectory() as tmp:
        input_file = Path(tmp) / "data.csv"
        input_file.touch()

        # input_path provided -> return its parent
        result = get_default_output_dir(str(input_file))
        if result != input_file.parent:
            fail(
                f"with input file, expected parent={input_file.parent}, got {result}"
            )
        else:
            ok("input file provided -> returns input file's parent directory")

        # no input_path -> return ~/Downloads
        result = get_default_output_dir(None)
        expected = Path.home() / "Downloads"
        if result != expected:
            fail(f"no input file, expected {expected}, got {result}")
        else:
            ok("no input file -> returns ~/Downloads")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    print("Checking output path priority (STANDARDS.md §26)...")
    print()
    check_html_layout()
    print()
    check_file_utils()
    print()

    if ERRORS:
        print(f"FAIL — {len(ERRORS)} error(s):")
        for e in ERRORS:
            print(f"  {e}")
        return 1

    print("OK — output path priority is correct")
    return 0


if __name__ == "__main__":
    sys.exit(main())
