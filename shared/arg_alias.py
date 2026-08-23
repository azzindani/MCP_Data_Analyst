"""Accept the name a caller would reasonably have guessed for an argument.

A census of every `@mcp.tool()` signature in this repo -- 69 tools, 162 distinct
parameter names -- shows one concept is nearly always spelled one way, and the
handful of exceptions are where callers get refused:

    a column          *_column   22 names     *_col      7 names
    the dataset       file_path  60 tools     file_path_a/_b  1 tool
    how many rows     top_n       5 tools     n          1 tool
    which test        test_type   1 tool      test       1 tool

`data_statistics/server.py` has the clearest case: `time_series_analysis` takes
`date_column`, `cohort_analysis` takes `date_column`, and `period_comparison`
sitting between them takes `date_col`. Nothing distinguishes them; the short
form is an accident of who wrote which function.

The cost is not cosmetic. The live MCP schema carries no property descriptions,
so **the parameter name is the entire contract**, and pydantic rejects an
unknown name before any server code runs -- the tool never gets the chance to
say what it wanted. Sending each outlier the spelling its siblings use gets:

    aggregate_dataset    row_column     Unexpected keyword argument
    period_comparison    date_col       Missing required argument
    regression_analysis  y_col          Missing required argument
    statistical_test     test           Missing required argument
    export_data          output_format  Unexpected keyword argument
    sample_data          top_n          Unexpected keyword argument
    compare_datasets     file_path_a    Missing required argument

Renaming the outliers would fix the guess and break every existing caller, so
each one accepts both spellings and resolves here. The canonical name is the
one the majority of tools use, so a caller following the convention is always
right, and the older spelling keeps working.

Existing parameters keep their original positions and the aliases are appended,
because putting a new name where an old one sat silently rebinds positional
callers -- that bug shipped once already in a sibling repo.
"""

from __future__ import annotations

from typing import Any


def pick(op: str, field: str, primary: Any, alias: Any) -> tuple[str, str]:
    """Resolve one string argument given under either spelling.

    Returns (value, note). `note` is empty unless the alias was used, in which
    case it records the substitution for the progress log. A blank value with a
    non-empty note means neither spelling was given; callers turn that into
    their own error dict rather than raising.
    """
    p = (primary or "").strip() if isinstance(primary, str) else primary
    a = (alias or "").strip() if isinstance(alias, str) else alias
    if p:
        return p, ""
    if a:
        return a, f"Read {field} from an accepted alternative spelling"
    return "", f"{op} needs {field}"


def pick_list(op: str, field: str, primary: Any, alias: Any) -> tuple[list, str]:
    """Resolve one list argument given under either spelling."""
    if primary:
        return list(primary), ""
    if alias:
        return list(alias), f"Read {field} from an accepted alternative spelling"
    return [], f"{op} needs {field}"


def missing(op: str, field: str, alias: str) -> dict:
    """The error dict for an argument given under neither spelling."""
    return {
        "success": False,
        "op": op,
        "error": f"{op} needs {field}",
        "hint": f"Pass {field}=. The older spelling {alias}= is still accepted.",
        "progress": [],
        "token_estimate": 20,
    }
