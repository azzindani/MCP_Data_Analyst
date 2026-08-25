"""Asking a tool for a column that is not in the file answered with the name.

    generate_chart(value_column="spends", category_column="Gender")
      error: "'Gender'"
      hint:  "Check file_path, column names, and chart_type."

The whole error is the word the caller sent, in quotes. Nothing says it was
looked up, nothing says it was not found, and nothing says whether the tool is
complaining about a column, a key, or a value it is quoting back at you. The
hint then offers three guesses at a failure that already knew exactly which
name it could not find -- and one of the three, chart_type, was right.

`str(KeyError("Gender"))` is `"'Gender'"`. That is the whole cause. Every other
exception here stringifies into a sentence, so seventy-five `except Exception`
handlers put `str(exc)` in their `error` field and get a readable one; the
KeyError is the single shape where that produces a bare word. The same
one-quoted-word error was recorded twice before under different names -- a
filter operand missing its `value` key, a condition dict missing `label` -- and
fixed both times at the call site that raised it, which left every other site
raising it the same way.

So the fix is at the rendering end, once: `error_text()` for the field and a
KeyError branch in `hint_for_error()` for the hint, wired into all seventy-five.

Found in a pre-flight check before a coverage sweep, on a column name typed
from memory that turned out not to exist.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from servers.data_visual import engine as dv
from shared.file_utils import error_text, hint_for_error, missing_name

FALLBACK = "Check file_path, column names, and chart_type."


class TestTheNameIsRecoverable:
    def test_a_key_error_yields_the_name_it_carried(self) -> None:
        assert missing_name(KeyError("Gender")) == "Gender"

    def test_anything_else_is_not_a_missing_name(self) -> None:
        assert missing_name(ValueError("Gender")) is None
        assert missing_name(KeyError()) is None

    def test_a_non_string_key_still_names_something(self) -> None:
        # df[0] on a frame with string columns raises KeyError(0).
        assert missing_name(KeyError(0)) == "0"


class TestTheErrorFieldIsASentence:
    def test_it_no_longer_is_the_bare_quoted_word(self) -> None:
        text = error_text(KeyError("Gender"))
        assert text != "'Gender'"
        assert "Gender" in text
        # Not just quotes and a name: it has to say what happened to it.
        assert re.search(r"\bnamed\b", text), text

    def test_an_empty_key_error_still_says_something(self) -> None:
        # str(KeyError()) is "", which would leave the error field empty --
        # the one shape the contract has no reading for.
        assert error_text(KeyError()).strip()

    def test_every_other_exception_is_left_alone(self) -> None:
        assert error_text(ValueError("rows have different lengths")) == "rows have different lengths"
        assert error_text(TypeError("unsupported operand")) == "unsupported operand"


class TestTheHintStopsGuessing:
    def test_it_names_what_was_not_found(self) -> None:
        hint = hint_for_error(KeyError("Gender"), FALLBACK)
        assert "Gender" in hint
        assert hint != FALLBACK

    def test_it_points_at_the_tool_that_lists_columns(self) -> None:
        assert "inspect_dataset" in hint_for_error(KeyError("Gender"), FALLBACK)

    def test_the_domain_hint_still_wins_where_it_was_right(self) -> None:
        assert hint_for_error(ValueError("bad chart"), FALLBACK) == FALLBACK

    def test_the_earlier_branches_are_unchanged(self) -> None:
        assert "Permission denied" in hint_for_error(PermissionError(13, "denied"), FALLBACK)
        assert "does not exist" in hint_for_error(FileNotFoundError(2, "nope"), FALLBACK)


class TestThroughARealTool:
    """The helpers are only worth anything where the tools actually call them."""

    @pytest.fixture()
    def csv_path(self, ad_data_full_csv: Path) -> str:
        return str(ad_data_full_csv)

    def test_a_column_the_file_does_not_have(self, csv_path: str, tmp_path: Path) -> None:
        res = dv.generate_chart(
            file_path=csv_path,
            chart_type="bar",
            value_column="spends",
            category_column="Gender",
            output_path=str(tmp_path / "chart.html"),
        )
        assert res["success"] is False
        assert res["error"] != "'Gender'"
        assert "Gender" in res["error"]
        assert "Gender" in res["hint"]
        assert res["hint"] != FALLBACK

    def test_a_column_that_is_there_still_works(self, csv_path: str, tmp_path: Path) -> None:
        out = tmp_path / "ok.html"
        res = dv.generate_chart(
            file_path=csv_path,
            chart_type="bar",
            value_column="spends",
            category_column="device",
            output_path=str(out),
        )
        assert res["success"] is True, res
        assert out.exists()
