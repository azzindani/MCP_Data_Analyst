"""The description is a contract, and four of them had drifted from the code.

Round 24 asked one question of every tool: is the sentence your client shows
true? Four answers here were no, and they are all the same shape -- a
vocabulary written down twice, where the copies drifted.

    dayfirst: auto true false.        -> "yes", "1", "banana" all accepted
    test: shapiro_wilk t_test anova   -> seventeen tests exist, six were listed
    vertically (rows) or horizontally -> the parser takes rows / columns
    auto features, or derive ...      -> there is no "auto" feature type

`dayfirst` was the one that changed an answer rather than a word. Every value
returned `success: true`; truthy-looking spellings became day-first and
everything else fell through to auto-detect, so `ture` and `flase` silently
picked a date interpretation and the response said nothing. Measured on
`Ad_Data.csv`, whose Date column is unambiguous ISO, `dayfirst="yes"` moved the
series from 2019-10-16..2020-07-07 to 2019-01-11..2020-12-06, and those dates
reach `trend`, `seasonality`, the rolling stats and the chart. Office's
bold/italic is the same tri-state string solved properly -- a wrong value is
refused with a hint naming the accepted forms -- so this is the copy that drifted.

The other three cost a caller a round trip rather than a wrong number, which is
cheaper but not free: the word the description used was the word the tool
rejected.

The static tests below exist so this cannot drift again. They read the
description out of the source and compare it against the vocabulary the code
actually enforces -- the check nobody was running when six of seventeen tests
became the documented set.
"""

from __future__ import annotations

import ast
import pathlib

import pandas as pd
import pytest

from shared.column_utils import DAYFIRST_CHOICES, parse_dates

REPO = pathlib.Path(__file__).resolve().parents[1]


def _docstring(relpath: str, func: str) -> str:
    """The tool's own description, read from the source it is declared in."""
    tree = ast.parse((REPO / relpath).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == func:
            return ast.get_docstring(node) or ""
    raise AssertionError(f"{func} not found in {relpath}")


ISO = pd.Series(["2019-10-16", "2019-11-01", "2020-07-07"])


class TestDayfirstRefusesWhatItDoesNotDocument:
    @pytest.mark.parametrize("value", sorted(DAYFIRST_CHOICES))
    def test_every_documented_value_is_accepted(self, value):
        _, meta = parse_dates(ISO, value)
        assert isinstance(meta["dayfirst"], bool)

    @pytest.mark.parametrize("value", ["banana", "ture", "flase", "maybe", "2"])
    def test_an_undocumented_value_is_refused(self, value):
        with pytest.raises(ValueError) as exc:
            parse_dates(ISO, value)
        assert value in str(exc.value)

    def test_and_the_refusal_names_the_three_that_work(self):
        with pytest.raises(ValueError) as exc:
            parse_dates(ISO, "ture")
        for choice in DAYFIRST_CHOICES:
            assert choice in str(exc.value)

    @pytest.mark.parametrize("value", ["yes", "1", "no", "0"])
    def test_the_generous_aliases_still_work(self, value):
        """A client that sends the JSON boolean puts "true"/"1" on the wire."""
        _, meta = parse_dates(ISO, value)
        assert meta["dayfirst"] is (value in {"yes", "1"})

    def test_empty_means_auto_not_a_refusal(self):
        """An omitted optional argument arrives as "", and that is the default."""
        _, meta = parse_dates(ISO, "")
        assert meta["dayfirst"] is False

    def test_auto_reads_iso_the_way_iso_is_written(self):
        _, meta = parse_dates(ISO, "auto")
        assert meta["dayfirst"] is False

    def test_forcing_day_first_on_iso_really_does_move_the_dates(self):
        """Why the silent coercion mattered: the answer changes, not just a flag."""
        auto, _ = parse_dates(ISO, "auto")
        forced, _ = parse_dates(ISO, "true")
        assert list(auto) != list(forced)

    def test_the_description_still_names_exactly_those_three(self):
        """If the vocabulary grows, the sentence has to grow with it."""
        doc = _docstring("servers/data_statistics/server.py", "time_series_analysis")
        assert "dayfirst" in doc
        for choice in DAYFIRST_CHOICES:
            assert choice in doc, f"{choice} missing from the description"


class TestTheDescriptionMatchesTheVocabularyTheCodeEnforces:
    def test_statistical_test_no_longer_lists_a_subset_as_if_complete(self):
        from servers.data_statistics._stats_tests import _VALID_TESTS

        doc = _docstring("servers/data_statistics/server.py", "statistical_test")
        listed = {t for t in _VALID_TESTS if t in doc}
        # Either name them all, or name none and say how many there are.
        # Naming six of seventeen is the failure this test exists for.
        assert not (0 < len(listed) < len(_VALID_TESTS)), (
            f"the description lists {len(listed)} of {len(_VALID_TESTS)} tests as if that were the set"
        )
        assert str(len(_VALID_TESTS)) in doc, "say how many there are if you will not list them"

    def test_an_unknown_test_still_enumerates_the_real_vocabulary(self, tmp_path):
        """The description now points here, so here has to keep working.

        A real file, because the missing-file guard runs first and its hint is
        about the path -- which is correct, and would have made this test pass
        against the wrong sentence.
        """
        from servers.data_statistics._stats_tests import _VALID_TESTS
        from servers.data_statistics.engine import statistical_test

        csv = tmp_path / "d.csv"
        csv.write_text("a,b\n1,2\n3,4\n5,6\n", encoding="utf-8")
        out = statistical_test(str(csv), test="definitely_not_a_test")
        assert out["success"] is False
        missing = [t for t in _VALID_TESTS if t not in out["hint"]]
        assert not missing, f"the hint does not name: {sorted(missing)}"

    def test_concat_datasets_documents_the_words_the_parser_takes(self):
        doc = _docstring("servers/data_transform/server.py", "concat_datasets")
        assert "rows" in doc and "columns" in doc
        assert "vertically" not in doc and "horizontally" not in doc

    def test_feature_engineering_does_not_document_a_type_that_does_not_exist(self):
        doc = _docstring("servers/data_transform/server.py", "feature_engineering")
        for real in ("bins", "date_parts", "one_hot", "text_length"):
            assert real in doc, f"{real} is a real feature type and is not documented"
        assert "auto features" not in doc
