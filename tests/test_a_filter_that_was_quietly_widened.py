"""dtype="int64" answered with columns the same reply calls float64.

    search_columns(Ad_Data.csv, dtype="int64")
      -> matched: 4
         columns: [spends, impressions, clicks, link_clicks]
         dtypes : {spends: float64, impressions: int64,
                   clicks: int64, link_clicks: float64}

Two of those four are float64 by the tool's own account, in the same response.

The behaviour is deliberate. search_columns filters by three groups -- numeric,
datetime, object -- and accepts concrete pandas names as aliases onto them,
because an earlier round found that rejecting "float64" was itself the bug:
float64 is exactly what load_dataset, inspect_dataset and this tool's own
dtypes field print, so the vocabulary the fleet emits was not the vocabulary
this tool accepted.

What was missing is that the widening never showed up anywhere. The caller
asked for int64, got float64 columns, and nothing in the response said the
filter had been read as "numeric". A round-16 phase read exactly that and
reported the filter as broken -- which is the evidence the disclosure was
needed: a careful reader, given the whole response, drew the wrong conclusion.

So the group actually applied is now a field, and a widened filter says so in
progress and in the hint. Asking for a group by its own name stays silent,
because nothing was widened.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from servers.data_basic import engine as db


@pytest.fixture()
def widened(ad_data_full_csv: Path) -> dict:
    r = db.search_columns(str(ad_data_full_csv), dtype="int64")
    assert r["success"] is True, r.get("error")
    return r


class TestTheWideningIsVisible:
    def test_the_match_really_does_include_a_float_column(self, widened: dict) -> None:
        """Without this the rest of the file would be arguing with nothing."""
        assert "float64" in widened["dtypes"].values(), widened["dtypes"]

    def test_the_group_applied_is_reported(self, widened: dict) -> None:
        assert widened["dtype_filter"] == "numeric", widened

    def test_progress_says_it_was_not_exact(self, widened: dict) -> None:
        msgs = " | ".join(str(p.get("message", "")) for p in widened["progress"])
        assert "not 'int64' exactly" in msgs, msgs

    def test_the_hint_explains_the_group(self, widened: dict) -> None:
        hint = widened["hint"]
        assert "'int64' was read as the 'numeric' group" in hint, hint
        assert "dtypes field" in hint, hint

    def test_the_original_advice_survives(self, widened: dict) -> None:
        assert "read_column_stats" in widened["hint"], widened["hint"]


class TestAnExactGroupNameSaysNothingExtra:
    """The note must mark widening, not fire on every call."""

    @pytest.mark.parametrize("group", ["numeric", "object"])
    def test_no_widening_note(self, ad_data_full_csv: Path, group: str) -> None:
        r = db.search_columns(str(ad_data_full_csv), dtype=group)
        assert r["success"] is True, r.get("error")
        assert r["dtype_filter"] == group
        assert "was read as" not in r["hint"], r["hint"]
        msgs = " | ".join(str(p.get("message", "")) for p in r["progress"])
        assert "exactly" not in msgs, msgs

    def test_no_filter_at_all_reports_an_empty_group(self, ad_data_full_csv: Path) -> None:
        r = db.search_columns(str(ad_data_full_csv))
        assert r["success"] is True, r.get("error")
        assert r["dtype_filter"] == ""
        assert "was read as" not in r["hint"]


class TestTheAliasesStillWork:
    """The widening is the documented behaviour; disclosing it must not undo it."""

    @pytest.mark.parametrize("alias,group", [("float64", "numeric"), ("str", "object"), ("int", "numeric")])
    def test_an_alias_resolves_to_its_group(self, ad_data_full_csv: Path, alias: str, group: str) -> None:
        r = db.search_columns(str(ad_data_full_csv), dtype=alias)
        assert r["success"] is True, r.get("error")
        assert r["dtype_filter"] == group
        assert r["matched"] > 0

    def test_an_unknown_dtype_is_still_refused(self, ad_data_full_csv: Path) -> None:
        r = db.search_columns(str(ad_data_full_csv), dtype="complex128")
        assert r["success"] is False
        assert "Cannot filter by dtype" in r["error"]
