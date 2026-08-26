"""It flagged 3,373 rows and no file held any of them.

    check_outliers(Ad_Data.csv, output_path="outliers.csv")
      -> flagged_rows_total: 3373
         flagged_rows: [ ...the first 50... ]
         output_path: outliers_flagged.HTML      <- a box plot
         hint: "...Use filter_rows() ... or apply_patch() with op=cap_outliers"

The tool's docstring ends "Flags anomalous rows", so a caller who wants those
rows passes an output path and expects them in it. This tool's output_path is
its chart: the extension is rewritten to .html, which it does warn about, and
what lands there is a box plot. A round-16 phase followed that through and
reported that "3373" appears zero times in the written file -- the flag list
existed only in the reply, capped at the first 50.

An earlier round had already put `flagged_rows`, `flagged_rows_total` and
`flagged_rows_truncated` into the response, which is why 50 of them are visible
at all. What was still missing was any route to the other 3,323. The hint named
filter_rows() and apply_patch(), and both make the caller rebuild the outlier
condition by hand from the per-column limits.

detect_anomalies() -- on the sibling server, docstring "Flag anomalous rows
using IQR and/or z-score. Saves flagged CSV." -- does exactly the thing. It
existed the whole time and nothing pointed at it. So this is not a missing
feature, it is a missing sentence, and the fix is to name the tool that already
does the job rather than to teach a second tool to do it.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from servers.data_statistics import engine as ds

COLS = ["spends", "impressions", "clicks"]


@pytest.fixture()
def flagged(ad_data_full_csv: Path, tmp_path: Path) -> dict:
    r = ds.check_outliers(
        str(ad_data_full_csv),
        columns=COLS,
        method="both",
        output_path=str(tmp_path / "outliers.csv"),
        open_after=False,
    )
    assert r["success"] is True, r.get("error")
    return r


class TestTheHintNamesTheToolThatWritesTheRows:
    def test_many_rows_are_flagged_and_the_list_is_capped(self, flagged: dict) -> None:
        """Without truncation there is nothing to point anywhere for."""
        assert flagged["flagged_rows_total"] > len(flagged["flagged_rows"])
        assert flagged["flagged_rows_truncated"] is True

    def test_it_names_detect_anomalies(self, flagged: dict) -> None:
        assert "detect_anomalies()" in flagged["hint"], flagged["hint"]

    def test_it_says_that_tool_writes_a_csv(self, flagged: dict) -> None:
        assert "CSV" in flagged["hint"], flagged["hint"]

    def test_it_says_this_output_path_is_the_chart(self, flagged: dict) -> None:
        """The specific misunderstanding that produced the finding."""
        assert "output_path is" in flagged["hint"] and "chart" in flagged["hint"], flagged["hint"]

    def test_the_older_routes_survive(self, flagged: dict) -> None:
        """filter_rows and apply_patch are still right for other intents."""
        assert "filter_rows()" in flagged["hint"]
        assert "cap_outliers" in flagged["hint"]

    def test_the_total_is_in_the_hint(self, flagged: dict) -> None:
        assert str(flagged["flagged_rows_total"]) in flagged["hint"]


class TestTheToolItPointsAtReallyDoesIt:
    """A hint naming a tool that cannot deliver would be worse than none."""

    def test_detect_anomalies_exists_and_takes_an_output_path(self) -> None:
        import inspect

        from servers.data_medium import engine as dm

        assert hasattr(dm, "detect_anomalies")
        assert "output_path" in inspect.signature(dm.detect_anomalies).parameters

    def test_it_writes_a_csv_holding_flagged_rows(self, ad_data_full_csv: Path, tmp_path: Path) -> None:
        import csv as _csv

        from servers.data_medium import engine as dm

        out = tmp_path / "anomalies.csv"
        r = dm.detect_anomalies(str(ad_data_full_csv), columns=COLS, method="both", output_path=str(out))
        assert r["success"] is True, r.get("error")
        written = Path(r.get("output_path") or out)
        assert written.suffix.lower() == ".csv", written
        assert written.is_file(), written
        with open(written, newline="", encoding="utf-8") as fh:
            rows = list(_csv.reader(fh))
        assert len(rows) > 1, "the flagged CSV holds only a header"


class TestNoHintWhenNothingWasTruncated:
    def test_a_small_frame_gets_no_paging_advice(self, tmp_path: Path) -> None:
        src = tmp_path / "small.csv"
        src.write_text("a\n" + "\n".join(str(i) for i in range(30)) + "\n1000\n", encoding="utf-8")
        r = ds.check_outliers(str(src), columns=["a"], method="iqr", output_path="", open_after=False)
        assert r["success"] is True, r.get("error")
        if not r.get("flagged_rows_truncated"):
            assert "detect_anomalies()" not in (r.get("hint") or "")
