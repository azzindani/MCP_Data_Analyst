"""period_comparison rejected all three period units its docstring names.

The tool description is "Compare periods: MoM QoQ YoY. Returns delta pct_change
direction." and the schema carries no enum, so those three strings are the only
vocabulary a caller can see. The code wanted single letters, and upper-cased the
input before checking, so every one of them came back as:

    Invalid period_unit 'MOM'.

MoM and M are not the same kind of name in general -- one is a comparison, the
other a period -- but for this tool they mean the same thing: a month-over-month
comparison is a monthly period compared against the one before it, which is what
compare_to="previous" already does. So the three names the docs promise are
accepted, and the hint now shows both spellings.
"""

from __future__ import annotations

import csv as csvmod
from pathlib import Path

import pytest

from servers.data_statistics.engine import period_comparison
from servers.data_statistics.server import period_comparison as tool

PAIRS = [("MoM", "M"), ("QoQ", "Q"), ("YoY", "Y")]


@pytest.fixture()
def csv_path(tmp_path: Path) -> str:
    path = tmp_path / "spend.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csvmod.writer(fh)
        w.writerow(["Date", "spends"])
        for year in (2019, 2020):
            for month in range(1, 13):
                w.writerow([f"{year}-{month:02d}-15", 1000 + month])
    return str(path)


def run(csv_path: str, unit: str, out: Path) -> dict:
    return period_comparison(csv_path, "Date", ["spends"], unit, output_path=str(out), open_after=False)


class TestEveryUnitTheDocstringNamesIsAccepted:
    @pytest.mark.parametrize("alias,_letter", PAIRS)
    def test_it_runs(self, alias: str, _letter: str, csv_path: str, tmp_path: Path):
        r = run(csv_path, alias, tmp_path / "c.html")
        assert r["success"] is True, f"{alias}: {r.get('error')} / {r.get('hint')}"

    @pytest.mark.parametrize("alias,letter", PAIRS)
    def test_it_means_the_same_as_the_letter(self, alias: str, letter: str, csv_path: str, tmp_path: Path):
        long = run(csv_path, alias, tmp_path / "a.html")
        short = run(csv_path, letter, tmp_path / "b.html")
        assert long.get("comparisons") == short.get("comparisons"), (alias, letter)

    @pytest.mark.parametrize("written", ["mom", "MOM", " MoM "])
    def test_case_and_padding_do_not_matter(self, written: str, csv_path: str, tmp_path: Path):
        assert run(csv_path, written, tmp_path / "c.html")["success"] is True

    def test_the_docstring_still_names_these_three(self):
        """If the docstring is reworded, the aliases must move with it."""
        doc = getattr(tool, "description", None) or tool.__doc__ or ""
        assert all(name in doc for name in ("MoM", "QoQ", "YoY")), doc


class TestEveryLetterTheHintOffersActuallyWorks:
    """H was listed as valid and then crashed on it.

    _FREQ_MAP carries "H" -> "h" for resampling, but the raw letter was handed
    to to_period(), and pandas 3 dropped the uppercase alias. The caller got
    'Invalid frequency: H ... Did you mean h?' nested inside a second copy of
    itself, from a value the tool's own hint had just recommended -- while the
    catch-all hint further down listed only D W M Q Y, so the two disagreed.
    """

    @pytest.mark.parametrize("unit", ["D", "W", "M", "Q", "Y", "H"])
    def test_they_are_unchanged(self, unit: str, csv_path: str, tmp_path: Path):
        r = run(csv_path, unit, tmp_path / "c.html")
        assert r["success"] is True, f"{unit}: {r.get('error')}"

    def test_no_pandas_frequency_error_reaches_the_caller(self, csv_path: str, tmp_path: Path):
        r = run(csv_path, "H", tmp_path / "c.html")
        assert "Invalid frequency" not in str(r.get("error", "")), r.get("error")

    def test_no_hint_in_the_module_lists_the_units_without_h(self):
        """The rejection hint and the catch-all hint used to disagree about H.

        One said "['D', 'H', 'M', 'Q', 'W', 'Y']" and the other "(D W M Q Y)",
        so which units the tool supports depended on which way you failed.
        """
        import inspect
        import re

        from servers.data_statistics import _stats_comparative

        source = inspect.getsource(_stats_comparative)
        lists = re.findall(r"period_unit \(([^)]*)\)", source)
        assert lists, "no period_unit vocabulary found in the hints"
        assert all("H" in listing for listing in lists), lists


class TestSomethingThatIsNotAPeriodIsStillRejected:
    @pytest.mark.parametrize("unit", ["monthly", "P", ""])
    def test_it_fails(self, unit: str, csv_path: str, tmp_path: Path):
        assert run(csv_path, unit, tmp_path / "c.html")["success"] is False

    def test_the_hint_offers_both_spellings(self, csv_path: str, tmp_path: Path):
        hint = run(csv_path, "monthly", tmp_path / "c.html")["hint"]
        assert "MoM" in hint and "'M'" in hint, hint
