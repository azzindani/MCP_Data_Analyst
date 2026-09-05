"""A dashboard can hold more than the one file it was built from.

The user review asked for this twice, once as a feature and once as a principle:

    AGI: 5k-row default + `Load full`; composable tabs (accept
    anomalies/chargedoff as second tab).

    data binding: one file -> one dashboard today. AGI needs multi-source:
    `chargedoff.csv` as tab 2, `anomalies_only.csv` as tab 3.

Those two files are what a session actually produces: you filter, you flag, and
then you have three files describing one dataset and three separate pages to
open. The tabs put them in one artifact.

**The extra tabs carry server-side totals, and that is the interesting part.**
Everything on the primary tab -- KPI cards, bar heights, pie shares -- is
computed in the browser from the rows embedded in the page, so it is only as
complete as that embedding. An extra source's totals are computed here, over all
of its rows, before any capping. So a capped table never drags its own totals
down with it, and the two kinds of number are not quietly mixed.

The embed cap itself is tested here too. The review asked for a 5,000-row
default; it is a lever instead, and these tests pin both halves of that decision
-- the default still embeds everything, and pulling the lever puts the word
"Estimates" on the page rather than leaving eight-times-too-small numbers under
unchanged headings.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pytest

from servers.data_visual import engine as dv
from shared.dashboard_spec import DEFAULT_INTERACTIONS


@pytest.fixture()
def primary(tmp_path: Path) -> Path:
    rows = 600
    csv = tmp_path / "Credit_Risk.csv"
    pd.DataFrame(
        {
            "grade": ["A", "B", "C"] * (rows // 3),
            "loan_amount": [1000 + i for i in range(rows)],
            "loan_status": ["Fully Paid"] * 400 + ["Charged Off"] * 200,
        }
    ).to_csv(csv, index=False)
    return csv


@pytest.fixture()
def chargedoff(primary: Path, tmp_path: Path) -> Path:
    out = tmp_path / "Credit_Risk_chargedoff.csv"
    df = pd.read_csv(primary)
    df[df.loan_status == "Charged Off"].to_csv(out, index=False)
    return out


@pytest.fixture()
def anomalies(primary: Path, tmp_path: Path) -> Path:
    out = tmp_path / "Credit_Risk_anomalies_only.csv"
    df = pd.read_csv(primary).head(40)
    df["reason"] = "loan_amount above the IQR fence"
    df.to_csv(out, index=False)
    return out


def build(primary: Path, tmp_path: Path, **kwargs):
    out = tmp_path / "dash.html"
    result = dv.generate_dashboard(str(primary), output_path=str(out), open_after=False, **kwargs)
    assert result["success"] is True, result.get("error")
    return result, out.read_text(encoding="utf-8")


class TestOneFileIsStillOneDashboard:
    def test_no_sources_means_no_tab_strip(self, primary, tmp_path):
        _result, html = build(primary, tmp_path)
        assert "src-tab-btn" not in html
        assert 'class="src-sec"' not in html

    def test_the_response_reports_no_sources(self, primary, tmp_path):
        result, _html = build(primary, tmp_path)
        assert result["sources"] == []


class TestEachSourceBecomesATab:
    @pytest.fixture()
    def three(self, primary, chargedoff, anomalies, tmp_path):
        return build(primary, tmp_path, sources=[str(chargedoff), str(anomalies)])

    def test_one_tab_per_file_with_the_primary_first(self, three, primary, chargedoff, anomalies):
        _result, html = three
        names = re.findall(r'class="src-tab-btn"[^>]*>([^<]+)</button>', html)
        assert names == [primary.name, chargedoff.name, anomalies.name]

    def test_one_section_per_tab(self, three):
        _result, html = three
        assert [m for m in re.findall(r'data-src="(\d+)"', html)] == ["0", "1", "2"]

    def test_only_the_primary_section_starts_visible(self, three):
        _result, html = three
        assert '<section class="src-sec" data-src="0">' in html
        assert '<section class="src-sec" data-src="1" hidden>' in html

    def test_the_charts_stay_on_the_primary_tab(self, three):
        """Cross-filter is client-side over one embedded frame; it belongs to one file."""
        _result, html = three
        primary_section = html.split('data-src="1"')[0]
        assert "cgrid" in primary_section

    def test_each_extra_tab_carries_its_own_rows(self, three):
        _result, html = three
        assert html.count("data-src-table") >= 2

    def test_the_response_describes_every_source(self, three, chargedoff, anomalies):
        result, _html = three
        assert [s["name"] for s in result["sources"]] == [chargedoff.name, anomalies.name]
        assert result["sources"][0]["rows"] == 200


class TestASubsetIsRecognisedAsOne:
    def test_a_matching_schema_gets_its_share_of_the_primary(self, primary, chargedoff, tmp_path):
        result, html = build(primary, tmp_path, sources=[str(chargedoff)])
        source = result["sources"][0]
        assert source["same_schema_as_primary"] is True
        assert source["share_of_primary_pct"] == round(200 / 600 * 100, 2)
        assert "of the primary dataset" in html

    def test_a_different_schema_makes_no_such_claim(self, primary, anomalies, tmp_path):
        """'40 of 600 rows' is meaningless about a table with an extra column."""
        result, html = build(primary, tmp_path, sources=[str(anomalies)])
        source = result["sources"][0]
        assert source["same_schema_as_primary"] is False
        assert "share_of_primary_pct" not in source


class TestASourceTabTellsTheTruthAboutItsOwnRows:
    @pytest.fixture()
    def capped(self, primary, tmp_path):
        from servers.data_advanced._adv_dashboard import SOURCE_ROW_CAP

        big = tmp_path / "big.csv"
        pd.DataFrame({"n": range(SOURCE_ROW_CAP + 500), "k": ["a"] * (SOURCE_ROW_CAP + 500)}).to_csv(big, index=False)
        return build(primary, tmp_path, sources=[str(big)]), SOURCE_ROW_CAP

    def test_the_table_is_capped_and_says_so(self, capped):
        (result, html), cap = capped
        assert result["sources"][0]["rows"] == cap + 500
        assert result["sources"][0]["rows_shown"] == cap
        assert f"first {cap:,}" in html

    def test_the_totals_come_from_every_row_not_the_capped_table(self, capped):
        (_result, html), cap = capped
        # sum(0..cap+499) rendered by _compact_num; the capped table would give
        # a visibly smaller number under the same heading.
        expected_total = sum(range(cap + 500))
        from servers.data_advanced._adv_dashboard import _compact_num

        assert _compact_num(float(expected_total)) in html


class TestABadSourceIsRefusedBeforeAnythingIsWritten:
    def test_a_missing_file_is_named(self, primary, tmp_path):
        out = tmp_path / "dash.html"
        result = dv.generate_dashboard(
            str(primary), output_path=str(out), open_after=False, sources=[str(tmp_path / "nope.csv")]
        )
        assert result["success"] is False
        assert "nope.csv" in result["error"]
        assert not out.exists(), "a tab that opens onto nothing is worse than no dashboard"

    def test_an_empty_file_is_refused(self, primary, tmp_path):
        empty = tmp_path / "empty.csv"
        empty.write_text("a,b\n", encoding="utf-8")
        out = tmp_path / "dash.html"
        result = dv.generate_dashboard(str(primary), output_path=str(out), open_after=False, sources=[str(empty)])
        assert result["success"] is False
        assert not out.exists()


class TestTheEmbedCapIsALeverNotADefault:
    def test_the_default_embeds_every_row(self, primary, tmp_path):
        result, html = build(primary, tmp_path)
        assert DEFAULT_INTERACTIONS["embed_rows"] == 0
        assert result["rows_embedded"] == result["rows_total"] == 600
        assert result["was_sampled"] is False
        assert "sample-banner" not in html

    def test_asking_for_a_cap_samples(self, primary, tmp_path):
        result, _html = build(primary, tmp_path, spec={"interactions": {"embed_rows": 100}})
        assert result["rows_embedded"] == 100
        assert result["rows_total"] == 600
        assert result["was_sampled"] is True

    def test_a_sampled_page_says_its_numbers_are_estimates(self, primary, tmp_path):
        """Every figure on the page is computed in the browser from these rows."""
        _result, html = build(primary, tmp_path, spec={"interactions": {"embed_rows": 100}})
        assert "sample-banner" in html
        assert "Estimates" in html
        assert "100 of 600 rows" in html

    def test_it_names_the_call_that_rebuilds_at_full_fidelity(self, primary, tmp_path):
        """The review's `Load full`, as a call: a standalone page has nothing to fetch."""
        _result, html = build(primary, tmp_path, spec={"interactions": {"embed_rows": 100}})
        assert "Load full" in html
        assert "embed_rows" in html

    def test_a_cap_above_the_row_count_changes_nothing(self, primary, tmp_path):
        result, html = build(primary, tmp_path, spec={"interactions": {"embed_rows": 5000}})
        assert result["was_sampled"] is False
        assert "sample-banner" not in html

    def test_the_cap_survives_into_the_spec_the_page_carries(self, primary, tmp_path):
        result, _html = build(primary, tmp_path, spec={"interactions": {"embed_rows": 100}})
        assert result["spec"]["interactions"]["embed_rows"] == 100
