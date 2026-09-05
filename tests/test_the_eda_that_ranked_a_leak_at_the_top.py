"""`target_column` ranked the columns that already knew the answer, and said so.

`run_eda(target_column=...)` answers "what relates to the target" by ranking
every column by association strength. On the review's loan book the top of that
ranking was `installment`, `total_payment` and `last_payment_date` -- all three
recorded *after* the loan resolved. The ranking was correct and the conclusion a
reader draws from it is wrong: from inside an association table, a strong
predictor and a column that contains the outcome look identical.

The review asked for the warning by name, and put it in the data-quality pass
rather than the training one:

    Suggest check_data_quality add a "possible leakage: post-outcome column"
    hint when target is loan_status.

`shared/leakage.py` existed only in MCP_Machine_Learning, so nothing in this
repo could say it. The module is copied here byte-identical rather than
reimplemented -- see `test_one_file_two_quality_scores.py` for what happens when
two repos answer one question with two implementations. `split_provenance` comes
along unused: this repo never splits, and carrying a dead function is cheaper
than maintaining a second version of a file whose whole purpose is agreement.

Two shapes are asserted here that the ML sibling also asserts, deliberately:
leakage never moves `quality_score`, and a run with no target says the check did
not run instead of going quiet. The panel in the HTML is this repo's own
addition, for the reason `shared/data_alerts.py` was written -- judgement that
lives only in the response never reaches the artifact someone forwards.
"""

from __future__ import annotations

import hashlib
import pathlib
import random

import pytest

from servers.data_advanced._adv_eda import run_eda

ROWS = 400
REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


def _sha(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write(path, header, rows) -> str:
    path.write_text("\n".join([header, *rows]), encoding="utf-8")
    return str(path)


@pytest.fixture()
def leaky(tmp_path):
    """`total_payment` separates the classes; `last_payment_date` is null for one."""
    random.seed(7)
    rows = []
    for i in range(ROWS):
        charged_off = i % 4 == 0
        income = random.gauss(60_000, 15_000)
        paid = random.uniform(0, 900) if charged_off else random.uniform(4_000, 30_000)
        last = "" if charged_off else "2024-03-01"
        status = "Charged Off" if charged_off else "Fully Paid"
        rows.append(f"{income:.2f},{paid:.2f},{last},{status}")
    return _write(
        tmp_path / "loans.csv",
        "annual_income,total_payment,last_payment_date,loan_status",
        rows,
    )


@pytest.fixture()
def clean(tmp_path):
    random.seed(9)
    rows = [
        f"{random.gauss(60_000, 15_000):.2f},{random.gauss(500, 120):.2f},"
        f"{random.choice('ABC')},{'Charged Off' if i % 4 == 0 else 'Fully Paid'}"
        for i in range(ROWS)
    ]
    return _write(tmp_path / "clean.csv", "annual_income,monthly_spend,grade,loan_status", rows)


def eda(path, tmp_path, **kw):
    out = tmp_path / f"report_{kw.get('target_column', 'none')}.html"
    return run_eda(path, output_path=str(out), open_after=False, **kw)


class TestTheFixtureIsWhatTheAssertionsAssume:
    def test_the_report_runs_at_all(self, leaky, tmp_path):
        result = eda(leaky, tmp_path)
        assert result["success"] is True, result.get("error")
        assert result["rows"] == ROWS

    def test_the_leak_really_does_top_the_association_ranking(self, leaky, tmp_path):
        """If it did not, this test file would be describing a different defect."""
        result = eda(leaky, tmp_path, target_column="loan_status")
        ranked = [a["column"] for a in result["target_association"] if a.get("strength") is not None]
        assert ranked, result["target_association"]
        assert ranked[0] in {"total_payment", "last_payment_date"}


class TestTheRankingNowSaysWhichOnesAlreadyKnow:
    def test_the_post_outcome_column_is_named(self, leaky, tmp_path):
        result = eda(leaky, tmp_path, target_column="loan_status")
        assert "total_payment" in {s["feature"] for s in result["leakage_suspects"]}

    def test_the_one_sided_null_column_is_named(self, leaky, tmp_path):
        result = eda(leaky, tmp_path, target_column="loan_status")
        assert "last_payment_date" in {s["feature"] for s in result["leakage_suspects"]}

    def test_an_honest_column_is_not(self, leaky, tmp_path):
        result = eda(leaky, tmp_path, target_column="loan_status")
        assert "annual_income" not in {s["feature"] for s in result["leakage_suspects"]}

    def test_a_clean_file_gets_a_stated_result_not_silence(self, clean, tmp_path):
        result = eda(clean, tmp_path, target_column="loan_status")
        assert result["leakage_count"] == 0
        assert "No feature looks like" in result["leakage_note"]

    def test_every_suspect_shows_its_working(self, leaky, tmp_path):
        for suspect in eda(leaky, tmp_path, target_column="loan_status")["leakage_suspects"]:
            assert suspect["signals"]
            assert all(s["evidence"].strip() for s in suspect["signals"])


class TestTheScoreIsAboutTheDataNotTheQuestion:
    def test_naming_a_target_does_not_move_the_quality_score(self, leaky, tmp_path):
        assert (
            eda(leaky, tmp_path)["quality_score"] == eda(leaky, tmp_path, target_column="loan_status")["quality_score"]
        )

    def test_naming_a_target_adds_no_insights(self, leaky, tmp_path):
        """This report publishes alerts as `insights`; leakage joins neither."""
        assert eda(leaky, tmp_path)["insights"] == eda(leaky, tmp_path, target_column="loan_status")["insights"]

    def test_naming_a_target_does_not_move_the_breakdown(self, leaky, tmp_path):
        without = eda(leaky, tmp_path)["quality_breakdown"]
        with_target = eda(leaky, tmp_path, target_column="loan_status")["quality_breakdown"]
        assert without == with_target


class TestSilenceIsNotACleanBillOfHealth:
    def test_no_target_says_the_check_did_not_run(self, leaky, tmp_path):
        result = eda(leaky, tmp_path)
        assert "not run" in result["leakage_check"]

    def test_no_target_claims_nothing(self, leaky, tmp_path):
        result = eda(leaky, tmp_path)
        assert "leakage_suspects" not in result
        assert "leakage_note" not in result

    def test_with_a_target_the_did_not_run_line_is_gone(self, leaky, tmp_path):
        assert "leakage_check" not in eda(leaky, tmp_path, target_column="loan_status")

    def test_the_wording_matches_the_ml_sibling(self, leaky, tmp_path):
        """Two tools, one question, one sentence. The 77-vs-53 lesson."""
        result = eda(leaky, tmp_path)
        assert result["leakage_check"] == (
            "not run — pass target_column to test whether a feature already contains the outcome"
        )


class TestTheArtifactCarriesItToo:
    def test_the_page_has_a_leakage_section(self, leaky, tmp_path):
        out = tmp_path / "leak.html"
        run_eda(leaky, output_path=str(out), open_after=False, target_column="loan_status")
        page = out.read_text(encoding="utf-8")
        assert 'id="leakage"' in page
        assert "total_payment" in page

    def test_the_page_says_suspects_not_verdicts(self, leaky, tmp_path):
        out = tmp_path / "leak2.html"
        run_eda(leaky, output_path=str(out), open_after=False, target_column="loan_status")
        assert "Suspects, not verdicts" in out.read_text(encoding="utf-8")

    def test_no_target_means_no_panel_and_no_dead_anchor(self, leaky, tmp_path):
        out = tmp_path / "plain.html"
        run_eda(leaky, output_path=str(out), open_after=False)
        page = out.read_text(encoding="utf-8")
        assert 'id="leakage"' not in page
        assert 'href="#leakage"' not in page

    def test_a_clean_target_still_gets_a_panel(self, clean, tmp_path):
        """ "Checked, found nothing" is a result and belongs on the page."""
        out = tmp_path / "cleanpage.html"
        run_eda(clean, output_path=str(out), open_after=False, target_column="loan_status")
        page = out.read_text(encoding="utf-8")
        assert 'id="leakage"' in page
        assert "No feature looks like" in page


class TestOneImplementationAcrossTheFleet:
    def test_the_module_is_byte_identical_with_the_ml_copy(self):
        """Reimplementing it here is how one file gets two answers."""
        ours = REPO_ROOT / "shared" / "leakage.py"
        assert ours.exists()
        sibling = pathlib.Path("/root/MCP_Machine_Learning/shared/leakage.py")
        if not sibling.exists():
            pytest.skip("sibling repo not present in this checkout")
        assert _sha(ours) == _sha(sibling)

    def test_the_formatter_is_kept_off_shared(self):
        """Runs on a GitHub runner, where the sibling check above cannot."""
        pyproject = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        assert "[tool.ruff.format]" in pyproject
        assert 'exclude = ["shared/**"]' in pyproject
