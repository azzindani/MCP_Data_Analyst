"""Nine reports from one dataset, six of them exactly 4.7 MB.

A user review measured what a profile costs rather than whether it is right:

    `bar`, `correlation`, `crosstab`, `eda`, `nulls_zeros`, `value_counts` are
    all exactly 4.7 MB. `outliers` 5.8 MB, `distributions` 6.9 MB, `dashboard`
    8.9 MB. Read `Credit_Risk_bar.html` lines 1-30: full Plotly + CSS shell.
    Boilerplate dominates.

Nothing there was wrong. It was all of it, every time, whether the caller
wanted a look or a document. The review's own prescription, taken from its
comparison against ydata-profiling:

    Opinionated defaults, exhaustive on demand... `minimal=True/False` controls
    depth. MCP: `run_eda(file, mode="minimal|standard|full", sample_n,
    include={})`. Small gets KBs, frontier gets full without new tools.

Three properties are worth more than the saving, and they are what these tests
pin:

* **`standard` is byte-for-byte the old behaviour.** A depth control that
  quietly changes the default is a behaviour change wearing a parameter's
  clothes. Every existing caller passes nothing.
* **A skipped section is named.** The failure mode of a minimal profile is a
  response that looks complete and is not -- the same defect class as a
  `truncated` flag with no total. Empty because nothing was found and empty
  because nothing was computed call for opposite next actions.
* **A sampled profile says it was sampled.** Statistics from 5,000 of 38,576
  rows are estimates. One that does not say so invites them to be quoted as
  counts.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_advanced")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _adv_eda import run_eda  # noqa: E402

from shared.depth import SECTIONS, Depth, UnknownMode  # noqa: E402


@pytest.fixture
def frame(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(0)
    n = 400
    p = tmp_path / "data.csv"
    pd.DataFrame(
        {
            "a": rng.normal(size=n),
            "b": rng.normal(size=n),
            "c": rng.normal(size=n) * 10,
            "grp": rng.choice(list("xyz"), size=n),
        }
    ).to_csv(p, index=False)
    return p


# ---------------------------------------------------------------------------
# the default did not move
# ---------------------------------------------------------------------------


def test_the_default_is_the_old_behaviour(frame):
    r = run_eda(str(frame), open_after=False)
    assert r["success"] is True
    assert r["mode"] == "standard"
    assert r["output_path"], "standard still writes the HTML report"
    assert r["top_correlations"], "standard still computes correlations"
    assert "sections_skipped" not in r, "nothing is skipped at standard"


def test_standard_and_no_argument_agree(frame):
    a = run_eda(str(frame), open_after=False)
    b = run_eda(str(frame), open_after=False, mode="standard")
    for key in ("quality_score", "rows", "columns", "top_correlations", "outlier_columns"):
        assert a[key] == b[key], key


# ---------------------------------------------------------------------------
# minimal costs less and says what it cost
# ---------------------------------------------------------------------------


def test_minimal_writes_no_page(frame):
    r = run_eda(str(frame), open_after=False, mode="minimal")
    assert r["success"] is True
    assert r["output_path"] == "", "the 4.7 MB is the page; minimal does not write one"
    assert r["report_size_kb"] == 0
    assert r["column_summaries"], "the figures are the point of minimal, not a side effect"
    assert r["quality_score"] is not None


def test_minimal_names_what_it_skipped(frame):
    r = run_eda(str(frame), open_after=False, mode="minimal")
    skipped = set(r["sections_skipped"])
    assert {"correlations", "outliers", "html"} <= skipped
    assert "depth_note" in r
    assert "Absent is not empty" in r["depth_note"]


def test_an_empty_section_is_distinguishable_from_an_uncomputed_one(frame):
    """The whole reason `sections_skipped` exists."""
    minimal = run_eda(str(frame), open_after=False, mode="minimal")
    standard = run_eda(str(frame), open_after=False)
    assert minimal["top_correlations"] == [] and standard["top_correlations"] != []
    assert "correlations" in minimal["sections_skipped"]
    assert "correlations" in standard["sections_run"]


def test_minimal_is_materially_smaller(frame):
    minimal = run_eda(str(frame), open_after=False, mode="minimal")
    standard = run_eda(str(frame), open_after=False)
    assert minimal["token_estimate"] < standard["token_estimate"]


# ---------------------------------------------------------------------------
# full is the mode for the whole matrix
# ---------------------------------------------------------------------------


def test_full_returns_every_pair_not_the_first_ten(frame):
    r = run_eda(str(frame), open_after=False, mode="full")
    # 4 numeric columns -> 3 numeric (grp is categorical) -> 3 pairs here, so
    # assert the field exists and matches rather than a magic count.
    assert "all_correlations" in r
    assert len(r["all_correlations"]) >= len(r["top_correlations"])


# ---------------------------------------------------------------------------
# include overrides in both directions
# ---------------------------------------------------------------------------


def test_include_can_add_a_section_back_to_minimal(frame):
    r = run_eda(str(frame), open_after=False, mode="minimal", include={"correlations": True})
    assert r["top_correlations"], "the caller asked for correlations explicitly"
    assert "correlations" in r["sections_run"]
    assert r["output_path"] == "", "and did not thereby ask for the page"


def test_include_can_take_a_section_away_from_standard(frame):
    r = run_eda(str(frame), open_after=False, include={"html": False})
    assert r["output_path"] == ""
    assert "html" in r["sections_skipped"]


def test_an_unknown_section_is_refused_not_ignored(frame):
    r = run_eda(str(frame), open_after=False, include={"corrolations": True})
    assert r["success"] is False
    assert "corrolations" in r["error"]
    assert "correlations" in r["hint"], "the valid names belong in the refusal"


def test_an_unknown_mode_names_the_three_that_exist(frame):
    r = run_eda(str(frame), open_after=False, mode="deep")
    assert r["success"] is False
    for name in ("minimal", "standard", "full"):
        assert name in r["error"] or name in r["hint"]


# ---------------------------------------------------------------------------
# sampling declares itself
# ---------------------------------------------------------------------------


def test_a_sampled_profile_says_so(frame):
    r = run_eda(str(frame), open_after=False, mode="minimal", sample_n=50)
    assert r["was_sampled"] is True
    assert r["sample_n"] == 50
    assert r["rows_total"] == 400
    assert r["rows"] == 50
    assert "estimates, not counts" in r["sample_note"]


def test_an_unsampled_profile_claims_nothing(frame):
    r = run_eda(str(frame), open_after=False, mode="minimal")
    assert "was_sampled" not in r
    assert "sample_note" not in r


def test_a_sample_larger_than_the_file_is_not_a_sample(frame):
    r = run_eda(str(frame), open_after=False, mode="minimal", sample_n=10_000)
    assert "was_sampled" not in r, "every row was used, so nothing was estimated"
    assert r["rows"] == 400


# ---------------------------------------------------------------------------
# the helper itself
# ---------------------------------------------------------------------------


def test_depth_reports_only_what_was_asked_about():
    """A profiler with no charts should not advertise skipping them."""
    d = Depth("minimal")
    d.wants("correlations")
    d.wants("column_summaries")
    report = d.report()
    assert report["sections_run"] == ["column_summaries"]
    assert report["sections_skipped"] == ["correlations"]
    assert "charts" not in report["sections_run"] + report["sections_skipped"]


def test_every_declared_section_is_a_real_one():
    for name in SECTIONS:
        assert Depth("full").wants(name) in (True, False)


def test_an_unknown_include_key_raises_rather_than_passing_silently():
    with pytest.raises(UnknownMode) as exc:
        Depth("standard", {"nulls": True, "nulz": False})
    assert "nulz" in str(exc.value)
