"""Every response that reports a count reports it the same way.

`shared/counts.py` was written to end one defect and then wired into two call
sites, which is not what the contract says. The bar is that the shared helper is
the *only* thing that emits the triple, because the failure mode is never a bad
helper -- it is a second place that does the arithmetic by hand and drifts.

Wiring the rest of this repo turned up six more, none of which any review had
found. Four were counts that named the wrong number:

* `detect_tables` computed `table_count` from the list *after* slicing it, so a
  sheet with 25 tables answered `table_count: 20, truncated: true` and named 25
  nowhere.
* `check_outliers` did the same to `scanned_columns` -- a name that claims to
  say how much work was done, reporting the size of the page instead.
* `search_columns` did it to `matched`, and only added `total_matched` when the
  response *was* truncated, so the honest number was missing from precisely the
  responses that were complete.
* `inspect_dataset` set `truncated` only when it was True. A caller could not
  tell "nothing was cut" from "this tool does not say".

Two were caps that ignored the deployment:

* `sample_data` fetched `get_max_rows()` into a variable it never read, and cut
  with a hardcoded 20 -- so constrained mode, whose entire job is making
  responses smaller, shrank this one by nothing.
* `aggregate_dataset` had a third copy of that same hardcoded 20.

And one was the inverse -- a truncation warning on a complete answer:

* `time_series_analysis` built `resampled_trunc = resampled.tail(max_r)`, never
  read it, and warned "Results truncated, showing last N periods" off the flag
  beside it. Every period was in the answer. A false warning costs a caller the
  same wasted second call as a real one.

The static test below is what stops the next one. It reads the source rather
than the behaviour, because a count that is never exercised by a test is exactly
where the eighth copy will go.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (
    str(ROOT),
    str(ROOT / "servers" / "data_medium"),
    str(ROOT / "servers" / "data_basic"),
    str(ROOT / "servers" / "data_ingest"),
):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from shared.counts import count_violations  # noqa: E402

# ---------------------------------------------------------------------------
# the static rule
# ---------------------------------------------------------------------------

SERVERS = ROOT / "servers"
SHARED = ROOT / "shared"

# A dict literal writing the key itself, e.g. `"truncated": something`. The
# helper is allowed to; nothing else is.
_HAND_WRITTEN = re.compile(r'"truncated"\s*:')


def _source_lines(path: Path) -> list[tuple[int, str]]:
    """Numbered lines with comments and docstring bodies left in.

    Comments are stripped for the match itself -- several of these modules
    explain the defect in a comment that quotes the very string being banned,
    and a test that fires on its own explanation is a test nobody keeps.
    """
    out = []
    for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        out.append((n, line))
    return out


def _py_files() -> list[Path]:
    files = [p for p in SERVERS.rglob("*.py") if "__pycache__" not in p.parts]
    files += [p for p in SHARED.rglob("*.py") if "__pycache__" not in p.parts]
    return [p for p in files if p.name != "counts.py"]


def test_no_module_writes_the_truncated_key_by_hand():
    """`counted()` derives the flag; anyone else writing it can disagree with it."""
    offenders: list[str] = []
    for path in _py_files():
        for lineno, line in _source_lines(path):
            if _HAND_WRITTEN.search(line):
                offenders.append(f"{path.relative_to(ROOT)}:{lineno}: {line.strip()}")
    assert not offenders, (
        "these write `truncated` by hand instead of calling counted():\n  "
        + "\n  ".join(offenders)
        + "\n\ncounted(returned, total) derives it, so the flag cannot disagree "
        "with the numbers printed beside it. That disagreement is the original "
        "defect: one cap did the cutting and a different limit set the flag."
    )


def test_the_helper_is_importable_from_every_server_package():
    """A contract nothing can import is a contract nothing will use."""
    import shared.counts as mod

    assert callable(mod.counted)
    assert callable(mod.count_violations)


# ---------------------------------------------------------------------------
# the counts that named the wrong number
# ---------------------------------------------------------------------------


@pytest.fixture
def unconstrained(monkeypatch):
    """A test about a limit must pin the mode, never inherit it from the runner."""
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)


@pytest.fixture
def wide_csv(tmp_path):
    """More columns than any cap here, so truncation is guaranteed to bite."""
    cols = {f"col_{i:03d}": [i, i + 1, i + 2] for i in range(300)}
    p = tmp_path / "wide.csv"
    pd.DataFrame(cols).to_csv(p, index=False)
    return p


@pytest.fixture
def outlier_csv(tmp_path):
    """Enough numeric columns to exceed get_max_results(), each with real spread."""
    rng = range(60)
    cols = {}
    for i in rng:
        values = list(range(20)) + [10_000]  # one clear outlier per column
        cols[f"n_{i:03d}"] = values
    p = tmp_path / "outliers.csv"
    pd.DataFrame(cols).to_csv(p, index=False)
    return p


def test_search_columns_reports_how_many_matched_not_how_many_fit(unconstrained, wide_csv):
    # Fully qualified: three servers ship a module called `engine`, and a bare
    # `from engine import ...` picks whichever landed on sys.path first.
    from servers.data_basic.engine import search_columns

    r = search_columns(str(wide_csv), name_contains="col_")
    assert r["success"] is True
    assert r["total"] == 300, "300 columns match 'col_'; that is what matched means"
    assert r["returned"] == len(r["columns"])
    assert r["returned"] < r["total"]
    assert r["truncated"] is True
    # The field a caller reads for "how many matched" must agree with the total.
    assert r["matched"] == r["total"]
    assert not count_violations(r)


def test_check_outliers_reports_how_many_columns_it_scanned(unconstrained, outlier_csv):
    from _med_inspect import check_outliers

    r = check_outliers(str(outlier_csv), open_after=False)
    assert r["success"] is True
    assert r["scanned_columns"] == r["total"]
    assert r["scanned_columns"] == 60, "it scanned all 60, whatever it could fit in the reply"
    assert r["returned"] == len(r["results"])
    assert not count_violations(r)


def test_inspect_dataset_says_so_even_when_nothing_was_cut(unconstrained, tmp_path):
    from servers.data_basic.engine import inspect_dataset

    p = tmp_path / "small.csv"
    pd.DataFrame({"a": [1, 2], "b": [3, 4]}).to_csv(p, index=False)
    r = inspect_dataset(str(p))
    assert r["success"] is True
    # The point: present and False, not absent.
    assert "truncated" in r
    assert r["truncated"] is False
    assert r["total"] == 2
    assert not count_violations(r)


# ---------------------------------------------------------------------------
# the caps that ignored the deployment
# ---------------------------------------------------------------------------


@pytest.fixture
def many_rows_csv(tmp_path):
    p = tmp_path / "rows.csv"
    pd.DataFrame({"a": range(200), "b": range(200)}).to_csv(p, index=False)
    return p


def test_sample_data_shrinks_when_the_deployment_says_to(many_rows_csv, monkeypatch):
    """`get_max_rows()` was read into a variable and thrown away."""
    from _med_inspect import sample_data

    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    wide = sample_data(str(many_rows_csv), n=150, method="head")

    monkeypatch.setenv("MCP_CONSTRAINED_MODE", "1")
    narrow = sample_data(str(many_rows_csv), n=150, method="head")

    assert narrow["returned"] < wide["returned"], "constrained mode must actually constrain"
    assert wide["total"] == narrow["total"] == 150, "the sample drawn is the same either way"
    assert not count_violations(wide)
    assert not count_violations(narrow)


def test_a_sample_that_fits_is_not_marked_truncated(many_rows_csv, monkeypatch):
    from _med_inspect import sample_data

    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    r = sample_data(str(many_rows_csv), n=5, method="head")
    assert r["returned"] == 5
    assert r["total"] == 5
    assert r["truncated"] is False


# ---------------------------------------------------------------------------
# the warning that fired on a complete answer
# ---------------------------------------------------------------------------


def test_time_series_does_not_warn_about_periods_it_kept(monkeypatch, tmp_path):
    """It warned "showing last N periods" having returned every one of them."""
    from _med_analysis import time_series_analysis

    monkeypatch.setenv("MCP_CONSTRAINED_MODE", "1")  # smallest cap, worst case
    dates = pd.date_range("2020-01-01", periods=120, freq="D")
    p = tmp_path / "ts.csv"
    pd.DataFrame({"when": dates, "value": range(120)}).to_csv(p, index=False)

    r = time_series_analysis(str(p), date_column="when", value_columns=["value"], period="D", open_after=False)
    assert r["success"] is True
    assert r["truncated"] is False, "every period was analysed"
    assert r["returned"] == r["total"] == r["total_periods"]
    truncation_warnings = [
        w for w in r["progress"] if w.get("status") == "warn" and "truncat" in str(w.get("title", "")).lower()
    ]
    assert not truncation_warnings, f"nothing was cut, so nothing should say it was: {truncation_warnings}"
