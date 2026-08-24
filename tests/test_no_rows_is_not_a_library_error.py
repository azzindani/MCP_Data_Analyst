"""A file with no rows failed deep inside a library, in the library's words.

A header row and no data rows is an ordinary thing to be handed: a filter that
matched nothing, an export that returned nothing, a query run before the data
landed. It is not a malformed file, so it parses cleanly and every guard that
checks for a missing or zero-byte file lets it through. The failure then came
out of numpy and pandas:

    regression_analysis   "zero-size array to reduction operation maximum
                           which has no identity"
    generate_dashboard    "cannot convert float NaN to integer"

Neither message names a file, a column or anything the caller chose, and both
hints sent the reader somewhere unrelated -- regression's at the column
arguments, which were correct, and the dashboard's at the file path being
absolute and the CSV being valid, which it was. A caller following either would
spend the effort in the wrong place and arrive back at the same error.

`no_rows_error()` refuses before the maths starts, names the row count, and
points at the thing that produced the file.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_statistics"), str(ROOT / "servers" / "data_advanced")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from servers.data_advanced import engine as adv  # noqa: E402
from servers.data_statistics import engine as stats  # noqa: E402

LIBRARY_WORDS = (
    "zero-size",
    "cannot convert float nan",
    "reduction operation",
    "index out of bounds",
    "list index out of range",
)


@pytest.fixture
def header_only(tmp_path) -> str:
    p = tmp_path / "no_rows.csv"
    p.write_text("Date,product,spends,clicks\n", encoding="utf-8")
    return str(p)


@pytest.fixture
def real(tmp_path) -> str:
    frame = pd.DataFrame(
        {
            "Date": pd.date_range("2024-01-01", periods=30, freq="D").astype(str),
            "product": ["a", "b"] * 15,
            "spends": [float(i) for i in range(30)],
            "clicks": [float(i) * 2 + 1 for i in range(30)],
        }
    )
    p = tmp_path / "real.csv"
    frame.to_csv(p, index=False)
    return str(p)


def says_library_words(result: dict) -> bool:
    return any(w in str(result.get("error", "")).lower() for w in LIBRARY_WORDS)


class TestRegressionRefusesInItsOwnWords:
    def test_it_refuses(self, header_only):
        r = stats.regression_analysis(header_only, y_col="clicks", x_cols=["spends"])
        assert r["success"] is False

    def test_the_error_is_not_a_numpy_message(self, header_only):
        r = stats.regression_analysis(header_only, y_col="clicks", x_cols=["spends"])
        assert not says_library_words(r), r["error"]

    def test_it_names_the_row_count(self, header_only):
        r = stats.regression_analysis(header_only, y_col="clicks", x_cols=["spends"])
        assert "no data rows" in r["error"], r["error"]

    def test_the_hint_does_not_blame_the_columns(self, header_only):
        # The columns were named correctly; sending the caller to check them
        # is what made the old message cost time.
        r = stats.regression_analysis(header_only, y_col="clicks", x_cols=["spends"])
        assert "inspect_dataset" in r["hint"], r["hint"]

    def test_it_reports_zero_rows_and_the_column_count(self, header_only):
        r = stats.regression_analysis(header_only, y_col="clicks", x_cols=["spends"])
        assert r["rows"] == 0 and r["columns"] == 4, r


class TestTheDashboardRefusesInItsOwnWords:
    def test_it_refuses(self, header_only):
        r = adv.generate_dashboard(file_path=header_only, open_after=False)
        assert r["success"] is False

    def test_the_error_is_not_a_pandas_message(self, header_only):
        r = adv.generate_dashboard(file_path=header_only, open_after=False)
        assert not says_library_words(r), r["error"]

    def test_the_hint_does_not_blame_the_path(self, header_only):
        r = adv.generate_dashboard(file_path=header_only, open_after=False)
        assert "absolute" not in r["hint"].lower(), r["hint"]

    def test_no_dashboard_file_is_written(self, header_only, tmp_path):
        adv.generate_dashboard(file_path=header_only, open_after=False)
        assert not list(tmp_path.glob("*.html")), "an empty dashboard was written anyway"


class TestARealFileIsUnaffected:
    def test_regression_still_fits(self, real):
        r = stats.regression_analysis(real, y_col="clicks", x_cols=["spends"], open_after=False)
        assert r["success"] is True, r.get("error")
        assert r["r_squared"] > 0.99, r["r_squared"]

    def test_the_dashboard_still_builds(self, real):
        r = adv.generate_dashboard(file_path=real, open_after=False)
        assert r["success"] is True, r.get("error")
        assert Path(r["output_path"]).exists()


class TestTheGuardItself:
    def test_it_passes_a_frame_with_rows_through(self):
        from shared.file_utils import no_rows_error

        assert no_rows_error("x", pd.DataFrame({"a": [1]}), "f.csv", "Doing it") is None

    def test_it_refuses_a_frame_with_none(self):
        from shared.file_utils import no_rows_error

        err = no_rows_error("x", pd.DataFrame({"a": [], "b": []}), "f.csv", "Doing it")
        assert err is not None
        assert err["success"] is False
        assert err["op"] == "x"
        assert err["columns"] == 2

    def test_the_hint_names_what_needed_rows(self):
        from shared.file_utils import no_rows_error

        err = no_rows_error("x", pd.DataFrame({"a": []}), "f.csv", "Fitting a model")
        assert "Fitting a model needs at least one row" in err["hint"], err["hint"]

    def test_it_carries_a_token_estimate(self):
        from shared.file_utils import no_rows_error

        err = no_rows_error("x", pd.DataFrame({"a": []}), "f.csv", "Doing it")
        assert isinstance(err["token_estimate"], int)
