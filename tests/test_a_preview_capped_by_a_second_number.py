"""`resample_timeseries` cut the preview with one cap and judged it by another.

    max_r = get_max_rows()          # 500
    truncated = total_periods > max_r     # 25 > 500 -> False
    _preview_cap = 20
    sample = result_df.head(_preview_cap)  # 20 rows

Resampled yearly, the SFO cargo file has 25 periods. The caller got 20 rows in
`data` beside `truncated: false`, and `truncated` was not lying about what it
measured -- it measured the FILE, which really is under the row limit. It was
answering a question nobody asked, next to the twenty rows that were the
answer.

`compute_aggregations`, four hundred lines up in this same file, had exactly
this bug and had already been fixed: its comment records returning 20 of 25
years, dropping the 2013 trough that was then quoted in a report. Nobody went
back for the sibling. That is the same shape as `fs_index list` in
MCP_File_System, whose sibling `fs_query` got the total and it did not.

Which is why the fix is not "use the right cap here". It is `counted()`, where
`truncated` is derived from `returned` and `total` and there is no way to
compute it against a third number.
"""

from __future__ import annotations

import pandas as pd
import pytest

from shared.counts import count_violations


@pytest.fixture()
def yearly(tmp_path, monkeypatch):
    """25 yearly periods -- the shape that produced the finding."""
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    src = tmp_path / "cargo.csv"
    dates = pd.date_range("1999-01-01", periods=25, freq="YS")
    pd.DataFrame({"date": dates.strftime("%Y-%m-%d"), "tons": range(100, 125)}).to_csv(src, index=False)
    return src


def _resample(src, **kw):
    from servers.data_medium._med_transform import resample_timeseries

    return resample_timeseries(str(src), date_col="date", freq="Y", agg_func="sum", value_cols=["tons"], **kw)


def test_the_preview_says_how_many_of_how_many(yearly):
    result = _resample(yearly)
    assert result.get("success") is True, result.get("error")

    assert result["total_periods"] == 25
    assert result["total"] == 25
    assert result["returned"] == len(result["data"])
    assert count_violations(result) == []


def test_a_short_preview_is_never_reported_as_complete(yearly, monkeypatch):
    """The exact regression: 20 rows of 25 must not say truncated false."""
    import servers.data_medium._med_transform as transform
    import shared.platform_utils as platform_utils

    monkeypatch.setattr(transform, "get_max_rows", lambda: 20)
    monkeypatch.setattr(platform_utils, "get_max_rows", lambda: 20)

    result = _resample(yearly)
    assert result.get("success") is True, result.get("error")

    assert len(result["data"]) == 20
    assert result["returned"] == 20
    assert result["total"] == 25
    assert result["truncated"] is True
    assert count_violations(result) == []


def test_the_full_result_is_still_on_disk_when_the_preview_is_cut(yearly, monkeypatch):
    """Truncation is about the response, never about the file."""
    import servers.data_medium._med_transform as transform
    import shared.platform_utils as platform_utils

    monkeypatch.setattr(transform, "get_max_rows", lambda: 20)
    monkeypatch.setattr(platform_utils, "get_max_rows", lambda: 20)

    result = _resample(yearly)
    written = pd.read_csv(result["output_path"])
    assert len(written) == 25
    assert result["returned"] == 20


def test_the_caller_is_told_where_the_rest_is(yearly, monkeypatch):
    import servers.data_medium._med_transform as transform
    import shared.platform_utils as platform_utils

    monkeypatch.setattr(transform, "get_max_rows", lambda: 20)
    monkeypatch.setattr(platform_utils, "get_max_rows", lambda: 20)

    result = _resample(yearly)
    said = " ".join(str(p) for p in result["progress"])
    assert "20 of 25" in said
    assert result["output_name"] in said


def test_no_second_hardcoded_cap_survives_in_this_module():
    """The bug was a bare literal doing the cutting. Keep it gone."""
    import pathlib
    import re

    src = pathlib.Path(__file__).resolve().parents[1] / "servers/data_medium/_med_transform.py"
    offenders = [
        line.strip()
        for line in src.read_text().splitlines()
        if re.search(r"_(preview|response)_cap\s*=", line) and not line.lstrip().startswith("#")
    ]
    assert not offenders, "a second cap is back:\n" + "\n".join(offenders)
