"""`detect_anomalies` wrote its output wherever the INPUT happened to live.

The default path was `source.parent`, which is correct right up until the
input arrives by URL. `MCP_FETCH_URLS=1` downloads those into
`MCP_OUTPUT_DIR/inbox`, so a dataset fetched from a link put an 8.5 MB
anomalies CSV in `inbox/` while every sibling tool in the session wrote to
`MCP_OUTPUT_DIR` itself.

Neither path is wrong on its own, and that is what made it expensive: the
caller went looking where the other twenty tools had put their output, did
not find it, and only located the file by listing directories. A user review
recorded it as "split locations break the outputs-in-data mental model".

`shared/exchange.default_output_path` is the fix, and it is deliberately not
"strip inbox from the parent" -- the rule is that a default output goes to the
output directory, full stop, whatever the input's provenance.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from shared.exchange import default_output_path, get_output_dir


@pytest.fixture()
def output_dir(tmp_path, monkeypatch):
    out = tmp_path / "workspace" / "data"
    out.mkdir(parents=True)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(out))
    return out


def test_a_url_input_does_not_drag_the_output_into_inbox(output_dir):
    """The exact shape from the session: input in inbox, output in data."""
    inbox = output_dir / "inbox"
    inbox.mkdir()
    source = inbox / "Credit_Risk.csv"

    landed = default_output_path(source, "anomalies")

    assert landed.parent == output_dir
    assert landed.parent.name != "inbox"
    assert landed.name == "Credit_Risk_anomalies.csv"


def test_an_input_from_anywhere_else_lands_in_the_same_place(output_dir, tmp_path):
    """Provenance does not get a vote. That is the whole rule."""
    elsewhere = tmp_path / "somewhere" / "else"
    elsewhere.mkdir(parents=True)

    a = default_output_path(elsewhere / "Credit_Risk.csv", "anomalies")
    b = default_output_path(output_dir / "Credit_Risk.csv", "anomalies")
    assert a == b == output_dir / "Credit_Risk_anomalies.csv"


def test_the_output_dir_is_created_rather_than_assumed(tmp_path, monkeypatch):
    target = tmp_path / "not" / "yet" / "there"
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(target))
    landed = default_output_path("x/y/report.csv", "summary")
    assert landed.parent.is_dir()
    assert landed == target / "report_summary.csv"


def test_detect_anomalies_writes_where_its_siblings_write(output_dir):
    """End to end, through the tool that earned the fix."""
    from servers.data_medium._med_analysis import detect_anomalies

    inbox = output_dir / "inbox"
    inbox.mkdir()
    source = inbox / "loans.csv"
    pd.DataFrame({"amount": list(range(40)) + [99999], "n": list(range(41))}).to_csv(source, index=False)

    result = detect_anomalies(str(source))
    assert result.get("success") is True, result.get("error")

    written = Path(result["output_path"])
    assert written.parent == get_output_dir()
    assert written.exists()
    # And nothing was left behind next to the input.
    assert not list(inbox.glob("*_anomalies.csv"))


def test_an_explicit_output_path_still_wins(output_dir, tmp_path):
    from servers.data_medium._med_analysis import detect_anomalies

    source = output_dir / "loans.csv"
    pd.DataFrame({"amount": list(range(40)) + [99999], "n": list(range(41))}).to_csv(source, index=False)
    chosen = output_dir / "my_own_name.csv"

    result = detect_anomalies(str(source), output_path=str(chosen))
    assert result.get("success") is True, result.get("error")
    assert Path(result["output_path"]) == chosen
