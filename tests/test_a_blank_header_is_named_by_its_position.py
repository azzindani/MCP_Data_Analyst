"""normalize_headers names a blank header by its position.

pandas reads a blank header cell as the placeholder `Unnamed: 0`, and
normalize_headers lowercased it and replaced its space: `unnamed:_0`, a name
with a colon in it that the file never said. The sweep's Ad_Data export with
its index column came back with exactly that.
"""

from __future__ import annotations

import pandas as pd
import pytest

from servers.data_ingest.engine import normalize_headers


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
    return tmp_path


def _headers(home, text: str) -> list[str]:
    (home / "in.csv").write_text(text, encoding="utf-8")
    r = normalize_headers(str(home / "in.csv"), output_path=str(home / "out.csv"))
    assert r["success"] is True, r
    return pd.read_csv(home / "out.csv").columns.tolist()


def test_a_blank_header_gets_its_position(home):
    assert _headers(home, ",Daily Time,Age\n1,2,3\n") == ["column_1", "daily_time", "age"]


def test_two_blank_headers_get_two_positions(home):
    assert _headers(home, "a,,b,\n1,2,3,4\n") == ["a", "column_2", "b", "column_4"]


def test_a_header_that_says_unnamed_is_kept(home):
    """Only pandas' exact placeholder is blank; a real header containing the word stays a header."""
    assert _headers(home, "Unnamed Region,x\n1,2\n") == ["unnamed_region", "x"]
