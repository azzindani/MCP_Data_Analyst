"""The compact payload has to rebuild the *identical* table in the browser.

A standalone dashboard carries its own data, and written as an array of row
objects every row repeats every column name — 5.85 MB of the 10.75 MB dashboard
was column names. Compressing it only helps if the page reconstructs exactly what
it had before, so these tests decode the emitted JavaScript in node and compare
it against the JSON the dashboard used to embed.
"""

from __future__ import annotations

import json
import shutil
import subprocess

import numpy as np
import pandas as pd
import pytest

from shared.table_payload import encode_frame, records_js

NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")


@pytest.fixture()
def frame() -> pd.DataFrame:
    rng = np.random.default_rng(7)
    n = 400
    return pd.DataFrame(
        {
            "day": pd.date_range("2024-01-01", periods=n).strftime("%Y-%m-%d"),
            "channel": rng.choice(["Google Ads", "Facebook Ads"], n),
            "device": rng.choice(["Mobile", "Desktop", "Tablet"], n),
            "note": [f"free text {i}" for i in range(n)],
            "spend": rng.uniform(0, 500, n).round(2),
            "clicks": rng.integers(0, 100, n),
        }
    )


def _decode_in_node(df: pd.DataFrame) -> list[dict]:
    program = f"console.log(JSON.stringify({records_js(df)}));"
    out = subprocess.run([NODE, "-e", program], capture_output=True, text=True, timeout=60)
    if out.returncode != 0:
        raise AssertionError(f"node failed: {out.stderr[:400]}")
    return json.loads(out.stdout)


class TestEncoding:
    def test_low_cardinality_columns_become_dictionaries(self, frame):
        enc = encode_frame(frame)["enc"]
        assert set(enc["channel"]) == {"d", "c"}
        assert enc["channel"]["d"] == ["Facebook Ads", "Google Ads"]

    def test_mostly_unique_text_is_left_alone(self, frame):
        """A dictionary the same size as the column saves nothing."""
        assert isinstance(encode_frame(frame)["enc"]["note"], list)

    def test_numeric_columns_stay_numeric(self, frame):
        assert isinstance(encode_frame(frame)["enc"]["spend"][0], float)

    def test_small_frames_are_not_dictionary_encoded(self):
        small = pd.DataFrame({"a": ["x", "y", "x"]})
        assert isinstance(encode_frame(small)["enc"]["a"], list)

    def test_it_is_actually_smaller(self, frame):
        before = len(frame.to_json(orient="records"))
        after = len(json.dumps(encode_frame(frame), separators=(",", ":"), default=str))
        assert after < before / 2


@needs_node
class TestRoundTripInTheBrowser:
    def test_decoded_rows_match_the_original_json_exactly(self, frame):
        expected = json.loads(frame.to_json(orient="records"))
        assert _decode_in_node(frame) == expected

    def test_column_order_is_preserved(self, frame):
        rows = _decode_in_node(frame)
        assert list(rows[0].keys()) == list(frame.columns)

    def test_row_count_is_preserved(self, frame):
        assert len(_decode_in_node(frame)) == len(frame)

    def test_nulls_survive_the_round_trip(self):
        df = pd.DataFrame({"a": [1.0, None, 3.0], "b": ["x", None, "y"]})
        rows = _decode_in_node(df)
        assert rows[1]["a"] is None
        assert rows[1]["b"] == ""

    def test_values_with_quotes_and_newlines_survive(self):
        df = pd.DataFrame({"t": ['he said "hi"', "line\nbreak", "tab\there"] * 8})
        rows = _decode_in_node(df)
        assert [r["t"] for r in rows[:3]] == ['he said "hi"', "line\nbreak", "tab\there"]

    def test_unicode_survives(self):
        df = pd.DataFrame({"t": ["café", "日本語", "emoji 🎯"] * 8})
        rows = _decode_in_node(df)
        assert [r["t"] for r in rows[:3]] == ["café", "日本語", "emoji 🎯"]

    def test_an_empty_frame_decodes_to_an_empty_array(self):
        assert _decode_in_node(pd.DataFrame({"a": []})) == []
