"""feature_engineering's derive says when a division wrote inf.

`cpc = spends / clicks` on Ad_Data.csv, where 4,530 rows have no clicks, wrote
426 rows of inf and said nothing: progress listed "Derived 'cpc'", and the file
carried the inf into every mean and chart built on it. apply_patch's column_math
already reports the same rows; this path now does too.
"""

from __future__ import annotations

import math

import pandas as pd

from servers.data_medium._med_transform import feature_engineering


def _run(tmp_path, monkeypatch, frame, derive):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
    src = tmp_path / "ads.csv"
    frame.to_csv(src, index=False)
    return feature_engineering(str(src), derive=derive, output_path=str(tmp_path / "out.csv"), open_after=False)


def test_the_inf_is_counted_and_named(tmp_path, monkeypatch):
    frame = pd.DataFrame({"spends": [10.0, 5.0, 0.0, 8.0], "clicks": [2, 0, 0, 4]})
    r = _run(
        tmp_path,
        monkeypatch,
        frame,
        [{"name": "cpc", "op": "arith", "column": "spends", "how": "div", "other": "clicks"}],
    )
    assert r["success"] is True
    assert r["non_finite"] == {"cpc": 1}, "5/0 is inf; 0/0 is a null, not an inf"
    assert "'cpc' 1" in r["warning"] and "if_else" in r["warning"]
    assert any(p.get("message") == "Infinite values written" for p in r["progress"])
    written = pd.read_csv(tmp_path / "out.csv")
    assert math.isinf(written.loc[1, "cpc"]), "the value itself is left as written"


def test_a_clean_derivation_says_nothing(tmp_path, monkeypatch):
    frame = pd.DataFrame({"spends": [10.0, 5.0], "clicks": [2, 5]})
    r = _run(
        tmp_path,
        monkeypatch,
        frame,
        [{"name": "cpc", "op": "arith", "column": "spends", "how": "div", "other": "clicks"}],
    )
    assert r["success"] is True and "non_finite" not in r and "warning" not in r
