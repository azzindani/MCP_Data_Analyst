"""A constant column has no correlation, and the page that draws the matrix must still load.

The loan file's `year` is 2019 in every row, so its row and column of the correlation matrix are
NaN. `run_eda` and `generate_auto_profile` wrote the matrix into the page with Python's own
`repr`, which spells that `nan` -- not a JavaScript name -- and the heatmap script died with
"ReferenceError: nan is not defined": one figure of three drawn in the EDA, two of 38 in the
profile. The x and y axes already went through `json_for_script`; the z matrix did not.
"""

from __future__ import annotations

import json
import re

import numpy as np
import pandas as pd
import pytest

from servers.data_advanced._adv_eda import run_eda
from servers.data_advanced._adv_profile import generate_auto_profile
from shared.table_payload import json_for_script


@pytest.fixture
def frame_path(tmp_path, monkeypatch) -> str:
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(3)
    a = rng.normal(0, 1, 200)
    pd.DataFrame(
        {
            "a": a,
            "b": a * 2 + rng.normal(0, 0.5, 200),
            "year": 2019,  # constant: every correlation with it is NaN
            "kind": rng.choice(["x", "y", "z"], 200),
        }
    ).to_csv(tmp_path / "const.csv", index=False)
    return str(tmp_path / "const.csv")


def _matrices(html: str) -> list[str]:
    """Every `var z = [[...]]` matrix the page assigns (the inlined Plotly bundle has its own `var z`)."""
    return [m.group(1) for m in re.finditer(r"var z\s*=\s*(\[\[.*?\]\]);", html, flags=re.DOTALL)]


class TestNoBareNanInThePage:
    def test_the_eda_matrices_are_parseable(self, frame_path, tmp_path):
        result = run_eda(frame_path, output_path=str(tmp_path / "eda.html"), open_after=False)
        assert result["success"] is True, result.get("error")
        found = _matrices((tmp_path / "eda.html").read_text(encoding="utf-8"))
        assert found, "no heatmap matrix in the page"
        for text in found:
            rows = json.loads(text)  # a bare `nan` raises here, as it did in the browser
            assert isinstance(rows, list)
        assert any("null" in text for text in found), "the constant column's cells should be gaps"

    def test_the_profile_matrices_are_parseable(self, frame_path, tmp_path):
        result = generate_auto_profile(frame_path, output_path=str(tmp_path / "profile.html"), open_after=False)
        assert result["success"] is True, result.get("error")
        found = _matrices((tmp_path / "profile.html").read_text(encoding="utf-8"))
        assert found
        for text in found:
            json.loads(text)

    def test_a_gap_is_not_labelled_with_a_method_call_on_null(self, frame_path, tmp_path):
        run_eda(frame_path, output_path=str(tmp_path / "eda.html"), open_after=False)
        html = (tmp_path / "eda.html").read_text(encoding="utf-8")
        assert "v===null?'':v.toFixed(2)" in html


class TestJsonForScript:
    def test_nan_and_infinity_become_null(self):
        assert json_for_script([[1.0, float("nan")], [float("inf"), -float("inf")]]) == "[[1.0,null],[null,null]]"

    def test_a_finite_payload_is_unchanged(self):
        assert json_for_script({"x": [1, 2.5, "a<b"]}) == '{"x":[1,2.5,"a\\u003cb"]}'

    def test_nan_inside_a_dict_and_a_tuple(self):
        assert json.loads(json_for_script({"z": (float("nan"), 1)})) == {"z": [None, 1]}
