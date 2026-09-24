"""A column name or a cell in the EDA report and the auto profile is text, never markup or code.

Both reports are the artifacts people open from someone else's data, and both
ran script out of it:

- run_eda wrote a column's top values into its Column Summary as markup --
  a cell holding <img src=x onerror=alert(1)> ran in every reader's browser
  when the report opened -- and a skew insight wrote a column name the same
  way;
- both reports put column names and category values into <script> blocks
  with json.dumps, which leaves </script> intact, so a value holding it ended
  the block and what followed ran as the page's own;
- the auto profile built each column's element id from its name, and wrote
  that id into an attribute, an href and a JavaScript string: a column
  called q"onmouseover="alert(7) gave its card an event handler.

Tested on each page as a whole, against a twin with harmless names and
values: the same tags, the same attribute names, the same number of scripts,
and every inline script still parses.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from servers.data_advanced._adv_eda import run_eda
from servers.data_advanced._adv_profile import generate_auto_profile
from tests.test_a_column_name_that_ran_as_code import _shape

HOSTILE_NAMES = {
    "kind": "k</script><svg/onload=alert(6)>",
    "size": 'q"onmouseover="alert(7)',
    "cost": "c'ost</script><b>",
    "gaps": "g</script>aps",
}
HOSTILE_VALUES = ["<img src=x onerror=alert(1)>", "</script><svg/onload=alert(2)>", 'x"onmouseover="alert(3)']


def _frame(names: dict[str, str], values: list[str]) -> pd.DataFrame:
    rng = np.random.default_rng(3)
    n = 120
    return pd.DataFrame(
        {
            names["kind"]: rng.choice(values, n),
            names["size"]: rng.normal(10, 2, n).round(2),
            names["cost"]: rng.normal(100, 20, n).round(2),
            "when": pd.date_range("2024-01-01", periods=n).strftime("%Y-%m-%d"),
            names["gaps"]: [None] * 12 + list(rng.normal(0, 1, n - 12)),
        }
    )


def _page(tmp_path: Path, report: str, hostile: bool) -> str:
    names = HOSTILE_NAMES if hostile else {k: k for k in HOSTILE_NAMES}
    values = HOSTILE_VALUES if hostile else ["alpha", "beta", "gamma"]
    tag = f"{report}_{'hostile' if hostile else 'benign'}"
    data = tmp_path / f"{tag}.csv"
    _frame(names, values).to_csv(data, index=False)
    out = tmp_path / f"{tag}.html"
    if report == "eda":
        r = run_eda(str(data), output_path=str(out), open_after=False, target_column=names["size"])
    else:
        r = generate_auto_profile(str(data), output_path=str(out), open_after=False)
    assert r["success"] is True, r
    return out.read_text(encoding="utf-8")


@pytest.fixture(autouse=True)
def _folders(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))


@pytest.mark.parametrize("report", ["eda", "profile"])
def test_hostile_names_and_values_add_no_markup(tmp_path, report):
    benign = _shape(_page(tmp_path, report, hostile=False))
    hostile = _shape(_page(tmp_path, report, hostile=True))
    assert hostile.tags == benign.tags, f"new tags: {hostile.tags - benign.tags}"
    assert hostile.attrs == benign.attrs, f"new attributes: {hostile.attrs - benign.attrs}"
    assert len(hostile.scripts) == len(benign.scripts), "a value opened or closed a <script>"


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
@pytest.mark.parametrize("report", ["eda", "profile"])
def test_every_inline_script_still_parses(tmp_path, report):
    scripts = _shape(_page(tmp_path, report, hostile=True)).scripts
    assert scripts
    for i, body in enumerate(scripts):
        try:
            json.loads(body)  # a data island (the provenance block): it has to stay valid JSON
            continue
        except ValueError:
            pass
        src = tmp_path / f"{report}_{i}.js"
        src.write_text(body, encoding="utf-8")
        done = subprocess.run(["node", "--check", str(src)], capture_output=True, text=True)
        assert done.returncode == 0, done.stderr[-2000:]


@pytest.mark.parametrize("report", ["eda", "profile"])
def test_the_names_and_values_still_read_as_themselves(tmp_path, report):
    html = _page(tmp_path, report, hostile=True)
    assert "&lt;img src=x onerror=alert(1)&gt;" in html
    assert "q&quot;onmouseover=&quot;alert(7)" in html
