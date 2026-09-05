"""A chart drawn from a sample looks exactly like one drawn from everything.

A user review read the first thirty lines of a generated page and reported
"full Plotly + CSS shell. Boilerplate dominates". Its fix had two halves:

    AGI: split shell from data (cached shell + JSON data); every HTML needs
    `rows_plotted / rows_total / was_sampled / data_hash` in header JSON;
    `preview_small:true` option for loop, full on demand.

**Only the second half is here, on purpose.** Splitting the shell from the data
gives a page that needs a sibling file to render, and that is the one thing
these artifacts may not do. It was tried once: pages loaded `plotly.min.js`
from a sidecar written per directory, 4.86 MB down to 12 KB. Correct for a
directory served whole, wrong for a deliverable, because the deliverable
travels -- downloaded alone, copied, attached. Each of those became a title, an
empty bordered box, and `Plotly is not defined` in a console nobody has open.
Every test passed the whole time, because they asserted the sidecar was
*present*, which is the implementation rather than the property.

So the page stays self-contained and gains the header, which is the half that
was genuinely missing: a chart with no provenance cannot be checked against its
source, and one drawn from 5,000 of 38,576 rows is indistinguishable from one
drawn from all of them.

`was_sampled` is derived from the two counts rather than passed in, for the
same reason `truncated` is derived in `shared/counts.py`: a flag that can be
set independently of the numbers beside it will eventually disagree with them.
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

from shared.provenance import frame_hash, provenance, read_provenance  # noqa: E402


@pytest.fixture
def frame(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(1)
    p = tmp_path / "wide.csv"
    pd.DataFrame({"a": rng.normal(size=500), "b": rng.normal(size=500)}).to_csv(p, index=False)
    return p


def test_the_page_says_what_it_is_a_picture_of(frame):
    r = run_eda(str(frame), open_after=False)
    header = read_provenance(Path(r["output_path"]).read_text(encoding="utf-8"))
    assert header["rows_plotted"] == 500
    assert header["rows_total"] == 500
    assert header["was_sampled"] is False
    assert header["source"] == "wide.csv"
    assert header["data_hash"].startswith("sha256:")


def test_a_sampled_page_says_so_in_the_file(frame):
    """Not only in the response -- the file is what travels."""
    r = run_eda(str(frame), open_after=False, sample_n=100)
    header = read_provenance(Path(r["output_path"]).read_text(encoding="utf-8"))
    assert header["was_sampled"] is True
    assert header["rows_plotted"] == 100
    assert header["rows_total"] == 500


def test_the_response_repeats_the_header(frame):
    """A caller should not have to open the page to learn what it holds."""
    r = run_eda(str(frame), open_after=False, sample_n=100)
    assert r["provenance"]["was_sampled"] is True
    assert r["provenance"]["rows_plotted"] == 100


def test_the_page_still_needs_nothing_beside_it(frame):
    """The half of the review's fix that is deliberately not implemented."""
    r = run_eda(str(frame), open_after=False)
    page = Path(r["output_path"])
    html = page.read_text(encoding="utf-8")
    # Plotly is inlined, not linked. A src= to a sibling file is the defect.
    assert "plotly" in html.lower()
    assert 'src="plotly' not in html and "src='plotly" not in html
    assert 'src="./plotly' not in html


def test_two_different_frames_hash_differently():
    a = pd.DataFrame({"x": [1, 2, 3]})
    b = pd.DataFrame({"x": [1, 2, 4]})
    assert frame_hash(a) != frame_hash(b)


def test_the_same_frame_hashes_the_same():
    a = pd.DataFrame({"x": [1, 2, 3]})
    assert frame_hash(a) == frame_hash(a.copy())


def test_was_sampled_is_derived_not_accepted():
    import inspect

    assert "was_sampled" not in inspect.signature(provenance).parameters
    assert provenance(rows_plotted=5, rows_total=5)["was_sampled"] is False
    assert provenance(rows_plotted=5, rows_total=9)["was_sampled"] is True


def test_a_string_in_the_data_cannot_close_the_script_tag():
    from shared.provenance import provenance_script

    block = provenance_script({"source": "</script><script>alert(1)</script>"})
    assert "</script><script>" not in block.replace("<\\/", "")
    assert block.count("</script>") == 1


def test_a_page_with_no_header_reads_as_empty():
    assert read_provenance("<html><body>nothing</body></html>") == {}
