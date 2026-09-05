"""What a generated page is a picture of.

A user review read the first thirty lines of `Credit_Risk_bar.html` and found
"full Plotly + CSS shell. Boilerplate dominates". Its fix had two halves:

    AGI: split shell from data (cached shell + JSON data); every HTML needs
    `rows_plotted / rows_total / was_sampled / data_hash` in header JSON;
    `preview_small:true` option for loop, full on demand.

**Only the second half is implemented, and deliberately.** Splitting the shell
from the data means a page that needs a sibling file to render, and that is the
one thing these artifacts may not do. It was tried: Plotly is 4.86 MB, so pages
were changed to load `plotly.min.js` from a sidecar written once per output
directory, taking each page from 4.86 MB to 12 KB. Correct for a directory
served whole, wrong for a deliverable, because the deliverable travels --
downloaded alone, copied, attached. Each of those was a title, an empty bordered
box, and `Plotly is not defined` in a console nobody has open. Every test passed
throughout, because they asserted the sidecar was *present*, which is the
implementation rather than the property.

So the page stays self-contained and gains the header instead. That is the half
that was actually missing: a chart with no provenance cannot be checked against
its source, and a chart drawn from a sample looks exactly like one drawn from
everything.

The block is a `<script type="application/json">` in the document, so it is
machine-readable without parsing HTML and invisible to a reader. `data_hash`
identifies the rows the picture was drawn from, so two charts can be compared,
or one can be matched to the file it came from months later.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

PROVENANCE_ID = "mcp-provenance"

# The spec a generated page was built from, embedded so a later `customize_*`
# call can read it back. Same mechanism as the provenance block, different
# question: one says what the page is a picture of, the other says what was
# asked for. Keeping them separate means a page can carry either without the
# other, and a reader knows which is which.
SPEC_ID = "mcp-spec"

# The same 64 MB ceiling the receipt log uses: above it a content hash costs
# more than the operation it describes.
_MAX_HASH_BYTES = 64 * 1024 * 1024


def frame_hash(df) -> str:
    """A stable id for the rows a chart was drawn from.

    Hashes the frame's values rather than the file, because the file may have
    been filtered, sampled or aggregated on the way here -- and it is the rows
    that were plotted that a reader needs to identify.
    """
    try:
        payload = df.to_csv(index=False).encode("utf-8")
    except Exception:
        return ""
    if len(payload) > _MAX_HASH_BYTES:
        # Shape and column names, which still distinguishes two different
        # frames, without pretending to be a content hash.
        try:
            spine = f"{len(df)}x{len(df.columns)}:{','.join(map(str, df.columns))}".encode()
        except Exception:
            return ""
        return "shape:" + hashlib.sha256(spine).hexdigest()[:16]
    return "sha256:" + hashlib.sha256(payload).hexdigest()[:16]


def provenance(
    *,
    rows_plotted: int,
    rows_total: int,
    source: str = "",
    data_hash: str = "",
    tool: str = "",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """The header a generated page carries, and the fields a response repeats.

    `was_sampled` is derived from the two counts for the same reason
    `truncated` is derived in `shared/counts.py`: a flag that can be set
    independently of the numbers beside it will eventually disagree with them.
    """
    rows_plotted, rows_total = int(rows_plotted), int(rows_total)
    out: dict[str, Any] = {
        "rows_plotted": rows_plotted,
        "rows_total": rows_total,
        "was_sampled": rows_plotted < rows_total,
    }
    if data_hash:
        out["data_hash"] = data_hash
    if source:
        out["source"] = source
    if tool:
        out["tool"] = tool
    if extra:
        out.update(extra)
    return out


def provenance_script(header: dict[str, Any]) -> str:
    """The block to drop into a page. Empty string for an empty header.

    Written as `application/json` rather than a comment so a later tool can
    read it without an HTML parser, and `</` is escaped so a string in the data
    cannot close the script tag early.
    """
    if not header:
        return ""
    blob = json.dumps(header, indent=2, default=str).replace("</", "<\\/")
    return f'<script type="application/json" id="{PROVENANCE_ID}">\n{blob}\n</script>'


def _json_block(html: str, block_id: str) -> dict[str, Any]:
    marker = f'<script type="application/json" id="{block_id}">'
    start = html.find(marker)
    if start == -1:
        return {}
    start += len(marker)
    end = html.find("</script>", start)
    if end == -1:
        return {}
    try:
        return json.loads(html[start:end].replace("<\\/", "</"))
    except json.JSONDecodeError:
        return {}


def spec_script(spec: dict[str, Any]) -> str:
    """The build document, embedded in the page it produced."""
    if not spec:
        return ""
    blob = json.dumps(spec, indent=2, default=str).replace("</", "<\\/")
    return f'<script type="application/json" id="{SPEC_ID}">\n{blob}\n</script>'


def read_spec(html: str) -> dict[str, Any]:
    """Pull the build document back out of a generated page. `{}` when absent.

    This is what makes a `customize_*` call cheap: it reads what was asked for
    last time rather than inferring it from what was rendered.
    """
    return _json_block(html, SPEC_ID)


def read_provenance(html: str) -> dict[str, Any]:
    """Pull the header back out of a page. `{}` when there is none.

    Exists so the property can be tested on the artifact itself rather than on
    the arguments that produced it -- the sidecar defect passed every test
    precisely because the tests checked the implementation.
    """
    return _json_block(html, PROVENANCE_ID)
