"""`shared/counts.py` is one file. It has already been three.

The module exists so that a response reporting a count cannot disagree with
itself: `truncated` is derived inside `counted()` rather than passed in, because
`resample_timeseries` sliced with a hardcoded 20 while computing the flag from a
different limit and reported twenty of twenty-five periods as all of them.

Copying it to the other repos re-created the problem one level up. Within a day
of being written it had two versions -- MCP_File_System lints at
`line-length = 100` and the rest at 120, so `ruff format` rewrapped one f-string
there, and that rewrapped copy was then propagated onward to MCP_Documents and
MCP_Microsoft_Office. Nothing was wrong with either version; they were the same
logic reflowed. That is exactly what makes it dangerous, and it is the same
mechanism that left `shared/oauth_bridge.py` 77 lines different from its twin
with no behaviour change in the diff.

Office's CI caught it, which is worth recording: `ruff format --check` failed on
`shared/shared/counts.py` on all three runners. The fix is not to reformat the
file -- that would fork it from its siblings, which is the whole thing it exists
to prevent -- but `[tool.ruff.format] exclude = ["shared/**"]`, so the formatter
lints these modules and never rewrites them.

Two tests below, with deliberately different reach:

* the pyproject guard is asserted for *this* repo, so it runs on a GitHub runner
  and a future commit cannot quietly drop it;
* byte-identity is asserted across the fleet, which only has the sibling repos
  to compare against on the box they all live on, so off-box it skips. That
  limitation is inherited from `test_one_file_two_quality_scores.py` and is the
  reason the guard above carries the real weight.
"""

from __future__ import annotations

import hashlib
import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

# Where each repo keeps its shared package. Office packages its own
# (`shared/pyproject.toml` + `shared/shared/`), so its path has the extra level;
# that is the real layout, not a mis-placed copy.
FLEET: dict[str, str] = {
    "MCP_Data_Analyst": "shared/counts.py",
    "MCP_Machine_Learning": "shared/counts.py",
    "MCP_File_System": "shared/counts.py",
    "MCP_Documents": "shared/counts.py",
    "MCP_Microsoft_Office": "shared/shared/counts.py",
}


def _sha(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_this_repo_excludes_shared_from_the_formatter():
    """The CI-enforceable half: runs anywhere, including a GitHub runner."""
    pyproject = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert "[tool.ruff.format]" in pyproject, "the formatter guard for shared/ is missing"
    guard = pyproject.split("[tool.ruff.format]", 1)[1].split("[tool.", 1)[0]
    assert 'exclude = ["shared/**"]' in guard, "shared/ must be linted but never rewritten"


def test_counted_derives_truncated_rather_than_accepting_it():
    """If this ever takes `truncated` as an argument, the contract is gone."""
    import inspect

    from shared.counts import counted

    params = set(inspect.signature(counted).parameters)
    assert "truncated" not in params
    assert counted(5, 5)["truncated"] is False
    assert counted(5, 9)["truncated"] is True


def test_the_module_is_one_file_across_the_fleet():
    """Byte-identity, on the box where all the repos actually live."""
    present = {
        name: pathlib.Path("/root") / name / rel
        for name, rel in FLEET.items()
        if (pathlib.Path("/root") / name / rel).exists()
    }
    if len(present) < 2:
        pytest.skip("sibling repos not present in this checkout")

    digests = {name: _sha(path) for name, path in present.items()}
    distinct = set(digests.values())
    assert len(distinct) == 1, (
        "shared/counts.py has forked across the fleet: "
        + ", ".join(f"{n}={d[:12]}" for n, d in sorted(digests.items()))
        + ". Do not reformat it back into agreement -- check that every repo "
        'carries [tool.ruff.format] exclude = ["shared/**"], then copy one '
        "canonical file over the others."
    )


def test_this_repos_copy_is_the_one_being_compared():
    """Guards against the fleet test passing on files none of which are ours."""
    ours = REPO_ROOT / "shared/counts.py"
    assert ours.exists()
    sibling = pathlib.Path("/root/MCP_File_System/shared/counts.py")
    if not sibling.exists():
        pytest.skip("sibling repo not present in this checkout")
    assert _sha(ours) == _sha(sibling)
