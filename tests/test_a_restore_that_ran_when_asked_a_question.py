"""Omitting the timestamp overwrote the file instead of listing versions.

    restore_version(d.csv)          # no timestamp
      -> success: true
         restored_from: .../d_2026-08-26T07-19-16Z.csv.bak
         available_versions: [ ...two of them... ]
         hint: "Call inspect_dataset() to confirm the restored state."

restore_version is annotated EDITS, and there is no separate tool for listing
what snapshots a file has. So the natural way to ask the question -- name the
file, leave the timestamp off, see what comes back -- is also the call that
writes the newest backup over the file. The old docstring, "timestamp from
backup filename", described where to get the argument and never said what
happens without it.

Nothing is lost when that happens: a counter-snapshot is taken first, and a
round-16 phase confirmed the file can be put back. This is not a data-safety
bug. It is that a mutating call chose its target from an absent argument and
the response read exactly like one that had been told what to do.

So the choice is now visible in three places -- newest_by_default in the
payload, a progress warning, and a hint naming the snapshot it wrote and how
to pick another. The behaviour is unchanged: restore-to-newest is a reasonable
default and callers rely on it, including the sweep that found this. Only the
silence is gone.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from servers.data_basic import engine as db


@pytest.fixture()
def versioned(tmp_path: Path) -> Path:
    """A file with two real snapshots behind it.

    The values must actually change per patch: an op that rewrites a file
    identically has its snapshot discarded as a duplicate, so integer columns
    rounded to 0 decimals would leave no versions at all.
    """
    f = tmp_path / "d.csv"
    f.write_text("a,b\n1.55,2.7\n3.44,4.2\n", encoding="utf-8")
    for col in ("a", "b"):
        r = db.apply_patch(str(f), ops=[{"op": "round_values", "column": col, "decimals": 0}])
        assert r["success"] is True, r.get("error")
    return f


class TestTheDefaultChoiceIsDeclared:
    def test_there_really_are_several_to_choose_from(self, versioned: Path) -> None:
        """With one snapshot there would be no choice to disclose."""
        r = db.restore_version(str(versioned))
        assert len(r["available_versions"]) >= 2, r["available_versions"]

    def test_it_says_the_default_was_used(self, versioned: Path) -> None:
        r = db.restore_version(str(versioned))
        assert r["success"] is True, r.get("error")
        assert r["newest_by_default"] is True, r

    def test_the_hint_names_the_snapshot_it_wrote(self, versioned: Path) -> None:
        r = db.restore_version(str(versioned))
        assert "No timestamp was given" in r["hint"], r["hint"]
        assert Path(r["restored_from"]).name in r["hint"], r["hint"]

    def test_the_hint_says_how_to_pick_another(self, versioned: Path) -> None:
        assert "available_versions" in db.restore_version(str(versioned))["hint"]

    def test_a_progress_line_carries_it_too(self, versioned: Path) -> None:
        r = db.restore_version(str(versioned))
        msgs = " | ".join(str(p.get("message", "")) for p in r["progress"])
        assert "No timestamp given" in msgs, msgs

    def test_the_original_advice_survives(self, versioned: Path) -> None:
        assert "inspect_dataset()" in db.restore_version(str(versioned))["hint"]


class TestAnExplicitTimestampSaysNothingExtra:
    """The disclosure marks a defaulted choice, not every restore."""

    def _newest_stamp(self, versioned: Path) -> str:
        versions = db.restore_version(str(versioned))["available_versions"]
        return versions[0].split("_")[-1].replace(".csv.bak", "")

    def test_not_flagged_as_defaulted(self, versioned: Path) -> None:
        stamp = self._newest_stamp(versioned)
        r = db.restore_version(str(versioned), timestamp=stamp)
        assert r["success"] is True, r.get("error")
        assert r["newest_by_default"] is False, r

    def test_no_extra_hint(self, versioned: Path) -> None:
        stamp = self._newest_stamp(versioned)
        r = db.restore_version(str(versioned), timestamp=stamp)
        assert r["hint"] == "Call inspect_dataset() to confirm the restored state."

    def test_no_progress_warning(self, versioned: Path) -> None:
        stamp = self._newest_stamp(versioned)
        r = db.restore_version(str(versioned), timestamp=stamp)
        msgs = " | ".join(str(p.get("message", "")) for p in r["progress"])
        assert "No timestamp given" not in msgs, msgs


class TestTheBehaviourItselfIsUnchanged:
    """Callers rely on restore-to-newest, including the sweep that found this."""

    def test_it_still_restores_and_is_still_undoable(self, versioned: Path) -> None:
        before = versioned.read_text(encoding="utf-8")
        r = db.restore_version(str(versioned))
        assert r["success"] is True, r.get("error")
        assert versioned.read_text(encoding="utf-8") != before, "nothing was restored"
        # The counter-snapshot of the pre-restore state is what makes it safe.
        assert any("Counter-snapshot" in str(p.get("message", "")) for p in r["progress"]), r["progress"]

    def test_a_bad_timestamp_still_refuses_and_lists(self, versioned: Path) -> None:
        r = db.restore_version(str(versioned), timestamp="not-a-timestamp")
        assert r["success"] is False
        assert r["available_versions"], r
