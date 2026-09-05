"""How much of a profile to compute, and saying which parts were skipped.

A user review put the cost plainly: nine HTML reports from one dataset, six of
them *exactly* 4.7 MB, a dashboard at 8.9 MB, and a model manifest around 1 MB
because it inlined a 28,000-entry encoding map. Nothing in that was wrong. It
was all of it, every time, whether the caller wanted a look or a document.

The review's own prescription, from its comparison against ydata-profiling:

    Opinionated defaults, exhaustive on demand. One call gives alerts + 5
    correlations + time-series mode; `minimal=True/False` controls depth.
    MCP: run_eda(file, mode="minimal|standard|full", sample_n, include={}).
    Small gets KBs, frontier gets full without new tools.

Three rules this module exists to keep:

**`standard` is exactly what the tool did before.** A depth control that quietly
changes the default answer is a behaviour change wearing a parameter's clothes.
Callers who pass nothing get what they got yesterday.

**A skipped section is named, not absent.** The failure mode of a `minimal` mode
is a response that looks complete and is not -- the same class of defect as a
`truncated` flag with no total. `sections_run` and `sections_skipped` say which
is which, so an agent can tell "no correlations were found" from "correlations
were not computed".

**Sampling is declared in the response that was sampled.** `sample_n` makes the
numbers estimates. A profile computed on 5,000 of 38,576 rows that does not say
so is worse than a slow one.
"""

from __future__ import annotations

from typing import Any

MODES: tuple[str, ...] = ("minimal", "standard", "full")

# Every optional part of a profile, and the modes that include it. `standard`
# reproduces the behaviour these tools had before the parameter existed.
_SECTIONS: dict[str, tuple[str, ...]] = {
    "column_summaries": ("minimal", "standard", "full"),
    "nulls": ("minimal", "standard", "full"),
    "quality_score": ("minimal", "standard", "full"),
    "correlations": ("standard", "full"),
    "outliers": ("standard", "full"),
    "alerts": ("standard", "full"),
    "spearman": ("standard", "full"),
    "charts": ("standard", "full"),
    "html": ("standard", "full"),
}

SECTIONS: tuple[str, ...] = tuple(_SECTIONS)


class UnknownMode(ValueError):
    """Raised for a mode outside MODES, with the valid list in the message."""


def resolve_mode(mode: str) -> str:
    """Normalise a mode name, or refuse with the three that exist."""
    got = (mode or "standard").strip().lower()
    if got not in MODES:
        raise UnknownMode(f"mode must be one of {', '.join(MODES)}; got {mode!r}")
    return got


class Depth:
    """Which sections to compute, and the record of that decision.

    `include` overrides the mode per section, in either direction: a caller who
    wants a minimal profile *plus* correlations passes
    `mode="minimal", include={"correlations": True}` rather than paying for a
    standard one. An unknown key in `include` is refused rather than ignored,
    because a silently dropped option is a caller believing they turned
    something on.
    """

    def __init__(self, mode: str = "standard", include: dict[str, bool] | None = None) -> None:
        self.mode = resolve_mode(mode)
        include = include or {}
        unknown = sorted(set(include) - set(_SECTIONS))
        if unknown:
            raise UnknownMode(
                f"include has unknown section(s): {', '.join(unknown)}. Valid: {', '.join(SECTIONS)}"
            )
        self.include = {k: bool(v) for k, v in include.items()}
        self._asked: set[str] = set()

    def wants(self, section: str) -> bool:
        """True if this section should be computed. Records the question."""
        if section not in _SECTIONS:
            raise UnknownMode(f"unknown section {section!r}. Valid: {', '.join(SECTIONS)}")
        self._asked.add(section)
        if section in self.include:
            return self.include[section]
        return self.mode in _SECTIONS[section]

    def report(self) -> dict[str, Any]:
        """The fields that go in the response, so a reader knows what it holds.

        Only sections the tool actually asked about are listed: a profiler that
        has no charts to draw should not advertise that it skipped them.
        """
        run = sorted(s for s in self._asked if self.wants(s))
        skipped = sorted(s for s in self._asked if not self.wants(s))
        out: dict[str, Any] = {"mode": self.mode, "sections_run": run}
        if skipped:
            out["sections_skipped"] = skipped
            out["depth_note"] = (
                f"{len(skipped)} section(s) were not computed at mode={self.mode!r}. "
                'Absent is not empty: pass mode="full" or include={"<section>": true} to compute them.'
            )
        return out


def sampled_frame(df, sample_n: int, random_state: int = 0):
    """Return `(frame, note)` — the frame to profile and what to say about it.

    `note` is empty when the whole frame is used. When it is not, it belongs in
    the response: numbers computed from 5,000 of 38,576 rows are estimates, and
    a profile that does not say so invites them to be quoted as counts.
    """
    total = len(df)
    if not sample_n or sample_n <= 0 or sample_n >= total:
        return df, {}
    frame = df.sample(n=sample_n, random_state=random_state)
    return frame, {
        "was_sampled": True,
        "sample_n": int(sample_n),
        "rows_total": int(total),
        "sample_note": (
            f"Statistics below were computed from {sample_n:,} of {total:,} rows "
            "and are estimates, not counts. Omit sample_n for exact figures."
        ),
    }
