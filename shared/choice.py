"""One table per closed set of dispatch values, and one refusal rendered from it.

`arg_alias.py` resolves a parameter NAME the caller reasonably guessed.
`value_alias.py` does the same for a filter OPERATOR. This is the third layer:
a parameter whose entire job is to select behaviour -- `method`, `mode`,
`action`, `agg_func`, `normalize` -- where the name is right and only the value
is wrong.

Round 28 probed all 55 such parameters in the fleet with a value they cannot
mean. Fifty refused and listed the legal set, which is the standard this module
exists to make automatic rather than remembered. Five did not, and the worst of
them was `check_outliers`:

    if method in ("iqr", "both"):
        ...
    if method in ("std", "both"):
        ...

No `else`. An unrecognised method ran neither branch and the tool returned
`success: True` with `columns_with_outliers: 0` on a column holding 2,178 of
them. Not a crash -- an answer, and a confident one.

The typo that reaches it is not hypothetical. `check_outliers` spells the
3-sigma scan `std`; its sibling `detect_anomalies`, in this same repo, spells
the identical statistic `zscore`. A caller who learns one and carries it to the
other was told the data was clean. So this module does two things at once:
it refuses what it cannot resolve, and it resolves the drift between siblings
rather than punishing a caller for it. Both spellings work at both tools now,
and `CANONICAL` is still what the tool switches on.

That is `value_alias.py`'s reasoning applied again, and deliberately not the
other option: renaming one sibling would fix the guess and break every existing
caller.
"""

from __future__ import annotations

import difflib
from collections.abc import Iterable, Mapping
from typing import Any, Final

# The 3-sigma scan, as spelled by the two tools that offer it. Neither name is
# removed; both resolve at both tools.
OUTLIER_METHODS: Final[tuple[str, ...]] = ("iqr", "std", "both")
OUTLIER_ALIASES: Final[dict[str, str]] = {
    "zscore": "std",
    "z_score": "std",
    "z-score": "std",
    "z": "std",
    "stdev": "std",
    "std_dev": "std",
    "standard_deviation": "std",
    "sigma": "std",
    "3sigma": "std",
    "iqr_and_std": "both",
    "all": "both",
}

# `detect_anomalies` switches on `zscore` where `check_outliers` switches on
# `std`, so the same table is rendered with the other canonical name.
ANOMALY_METHODS: Final[tuple[str, ...]] = ("iqr", "zscore", "both")
ANOMALY_ALIASES: Final[dict[str, str]] = {
    "std": "zscore",
    "stdev": "zscore",
    "std_dev": "zscore",
    "standard_deviation": "zscore",
    "z_score": "zscore",
    "z-score": "zscore",
    "z": "zscore",
    "sigma": "zscore",
    "all": "both",
}

# The aggregations. `compute_aggregations` accepted five of these and refused
# the rest by name; `pivot_table` validated nothing and let pandas raise, which
# arrived as "'typo' is not a valid function for 'DataFrameGroupBy' object"
# under a hint telling the caller to check file_path and their column names --
# neither of which was wrong. One table, both tools, same sentence.
AGG_FUNCS: Final[tuple[str, ...]] = (
    "count",
    "first",
    "last",
    "max",
    "mean",
    "median",
    "min",
    "nunique",
    "std",
    "sum",
    "var",
)
AGG_ALIASES: Final[dict[str, str]] = {
    "average": "mean",
    "avg": "mean",
    "total": "sum",
    "n": "count",
    "size": "count",
    "len": "count",
    "stdev": "std",
    "std_dev": "std",
    "variance": "var",
    "distinct": "nunique",
    "unique": "nunique",
    "maximum": "max",
    "minimum": "min",
}
# The ones that need a numeric column, so a caller aggregating text is told
# what happened rather than handed NaN.
NUMERIC_AGG_FUNCS: Final[frozenset[str]] = frozenset(
    {"sum", "mean", "median", "min", "max", "std", "var"}
)

# Correlation methods. `correlation_analysis` refuses an unknown one and names
# these three; `generate_correlation_heatmap` let pandas raise and paired the
# message with "Check file_path is absolute and the file is a valid CSV" -- a
# hint about the file, for a fault in the method.
CORRELATION_METHODS: Final[tuple[str, ...]] = ("pearson", "spearman", "kendall")
CORRELATION_ALIASES: Final[dict[str, str]] = {
    "rank": "spearman",
    "spearmans": "spearman",
    "rho": "spearman",
    "pearsons": "pearson",
    "linear": "pearson",
    "r": "pearson",
    "tau": "kendall",
    "kendalls": "kendall",
}

# pandas' crosstab normalisation. The falsy spellings mean "do not normalise",
# which is a real choice and distinct from an unknown one.
NORMALIZE_MODES: Final[tuple[str, ...]] = ("index", "columns", "all")
NORMALIZE_ALIASES: Final[dict[str, str]] = {
    "row": "index",
    "rows": "index",
    "col": "columns",
    "column": "columns",
    "both": "all",
    "true": "all",
}
_NORMALIZE_OFF: Final[frozenset[str]] = frozenset({"", "false", "none", "no", "off"})


class UnknownChoice(ValueError):
    """A dispatch value no spelling in the given table maps to."""


def render(allowed: Iterable[str]) -> str:
    return ", ".join(allowed)


def suggest(sent: str, allowed: Iterable[str], aliases: Mapping[str, str] | None = None) -> str | None:
    """The canonical value a misspelling probably meant, or None."""
    pool = list(allowed) + list(aliases or {})
    near = difflib.get_close_matches(str(sent).strip().lower(), pool, n=1, cutoff=0.6)
    if not near:
        return None
    return (aliases or {}).get(near[0], near[0])


def resolve(
    sent: Any,
    allowed: Iterable[str],
    *,
    field: str,
    aliases: Mapping[str, str] | None = None,
) -> str:
    """Canonical value for whatever the caller sent, or raise `UnknownChoice`.

    The message always carries the legal set, because the whole cost of this
    class of defect is a caller who cannot tell what would have worked.
    """
    allowed = tuple(allowed)
    raw = str(sent).strip()
    key = raw.lower()
    if key in allowed:
        return key
    if aliases and key in aliases:
        return aliases[key]

    parts = [f"Unknown {field} {raw!r}."]
    guess = suggest(raw, allowed, aliases)
    if guess:
        parts.append(f"Did you mean {guess!r}?")
    parts.append(f"Valid: {render(allowed)}.")
    raise UnknownChoice(" ".join(parts))


def refusal(op: str, exc: Exception, *, hint: str = "") -> dict[str, Any]:
    """The fleet's failure dict for an `UnknownChoice`, ready to return."""
    from shared.progress import fail

    message = str(exc)
    out: dict[str, Any] = {
        "success": False,
        "op": op,
        "error": message,
        # The message already names the legal set; a hint that repeats it wastes
        # tokens, so it either adds something or is the message itself.
        "hint": hint or message,
        "progress": [fail(f"Invalid {op} argument", message)],
    }
    out["token_estimate"] = len(str(out)) // 4
    return out


def normalize_mode(sent: Any, *, field: str = "normalize") -> str | bool:
    """`index` / `columns` / `all`, or False for the falsy spellings.

    Raises `UnknownChoice` for anything else. The old line --

        norm = normalize if normalize in ("index", "columns", "all") else False

    -- turned a typo into `False` silently and then echoed the caller's word
    back in the response, so a crosstab that was never normalised reported that
    it was.
    """
    if sent is None or sent is False:
        return False
    if sent is True:
        return "all"
    if str(sent).strip().lower() in _NORMALIZE_OFF:
        return False
    return resolve(sent, NORMALIZE_MODES, field=field, aliases=NORMALIZE_ALIASES)
