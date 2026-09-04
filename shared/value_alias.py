"""One table of filter operators, and the spellings a caller will actually send.

`shared/arg_alias.py` solved this one layer up: a parameter *name* the caller
would reasonably have guessed resolves to the name the tool declares. This is
the same problem for a parameter *value*, and it costs more, because an
operator is chosen fresh on every call while a parameter name is copied from
whatever worked last time.

Two findings sit behind this module.

**The `==` tax.** A user review of a 38,576-row session opened with it: the
first `filter_dataset` call sent `op: "=="` and was refused with
"Unknown filter op '=='. Valid: ... equals ...". The retry with `equals`
worked. Every model reaches for `==` first, because every language it was
trained on spells it that way, so this is one wasted turn per session, forever,
for a vocabulary difference that carries no meaning. Nothing is ambiguous about
`==` in a filter condition -- there is no second thing it could mean.

**The vocabulary had drifted, and nobody had noticed.** The set is written down
in three places, and the copies do not agree:

    servers/data_transform/engine.py   _FILTER_OPS   starts_with  ends_with  not_contains
    servers/data_medium/_med_inspect.py  if-chain    startswith   endswith   (absent)
    servers/data_medium/_med_inspect.py:938  hint    neither, and omits four more

So `starts_with` filters a dataset through data-transform and is refused by
data-medium; `startswith` does the reverse; `not_contains` exists in one server
only. A caller that learned the vocabulary from one tool is wrong at the next,
with nothing in either response to explain why. This is Round 14's lesson
arriving again -- the cause of a chain of defects is always a second table
whose copies drifted -- and the fix is the same one: one table, and every
message rendered from it rather than restating it.

Both spellings resolve here, and neither is removed. That is deliberate and
follows `arg_alias.py`'s own reasoning: renaming the outlier would fix the
guess and break every existing caller. `CANONICAL` is what a tool switches on;
what a caller sent is never what it switches on.
"""

from __future__ import annotations

import difflib
from collections.abc import Iterable
from typing import Final

# The vocabulary. Adding an operator means adding it here and nowhere else --
# every valid-ops sentence in the fleet renders from `render_valid()`.
CANONICAL: Final[tuple[str, ...]] = (
    "equals",
    "not_equals",
    "contains",
    "not_contains",
    "gt",
    "gte",
    "lt",
    "lte",
    "is_null",
    "not_null",
    "isin",
    "not_isin",
    "between",
    "date_range",
    "regex",
    "quantile_between",
    "starts_with",
    "ends_with",
)

# What a caller sends -> what the tool switches on.
#
# Three groups, and they are here for three different reasons:
#   symbols   -- what a model emits before it has read anything
#   spellings -- the drift between two servers, both of which shipped
#   words     -- ordinary English for the same idea
_ALIASES: Final[dict[str, str]] = {
    # symbols
    "==": "equals",
    "=": "equals",
    "!=": "not_equals",
    "<>": "not_equals",
    ">": "gt",
    ">=": "gte",
    "<": "lt",
    "<=": "lte",
    # the drift between data_transform and data_medium, both directions
    "startswith": "starts_with",
    "endswith": "ends_with",
    "not_contain": "not_contains",
    "doesnt_contain": "not_contains",
    # ordinary English
    "eq": "equals",
    "ne": "not_equals",
    "neq": "not_equals",
    "greater_than": "gt",
    "less_than": "lt",
    "greater_equal": "gte",
    "less_equal": "lte",
    "in": "isin",
    "not_in": "not_isin",
    "isnull": "is_null",
    "notnull": "not_null",
    "is_not_null": "not_null",
    "matches": "regex",
}

_CANONICAL_SET: Final[frozenset[str]] = frozenset(CANONICAL)


class UnknownOp(ValueError):
    """An operator no spelling in this module maps to a canonical name."""


def render_valid(allowed: Iterable[str] | None = None) -> str:
    """The valid-ops sentence, so no message writes its own copy.

    `allowed` narrows it for a tool that implements a subset -- `apply_patch`'s
    conditional labelling answers eight of these, and saying so is the honest
    message. The names still come from this table; only the selection differs.
    """
    ops = CANONICAL if allowed is None else tuple(o for o in CANONICAL if o in set(allowed))
    return ", ".join(ops)


def suggest(sent: str) -> str | None:
    """The canonical op a misspelling probably meant, or None."""
    near = difflib.get_close_matches(str(sent).strip().lower(), list(CANONICAL) + list(_ALIASES), n=1, cutoff=0.6)
    if not near:
        return None
    return _ALIASES.get(near[0], near[0])


def resolve(sent: object, *, field: str = "op", allowed: Iterable[str] | None = None) -> str:
    """Canonical operator for whatever the caller sent.

    Raises `UnknownOp` with the valid list and, where one exists, the
    `did_you_mean` that turns a dead end into a retry the caller can make
    without reading a guide.

    `allowed` is for a tool that implements a subset. An alias still resolves
    -- `==` means `equals` everywhere -- but a canonical op outside the subset
    is refused against the subset's own list, so the message never offers
    something the tool cannot do.
    """
    raw = str(sent).strip()
    key = raw.lower()
    permitted = _CANONICAL_SET if allowed is None else frozenset(allowed)

    canonical: str | None = None
    if key in _CANONICAL_SET:
        canonical = key
    elif key in _ALIASES:
        canonical = _ALIASES[key]
    if canonical is not None and canonical in permitted:
        return canonical

    parts = [f"Unknown {field} {raw!r}."]
    if canonical is not None:
        # Known name, wrong tool. Say which, or the caller retries the spelling.
        parts = [f"{field.capitalize()} {canonical!r} is not supported here."]
    else:
        guess = suggest(raw)
        if guess and guess in permitted:
            parts.append(f"Did you mean {guess!r}?")
    parts.append(f"Valid: {render_valid(permitted)}.")
    raise UnknownOp(" ".join(parts))


def is_known(sent: object) -> bool:
    """True when `resolve` would succeed. For validating without raising."""
    key = str(sent).strip().lower()
    return key in _CANONICAL_SET or key in _ALIASES
