"""What each column has to do with the target, and what changed since last time.

The review took this from its sweetviz comparison and stated it plainly:

    Target-aware + compare, never vacuum: `analyze(df, target_feat)` +
    `compare(train,test)` side-by-side. MCP: every profiler takes
    `target_column="loan_status"` + `compare_to="chargedoff.csv"`.
    **EDA without target/comparison is a toy.**

That last sentence is the argument. A profile of 24 columns tells a reader what
is in each one. It does not tell them which three matter for the thing they came
to predict, or which four have moved since the model was trained -- and those
are the two questions that make a profile worth running rather than a set of
histograms worth scrolling.

**Association is measured per pair of dtypes, and the measure is named.** A
number without its measure invites comparison between things that are not
comparable: an AUC of 0.75 and a Cramer's V of 0.75 are not the same claim.
Every row here says which statistic produced it and what its range means.

**Drift needs a baseline, which is why `shared/quality.py` reported it as
`None`.** That module's comment says the component "becomes computable when
profilers take `compare_to`". This is that. PSI is the standard measure in
credit and churn work and the conventional reading is quoted with it, because a
drift number nobody can interpret is worse than none.

Everything is pandas-only: no scipy, no sklearn, no model fit. These run inside
a profiler that is meant to be the cheap call.
"""

from __future__ import annotations

import math
from typing import Any

import pandas as pd

# Below this many usable rows a statistic is noise wearing a number's clothes.
MIN_ROWS = 30

# Bins for the numeric drift measure. Ten is the convention PSI is quoted with.
PSI_BINS = 10

# The readings everyone in credit and churn uses, quoted so a caller does not
# have to look them up.
PSI_BANDS: tuple[tuple[float, str], ...] = (
    (0.25, "major shift -- retrain before trusting a model built on the baseline"),
    (0.10, "moderate shift -- worth investigating"),
    (0.0, "stable"),
)

# What a measure's number means, carried beside it so two different statistics
# are never silently compared.
MEASURES: dict[str, str] = {
    "auc": "rank AUC against a binary target; 0.5 is no association, 1.0 is perfect separation",
    "pearson_abs": "absolute Pearson correlation with a numeric target; 0 to 1",
    "correlation_ratio": "eta -- how much of a numeric target's variance the categories explain; 0 to 1",
    "cramers_v": "Cramer's V between two categoricals; 0 to 1",
}


def _is_numeric(s: pd.Series) -> bool:
    return pd.api.types.is_numeric_dtype(s) and not pd.api.types.is_bool_dtype(s)


def _binary_auc(values: pd.Series, positive: pd.Series) -> float | None:
    """Rank AUC, folded to >= 0.5 so direction does not change the strength."""
    mask = values.notna() & positive.notna()
    x = pd.to_numeric(values[mask], errors="coerce")
    y = positive[mask].astype(bool)
    ok = x.notna()
    x, y = x[ok], y[ok]
    n_pos, n_neg = int(y.sum()), int((~y).sum())
    if n_pos < MIN_ROWS or n_neg < MIN_ROWS:
        return None
    ranks = x.rank(method="average")
    auc = (ranks[y.to_numpy()].sum() - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg)
    return float(max(auc, 1.0 - auc))


def _correlation_ratio(categories: pd.Series, values: pd.Series) -> float | None:
    """Eta: the share of a numeric target's variance explained by the groups."""
    mask = categories.notna() & values.notna()
    cats, vals = categories[mask], pd.to_numeric(values[mask], errors="coerce")
    ok = vals.notna()
    cats, vals = cats[ok], vals[ok]
    if len(vals) < MIN_ROWS or cats.nunique() < 2:
        return None
    grand = vals.mean()
    between = sum(len(g) * (g.mean() - grand) ** 2 for _, g in vals.groupby(cats, observed=True))
    total = ((vals - grand) ** 2).sum()
    if total <= 0:
        return None
    return float(math.sqrt(max(0.0, min(1.0, between / total))))


def _cramers_v(a: pd.Series, b: pd.Series) -> float | None:
    """Cramer's V, without scipy: chi-square from the contingency table."""
    mask = a.notna() & b.notna()
    table = pd.crosstab(a[mask], b[mask])
    n = table.to_numpy().sum()
    if n < MIN_ROWS or min(table.shape) < 2:
        return None
    row_tot = table.sum(axis=1).to_numpy().reshape(-1, 1)
    col_tot = table.sum(axis=0).to_numpy().reshape(1, -1)
    expected = row_tot @ col_tot / n
    # No `option_context("mode.use_inf_as_na")` here: that option was removed in
    # pandas 3 and raised OptionError, which the caller's `except Exception`
    # turned into `strength: None` under the note "fewer than 30 usable rows" --
    # a crash wearing a plausible explanation. Guard the divide directly.
    # bool() because `.any()` on a numpy array is np.bool_, which pyright
    # will not accept as a conditional operand.
    if bool((expected <= 0).any()):
        return None
    chi2 = float((((table.to_numpy() - expected) ** 2) / expected).sum())
    denom = n * (min(table.shape) - 1)
    if denom <= 0:
        return None
    return float(math.sqrt(max(0.0, min(1.0, chi2 / denom))))


def _as_binary(target: pd.Series) -> pd.Series | None:
    values = target.dropna().unique()
    if len(values) != 2:
        return None
    return target == target.value_counts().index[-1]


def target_association(df: pd.DataFrame, target_column: str) -> list[dict[str, Any]]:
    """How strongly each column relates to the target, strongest first.

    Every row names its measure, because 0.75 means different things under
    different statistics and a ranked list invites exactly that comparison.
    Columns the measure cannot be computed for are returned with
    `strength: None` and a reason rather than dropped -- a column missing from
    a ranking reads as "unrelated", which is a claim nobody made.
    """
    if target_column not in df.columns:
        return []
    target = df[target_column]
    target_numeric = _is_numeric(target)
    positive = _as_binary(target)
    out: list[dict[str, Any]] = []

    for col in df.columns:
        if col == target_column:
            continue
        s = df[col]
        measure: str | None = None
        value: float | None = None
        failed = ""
        try:
            if _is_numeric(s) and positive is not None:
                measure, value = "auc", _binary_auc(s, positive)
            elif _is_numeric(s) and target_numeric:
                pair = df[[col, target_column]].dropna()
                if len(pair) >= MIN_ROWS:
                    r = pair[col].corr(pair[target_column])
                    # bool() because pd.isna is typed as returning an array for
                    # frame-shaped input; here it is one float.
                    measure, value = "pearson_abs", (None if bool(pd.isna(r)) else abs(float(r)))
            elif not _is_numeric(s) and target_numeric:
                measure, value = "correlation_ratio", _correlation_ratio(s, target)
            elif not _is_numeric(s) and not target_numeric:
                measure, value = "cramers_v", _cramers_v(s, target)
            elif _is_numeric(s) and not target_numeric:
                measure, value = "correlation_ratio", _correlation_ratio(target, s)
        except Exception as exc:
            # Named, not folded into the "not enough rows" note below. A crash
            # reported as a clean "not computable" is indistinguishable from a
            # column that genuinely could not be measured, and one of those is
            # a bug someone should see.
            failed = f"{type(exc).__name__}: {exc}"
            value = None

        row: dict[str, Any] = {"column": col, "measure": measure}
        if value is None:
            row["strength"] = None
            row["note"] = (
                f"measure raised {failed}" if failed
                else (
                    f"not computable: fewer than {MIN_ROWS} usable rows, a constant column, "
                    "or a dtype pairing with no measure here"
                )
            )
        else:
            row["strength"] = round(float(value), 4)
            row["measure_note"] = MEASURES.get(measure or "", "")
        out.append(row)

    out.sort(key=lambda r: (r["strength"] is None, -(r["strength"] or 0.0)))
    return out


# ---------------------------------------------------------------------------
# comparison against a baseline
# ---------------------------------------------------------------------------


def _psi(base: pd.Series, other: pd.Series, bins: int = PSI_BINS) -> float | None:
    """Population Stability Index over quantile bins of the baseline."""
    b = pd.to_numeric(base, errors="coerce").dropna()
    o = pd.to_numeric(other, errors="coerce").dropna()
    if len(b) < MIN_ROWS or len(o) < MIN_ROWS:
        return None
    try:
        edges = b.quantile([i / bins for i in range(bins + 1)]).drop_duplicates().to_list()
    except Exception:
        return None
    if len(edges) < 3:
        return None
    edges[0], edges[-1] = -math.inf, math.inf
    b_share = pd.cut(b, edges).value_counts(normalize=True, sort=False)
    o_share = pd.cut(o, edges).value_counts(normalize=True, sort=False).reindex(b_share.index).fillna(0.0)
    total = 0.0
    for expected, actual in zip(b_share.to_list(), o_share.to_list()):
        # A zero share makes the log undefined; the usual floor keeps one empty
        # bin from turning the whole index into infinity.
        e, a = max(float(expected), 1e-4), max(float(actual), 1e-4)
        total += (a - e) * math.log(a / e)
    return float(total)


def _total_variation(base: pd.Series, other: pd.Series) -> float | None:
    """Half the L1 distance between two category distributions; 0 to 1."""
    b = base.dropna().astype(str)
    o = other.dropna().astype(str)
    if len(b) < MIN_ROWS or len(o) < MIN_ROWS:
        return None
    bs = b.value_counts(normalize=True)
    os_ = o.value_counts(normalize=True)
    keys = set(bs.index) | set(os_.index)
    return float(sum(abs(float(bs.get(k, 0.0)) - float(os_.get(k, 0.0))) for k in keys) / 2)


def _psi_reading(value: float) -> str:
    for threshold, text in PSI_BANDS:
        if value >= threshold:
            return text
    return "stable"


def compare_frames(baseline: pd.DataFrame, current: pd.DataFrame) -> dict[str, Any]:
    """What changed between two versions of the same table.

    Returns the schema difference and a per-column drift measure. `drift_pct`
    is the share of shared columns that moved beyond the "moderate" band, which
    is what `shared/quality.py` takes as its fourth component -- the one it has
    been reporting as `None` with "pass compare_to to measure drift".
    """
    base_cols, cur_cols = list(baseline.columns), list(current.columns)
    shared = [c for c in base_cols if c in cur_cols]
    drift: list[dict[str, Any]] = []

    for col in shared:
        b, c = baseline[col], current[col]
        if _is_numeric(b) and _is_numeric(c):
            value, measure = _psi(b, c), "psi"
        else:
            value, measure = _total_variation(b, c), "total_variation"
        row: dict[str, Any] = {"column": col, "measure": measure}
        if value is None:
            row["drift"] = None
            row["note"] = f"not computable: fewer than {MIN_ROWS} usable rows on one side"
        else:
            row["drift"] = round(value, 4)
            row["reading"] = _psi_reading(value) if measure == "psi" else (
                "major shift" if value >= 0.25 else "moderate shift" if value >= 0.10 else "stable"
            )
        drift.append(row)

    drift.sort(key=lambda r: (r["drift"] is None, -(r["drift"] or 0.0)))
    measured = [r for r in drift if r["drift"] is not None]
    moved = [r for r in measured if r["drift"] >= 0.10]
    drift_pct = round(len(moved) / len(measured) * 100, 2) if measured else None

    return {
        "columns_added": [c for c in cur_cols if c not in base_cols],
        "columns_removed": [c for c in base_cols if c not in cur_cols],
        "columns_compared": len(shared),
        "rows_baseline": len(baseline),
        "rows_current": len(current),
        "drift": drift,
        "columns_drifted": len(moved),
        "drift_pct": drift_pct,
        "drift_note": (
            "drift_pct is the share of comparable columns past the moderate band (PSI or "
            "total-variation >= 0.10). Columns that could not be measured are listed with a reason "
            "rather than counted as stable."
        ),
    }
