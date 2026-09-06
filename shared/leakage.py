"""Features that already know the answer.

A user review trained on a loan book and got 0.9628 accuracy. The top three
features by importance were `installment` (0.379), `total_payment` (0.298) and
`last_payment_date` (0.180). All three are recorded *after* the loan resolves.
The model was not predicting default; it was reading the repayment history of a
loan that had already defaulted or not. Its verdict:

    Tool is honest about importance -- agent must not ship without time-split +
    drop of id/member_id/total_payment. Suggest check_data_quality add a
    "possible leakage: post-outcome column" hint when target is loan_status.

Nothing in the response said any of that. `compare_models` reported three
models, ranked them, and named the features -- all true, and an agent with no
domain knowledge ships a 96% model that is worth nothing.

**Why the existing check could not catch it.** `leakage_warning` in ml_utils
fires at `_NEAR_PERFECT_SCORE = 0.999` and looks for a feature that *determines*
the target exactly. 0.9628 is nowhere near 0.999, and no single column here
determines the outcome -- the leak is statistical, not functional. A guard tuned
for "this is obviously impossible" misses "this is quietly meaningless", and the
second is the one that gets shipped.

**What is detectable without domain knowledge.** Not "this column is recorded
after the event" -- that is a fact about the world. But three of its shadows:

* one feature separating the classes almost as well as the whole model does;
* a feature whose *missingness* predicts the class, which is the signature of a
  field only populated for one outcome (`last_payment_date` is null exactly
  when nothing was ever repaid);
* a name from the vocabulary of post-outcome accounting.

The first two are evidence. The third is a hint and is labelled as one, because
a column called `total_payment` might be a budget rather than a settlement, and
a guess dressed as a finding is the thing this module exists to stop.

Everything here is a *suspect*, never a verdict. The response says what was
measured and what to do about it; it does not drop columns or refuse to train.
"""

from __future__ import annotations

import math
import re
from typing import Any

import pandas as pd

# A single feature that reaches this on its own is doing the model's whole job.
# Set from what an honest strong predictor looks like: real single-feature AUCs
# in credit and churn work sit in 0.65-0.80, and 0.90 is the point where the
# feature is better described as an encoding of the answer.
SINGLE_FEATURE_AUC = 0.90

# How much the outcome rate may differ between "this field is filled in" and
# "it is not" before the missingness itself is the signal. A field that is null
# for 90% of one class and 5% of the other is not missing at random.
MISSINGNESS_GAP = 0.35

# Both sides of a missingness split need enough rows for the gap to mean
# anything.
MIN_GROUP = 30

# The continuous-target analogue of SINGLE_FEATURE_AUC, and deliberately much
# higher than it. 0.90 is where an AUC stops being a predictor and starts being
# an encoding; a correlation of 0.90 is nowhere near that. Measured on this
# fleet's own ad data, `spends` ranks `clicks` at rho 0.923 and `impressions` at
# 0.839 -- both honest causal predictors of clicks, and a check that calls them
# leakage is a check nobody will read twice. At 0.98 a feature is a monotone
# transform of the target, which is the claim this signal is making.
SINGLE_FEATURE_RHO = 0.98

# A part of a whole. `link_clicks <= clicks` in 100% of rows because a link
# click IS a click. Containment is the evidence; the correlation gate only
# excludes a column of constants, which is trivially "contained" and means
# nothing.
#
# The gate takes the LARGER of Spearman and Pearson, and that is not a detail:
# `link_clicks` is zero in 89.7% of rows, so the ties crush its Spearman to
# 0.261 while its Pearson is 0.926. Requiring rank correlation here would have
# missed the one case this signal exists for.
CONTAINMENT_SHARE = 0.99
CONTAINMENT_RHO = 0.80

# Below this many distinct values a numeric target is a code, not a quantity.
MIN_DISTINCT_CONTINUOUS = 10

# Words that name something recorded after an outcome is known. A hint only.
_POST_OUTCOME = re.compile(
    r"(?:^|_)(?:total_payment|last_payment|recover|recovery|recoveries|settlement|settled|"
    r"chargeoff|charged_off|writeoff|write_off|collection|payoff|paid_off|closed_date|"
    r"resolution|outcome|final|actual|realized|realised)(?:_|$)",
    re.IGNORECASE,
)


def _binary_auc(values: pd.Series, positive: pd.Series) -> float | None:
    """Rank-based AUC of one numeric feature against a binary label.

    Mann-Whitney U over ranks, which needs no model and no split, and is
    symmetric: a feature that predicts the negative class perfectly is just as
    leaky, so the result is folded to >= 0.5.
    """
    mask = values.notna() & positive.notna()
    x = pd.to_numeric(values[mask], errors="coerce")
    y = positive[mask].astype(bool)
    ok = x.notna()
    x, y = x[ok], y[ok]
    n_pos, n_neg = int(y.sum()), int((~y).sum())
    if n_pos < MIN_GROUP or n_neg < MIN_GROUP:
        return None
    ranks = x.rank(method="average")
    auc = (ranks[y.to_numpy()].sum() - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg)
    return float(max(auc, 1.0 - auc))


def _missingness_gap(values: pd.Series, positive: pd.Series) -> tuple[float, float, float] | None:
    """(gap, rate_when_missing, rate_when_present), or None if not measurable."""
    missing = values.isna()
    n_missing, n_present = int(missing.sum()), int((~missing).sum())
    if n_missing < MIN_GROUP or n_present < MIN_GROUP:
        return None
    rate_missing = float(positive[missing].mean())
    rate_present = float(positive[~missing].mean())
    if math.isnan(rate_missing) or math.isnan(rate_present):
        return None
    return abs(rate_missing - rate_present), rate_missing, rate_present


def _rank_rho(values: pd.Series, target: pd.Series) -> float | None:
    """Spearman correlation of one feature against a continuous target.

    Rank-based for the same reason `_binary_auc` is: it needs no model, no
    split and no distributional assumption, and it is symmetric -- a feature
    that ranks the target perfectly backwards is just as leaky, so the result
    is folded to its absolute value.
    """
    numeric = pd.to_numeric(values, errors="coerce")
    mask = numeric.notna() & target.notna()
    if int(mask.sum()) < MIN_GROUP:
        return None
    rho = numeric[mask].corr(target[mask], method="spearman")
    if rho is None or math.isnan(float(rho)):
        return None
    return abs(float(rho))


def _linear_r(values: pd.Series, target: pd.Series) -> float | None:
    """Pearson, used only to keep a zero-inflated component from hiding in ties."""
    numeric = pd.to_numeric(values, errors="coerce")
    mask = numeric.notna() & target.notna()
    if int(mask.sum()) < MIN_GROUP:
        return None
    r = numeric[mask].corr(target[mask], method="pearson")
    if r is None or math.isnan(float(r)):
        return None
    return abs(float(r))


def _containment(values: pd.Series, target: pd.Series) -> float | None:
    """Share of rows where 0 <= feature <= target -- a part inside its whole.

    Correlation cannot tell a component from a strong predictor. Containment
    can: spend correlates with clicks and routinely exceeds it, while a subset
    of the target never does.
    """
    numeric = pd.to_numeric(values, errors="coerce")
    mask = numeric.notna() & target.notna()
    if int(mask.sum()) < MIN_GROUP:
        return None
    feature, whole = numeric[mask], target[mask]
    return float(((feature >= 0) & (feature <= whole)).mean())


def _as_continuous(target: pd.Series) -> pd.Series | None:
    """The target as numbers, when it is a continuous quantity rather than a label.

    A numeric column with only two distinct values is a binary label wearing an
    int dtype, and belongs to `_as_binary`; the cut at MIN_DISTINCT_CONTINUOUS
    keeps a small ordinal code (0-4 severity, say) out of a rank correlation
    that would read it as a measurement.
    """
    numeric = pd.to_numeric(target, errors="coerce")
    # to_numeric is declared as returning any of a dozen array-ish types --
    # scalar, Index, ExtensionArray -- because it accepts any of them. It gives
    # a Series for a Series, and narrowing that here is both what pyright needs
    # and the honest statement: anything else is not a column we can rank.
    if not isinstance(numeric, pd.Series):
        return None
    if numeric.notna().sum() < MIN_GROUP:
        return None
    if numeric.dropna().nunique() < MIN_DISTINCT_CONTINUOUS:
        return None
    return numeric


def _as_binary(target: pd.Series) -> pd.Series | None:
    """A boolean 'is the minority class' series, or None if not binary.

    Multiclass is not refused out of laziness: rank AUC and a rate gap are both
    two-class statistics, and pretending otherwise would produce a number that
    looks like evidence and is not.
    """
    values = target.dropna().unique()
    if len(values) != 2:
        return None
    counts = target.value_counts()
    minority = counts.index[-1]
    return target == minority


def leakage_suspects(
    df: pd.DataFrame,
    target_column: str,
    feature_cols: list[str],
    importances: dict[str, float] | None = None,
) -> list[dict[str, Any]]:
    """Features that may already contain the answer, with the evidence.

    Returns a list of `{feature, reason, evidence, confidence}`, strongest
    first. Empty when nothing is suspect. Never raises: a profiler that dies on
    an odd column is worse than one that reports nothing.
    """
    if target_column not in df.columns:
        return []
    positive = _as_binary(df[target_column])
    # Every signal above this line is a two-class statistic, so a regression
    # target reached the name regex and nothing else -- and that regex is credit
    # vocabulary, which no ad, traffic or revenue column will ever match. A
    # model predicting `clicks` scored r2 0.983 on a feature set containing
    # `link_clicks`, which is <= clicks in 100% of rows because a link click is
    # a click, and the check reported nothing. These two are the continuous
    # analogues: rank correlation for "this feature is the answer", containment
    # for "this feature is part of the answer".
    continuous = None if positive is not None else _as_continuous(df[target_column])
    suspects: list[dict[str, Any]] = []

    for col in feature_cols:
        if col == target_column or col not in df.columns:
            continue
        found: list[dict[str, Any]] = []

        if positive is not None:
            try:
                auc = _binary_auc(df[col], positive)
            except Exception:
                auc = None
            if auc is not None and auc >= SINGLE_FEATURE_AUC:
                found.append(
                    {
                        "reason": "alone_predicts_target",
                        "evidence": (
                            f"'{col}' alone separates the classes with AUC {auc:.3f}. A single "
                            "feature at this level is usually an encoding of the outcome rather "
                            "than a predictor of it."
                        ),
                        "auc": round(auc, 4),
                        "confidence": "high" if auc >= 0.97 else "medium",
                    }
                )
            try:
                gap = _missingness_gap(df[col], positive)
            except Exception:
                gap = None
            if gap is not None and gap[0] >= MISSINGNESS_GAP:
                diff, when_missing, when_present = gap
                found.append(
                    {
                        "reason": "missingness_tracks_target",
                        "evidence": (
                            f"'{col}' is null for {when_missing:.0%} of one class and "
                            f"{when_present:.0%} of the other (gap {diff:.0%}). A field populated "
                            "only for one outcome is recorded after that outcome is known."
                        ),
                        "missingness_gap": round(diff, 4),
                        "confidence": "high" if diff >= 0.6 else "medium",
                    }
                )

        if continuous is not None:
            try:
                rho = _rank_rho(df[col], continuous)
            except Exception:
                rho = None
            try:
                share = _containment(df[col], continuous)
            except Exception:
                share = None
            try:
                r_lin = _linear_r(df[col], continuous)
            except Exception:
                r_lin = None
            strength = max([v for v in (rho, r_lin) if v is not None], default=None)
            # Containment first: it is the stronger claim and the more specific
            # evidence, so it should be the reason the suspect is labelled with.
            if (
                share is not None
                and strength is not None
                and share >= CONTAINMENT_SHARE
                and strength >= CONTAINMENT_RHO
            ):
                found.append(
                    {
                        "reason": "component_of_target",
                        "evidence": (
                            f"'{col}' is between 0 and '{target_column}' in {share:.1%} of rows and "
                            f"tracks it at {strength:.3f}. A feature bounded by the target that moves "
                            "with it is usually a part of it, and a part cannot predict its whole."
                        ),
                        "containment": round(share, 4),
                        "rho": round(rho, 4) if rho is not None else None,
                        "r": round(r_lin, 4) if r_lin is not None else None,
                        "confidence": "high",
                    }
                )
            elif rho is not None and rho >= SINGLE_FEATURE_RHO:
                found.append(
                    {
                        "reason": "alone_predicts_target",
                        "evidence": (
                            f"'{col}' alone ranks '{target_column}' at Spearman rho {rho:.3f}. A single "
                            "feature at this level is usually an encoding of the outcome rather than a "
                            "predictor of it."
                        ),
                        "rho": round(rho, 4),
                        "confidence": "high" if rho >= 0.97 else "medium",
                    }
                )

        if _POST_OUTCOME.search(col):
            found.append(
                {
                    "reason": "post_outcome_name",
                    "evidence": (
                        f"'{col}' is named like a field recorded after the outcome. This is a "
                        "hint from the column name only -- nothing was measured -- so confirm it "
                        "against how the data was collected."
                    ),
                    "confidence": "hint",
                }
            )

        if not found:
            continue
        entry: dict[str, Any] = {"feature": col, "signals": found}
        if importances and col in importances:
            entry["importance"] = round(float(importances[col]), 4)
        # Strongest signal decides how the suspect is ranked and labelled.
        order = {"high": 0, "medium": 1, "hint": 2}
        found.sort(key=lambda f: order.get(f["confidence"], 3))
        entry["confidence"] = found[0]["confidence"]
        entry["reason"] = found[0]["reason"]
        suspects.append(entry)

    suspects.sort(
        key=lambda s: (
            {"high": 0, "medium": 1, "hint": 2}.get(s["confidence"], 3),
            -float(s.get("importance") or 0.0),
        )
    )
    return suspects


def leakage_note(suspects: list[dict[str, Any]], score: float | None = None) -> str:
    """One sentence for the response, or '' when nothing is suspect."""
    if not suspects:
        return ""
    measured = [s for s in suspects if s["confidence"] != "hint"]
    named = ", ".join(f"'{s['feature']}'" for s in suspects[:3])
    more = f" and {len(suspects) - 3} more" if len(suspects) > 3 else ""
    lead = f"Score {score:.4f} may not be real: " if score is not None else "Possible target leakage: "
    if measured:
        return (
            f"{lead}{named}{more} look like they already contain the outcome. "
            "Re-train without them, and split on time rather than at random, before trusting "
            "this number. leakage_suspects carries the evidence for each."
        )
    return (
        f"{lead}{named}{more} are named like post-outcome fields. Nothing was measured -- this "
        "is a name-based hint -- so check how those columns were collected."
    )


def split_provenance(
    test_size: float,
    random_state: int | None,
    stratified: bool = False,
    cv_folds: int | None = None,
    time_ordered: bool = False,
    calibration: str = "",
) -> dict[str, Any]:
    """How the evaluation was set up, for the manifest.

    A score is a claim about unseen data, and the claim is only as good as the
    split that produced it. The review asked for split, seed, CV and
    calibration in the manifest for exactly this reason: a 0.9628 from a random
    split of time-ordered rows is not the same number as a 0.9628 from a
    forward-chained one, and nothing in the manifest let a reader tell.

    `calibration` is the fourth of those, and it is reported as `"none"` rather
    than omitted. An absent key reads as "not applicable"; the truth is that no
    trainer here calibrates, so a probability from these models is a decision
    function's output and not a probability of anything. A reader thresholding
    at 0.5 deserves to be told that, and the day a trainer does calibrate it
    passes `"sigmoid"` or `"isotonic"` and the field stops being a caveat.
    """
    out: dict[str, Any] = {
        "test_size": test_size,
        "random_state": random_state,
        "stratified": bool(stratified),
        "time_ordered_split": bool(time_ordered),
        "cv_folds": cv_folds,
        "calibration": calibration or "none",
    }
    if not calibration:
        out["calibration_note"] = (
            "Predicted probabilities are uncalibrated: they rank cases correctly but are not "
            "probabilities. Do not read 0.8 as an 80% chance, and do not pick a threshold from one."
        )
    if not time_ordered:
        out["split_note"] = (
            "Rows were split at random. If they are ordered in time, this lets the model "
            "learn from the future: hold out the most recent rows instead."
        )
    return out
