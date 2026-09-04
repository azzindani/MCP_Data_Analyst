"""Data-quality judgement, in one place.

The EDA report has always led with a panel of alerts — constants, zero-inflated
columns, imbalance, skew, outliers, multicollinearity, duplicates — and it is the
most useful thing this repo produces: it says what is wrong with the data rather
than restating it.

That judgement lived inside the EDA page builder, so the interactive dashboard —
the artifact someone actually sends to a colleague — carried none of it. It
showed 26 charts of a dataset without mentioning that two of its columns were
constant and two more were 90% one value.

Same alerts, same wording, one implementation.
"""

from __future__ import annotations

from html import escape

import pandas as pd

from shared.quality import quality_score as _shared_quality_score

_SEVERITIES = ("error", "warning", "info")


def compute_alerts(
    df: pd.DataFrame,
    numeric_cols: list[str],
    cat_cols: list[str],
    corr_pairs: list[dict],
    rows: int,
    dup_count: int,
) -> list[dict]:
    """Return every data-quality alert for `df`, worst first in reading order.

    Alerts a single row cannot support are not raised. With one row every
    column holds exactly one unique value and its top category is 100% of the
    column, by arithmetic rather than by any property of the data -- and each
    CONSTANT alert costs 8 points, so a clean one-row file with no nulls and no
    duplicates scored 0/100 off 31 of them, under a panel reading "no issues".

    ml-medium's check_data_quality had the same defect and was fixed earlier
    the same day; this is its Data_Analyst counterpart, and the fix had not
    crossed repos. Both reports built on these alerts -- run_eda and
    generate_dashboard -- inherited it.
    """
    alerts: list[dict] = []

    # A column of nulls is genuinely empty whatever the row count, so that half
    # of the old `<= 1` test stays; the constant half needs a second row before
    # "constant" describes the data rather than the shape of the frame.
    for c in df.columns:
        n_unique = int(df[c].nunique(dropna=True))
        if n_unique == 0:
            alerts.append(
                {
                    "col": c,
                    "type": "ALL NULL",
                    "sev": "error",
                    "msg": f"'{c}' has no values at all — every row is null.",
                }
            )
        elif n_unique == 1 and rows > 1:
            alerts.append(
                {
                    "col": c,
                    "type": "CONSTANT",
                    "sev": "error",
                    "msg": f"'{c}' has only 1 unique value — constant, no predictive value.",
                }
            )

    for c in df.columns:
        null_pct = round(df[c].isna().mean() * 100, 1)
        if null_pct > 50:
            alerts.append(
                {
                    "col": c,
                    "type": "HIGH NULLS",
                    "sev": "error",
                    "msg": f"'{c}': {null_pct}% missing values — consider dropping.",
                }
            )
        elif null_pct > 20:
            alerts.append(
                {
                    "col": c,
                    "type": "HIGH NULLS",
                    "sev": "warning",
                    "msg": f"'{c}': {null_pct}% missing — imputation needed.",
                }
            )

    for c in numeric_cols:
        zero_pct = round((df[c] == 0).mean() * 100, 1)
        if zero_pct > 50:
            alerts.append(
                {
                    "col": c,
                    "type": "ZEROS",
                    "sev": "warning",
                    "msg": f"'{c}': {zero_pct}% zero values — zero-inflated distribution.",
                }
            )

    for c in cat_cols:
        uniq = df[c].nunique()
        if uniq > max(50, rows * 0.5):
            alerts.append(
                {
                    "col": c,
                    "type": "HIGH CARDINALITY",
                    "sev": "warning",
                    "msg": f"'{c}': {uniq:,} unique values — likely an ID, consider dropping.",
                }
            )

    for c in cat_cols:
        # `rows > 1` for the same reason as CONSTANT above: the top category of
        # a one-row column is 100% of it, always.
        if rows > 1 and df[c].notna().sum() > 0:
            top_pct = round(df[c].value_counts(normalize=True).iloc[0] * 100, 1)
            if top_pct > 90:
                alerts.append(
                    {
                        "col": c,
                        "type": "IMBALANCED",
                        "sev": "warning",
                        "msg": f"'{c}': top category = {top_pct}% of values — highly imbalanced.",
                    }
                )

    for c in numeric_cols:
        try:
            skew = round(float(df[c].skew()), 2)
        except TypeError, ValueError:
            continue
        if abs(skew) > 2:
            alerts.append(
                {
                    "col": c,
                    "type": "SKEWED",
                    "sev": "warning",
                    "msg": f"'{c}': skewness={skew:+.2f} — consider log/sqrt transform.",
                }
            )

    for c in numeric_cols:
        q1, q3 = df[c].quantile(0.25), df[c].quantile(0.75)
        iqr = q3 - q1
        count = int(((df[c] < q1 - 1.5 * iqr) | (df[c] > q3 + 1.5 * iqr)).sum())
        pct = round(count / max(rows, 1) * 100, 1)
        if pct > 10:
            alerts.append(
                {
                    "col": c,
                    "type": "OUTLIERS",
                    "sev": "warning",
                    "msg": f"'{c}': {count:,} outliers ({pct}%) — investigate or cap.",
                }
            )

    for pair in corr_pairs[:20]:
        if abs(pair["correlation"]) > 0.9:
            alerts.append(
                {
                    "col": pair["col_a"],
                    "type": "HIGH CORR",
                    "sev": "warning",
                    "msg": (
                        f"'{pair['col_a']}' ↔ '{pair['col_b']}': "
                        f"r={pair['correlation']:+.3f} — possible multicollinearity."
                    ),
                }
            )

    if dup_count > 0:
        dup_pct = round(dup_count / max(rows, 1) * 100, 1)
        alerts.append(
            {
                "col": None,
                "type": "DUPLICATES",
                "sev": "warning" if dup_pct < 5 else "error",
                "msg": f"{dup_count:,} duplicate rows ({dup_pct}%) — consider deduplication.",
            }
        )

    return alerts


def quality_score(null_pct: float, dup_pct: float, alerts: list[dict]) -> int:
    """Score the dataset the report is actually describing.

    Scored from nulls and duplicates alone, a frame whose nulls had been imputed
    and duplicates dropped came out near 100 -- directly above a panel reading
    "16 alerts, 2 serious" about constant columns, zero-inflation, skew and
    outliers. The headline contradicted the list under it, and the headline is
    the part people read.

    Alerts carry the findings the percentages cannot see, so they are priced in
    here: a serious one costs more than a warning, and the floor stays at 0.

    It lives beside compute_alerts, and not in either report, because the
    dashboard and the EDA report describe the same frames. When each kept its
    own formula they disagreed by 57 points on one dataset -- the dashboard said
    41, the EDA report said 98, from identical alerts. Outliers are deliberately
    not charged separately: they already arrive as alerts, and the EDA report's
    own outlier_penalty term was double-counting them.

    That reasoning held inside this repo and stopped at its edge.
    MCP_Machine_Learning kept a formula of its own, with different weights, a
    different severity vocabulary and capped terms, and the two scored one file
    77 and 53. Each side had already noticed a sibling disagreeing and fixed it
    locally -- twice, in two repos, without converging. So the arithmetic now
    lives in `shared/quality.py`, byte-identical in both, and this stays as the
    name eighteen call sites here already import.
    """
    return int(round(_shared_quality_score(null_pct, dup_pct, alerts)))


def alerts_html(alerts: list[dict]) -> str:
    """Render alerts as the badge panel the EDA report has always shown."""
    if not alerts:
        return (
            '<div class="alert-panel"><div class="alert-item info">'
            '<span class="alert-badge info">OK</span> No data quality alerts detected.</div></div>'
        )
    items = []
    for a in alerts:
        severity = a["sev"] if a["sev"] in _SEVERITIES else "info"
        items.append(
            f'<div class="alert-item {severity}">'
            f'<span class="alert-badge {severity}">{escape(str(a["type"]))}</span> '
            f"{escape(str(a['msg']))}</div>"
        )
    return f'<div class="alert-panel">{"".join(items)}</div>'


def alerts_for_frame(df: pd.DataFrame, numeric_cols: list[str], cat_cols: list[str]) -> list[dict]:
    """Compute alerts from a frame alone, working out what the fuller call needs.

    Lets a caller that has not already built a correlation table — the dashboard —
    get the same alerts without duplicating that preparation.
    """
    rows = len(df)
    try:
        dup_count = int(df.duplicated().sum())
    except TypeError, ValueError:
        dup_count = 0

    corr_pairs: list[dict] = []
    if len(numeric_cols) >= 2:
        try:
            corr = df[numeric_cols].corr(numeric_only=True)
            for i, a in enumerate(numeric_cols):
                for b in numeric_cols[i + 1 :]:
                    value = corr.loc[a, b]
                    if pd.notna(value):
                        corr_pairs.append({"col_a": a, "col_b": b, "correlation": float(value)})
            corr_pairs.sort(key=lambda p: abs(p["correlation"]), reverse=True)
        except TypeError, ValueError, KeyError:
            corr_pairs = []

    return compute_alerts(df, numeric_cols, cat_cols, corr_pairs, rows, dup_count)
