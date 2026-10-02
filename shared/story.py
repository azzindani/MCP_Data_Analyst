"""A dashboard that answers first: a headline, the insights behind it, and the pages that prove them.

The sweep's dashboard opened on a 16-row data-quality alert wall, then drew
one chart per column pair -- six bars and three donuts of one 90/10 split, and
nowhere on the page the finding a director would act on: Facebook buys
impressions eighteen times cheaper than Google, and Google brings the clicks.
A director reads the answer first, then the evidence, and data quality last.

`build` plans that page from the analysis plan (shared/analysis_plan.py) and
the metrics (shared/metrics.py):

- insights, each a number, what it is compared with, and what it means --
  shares, efficiency gaps between segments, the last period against the one
  before, concentration, spikes, and the risks in the data itself -- ranked;
- a storyline of tabs: Summary (headline, KPIs with their change, insight
  cards, the trend), Drivers (the headline metric and the ratios by the
  segments that explain them), Segments (mix, Pareto, small multiples, a
  table with trends), Risks (what could mislead, what changed), Appendix (data
  quality, metric definitions, notes on the data).

The result is a spec in the dashboard's own vocabulary, so the page is drawn by
the same engine as any other and `customize_dashboard` can edit it panel by
panel.
"""

from __future__ import annotations

import math
import re
from typing import Any

import numpy as np
import pandas as pd

from shared.analysis_plan import ROWS_COLUMN
from shared.column_utils import agg_label
from shared.dashboard_spec import MAX_TEXT, MAX_TITLE, STYLE_TEXT
from shared.metrics import Metric, by_group, hidden_columns, value

MAX_INSIGHTS = 8
MIN_GROUP_SHARE = 0.05  # a segment under 5% of rows is too thin to headline
SAFE_PALETTE = ["#0072B2", "#E69F00", "#009E73", "#D55E00", "#56B4E9", "#CC79A7", "#F0E442", "#999999"]


# ---------------------------------------------------------------------------
# Numbers in words
# ---------------------------------------------------------------------------


def fmt(x: float, unit: str, currency: str = "") -> str:
    """A number as a reader expects it in its unit."""
    if x is None or not math.isfinite(float(x)):
        return "n/a"
    x = float(x)
    if unit == "percent":
        return f"{x * 100:.{2 if abs(x) < 0.1 else 1}f}%"
    if unit == "ratio":
        return f"{x:.2f}x"
    sign = "-" if x < 0 else ""
    a = abs(x)
    if a >= 1e9:
        body = f"{a / 1e9:.1f}B"
    elif a >= 1e6:
        body = f"{a / 1e6:.1f}M"
    elif a >= 1e4:
        body = f"{a / 1e3:.1f}K"
    elif unit == "count" or a >= 100:
        body = f"{a:,.0f}"
    else:
        body = f"{a:,.2f}"
    return f"{sign}{currency if unit == 'currency' else ''}{body}"


def times(a: float, b: float) -> str:
    r = max(a, b) / min(a, b) if min(a, b) > 0 else float("inf")
    return f"{r:.1f}x" if r < 10 else f"{r:.0f}x"


# ---------------------------------------------------------------------------
# Periods
# ---------------------------------------------------------------------------


def grain_for(planned: dict[str, Any]) -> dict[str, str]:
    """The period a story compares by, and the last one the data has completely."""
    g = planned.get("grain") or {}
    if not g.get("date") or not g.get("start") or not g.get("end"):
        return {}
    start, end = pd.Timestamp(g["start"]), pd.Timestamp(g["end"])
    span = (end - start).days
    grain = "month" if span >= 90 else "week" if span >= 21 else "day"
    if grain == "month":
        month_end = end + pd.offsets.MonthEnd(0)
        complete = (
            end if end.normalize() == month_end.normalize() else end - pd.offsets.MonthBegin(1) - pd.Timedelta(days=1)
        )
        label = complete.strftime("%Y-%m")
    elif grain == "week":
        monday = end - pd.Timedelta(days=end.weekday())
        complete = monday if end.weekday() == 6 else monday - pd.Timedelta(days=7)
        label = complete.strftime("%Y-%m-%d")
    else:
        label = end.strftime("%Y-%m-%d")
    return {"date": g["date"], "grain": grain, "complete": label}


def bucket(dates: pd.Series, grain: str) -> pd.Series:
    """The page's period labels, computed the same way (_bucket in _dash_ext.py)."""
    d = pd.to_datetime(dates, errors="coerce")
    if grain == "year":
        return d.dt.strftime("%Y")
    if grain == "month":
        return d.dt.strftime("%Y-%m")
    if grain == "quarter":
        return d.dt.year.astype("Int64").astype(str) + "-Q" + d.dt.quarter.astype("Int64").astype(str)
    if grain == "week":
        return (d - pd.to_timedelta(d.dt.weekday, unit="D")).dt.strftime("%Y-%m-%d")
    return d.dt.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Insights
# ---------------------------------------------------------------------------


def _insight(
    kind: str, headline: str, number: str, comparison: str, text: str, tone: str, score: float, page: str
) -> dict:
    return {
        "kind": kind,
        "headline": headline,
        "value": number,
        "comparison": comparison,
        "text": text,
        "tone": tone,
        "score": round(float(score), 4),
        "page": page,
    }


def _summed(planned: dict[str, Any], metrics: dict[str, Metric]) -> list[str]:
    """Measures whose parts add up to the whole, as the page aggregates them.

    An agg_overrides median of spend is no longer a total: a share or a Pareto
    of it would be a wrong number.
    """
    return [
        m
        for m in planned.get("measures", [])
        if planned["columns"][m].get("additive") and (m not in metrics or metrics[m].additive)
    ]


def noun(column: str) -> str:
    """What an identifier column counts: customer_id counts customers."""
    return re.sub(r"[\s_-]*(id|key|no|number|code|ref)$", "", str(column), flags=re.IGNORECASE) or str(column)


def cohorts(df: pd.DataFrame, entity: str, date: str, periods: int = 12) -> dict[str, Any]:
    """Each entity's first month, and the share of each month's newcomers active in each month after it."""
    when = pd.to_datetime(df[date], errors="coerce")
    if when.dt.tz is not None:
        when = when.dt.tz_localize(None)  # a month is a month in the file's own clock; Period drops the zone with a warning
    frame = pd.DataFrame({"id": df[entity].astype(str), "month": when.dt.to_period("M")})
    frame = frame.dropna().drop_duplicates()
    first = frame.groupby("id")["month"].min().rename("first")
    frame = frame.join(first, on="id")
    months = frame["month"].dt.year * 12 + frame["month"].dt.month
    frame["age"] = months - (frame["first"].dt.year * 12 + frame["first"].dt.month)
    active = frame.groupby(["first", "age"])["id"].nunique().unstack(fill_value=0).sort_index()
    last = frame["month"].max()
    out: dict[str, Any] = {"entity": entity, "noun": noun(entity), "grain": "month", "cohorts": [], "sizes": [], "matrix": []}
    for first_month, row in active.tail(periods).iterrows():
        size = int(row.get(0, 0))
        if not size:
            continue
        span = (last - first_month).n
        shares = [round(float(row.get(k, 0)) / size, 4) if k <= span else None for k in range(periods)]
        out["cohorts"].append(str(first_month))
        out["sizes"].append(size)
        out["matrix"].append(shares)
    month_one = [m[1] for m in out["matrix"] if len(m) > 1 and m[1] is not None]
    out["note"] = (
        f"Of each month's new {out['noun']}s, the share active again k months later"
        + (f"; after one month, {float(np.mean(month_one)):.0%} on average." if month_one else ".")
    )
    return out


def _entity(df: pd.DataFrame, planned: dict[str, Any]) -> str:
    """A customer or user identifier that recurs across periods: what cohorts are made of."""
    words = ("customer", "user", "client", "account", "member", "subscriber", "buyer", "player")
    g = planned.get("grain") or {}
    if not g.get("date"):
        return ""
    for col, info in planned["columns"].items():
        if info["role"] in ("id", "dimension") and any(w in col.lower() for w in words):
            per = df.groupby(df[col].astype(str))[g["date"]].nunique()
            if len(per) >= 20 and float(per.median()) >= 2:
                return col
    return ""


def _thin(frame: pd.DataFrame, dim: str) -> set:
    shares = frame[dim].astype(str).value_counts(normalize=True)
    return set(shares[shares < MIN_GROUP_SHARE].index)


_BARE_CODE = re.compile(r"^-?\d+(\.\d+)?$|^(true|false|yes|no|y|n|t|f)$", re.IGNORECASE)


def _segment(dim: str, value: str) -> str:
    """A segment's name as a sentence can use it: a bare code (1, False, Y) is `is_canceled = 1`, not "1"."""
    return f"{dim} = {value}" if _BARE_CODE.match(value.strip()) else value


MIN_RATE_GAP = 0.01  # a rate must differ by a percentage point between segments to be a finding
MIN_EFFECT = 0.2  # an average must differ by a fifth of the column's own spread


def _material(metric: Metric, frame: pd.DataFrame, a: float, b: float) -> bool:
    """Whether the gap between two segments is a finding: "11x higher" between 0.01 and 0.00 is not one."""
    gap = abs(a - b)
    if metric.unit == "percent":
        return gap >= MIN_RATE_GAP
    if metric.source == "column" and metric.columns and metric.columns[0] in frame.columns:
        spread = float(pd.to_numeric(frame[metric.columns[0]], errors="coerce").std())
        return bool(spread == spread and gap >= MIN_EFFECT * spread)
    return True


def _comparables(metrics: dict[str, Metric]) -> list[Metric]:
    """The metrics a story compares across segments and over time: the ratios first, then the average of each
    column that does not add up (so a table of ages and speeds has findings too: "X's average is 1.4x Y's")."""
    ratios = [m for m in metrics.values() if m.source in ("auto", "spec") and not m.additive]
    means = [m for m in metrics.values() if m.source == "column" and not m.additive]
    return ratios + means


def _segments(planned: dict[str, Any]) -> list[str]:
    """The dimensions a story splits by: a 0/1 outcome (default, churn, fraud) is what the rates measure, and
    "0 brings 50% of the amount" is no finding, so flags come last and only when nothing else is there."""
    dims = list(planned.get("dimensions", []))
    columns = planned.get("columns", {})
    named = [d for d in dims if columns.get(d, {}).get("role") != "flag"]
    return named or dims


def insights(df: pd.DataFrame, planned: dict[str, Any], metrics: dict[str, Metric], currency: str = "") -> list[dict]:
    """Findings a reader would act on, each a number, a comparison and what it means; best first."""
    found: list[dict] = []
    dims = _segments(planned)[:4]
    measures = _summed(planned, metrics)
    ratios = _comparables(metrics)
    work = df.copy()
    for col, vals in hidden_columns(metrics.values()).items():
        work[col] = vals
    # A placeholder is not a segment: "device" in the device column is a header
    # row, and it headlined as the cheapest device of all.
    for dim in dims:
        held = set(planned["columns"][dim].get("placeholders", {}))
        if held:
            work[dim] = work[dim].where(~work[dim].astype(str).isin(held))

    # Share: which segment brings most of the headline measure, against its share of another.
    for dim in dims[:2]:
        for i, meas in enumerate(measures[:2]):
            totals = work.groupby(work[dim].astype(str))[meas].sum()
            total = float(totals.sum())
            if total <= 0 or len(totals) < 2:
                continue
            top = str(totals.idxmax())
            label = _segment(dim, top)
            share = float(totals.max()) / total
            if share < 0.35:
                continue
            other = next((o for o in measures if o != meas), "")
            compare = ""
            gap = 0.0
            if other:
                other_totals = work.groupby(work[dim].astype(str))[other].sum()
                other_share = float(other_totals.get(top, 0.0)) / float(other_totals.sum() or 1)
                compare = f"of {meas}, from {other_share:.0%} of {other}"
                gap = abs(share - other_share)
            text = f"{label} is where most of the {meas} comes from" + (
                f", out of proportion to its {other}." if gap >= 0.15 else "."
            )
            found.append(
                _insight(
                    "share",
                    f"{label} brings {share:.0%} of {meas}",
                    f"{share:.0%}",
                    compare or f"of all {meas}",
                    text,
                    "info",
                    0.5 + gap + (0.1 if i == 0 else 0),
                    "Summary",
                )
            )

    # Efficiency: the same ratio, far apart between segments.
    for metric in ratios:
        for rank, dim in enumerate(dims[:3]):
            others = [
                d
                for d in dims
                if d != dim and d not in {h["child"] for h in planned.get("hierarchies", []) if h["parent"] == dim}
            ]
            thin = _thin(work, dim)
            per = by_group(metric, work, [dim])
            per = per.drop(labels=[t for t in thin if t in set(per.index)])
            per = per[np.isfinite(per) & (per > 0)]
            if len(per) < 2:
                continue
            hi, lo = str(per.idxmax()), str(per.idxmin())
            a, b = float(per.max()), float(per.min())
            if a / b < 1.5 or not _material(metric, work, a, b):
                continue
            best, worst = (lo, hi) if metric.better == "down" else (hi, lo)
            best, worst = _segment(dim, best), _segment(dim, worst)
            bv, wv = (b, a) if metric.better == "down" else (a, b)
            word = ("cheaper" if metric.unit == "currency" else "lower") if metric.better == "down" else "higher"  # a rate is not a price
            headline = f"{best} {metric.name} {fmt(bv, metric.unit, currency)} against {fmt(wv, metric.unit, currency)} for {worst}"
            text = (
                f"On {metric.name} ({metric.description.split(' (')[0]}), {best} is {times(a, b)} {word} than {worst}. "
                + (
                    f"Worth checking that it holds within each {others[0]} before acting on it."
                    if others
                    else "Worth checking that it holds over time before acting on it."
                )
            )
            found.append(
                _insight(
                    "efficiency",
                    headline,
                    times(a, b),
                    f"{metric.name}: {best} vs {worst}",
                    text,
                    "good" if metric.better else "info",  # no good direction: nothing is good news
                    0.6 + min(math.log(a / b), 3.0) / 3 - 0.15 * rank,
                    "Summary",
                )
            )

    # Change: the last complete period against the one before.
    g = grain_for(planned)
    if g:
        periods = bucket(work[g["date"]], g["grain"])
        for name in [*measures[:2], *[m.name for m in ratios[:2]]]:
            metric = metrics.get(name)
            if metric is None:
                continue
            labels = sorted(p for p in periods.dropna().unique() if p <= g["complete"])
            if len(labels) < 2:
                continue
            cur, prev = labels[-1], labels[-2]
            a = value(metric, work[periods == cur])
            b = value(metric, work[periods == prev])
            if not (math.isfinite(a) and math.isfinite(b)) or b == 0:
                continue
            change = (a - b) / abs(b)
            if abs(change) < 0.05:
                continue
            good = (change > 0) == (metric.better != "down")
            verb = "rose" if change > 0 else "fell"
            delta = f"{(a - b) * 100:+.1f} pp" if metric.unit == "percent" else f"{change:+.0%}"
            found.append(
                _insight(
                    "change",
                    f"{metric.name} {verb} {abs(change):.0%} in {cur}",
                    delta,
                    f"{fmt(a, metric.unit, currency)} in {cur}, {fmt(b, metric.unit, currency)} in {prev}",
                    f"{'Good news' if good else 'Worth a look'}: {metric.name} {verb} from {fmt(b, metric.unit, currency)} to {fmt(a, metric.unit, currency)} between the last two complete {g['grain']}s.",
                    "good" if good else "warn",
                    0.4 + min(abs(change), 1.0) * 0.5,
                    "Summary",
                )
            )
        if g["grain"] == "month":
            months = sorted(p for p in periods.dropna().unique() if p <= g["complete"])
            cur = months[-1] if months else ""
            prior = f"{int(cur[:4]) - 1}{cur[4:]}" if cur else ""
            if prior in months:
                for name in [*measures[:1], *[m.name for m in ratios[:1]]]:
                    metric = metrics.get(name)
                    if metric is None:
                        continue
                    a, b = value(metric, work[periods == cur]), value(metric, work[periods == prior])
                    if not (math.isfinite(a) and math.isfinite(b)) or b == 0:
                        continue
                    growth = (a - b) / abs(b)
                    good = (growth > 0) == (metric.better != "down")
                    found.append(
                        _insight(
                            "yoy",
                            f"{metric.name} {'up' if growth > 0 else 'down'} {abs(growth):.0%} year on year in {cur}",
                            f"{growth:+.0%}",
                            f"{fmt(a, metric.unit, currency)} in {cur}, {fmt(b, metric.unit, currency)} a year before",
                            f"Year on year, {metric.name} {'grew' if growth > 0 else 'shrank'} from "
                            f"{fmt(b, metric.unit, currency)} to {fmt(a, metric.unit, currency)}: the season is the same, "
                            "so the change is the business's.",
                            "good" if good else "warn",
                            0.5 + min(abs(growth), 1.0) * 0.4,
                            "Summary",
                        )
                    )
        # Spikes: a period far from the typical one.
        if measures:
            series = work.groupby(periods)[measures[0]].sum().sort_index()
            if len(series) >= 8 and series.std() > 0:
                z = (series - series.median()) / (1.4826 * (series - series.median()).abs().median() or series.std())
                peak = z.abs().idxmax()
                if abs(float(z[peak])) >= 4:
                    typical = float(series.median())
                    found.append(
                        _insight(
                            "spike",
                            f"{measures[0]} in {peak} was {times(float(series[peak]), typical)} a typical {g['grain']}",
                            fmt(float(series[peak]), planned["columns"][measures[0]].get("unit", "number"), currency),
                            f"typical {g['grain']}: {fmt(typical, planned['columns'][measures[0]].get('unit', 'number'), currency)}",
                            "One period this far from the rest is either an event worth naming or an error in the data.",
                            "warn",
                            0.45,
                            "Risks",
                        )
                    )

    # Concentration: a few segments make most of the total.
    for dim in planned.get("dimensions", []):
        levels = planned["columns"][dim]["levels"]
        if levels < 8 or not measures:
            continue
        totals = work.groupby(work[dim].astype(str))[measures[0]].sum().sort_values(ascending=False)
        if totals.sum() <= 0:
            continue
        cumulative = totals.cumsum() / totals.sum()
        k = int((cumulative < 0.8).sum()) + 1
        if k / len(totals) <= 0.3:
            found.append(
                _insight(
                    "concentration",
                    f"{k} of {len(totals)} {dim} values make 80% of {measures[0]}",
                    f"{k}/{len(totals)}",
                    f"80% of {measures[0]}",
                    f"The rest are a long tail; a change in the top {k} moves the total.",
                    "info",
                    0.35 + (0.3 - k / len(totals)),
                    "Segments",
                )
            )
        break

    # Risks in the data itself.
    for col, info in planned["columns"].items():
        share = info.get("placeholder_share", 0)
        if share >= 0.2:
            found.append(
                _insight(
                    "placeholder",
                    f"{col} is a placeholder in {share:.0%} of rows",
                    f"{share:.0%}",
                    f"of {col} is {', '.join(repr(k) for k in list(info.get('placeholders', {}))[:2])}",
                    f"Views split by {col} describe the minority of rows that fill it in; it is left out of the segment charts.",
                    "warn",
                    0.3 + share / 3,
                    "Risks",
                )
            )
    for group in planned.get("aliases", []):
        found.append(
            _insight(
                "alias",
                f"{', '.join(group)} are one split under {len(group)} names",
                str(len(group)),
                "identical groupings",
                f"Each is shown once, as {group[0]}; charting all of them repeats one fact {len(group)} times.",
                "info",
                0.2,
                "Risks",
            )
        )

    # Varied, not eight versions of one finding: a kind at most twice, and one
    # efficiency gap per metric -- its widest.
    found.sort(key=lambda f: -f["score"])
    seen: set[str] = set()
    per_kind: dict[str, int] = {}
    per_metric: set[str] = set()
    unique = []
    for f in found:
        if f["headline"] in seen or per_kind.get(f["kind"], 0) >= (3 if f["kind"] == "placeholder" else 2):
            continue
        if f["kind"] == "efficiency":
            name = f["comparison"].split(":")[0]
            if name in per_metric:
                continue
            per_metric.add(name)
        seen.add(f["headline"])
        per_kind[f["kind"]] = per_kind.get(f["kind"], 0) + 1
        unique.append(f)
    return unique[:MAX_INSIGHTS]


# ---------------------------------------------------------------------------
# The page
# ---------------------------------------------------------------------------


def build(
    df: pd.DataFrame, planned: dict[str, Any], metrics: dict[str, Metric], *, title: str, currency: str = ""
) -> dict[str, Any]:
    """A storyline spec: layout, tabs, filters and the insights it was built from."""
    found = insights(df, planned, metrics, currency)
    g = grain_for(planned)
    dims = _segments(planned)
    measures = _summed(planned, metrics) or planned.get("measures", [])
    ratios = [m.name for m in _comparables(metrics)]
    headline_metric = measures[0] if measures else (ratios[0] if ratios else "")
    layout: list[dict] = []
    tabs: dict[str, list[int]] = {"Summary": [], "Drivers": [], "Segments": [], "Risks": [], "Appendix": []}

    def add(tab: str, panel: dict) -> None:
        # The spec's limits: a long column name or a long segment label made a title, a comparison or a note over them.
        if isinstance(panel.get("title"), str) and len(panel["title"]) > MAX_TITLE:
            panel["title"] = panel["title"][: MAX_TITLE - 3].rstrip() + "..."
        if isinstance(panel.get("text"), str) and len(panel["text"]) > MAX_TEXT:
            panel["text"] = panel["text"][: MAX_TEXT - 3].rstrip() + "..."
        style = panel.get("style")
        if isinstance(style, dict) and isinstance(style.get("comparison"), str) and len(style["comparison"]) > STYLE_TEXT["comparison"]:
            style["comparison"] = style["comparison"][: STYLE_TEXT["comparison"] - 3].rstrip() + "..."
        tabs[tab].append(len(layout))
        layout.append(panel)

    def named(name: str) -> str:
        """A column's number says how it is aggregated -- "Avg revenue" is not a total; a metric is its name."""
        m = metrics.get(name)
        if name == ROWS_COLUMN:
            return "Rows"
        return f"{agg_label(m.tree['agg'])} {name}" if m is not None and m.source == "column" else name

    def agg_of(name: str) -> dict[str, str]:
        """The aggregate `named` promises, on the panel itself. A title that says "Avg" over a panel
        that carries no `agg` is drawn with the page's default, which is a sum: the hotel file's
        "Avg arrival_date_year" showed 240.7M."""
        m = metrics.get(name)
        return {"agg": m.tree["agg"]} if m is not None and m.source == "column" and m.tree.get("agg") else {}

    summary = [f for f in found if f["page"] == "Summary"]
    headline = (
        summary[0]["headline"] if summary else (found[0]["headline"] if found else f"{title}: {planned['rows']:,} rows")
    )
    span = f" from {planned['grain']['start']} to {planned['grain']['end']}" if planned.get("grain") else ""
    add(
        "Summary",
        {
            "chart": "markdown",
            "title": "The answer first",
            "text": f"### {headline}\n{planned['rows']:,} rows{span}. "
            + " ".join(f"**{f['headline']}.**" for f in summary[1:3]),
            "place": {"span": 12},
        },
    )

    # A KPI need not add up -- an average is a fine headline number -- so any measure may lead.
    kpi_names = list(dict.fromkeys([*planned.get("measures", [])[:2], *ratios[:2]]))[:4]
    for name in kpi_names:
        add(
            "Summary",
            {
                "chart": "kpi",
                "cols": {"value": name},
                **agg_of(name),
                "title": named(name),
                "place": {"span": 12 // max(len(kpi_names), 1)},
            },
        )
    for f in summary[:3]:
        add(
            "Summary",
            {
                "chart": "insight",
                "title": f["headline"],
                "text": f["text"],
                "style": {"tone": f["tone"], "comparison": f"{f['value']} · {f['comparison']}"},
                "place": {"span": 4},
            },
        )
    if g and headline_metric:
        add(
            "Summary",
            {
                "chart": "time_series",
                "cols": {"date": g["date"], "value": headline_metric},
                **agg_of(headline_metric),
                "title": f"{named(headline_metric)} by {g['grain']}",
                # A forecast once there is a trend to extend: six complete periods.
                "style": {"ma": 0, **({"forecast": 3} if bucket(df[g["date"]], g["grain"]).nunique() >= 7 else {})},
                "place": {"span": 12},
            },
        )

    primary = dims[0] if dims else ""
    # With no date to trend over the first page would hold no chart at all: it draws the headline's split instead.
    split_on_summary = bool(primary and headline_metric and not (g and headline_metric))
    if split_on_summary:
        add(
            "Summary",
            {
                "chart": "bar",
                "cols": {"category": primary, "value": headline_metric},
                **agg_of(headline_metric),
                "title": f"{named(headline_metric)} by {primary}",
                "style": {"orientation": "h", "other": True, "top_n": 10},
                "place": {"span": 12},
            },
        )
    if primary and headline_metric:
        if not split_on_summary:
            add(
                "Drivers",
                {
                    "chart": "bar",
                    "cols": {"category": primary, "value": headline_metric},
                    **agg_of(headline_metric),
                    "title": f"{named(headline_metric)} by {primary}",
                    "style": {"orientation": "h", "other": True, "top_n": 10},
                    "place": {"span": 6},
                },
            )
        for name in ratios[:3]:
            add(
                "Drivers",
                {
                    "chart": "bar",
                    "cols": {"category": primary, "value": name},
                    "title": f"{name} by {primary}",
                    "style": {"orientation": "h", "top_n": 10},
                    "place": {"span": 6},
                },
            )
    if len(planned.get("measures", [])) >= 2:
        x, y = planned["measures"][0], planned["measures"][1]
        add(
            "Drivers",
            {"chart": "scatter", "cols": {"x": x, "y": y}, "title": f"{x} against {y}", "place": {"span": 12}},
        )

    secondary = dims[1] if len(dims) > 1 else ""
    if primary and secondary and headline_metric in measures:
        add(
            "Segments",
            {
                "chart": "stacked_bar",
                "cols": {"category": primary, "group": secondary, "value": headline_metric},
                "title": f"Mix of {secondary} within each {primary}",
                "style": {"normalize": True},
                "place": {"span": 12},
            },
        )
    many = next((d for d in dims if planned["columns"][d]["levels"] >= 6), "")
    if many and headline_metric in measures:
        add(
            "Segments", {"chart": "pareto", "cols": {"category": many, "value": headline_metric}, "place": {"span": 12}}
        )
    if primary and g and headline_metric:
        add(
            "Segments",
            {
                "chart": "small_multiples",
                "cols": {"facet": primary, "value": headline_metric, "date": g["date"]},
                "place": {"span": 12},
            },
        )
    if primary and headline_metric:
        cols = {"category": primary, "value": headline_metric}
        if g:
            cols["date"] = g["date"]
        add(
            "Segments",
            {
                "chart": "table",
                "cols": cols,
                **agg_of(headline_metric),
                "title": f"{named(headline_metric)} by {primary}",
                "style": {"heat": True, "totals": True},
                "place": {"span": 12},
            },
        )

    who = _entity(df, planned)
    if who:
        add(
            "Segments",
            {
                "chart": "cohort",
                "cols": {"id": who, "date": g.get("date") or planned["grain"]["date"]},
                "title": f"Retention of {noun(who)}s by first month",
                "place": {"span": 12},
            },
        )
    risks = [f for f in found if f["page"] == "Risks"]
    for f in risks[:4]:
        add(
            "Risks",
            {
                "chart": "callout",
                "title": f["headline"],
                "text": f["text"],
                "style": {"tone": "warn" if f["tone"] == "warn" else "info"},
                "place": {"span": 6},
            },
        )
    if primary and g and headline_metric in measures:
        add(
            "Risks",
            {
                "chart": "waterfall",
                "cols": {"category": primary, "value": headline_metric, "date": g["date"]},
                "place": {"span": 12},
            },
        )
    if not tabs["Risks"]:
        add(
            "Risks",
            {
                "chart": "callout",
                "title": "Nothing flagged",
                "text": "No placeholders, aliases, spikes or large changes were found.",
                "place": {"span": 12},
            },
        )

    add("Appendix", {"chart": "quality", "title": "Data quality", "place": {"span": 12}})
    glossary = "\n".join(
        f"- **{m.name}** ({m.unit}{', lower is better' if m.better == 'down' else ''}): {m.description.replace('`', '')}"
        for m in metrics.values()
        if m.source in ("auto", "spec")
    )
    if glossary:
        add("Appendix", {"chart": "markdown", "title": "Metric definitions", "text": glossary, "place": {"span": 6}})
    notes = (
        "\n".join(f"- {n}" for n in planned.get("notes", [])) or "- No aliases, placeholders or hierarchies were found."
    )
    grain_note = (
        f"\n\nRows are at {planned['grain'].get('frequency')} grain, {planned['grain'].get('periods')} periods."
        if planned.get("grain")
        else ""
    )
    add("Appendix", {"chart": "markdown", "title": "About the data", "text": notes + grain_note, "place": {"span": 6}})

    tab_list = [{"name": name, "slots": slots} for name, slots in tabs.items() if slots]
    filters: list[Any] = [d for d in dims[:3] if planned["columns"][d]["levels"] <= 12]
    if g:
        filters.append({"column": g["date"], "control": "date_range"})
    return {
        "layout": layout,
        "tabs": tab_list,
        "filters": filters,
        "kpis": [],
        "insights": found,
        "headline": headline,
        "grain": g,
    }


# ---------------------------------------------------------------------------
# Several datasets: where the numbers came from
# ---------------------------------------------------------------------------


def add_panels(story: dict[str, Any], tab: str, panels: list[dict]) -> None:
    """Panels appended to a storyline's tab, the tab made if it has none."""
    entry = next((t for t in story["tabs"] if t["name"] == tab), None)
    if entry is None:
        entry = {"name": tab, "slots": []}
        story["tabs"].append(entry)
    for panel in panels:
        entry["slots"].append(len(story["layout"]))
        story["layout"].append(panel)


def _num(x: float) -> str:
    return fmt(x, "number")


def sources_markdown(joined: dict[str, Any]) -> str:
    """Each dataset, how they relate, and how they were blended, as the appendix says it."""
    lines = []
    for name, t in joined["datasets"].items():
        g = t.get("grain") or {}
        when = f", by {g['frequency']} from {g['start']} to {g['end']}" if g.get("frequency") else ""
        dup = f", {t['duplicate_rows']:,} duplicate rows" if t["duplicate_rows"] else ""
        lines.append(f"- **{name}**: {t['origin']}, {t['rows']:,} rows{when}{dup}")
    for r in joined["relationships"]:
        on = r["on"][0] if r["on"][0] == r["on"][1] else f"{r['on'][0]} = {r['on'][1]}"
        orphans = []
        if r["orphans_left_count"]:
            orphans.append(f"{r['orphans_left_count']} of {r['left']}'s not in {r['right']}")
        if r["orphans_right_count"]:
            orphans.append(f"{r['orphans_right_count']} of {r['right']}'s not in {r['left']}")
        lines.append(
            f"- {r['left']} and {r['right']} share **{on}** ({r['cardinality']}): "
            f"{r['match_left']:.0%} and {r['match_right']:.0%} of their values match"
            + (f"; {', '.join(orphans)}" if orphans else "")
        )
    b = joined["blend"]
    keys = ", ".join([*([b["date"]] if b["date"] else []), *b["on"]])
    lines.append(
        f"- Blended{' by ' + b['grain'] if b['grain'] else ''} on {keys}: each dataset's totals summed to that grain, "
        f"{b['rows']:,} rows."
        + (f" Not blended, as they do not add up: {', '.join(b['left_out'])}." if b["left_out"] else "")
    )
    return "\n".join(lines)


def reconciliation_callouts(joined: dict[str, Any]) -> list[dict]:
    """A measure two datasets both hold, and whether their totals agree."""
    out = []
    for r in joined["reconciliation"]:
        (a, ta), (b, tb) = list(r["totals"].items())[:2]
        pct = r["difference_pct"]
        agree = pct is not None and abs(pct) < 0.5
        gaps = r.get("largest_gaps") or []
        where = ""
        if gaps and not agree:
            first = gaps[0]
            where = " The largest gap is at " + ", ".join(f"{k} {v}" for k, v in first.items() if k != "difference")
            where += f" ({_num(first['difference'])})."
        out.append(
            {
                "chart": "callout",
                "title": f"{r['measure']}: {a} and {b} " + ("agree" if agree else "differ"),
                "text": f"{a} totals {_num(ta)}, {b} {_num(tb)}"
                + (f", {pct:+.1f}%." if pct is not None else ".")
                + where,
                "style": {"tone": "info" if agree else "warn"},
                "place": {"span": 6},
            }
        )
    return out


def add_sources(story: dict[str, Any], joined: dict[str, Any]) -> None:
    """The appendix says where the rows came from; Risks says where two sources disagree."""
    add_panels(
        story,
        "Appendix",
        [{"chart": "markdown", "title": "Sources", "text": sources_markdown(joined), "place": {"span": 12}}],
    )
    calls = reconciliation_callouts(joined)
    risks = next((t for t in story["tabs"] if t["name"] == "Risks"), None)
    quiet = [i for i in (risks or {}).get("slots", []) if story["layout"][i].get("title") == "Nothing flagged"]
    if quiet and any(c["style"]["tone"] == "warn" for c in calls):
        # Two sources that disagree are something flagged.
        story["layout"][quiet[0]] = calls.pop(0)
    add_panels(story, "Risks", calls)
