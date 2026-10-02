"""Metrics: the numbers a dashboard reports, defined once and computed one way everywhere.

A metric is an aggregate formula -- ``sum(clicks) / sum(impressions)`` -- in the
language run_chain's group_by already speaks (shared/expr.py). The sweep's
dashboards charted each numeric column summed and nothing else: no CTR, no CPC,
no CPM, so "Facebook buys impressions 25x cheaper" was on no page. And a ratio
averaged row by row is not the ratio the business means: the CTR of a platform
is its clicks over its impressions, not the mean of every row's CTR. A ratio
metric here is a ratio of sums, whatever it is sliced by.

The page recomputes every metric for every filter, so each is compiled to a
small tree (``compile_metric``) that the page's renderer and ``evaluate_tree``
here both walk -- the same arithmetic in both places, so a KPI card and the
insight quoting it cannot disagree. The tree holds aggregates of columns,
+ - * /, numbers and what-if parameters. An aggregate over a row formula --
``sum(price * qty)`` -- is computed here into a hidden column the tree then
aggregates, so the page never evaluates a formula over rows.
"""

from __future__ import annotations

import ast
import re
import zlib
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from pandas.api.types import is_bool_dtype, is_numeric_dtype

from shared.expr import AGGREGATE_ALLOWED, FormulaError, _Evaluator, _parse, aggregate

# What the page can compute over the rows it holds. percentile, std, first and
# last need the row order or a second pass the page does not keep.
TREE_AGGREGATES = ("sum", "mean", "median", "min", "max", "count", "count_distinct", "count_if")
UNITS = ("number", "count", "currency", "percent", "ratio")
_PARAM = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")
_PARAM_IDENT = "__param_{}__"
MAX_METRICS = 40


class MetricError(ValueError):
    """A metric the dashboard cannot compute, said as the caller's problem to fix."""


@dataclass
class Metric:
    name: str
    formula: str
    unit: str = "number"
    better: str = ""  # "up", "down", or "" when neither direction is good news
    description: str = ""
    source: str = "spec"  # "column", "auto" or "spec"
    tree: dict = field(default_factory=dict)
    hidden: dict[str, pd.Series] = field(default_factory=dict)
    additive: bool = False  # a plain sum: its parts add up to its whole
    columns: list[str] = field(default_factory=list)  # the file's columns it reads

    def entry(self) -> dict[str, str]:
        """The metric as a spec writes it, so a saved spec defines every metric it names."""
        return {"formula": self.formula, "unit": self.unit, "better": self.better, "description": self.description}

    def doc(self) -> dict[str, Any]:
        """What the page and the response carry: never the hidden columns themselves."""
        return {
            "name": self.name,
            "formula": self.formula,
            "unit": self.unit,
            "better": self.better,
            "description": self.description,
            "source": self.source,
            "additive": self.additive,
            "tree": self.tree,
        }


# ---------------------------------------------------------------------------
# Compiling a formula to the tree the page evaluates
# ---------------------------------------------------------------------------


def _with_param_idents(formula: str, parameters: dict[str, float]) -> str:
    def swap(match: re.Match) -> str:
        name = match.group(1)
        if name not in parameters:
            known = ", ".join(sorted(parameters)) or "none"
            raise MetricError(f"${name} in {formula!r} is not a parameter of this page. Parameters: {known}.")
        return _PARAM_IDENT.format(name)

    return _PARAM.sub(swap, formula)


def compile_metric(
    name: str, formula: str, df: pd.DataFrame, parameters: dict[str, float] | None = None, slot: int = 0
) -> tuple[dict, dict[str, pd.Series], list[str]]:
    """The formula as a tree over columns, the hidden columns its row formulas need, and the columns it reads.

    Raises MetricError naming what the page cannot compute.
    """
    parameters = parameters or {}
    # The full language checks the formula first, so a typo or a bare column
    # gets expr's own message rather than one written here.
    defaults = _PARAM.sub(lambda m: repr(float(parameters.get(m.group(1), 0.0))), formula)
    try:
        aggregate(defaults, df)
    except FormulaError as exc:
        raise MetricError(f"metric {name!r}: {exc}") from None
    text = _with_param_idents(formula, parameters)
    try:
        tree, names = _parse(text, df, AGGREGATE_ALLOWED)
    except FormulaError as exc:
        raise MetricError(f"metric {name!r}: {exc}") from None
    params = {_PARAM_IDENT.format(p): p for p in parameters}
    hidden: dict[str, pd.Series] = {}
    rows = _Evaluator(df, names, text)

    def refuse(what: str) -> MetricError:
        return MetricError(
            f"metric {name!r} uses {what}, which a dashboard recomputes for no filter. A metric is "
            f"{', '.join(TREE_AGGREGATES)} of a column or a row formula, combined with + - * / and numbers, "
            "e.g. 'sum(clicks) / sum(impressions)'."
        )

    def visit(node: ast.AST) -> dict:
        match node:
            case ast.Constant(value=value) if isinstance(value, (int, float)) and not isinstance(value, bool):
                return {"k": float(value)}
            case ast.Name(id=ident) if ident in params:
                return {"param": params[ident]}
            case ast.BinOp(left=left, op=op, right=right):
                sym = {ast.Add: "+", ast.Sub: "-", ast.Mult: "*", ast.Div: "/"}.get(type(op))
                if sym is None:
                    raise refuse(type(op).__name__)
                return {"op": sym, "a": visit(left), "b": visit(right)}
            case ast.UnaryOp(op=ast.USub(), operand=operand):
                return {"op": "neg", "a": visit(operand)}
            case ast.UnaryOp(op=ast.UAdd(), operand=operand):
                return visit(operand)
            case ast.Call(func=ast.Name(id="abs"), args=[arg]) if not node.keywords:
                return {"op": "abs", "a": visit(arg)}
            case ast.Call(func=ast.Name(id=agg), args=args) if agg in TREE_AGGREGATES:
                if not args:
                    return {"agg": "count"}
                arg = args[0]
                if isinstance(arg, ast.Name) and names.get(arg.id, arg.id) in df.columns and agg != "count_if":
                    return {"agg": agg, "col": str(names.get(arg.id, arg.id))}
                values = rows.visit(arg)
                if not isinstance(values, pd.Series):
                    values = pd.Series([values] * len(df), index=df.index)
                col = f"__m{zlib.crc32(name.encode()):08x}_{len(hidden)}"  # named by its metric: two metrics never share one
                if agg == "count_if":
                    hidden[col] = values.fillna(False).astype(bool).astype(float)
                    return {"agg": "sum", "col": col}
                hidden[col] = pd.to_numeric(values, errors="coerce")
                return {"agg": agg, "col": col}
            case ast.Call(func=ast.Name(id=other)):
                raise refuse(f"{other}()")
            case _:
                raise refuse(type(node).__name__)

    read = [str(c) for c in dict.fromkeys(names.values()) if c in df.columns]
    return visit(tree.body), hidden, read


# ---------------------------------------------------------------------------
# Evaluating the tree -- the renderer's arithmetic, in pandas
# ---------------------------------------------------------------------------


def hidden_columns(metrics: Any) -> dict[str, pd.Series]:
    """The computed columns every metric needs on the page, by name. Two metrics owning one name is refused:
    the page would hold one column and both metrics would read it."""
    found: dict[str, pd.Series] = {}
    owner: dict[str, str] = {}
    for metric in metrics:
        for col, values in metric.hidden.items():
            if col in found:
                raise MetricError(f"metrics {owner[col]!r} and {metric.name!r} compute the same hidden column {col!r}")
            found[col], owner[col] = values, metric.name
    return found


def _numbers(values: pd.Series) -> pd.Series:
    return values if is_numeric_dtype(values) else pd.to_numeric(values, errors="coerce")


def _reduce(values: pd.Series, how: str) -> float:
    present = _numbers(values).dropna()
    if how == "count":
        return float(len(present))
    if how == "count_distinct":
        return float(present.nunique())
    if present.empty:
        return 0.0 if how == "sum" else float("nan")  # a sum of nothing is 0; the mean, median or extreme of nothing is nothing
    return float(getattr(present, how)())


def evaluate_tree(tree: dict, frame: pd.DataFrame, params: dict[str, float] | None = None) -> float:
    """One value of a compiled metric over `frame`, as the page computes it (NaN for x/0)."""
    params = params or {}
    if "k" in tree:
        return float(tree["k"])
    if "param" in tree:
        return float(params.get(tree["param"], 0.0))
    if "op" in tree:
        a = evaluate_tree(tree["a"], frame, params)
        if tree["op"] == "neg":
            return -a
        if tree["op"] == "abs":
            return abs(a)
        b = evaluate_tree(tree["b"], frame, params)
        if tree["op"] == "/":
            return a / b if b else float("nan")
        return {"+": a + b, "-": a - b, "*": a * b}[tree["op"]]
    if tree["agg"] == "count" and "col" not in tree:
        return float(len(frame))
    return _reduce(frame[tree["col"]], tree["agg"])


def _tree_columns(tree: dict) -> list[str]:
    if "col" in tree:
        return [tree["col"]]
    return [c for side in ("a", "b") if side in tree for c in _tree_columns(tree[side])]


def by_group(metric: Metric, frame: pd.DataFrame, by: list[str], params: dict[str, float] | None = None) -> pd.Series:
    """The metric per group of `by`, largest value first."""
    work = frame.assign(**metric.hidden) if metric.hidden else frame
    read = _tree_columns(metric.tree)
    # Only what the metric reads goes into each group, and it is made numeric once rather than once per group.
    work = work[list(dict.fromkeys([*by, *read]))].assign(**{c: _numbers(work[c]) for c in read if c not in by})
    out = {
        key if len(by) > 1 else key[0] if isinstance(key, tuple) else key: evaluate_tree(metric.tree, part, params)
        for key, part in work.groupby(by, dropna=True, sort=False)
    }
    series = pd.Series(out, dtype=float)
    return series.sort_values(ascending=False)


def value(metric: Metric, frame: pd.DataFrame, params: dict[str, float] | None = None) -> float:
    work = frame.assign(**metric.hidden) if metric.hidden else frame
    return evaluate_tree(metric.tree, work, params)


# ---------------------------------------------------------------------------
# Metrics a page gets without asking
# ---------------------------------------------------------------------------

# Column words, by the role they play in the ratios below. A column is matched
# by its name's words: `clicks` and `Clicks` exactly, `total_clicks` by a word;
# an exact match wins, so `clicks` is chosen over `link_clicks`.
ROLE_WORDS: dict[str, tuple[str, ...]] = {
    "clicks": ("clicks", "click"),
    "impressions": ("impressions", "impression", "impr", "views", "reach"),
    "spend": ("spend", "spends", "spent", "cost", "costs", "adspend", "ad_spend"),
    "conversions": ("conversions", "conversion", "conv", "purchases", "leads", "signups", "installs"),
    "revenue": ("revenue", "revenues", "sales", "income", "gmv", "turnover"),
    "profit": ("profit", "profits", "gross_profit", "margin_amount"),
    "orders": ("orders", "order_count", "transactions"),
    "quantity": ("quantity", "qty", "units", "units_sold"),
    "acquisitions": ("new_customers", "acquisitions", "new_users", "customers_acquired", "new_accounts", "new_signups"),
}

# (name, numerator role, denominator role, scale, unit, better, what it means)
AUTO_RATIOS: tuple[tuple[str, str, str, float, str, str, str], ...] = (
    ("CTR", "clicks", "impressions", 1.0, "percent", "up", "click-through rate: clicks per impression"),
    ("CPC", "spend", "clicks", 1.0, "currency", "down", "cost per click: spend over clicks"),
    ("CPM", "spend", "impressions", 1000.0, "currency", "down", "cost per thousand impressions"),
    ("CVR", "conversions", "clicks", 1.0, "percent", "up", "conversion rate: conversions per click"),
    ("CPA", "spend", "conversions", 1.0, "currency", "down", "cost per conversion"),
    ("CAC", "spend", "acquisitions", 1.0, "currency", "down", "customer acquisition cost: spend per new customer"),
    ("ROAS", "revenue", "spend", 1.0, "ratio", "up", "return on ad spend: revenue per unit of spend"),
    ("Margin", "profit", "revenue", 1.0, "percent", "up", "profit as a share of revenue"),
    ("AOV", "revenue", "orders", 1.0, "currency", "up", "average order value: revenue per order"),
    ("Price per unit", "revenue", "quantity", 1.0, "currency", "", "revenue per unit sold"),
)

_UNIT_WORDS: dict[str, tuple[str, ...]] = {
    "currency": ("spend", "spends", "spent", "cost", "costs", "revenue", "sales", "price", "amount", "income",
                 "profit", "budget", "gmv", "fee", "fees", "salary", "wage", "cpc", "cpm", "cpa", "value", "usd",
                 "eur", "idr", "gbp", "turnover"),
    "count": ("clicks", "click", "impressions", "impression", "views", "visits", "sessions", "users", "orders",
              "units", "quantity", "qty", "conversions", "leads", "installs", "count", "signups", "purchases"),
    "percent": ("rate", "pct", "percent", "percentage", "ctr", "cvr", "share", "ratio"),
}  # fmt: skip

# Down is good news for these, whatever else they are. Spend and cost are not
# among them: less spend is less activity as often as it is a saving, so a
# change in either is shown without a verdict.
_BETTER_DOWN = ("cpc", "cpm", "cpa", "cac", "churn", "returns", "refunds", "defects", "errors", "complaints", "latency",
                "bounce", "cancellations", "chargebacks")  # fmt: skip
# A 0/1 column whose "1" is bad news: its rate is better down. Any other flag has no good direction.
_BAD_FLAG = ("churn", "churned", "default", "defaulted", "fraud", "fraudulent", "cancel", "canceled", "cancelled",
             "canceled", "attrition", "bounce", "bounced", "refund", "refunded", "return", "returned", "defect",
             "defective", "complaint", "late", "delayed", "failure", "failed", "fail", "dropout", "spam", "risk")  # fmt: skip
_NEUTRAL = ("cost", "costs", "spend", "spends", "spent", "budget", "expenses")


def _words(name: str) -> list[str]:
    return [w for w in re.split(r"[^a-z0-9]+", str(name).lower()) if w]


def unit_of(name: str) -> str:
    """The unit a name says: a rate word first -- click_rate is a share, not a count of clicks."""
    words = set(_words(name))
    for unit in ("percent", "currency", "count"):
        if words & set(_UNIT_WORDS[unit]):
            return unit
    return "number"


# Up is good news for these. A column the name does not place in either list has no verdict: a fall in
# lead time, mileage or temperature is not "bad" and a rise in it is not "good", and a page that paints one
# red and the other green says what the author did not know.
_BETTER_UP = ("revenue", "sales", "profit", "income", "gmv", "margin", "roas", "ctr", "cvr", "clicks", "impressions",
              "conversions", "orders", "users", "sessions", "visits", "views", "customers", "rating", "ratings",
              "score", "satisfaction", "retention", "growth", "yield", "quantity", "units", "volume", "installs",
              "downloads", "subscribers", "followers", "attendance", "engagement")  # fmt: skip


def better_of(name: str) -> str:
    words = set(_words(name))
    if words & set(_BETTER_DOWN):
        return "down"
    if words & set(_NEUTRAL):
        return ""
    return "up" if words & set(_BETTER_UP) else ""


def match_role(role: str, columns: list[str]) -> str:
    """The column that plays `role`, or "": an exact name first, then a name containing the word."""
    words = ROLE_WORDS[role]
    exact = [c for c in columns if "_".join(_words(c)) in words]
    if exact:
        return exact[0]
    partial = [c for c in columns if set(_words(c)) & set(words)]
    return min(partial, key=lambda c: len(_words(c))) if partial else ""


def _formula_col(col: str) -> str:
    return col if str(col).isidentifier() else f"`{col}`"


_AGG_WORDS = {"sum": "total", "mean": "average", "median": "median", "min": "lowest", "max": "highest",
              "count": "count of", "count_distinct": "distinct"}  # fmt: skip


def column_metrics(
    df: pd.DataFrame,
    columns: list[str],
    additive: dict[str, bool],
    overrides: dict[str, str] | None = None,
    units: dict[str, str] | None = None,
) -> list[Metric]:
    """One metric per measure column: its sum where parts add up to the whole, else its mean.

    `overrides` is the caller's agg_overrides, column to aggregate, and wins.
    `units` is the plan's unit per column, read from its values as well as its name.
    """
    out = []
    for col in columns:
        agg = (overrides or {}).get(str(col)) or ("sum" if additive.get(col, True) else "mean")
        out.append(
            Metric(
                name=str(col),
                formula=f"{agg}({_formula_col(col)})",
                unit="count" if agg in ("count", "count_distinct") else (units or {}).get(str(col)) or unit_of(col),
                better=better_of(col),
                description=f"{_AGG_WORDS.get(agg, agg)} {col}",
                source="column",
                tree={"agg": agg, "col": str(col)},
                additive=agg == "sum",
                columns=[str(col)],
            )
        )
    return out


def auto_ratios(df: pd.DataFrame, measures: list[str], origin: dict[str, str] | None = None) -> list[Metric]:
    """The standard business ratios whose parts this data has, each a ratio of sums.

    `origin` is the dataset each blended column came from: a ratio takes both
    parts from one dataset where it can, so CPC is the ads file's spend over
    the ads file's clicks, not over the clicks a sales file counts.
    """
    out = []
    for name, num_role, den_role, scale, unit, better, meaning in AUTO_RATIOS:
        num = match_role(num_role, measures)
        same = [c for c in measures if origin and num and origin.get(c) == origin.get(num)]
        den = match_role(den_role, same) or match_role(den_role, measures)
        if not num or not den or num == den:
            continue
        if pd.to_numeric(df[den], errors="coerce").fillna(0).sum() == 0:
            continue
        formula = f"sum({_formula_col(num)}) / sum({_formula_col(den)})" + (f" * {scale:g}" if scale != 1 else "")
        tree: dict = {"op": "/", "a": {"agg": "sum", "col": str(num)}, "b": {"agg": "sum", "col": str(den)}}
        if scale != 1:
            tree = {"op": "*", "a": tree, "b": {"k": scale}}
        out.append(
            Metric(
                name=name,
                formula=formula,
                unit=unit,
                better=better,
                description=f"{meaning} ({formula})",
                source="auto",
                tree=tree,
                columns=[str(num), str(den)],
            )
        )
    return out


def flag_rates(df: pd.DataFrame, planned: dict[str, Any], taken: set[str] | None = None) -> list[Metric]:
    """The share of rows where each 0/1 column is 1: a default rate, a churn rate, a cancellation rate.

    A table with an outcome flag is almost always about that outcome, and what a reader wants is how it
    differs between segments -- "defaults are 2.4x higher for ..." -- which a rate lets the page say.
    Skipped when the flag is nearly constant: a rate that never moves has nothing to compare.
    """
    taken = set(taken or ())
    out = []
    for col, info in planned.get("columns", {}).items():
        if info.get("role") != "flag" or col not in df.columns:
            continue
        values = pd.to_numeric(df[col], errors="coerce").dropna()
        positives = int(values.sum())
        if len(values) < 50 or positives < 20 or positives > len(values) - 20:
            continue
        name = f"{col} rate"
        if name in taken:
            continue
        words = set(_words(col))
        better = "down" if words & set(_BAD_FLAG) else ""
        if is_bool_dtype(df[col]):
            # A True/False column is a dimension of an aggregated (cube) page, which has no row values to average:
            # its rate counts the Trues, as a rate of words does.
            entry = {
                "formula": f"count_if({_formula_col(col)}) / count()",
                "unit": "percent",
                "better": better,
                "description": f"share of rows where {col} is True",
            }
            out.extend(spec_metrics(df, {name: entry}, taken={*taken}))
            continue
        formula = f"mean({_formula_col(col)})"
        out.append(
            Metric(
                name=name,
                formula=formula,
                unit="percent",
                better=better,
                description=f"share of rows where {col} is 1 ({formula})",
                source="auto",
                tree={"agg": "mean", "col": str(col)},
                columns=[str(col)],
            )
        )
    return out


_YES = ("yes", "true", "y", "t", "positive")
_NO = ("no", "false", "n", "f", "negative")


def _yes_no(series: pd.Series) -> str:
    """The "yes" value of a two-valued text column that says yes and no ("Yes"/"No", "True"/"False", "Y"/"N"), else ""."""
    values = [str(v) for v in series.dropna().unique()]
    if len(values) != 2:
        return ""
    yes = [v for v in values if v.strip().lower() in _YES and v.isalnum()]
    no = [v for v in values if v.strip().lower() in _NO]
    return yes[0] if len(yes) == 1 and len(no) == 1 else ""


def _answered(df: pd.DataFrame, col: str) -> str:
    """The rows that answered, as a formula. `count(col)` of words counts no numbers, so it is rows or a hidden flag."""
    if bool(df[col].notna().all()):
        return "count()"
    return f"count_if(notnull({_formula_col(col)}))"


def text_flag_rates(df: pd.DataFrame, planned: dict[str, Any], taken: set[str] | None = None) -> list[Metric]:
    """flag_rates for the flags written in words: a `HeartDisease` column of Yes and No has a rate too."""
    taken = set(taken or ())
    entries: dict[str, dict[str, str]] = {}
    for col, info in planned.get("columns", {}).items():
        if info.get("role") != "dimension" or col not in df.columns or is_numeric_dtype(df[col]):
            continue
        yes = _yes_no(df[col])
        if not yes:
            continue
        positives = int((df[col] == yes).sum())
        if len(df) < 50 or positives < 20 or positives > int(df[col].notna().sum()) - 20:
            continue
        name = f"{col} rate"
        if name in taken:
            continue
        words = set(_words(col))
        entries[name] = {
            "formula": f'count_if({_formula_col(col)} == "{yes}") / {_answered(df, col)}',
            "unit": "percent",
            "better": "down" if words & set(_BAD_FLAG) else "",
            "description": f"share of rows where {col} is {yes}",
        }
    return spec_metrics(df, entries, taken={*taken}) if entries else []


def spec_metrics(
    df: pd.DataFrame, entries: Any, parameters: dict[str, float] | None = None, taken: set[str] | None = None
) -> list[Metric]:
    """The caller's metrics: {name: formula} or {name: {formula, unit, better, description}}."""
    if entries in (None, {}):
        return []
    if not isinstance(entries, dict):
        raise MetricError("metrics maps a name to a formula, e.g. {'CTR': 'sum(clicks) / sum(impressions)'}")
    if len(entries) > MAX_METRICS:
        raise MetricError(f"metrics holds {len(entries)}; a page takes at most {MAX_METRICS}")
    taken = set(taken or ())
    out = []
    for i, (name, entry) in enumerate(entries.items()):
        body = {"formula": entry} if isinstance(entry, str) else entry
        if not isinstance(body, dict) or not isinstance(body.get("formula"), str):
            raise MetricError(f"metric {name!r} needs a formula, e.g. 'sum(spend) / sum(clicks)'")
        extra = sorted(set(body) - {"formula", "unit", "better", "description"})
        if extra:
            raise MetricError(
                f"metric {name!r} has unknown key(s) {', '.join(extra)}; it takes formula, unit, better, description"
            )
        unit = body.get("unit") or unit_of(name)
        if unit not in UNITS:
            raise MetricError(f"metric {name!r} unit {unit!r}: use one of {', '.join(UNITS)}")
        better = body.get("better", better_of(name))
        if better not in ("up", "down", ""):
            raise MetricError(f"metric {name!r} better {better!r}: up, down, or '' for neither")
        if str(name) in df.columns and str(name) not in taken:
            raise MetricError(f"metric {name!r} is also a column of the file; give the metric another name")
        tree, hidden, read = compile_metric(str(name), body["formula"], df, parameters, slot=i)
        out.append(
            Metric(
                name=str(name),
                formula=body["formula"],
                unit=unit,
                better=better,
                description=str(body.get("description") or body["formula"]),
                source="spec",
                tree=tree,
                hidden=hidden,
                additive=_is_plain_sum(tree),
                columns=read,
            )
        )
    return out


def _is_plain_sum(tree: dict) -> bool:
    return tree.get("agg") == "sum" and "col" in tree


def parameters_of(entries: Any) -> dict[str, float]:
    """What-if parameters: {name: {default, min, max, step, label}} -> {name: default}."""
    if entries in (None, {}, []):
        return {}
    if not isinstance(entries, dict):
        raise MetricError(
            "parameters maps a name to {default, min, max, step, label}, e.g. {'growth': {'default': 0.1, 'min': 0, 'max': 0.5, 'step': 0.01}}"
        )
    out = {}
    for name, body in entries.items():
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(name)):
            raise MetricError(f"parameter {name!r}: a name of letters, digits and _, used as ${name} in a metric")
        if not isinstance(body, dict) or not all(
            isinstance(body.get(k), (int, float)) for k in ("default", "min", "max")
        ):
            raise MetricError(f"parameter {name!r} needs numbers default, min and max")
        if not body["min"] <= body["default"] <= body["max"]:
            raise MetricError(f"parameter {name!r}: default {body['default']} is outside {body['min']}..{body['max']}")
        out[str(name)] = float(body["default"])
    return out


def is_finite(x: Any) -> bool:
    try:
        return bool(np.isfinite(float(x)))
    except TypeError, ValueError:
        return False
