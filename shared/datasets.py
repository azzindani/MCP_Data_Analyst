"""Several files, one dashboard: each triaged, how they relate found, and one row set blended from them.

`sources=[...]` put each extra file on a tab of its own: totals and a paged
table, no charts, nothing joined, and a filter on one tab narrowed nothing on
another. A director asking "what did we earn on what we spent" needs the ads
file and the sales file on one page, and the ratio between them computed
right. `spec.datasets` names the files, and this module:

- loads each: a CSV, an .xlsx or .parquet file, a union of same-schema files
  (monthly exports), or the last table of a saved run_chain;
- triages each: its grain, its keys, its date coverage, its duplicates;
- relates them: the columns they share, by name or by values, with the share
  of each side's values the other has, the orphans, and the cardinality;
- blends them on a shared grain -- the keys every dataset has, and the date
  bucketed to the coarsest period any of them is kept at -- summing each
  dataset's additive measures to that grain and joining the sums, so a ratio
  across datasets (ROAS: sales revenue over ads spend) is a ratio of sums,
  and a filter on a shared key narrows every dataset's numbers at once;
- reconciles a measure two datasets both hold: their totals, the difference,
  and where along the keys it arises;
- records where each blended column came from, so every number on the page
  can say which files are behind it.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from itertools import combinations
from pathlib import Path
from typing import Any

import pandas as pd

from shared.analysis_plan import parsed_dates, plan

MAX_DATASETS = 8
MAX_UNION_FILES = 60
_NAME = re.compile(r"[A-Za-z][A-Za-z0-9_]{0,39}")
GRAINS = ("day", "week", "month", "quarter", "year")
_GRAIN_ORDER = {g: i for i, g in enumerate(GRAINS)}
_FREQUENCY_GRAIN = {"sub-day": "day", "day": "day", "week": "week", "month": "month", "quarter": "quarter", "year": "year"}
MIN_MATCH = 0.5  # a key is shared when at least half of one side's values are on the other


class DatasetError(ValueError):
    """A dataset the dashboard cannot use, said as the caller's problem to fix."""


@dataclass
class Dataset:
    name: str
    df: pd.DataFrame
    origin: str
    planned: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def _read(path: Path, sheet: Any, read_csv: Callable[[str], pd.DataFrame]) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in (".xlsx", ".xlsm", ".xls"):
        return pd.read_excel(path, sheet_name=sheet if sheet is not None else 0)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    return read_csv(str(path))


def load(
    name: str,
    entry: Any,
    resolve: Callable[[str], Path],
    read_csv: Callable[[str], pd.DataFrame],
    chain_table: Callable[[str, dict], pd.DataFrame] | None = None,
) -> Dataset:
    """One named dataset: a file, a union of files, or a saved chain's table."""
    body = {"path": entry} if isinstance(entry, str) else entry
    if not isinstance(body, dict):
        raise DatasetError(f"datasets.{name} is a file path or {{path}}, {{paths}} or {{chain, args}}")
    kinds = [k for k in ("path", "paths", "chain") if k in body]
    extra = sorted(set(body) - {"path", "paths", "chain", "args", "sheet"})
    if len(kinds) != 1 or extra:
        raise DatasetError(
            f"datasets.{name} takes one of path, paths or chain"
            + (f"; unknown key(s): {', '.join(extra)}" if extra else "")
        )
    try:
        if "path" in body:
            path = resolve(str(body["path"]))
            if not path.is_file():
                raise DatasetError(f"datasets.{name}: {path.name} was not found")
            df, origin = _read(path, body.get("sheet"), read_csv), path.name
        elif "paths" in body:
            df, origin = _union(name, body["paths"], resolve, read_csv)
        else:
            if chain_table is None:
                raise DatasetError(f"datasets.{name}: a chain cannot be read here")
            path = resolve(str(body["chain"]))
            df = chain_table(str(path), dict(body.get("args") or {}))
            origin = f"the chain {path.name}"
    except DatasetError:
        raise
    except Exception as exc:
        raise DatasetError(f"datasets.{name} could not be read: {exc}") from None
    if df.empty:
        raise DatasetError(f"datasets.{name} ({origin}) has no rows")
    df.columns = [str(c) for c in df.columns]
    planned = plan(df)
    return Dataset(name, parsed_dates(df, planned), origin, planned)


def _union(
    name: str, paths: Any, resolve: Callable[[str], Path], read_csv: Callable[[str], pd.DataFrame]
) -> tuple[pd.DataFrame, str]:
    """Same-schema files stacked, each row saying which file it came from."""
    if not isinstance(paths, list) or not 2 <= len(paths) <= MAX_UNION_FILES:
        raise DatasetError(f"datasets.{name}.paths is a list of 2 to {MAX_UNION_FILES} files with the same columns")
    frames, first = [], None
    for raw in paths:
        path = resolve(str(raw))
        if not path.is_file():
            raise DatasetError(f"datasets.{name}: {path.name} was not found")
        df = _read(path, None, read_csv)
        cols = [str(c) for c in df.columns]
        if first is None:
            first = (path.name, cols)
        elif set(cols) != set(first[1]):
            lacks = sorted(set(first[1]) - set(cols))
            adds = sorted(set(cols) - set(first[1]))
            raise DatasetError(
                f"datasets.{name}: {path.name} does not have {first[0]}'s columns"
                + (f"; it lacks {', '.join(lacks)}" if lacks else "")
                + (f"; it adds {', '.join(adds)}" if adds else "")
                + ". A union stacks files with the same columns."
            )
        frames.append(df.assign(**{"source file": path.name}))
    return pd.concat(frames, ignore_index=True), f"{len(frames)} files stacked"


# ---------------------------------------------------------------------------
# Triage
# ---------------------------------------------------------------------------


def triage(ds: Dataset) -> dict[str, Any]:
    """What a reader checks before trusting a file: its grain, keys, coverage and duplicates."""
    p, df = ds.planned, ds.df
    keys = [c for c, info in p["columns"].items() if info["role"] in ("id", "dimension") and df[c].is_unique]
    grain = p.get("grain") or {}
    out: dict[str, Any] = {
        "origin": ds.origin,
        "rows": len(df),
        "columns": len(df.columns),
        "measures": p["measures"],
        "dimensions": p["dimensions"],
        "unique_keys": keys,
        "duplicate_rows": int(df.duplicated().sum()),
        "missing_pct": round(float(df.isna().mean().mean() * 100), 2) if len(df.columns) else 0.0,
    }
    if grain:
        out["grain"] = {k: grain[k] for k in ("date", "frequency", "start", "end", "periods") if k in grain}
        out["one_row_per_period_and_segment"] = bool(grain.get("unique_rows"))
    return out


# ---------------------------------------------------------------------------
# Relationships
# ---------------------------------------------------------------------------


def _values(ds: Dataset, col: str) -> set[str]:
    s = ds.df[col].dropna()
    if pd.api.types.is_datetime64_any_dtype(s):
        return set(s.dt.strftime("%Y-%m-%d"))
    return set(s.astype(str).str.strip())


def _joinable(ds: Dataset, col: str) -> bool:
    return ds.planned["columns"].get(col, {}).get("role") in ("dimension", "id", "date", "flag")


def _pair(a: Dataset, ca: str, b: Dataset, cb: str) -> dict[str, Any] | None:
    va, vb = _values(a, ca), _values(b, cb)
    if not va or not vb:
        return None
    both = va & vb
    ua, ub = a.df[ca].dropna().is_unique, b.df[cb].dropna().is_unique
    cardinality = {
        (True, True): "one-to-one",
        (True, False): "one-to-many",
        (False, True): "many-to-one",
        (False, False): "many-to-many",
    }[(ua, ub)]
    return {
        "left": a.name,
        "right": b.name,
        "on": [ca, cb],
        "match_left": round(len(both) / len(va), 4),
        "match_right": round(len(both) / len(vb), 4),
        "orphans_left": sorted(va - vb)[:5],
        "orphans_left_count": len(va - vb),
        "orphans_right": sorted(vb - va)[:5],
        "orphans_right_count": len(vb - va),
        "cardinality": cardinality,
    }


def relate(datasets: list[Dataset]) -> list[dict[str, Any]]:
    """The columns two datasets share, by name or, failing that, by the values they hold."""
    found = []
    for a, b in combinations(datasets, 2):
        named = [c for c in a.df.columns if c in b.df.columns and _joinable(a, c) and _joinable(b, c)]
        for col in named:
            rel = _pair(a, col, b, col)
            if rel:
                found.append({**rel, "found_by": "name"})
        # Differently named columns holding the same values: region and area.
        taken_a, taken_b = set(named), set(named)
        dims_a = [c for c in a.planned["dimensions"] if c not in taken_a][:20]
        dims_b = [c for c in b.planned["dimensions"] if c not in taken_b][:20]
        for ca in dims_a:
            best = None
            for cb in dims_b:
                rel = _pair(a, ca, b, cb)
                # Most of the smaller side's values on the other: orphans on the larger side do not undo it.
                if rel and max(rel["match_left"], rel["match_right"]) >= 0.8 and len(_values(a, ca)) >= 2:
                    if best is None or rel["match_left"] + rel["match_right"] > best["match_left"] + best["match_right"]:
                        best = rel
            if best:
                found.append({**best, "found_by": "values"})
                dims_b.remove(best["on"][1])
    return found


# ---------------------------------------------------------------------------
# Blending
# ---------------------------------------------------------------------------


def _bucket(dates: pd.Series, grain: str) -> pd.Series:
    d = pd.to_datetime(dates, errors="coerce")
    if grain == "week":
        return (d - pd.to_timedelta(d.dt.weekday, unit="D")).dt.normalize()
    if grain == "month":
        return d.dt.to_period("M").dt.to_timestamp()
    if grain == "quarter":
        return d.dt.to_period("Q").dt.to_timestamp()
    if grain == "year":
        return d.dt.to_period("Y").dt.to_timestamp()
    return d.dt.normalize()


def blend(
    datasets: list[Dataset], relationships: list[dict[str, Any]], on: list[str] | None = None, grain: str = ""
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """One row per shared key and period, with every dataset's additive measures summed to it."""
    first = datasets[0]
    # A column matched by its values is renamed to the first dataset's name for it.
    renames: dict[str, dict[str, str]] = {ds.name: {} for ds in datasets}
    for rel in relationships:
        if rel["found_by"] == "values" and rel["left"] == first.name:
            renames[rel["right"]][rel["on"][1]] = rel["on"][0]

    def cols(ds: Dataset) -> set[str]:
        return {renames[ds.name].get(c, c) for c in ds.df.columns}

    dated = all(ds.planned.get("grain") for ds in datasets)
    date_col = first.planned["grain"]["date"] if dated else ""
    if on is None:
        shared = []
        for col in first.planned["dimensions"]:
            if col == date_col or not all(col in cols(ds) for ds in datasets[1:]):
                continue
            rels = [
                r
                for r in relationships
                if r["left"] == first.name and r["on"][0] == col and max(r["match_left"], r["match_right"]) >= MIN_MATCH
            ]
            if len(rels) == len(datasets) - 1:
                shared.append(col)
        keys = shared
    else:
        missing = [(c, ds.name) for c in on for ds in datasets if c not in cols(ds)]
        if missing:
            col, where = missing[0]
            raise DatasetError(f"blend.on names {col!r}, which datasets.{where} does not have")
        keys = [c for c in on if c != date_col]
    if not keys and not dated:
        raise DatasetError(
            "the datasets share no key and not all have dates, so nothing lines them up. "
            "Name the column they share in blend.on, or give each a date column."
        )
    if dated:
        coarsest = max(
            (_FREQUENCY_GRAIN.get(ds.planned["grain"].get("frequency") or "day", "day") for ds in datasets),
            key=lambda g: _GRAIN_ORDER[g],
        )
        if grain and _GRAIN_ORDER[grain] < _GRAIN_ORDER[coarsest]:
            raise DatasetError(f"blend.grain {grain!r} is finer than a dataset kept by {coarsest}; use {coarsest} or coarser")
        grain = grain or coarsest

    counts: dict[str, int] = {}
    for ds in datasets:
        for c in ds.planned["measures"]:
            counts[c] = counts.get(c, 0) + 1
    provenance: dict[str, dict[str, str]] = {}
    left_out: list[str] = []
    blended: pd.DataFrame | None = None
    group_by = ([date_col] if dated else []) + keys
    for ds in datasets:
        df = ds.df.rename(columns=renames[ds.name])
        if dated:
            df = df.assign(**{date_col: _bucket(df[ds.planned["grain"]["date"]], grain)})
        adds = [c for c in ds.planned["measures"] if ds.planned["columns"][c].get("additive")]
        left_out += [f"{ds.name}.{c}" for c in ds.planned["measures"] if c not in adds]
        named = {c: (f"{c} ({ds.name})" if counts[c] > 1 else c) for c in adds}
        for c, shown in named.items():
            provenance[shown] = {"dataset": ds.name, "column": c, "origin": ds.origin}
        part = df.groupby(group_by, dropna=False)[adds].sum(min_count=1).rename(columns=named).reset_index()
        blended = part if blended is None else blended.merge(part, on=group_by, how="outer")
    assert blended is not None
    measures = [c for c in blended.columns if c not in group_by]
    blended[measures] = blended[measures].fillna(0)
    blended = blended.sort_values(group_by).reset_index(drop=True)
    return blended, {
        "on": keys,
        "date": date_col,
        "grain": grain if dated else "",
        "columns": provenance,
        "left_out": left_out,
        "rows": len(blended),
    }


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------


def reconcile(blended: pd.DataFrame, info: dict[str, Any]) -> list[dict[str, Any]]:
    """A measure two datasets both hold: their totals, the difference, and where it arises."""
    by_column: dict[str, list[str]] = {}
    for shown, src in info["columns"].items():
        if src["column"]:
            by_column.setdefault(src["column"], []).append(shown)
    out = []
    keys = [c for c in [info["date"], *info["on"]] if c]
    for column, shown in by_column.items():
        if len(shown) < 2:
            continue
        a, b = shown[0], shown[1]
        ta, tb = float(blended[a].sum()), float(blended[b].sum())
        entry: dict[str, Any] = {
            "measure": column,
            "totals": {info["columns"][a]["dataset"]: ta, info["columns"][b]["dataset"]: tb},
            "difference": ta - tb,
            "difference_pct": round((ta - tb) / tb * 100, 2) if tb else None,
        }
        if keys:
            gap = (blended[a] - blended[b]).abs()
            worst = blended.loc[gap.nlargest(5).index]
            entry["largest_gaps"] = [
                {**{k: str(r[k].date()) if isinstance(r[k], pd.Timestamp) else str(r[k]) for k in keys}, "difference": float(r[a] - r[b])}
                for _, r in worst.iterrows()
                if r[a] != r[b]
            ]
        out.append(entry)
    return out


def names_ok(name: str) -> bool:
    return bool(_NAME.fullmatch(name))
