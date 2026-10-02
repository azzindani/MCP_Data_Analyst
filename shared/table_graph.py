"""Which tables of a folder relate to which, and a join that keeps the grain.

Real analysis rarely starts from one file: a shipment table points at drivers, trucks and routes,
and the question ("on-time rate by driver tenure") lives across them. This reads a set of tables
where they lie (DuckDB, in chunks) and answers three things without the caller having to know the
schema:

* which column of each table is its key (unique, no gaps);
* which columns of one table refer to the key of another, how completely (the share of rows whose
  value is missing from the parent) and how many to one (one-to-one, or many-to-one);
* how to join them from a chosen fact table: only along many-to-one and one-to-one edges, so the
  result has exactly one row per fact row (the grain is kept), and the tables that point *at* the
  fact -- one-to-many -- are named to be aggregated first rather than fanned out into it.

A relationship is proposed only when a name says so (the same name, the parent's name plus its key
column, or a role such as `origin_country` for `country`) AND the values agree: at least 90% of the
child's values exist in the parent. Values alone are never enough: small integers "match" every
table that has an id.
"""

from __future__ import annotations

import re
import time
from collections import Counter
from pathlib import Path
from typing import Any

from shared.sql_query import QueryRefused, _identifier, _quote, _view_sql

MAX_TABLES = 40
MIN_CONTAINMENT = 0.9
HIGH_CONTAINMENT = 0.999
MAX_JOIN_DEPTH = 3
MAX_AGGREGATED_COLUMNS = 5

_KEY_TYPES = ("VARCHAR", "UUID", "DATE", "TIMESTAMP")
_NUMERIC = ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "FLOAT", "DOUBLE", "DECIMAL", "UBIGINT", "UINTEGER")


def table_name(path: Path) -> str:
    """A SQL-safe table name from a file name: `Product Sales.csv` is `Product_Sales`."""
    return re.sub(r"\W+", "_", path.stem).strip("_") or "table"


def _singular(name: str) -> str:
    lowered = name.lower()
    if lowered.endswith("ies"):
        return lowered[:-3] + "y"
    if lowered.endswith("ses"):
        return lowered[:-2]
    return lowered[:-1] if lowered.endswith("s") else lowered


def _is_key_type(duckdb_type: str) -> bool:
    """Integers of any width, text, uuids, dates: what an identifier is stored as (not floats, not intervals)."""
    if "INTERVAL" in duckdb_type:
        return False
    return "INT" in duckdb_type or duckdb_type.startswith(_KEY_TYPES)


def _same_kind(a: str, b: str) -> bool:
    """Two column types a value of one can be looked up in the other: integers with integers, text with text."""
    if a == b:
        return True
    if "INT" in a and "INT" in b:
        return True
    return a.startswith("TIMESTAMP") and b.startswith("TIMESTAMP")


def _profile(con, tables: dict[str, Path]) -> dict[str, dict[str, Any]]:
    """Rows per table and, for each column a key could be, its distinct and non-null counts."""
    profile: dict[str, dict[str, Any]] = {}
    for name in tables:
        columns = [(row[0], row[1]) for row in con.execute(f"DESCRIBE {_identifier(name)}").fetchall()]
        keyable = [(c, t) for c, t in columns if _is_key_type(t)]
        select = ", ".join(f"count({_identifier(c)}), count(DISTINCT {_identifier(c)})" for c, _ in keyable)
        row = con.execute(f"SELECT count(*){', ' + select if select else ''} FROM {_identifier(name)}").fetchone()
        rows = row[0]
        info: dict[str, dict[str, Any]] = {c: {"type": t, "keyable": False, "unique": False} for c, t in columns}
        for i, (column, _type) in enumerate(keyable):
            present, distinct = row[1 + 2 * i], row[2 + 2 * i]
            info[column].update(
                keyable=True, nonnull=present, distinct=distinct, unique=bool(rows and distinct == rows == present)
            )
        profile[name] = {"rows": rows, "columns": info}
    return profile


def _names_agree(child_table: str, child_col: str, parent_table: str, parent_col: str) -> str:
    """Why these two columns may be the same thing, or "" -- the name has to say so."""
    c, p = child_col.lower(), parent_col.lower()
    if c == p:
        return "same name"
    if c == f"{_singular(parent_table)}_{p}" or c == f"{parent_table.lower()}_{p}":
        return f"{child_col} is {parent_table}'s {parent_col}"
    if p != "id" and c.endswith("_" + p):
        return f"{child_col} is a role of {parent_col}"
    return ""


def _owner_rank(parent_table: str, parent_col: str) -> int:
    """0 when the column is named for the table it is the key of (`load_id` in `loads`): its home."""
    p = parent_col.lower()
    return 0 if p in (f"{_singular(parent_table)}_id", "id", f"{parent_table.lower()}_id") else 1


def find_relationships(con, tables: dict[str, Path], profile: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for parent, pinfo in profile.items():
        for pcol, pmeta in pinfo["columns"].items():
            if not pmeta["unique"]:
                continue
            for child, cinfo in profile.items():
                if child == parent:
                    continue
                for ccol, cmeta in cinfo["columns"].items():
                    if not cmeta["keyable"] or not _same_kind(cmeta["type"], pmeta["type"]):
                        continue
                    why = _names_agree(child, ccol, parent, pcol)
                    if not why:
                        continue
                    present, orphans = con.execute(
                        f"SELECT count({_identifier(ccol)}), "
                        f"count(*) FILTER (WHERE {_identifier(ccol)} IS NOT NULL AND {_identifier(ccol)} NOT IN "
                        f"(SELECT {_identifier(pcol)} FROM {_identifier(parent)})) FROM {_identifier(child)}"
                    ).fetchone()
                    if not present:
                        continue
                    contained = 1 - orphans / present
                    if contained < MIN_CONTAINMENT:
                        continue
                    candidates.append(
                        {
                            "child": child,
                            "child_column": ccol,
                            "parent": parent,
                            "parent_column": pcol,
                            "cardinality": "one_to_one" if cmeta["unique"] else "many_to_one",
                            "orphan_rate": round(orphans / present, 4),
                            "null_rate": round(1 - present / max(cinfo["rows"], 1), 4),
                            "confidence": "high" if contained >= HIGH_CONTAINMENT and why == "same name" else "medium",
                            "why": f"{why}; {contained:.1%} of its values are in {parent}.{pcol}",
                            "_owner": _owner_rank(parent, pcol),
                        }
                    )
    # A column that is unique in two tables (one-to-one) matches both ways, and a third table's
    # `load_id` matches every table that holds one. Keep the table the column is the key OF.
    best: dict[tuple[str, str], int] = {}
    for c in candidates:
        key = (c["child"], c["child_column"])
        best[key] = min(best.get(key, 9), c["_owner"])
    kept = [c for c in candidates if c["_owner"] == best[(c["child"], c["child_column"])]]
    seen_pairs: set[frozenset] = set()
    final = []
    for c in sorted(
        kept, key=lambda c: (c["cardinality"] != "many_to_one", c["_owner"], c["child"], c["child_column"])
    ):
        if c["cardinality"] == "one_to_one":
            pair = frozenset({(c["child"], c["child_column"]), (c["parent"], c["parent_column"])})
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
        final.append({k: v for k, v in c.items() if k != "_owner"})
    return sorted(final, key=lambda c: (c["child"], c["parent"], c["child_column"]))


def primary_key(profile_entry: dict[str, Any], name: str) -> str:
    """The column that identifies a row: unique, and named for the table when one is."""
    unique = [c for c, m in profile_entry["columns"].items() if m["unique"]]
    if not unique:
        return ""
    unique.sort(key=lambda c: (_owner_rank(name, c), list(profile_entry["columns"]).index(c)))
    return unique[0]


def _numeric_columns(entry: dict[str, Any], skip: set[str]) -> list[str]:
    return [
        c
        for c, m in entry["columns"].items()
        if c not in skip
        and any(m["type"].startswith(n) for n in _NUMERIC)
        and not m["unique"]
        and not c.lower().endswith(("_id", "id", "_key", "_code"))
    ][:MAX_AGGREGATED_COLUMNS]


def join_plan(
    fact: str,
    profile: dict[str, dict[str, Any]],
    relationships: list[dict[str, Any]],
    *,
    aggregate_children: bool = False,
) -> dict[str, Any]:
    """SELECT one row per `fact` row, joined along every edge that cannot multiply rows."""
    if fact not in profile:
        raise QueryRefused(f"fact table {fact!r} is not among the tables: {', '.join(profile)}.")
    outgoing: dict[str, list[dict[str, Any]]] = {}
    incoming: dict[str, list[dict[str, Any]]] = {}
    for r in relationships:
        outgoing.setdefault(r["child"], []).append(r)
        incoming.setdefault(r["parent"], []).append(r)
        if r["cardinality"] == "one_to_one":  # a one-to-one edge reads both ways
            outgoing.setdefault(r["parent"], []).append(
                {
                    **r,
                    "child": r["parent"],
                    "child_column": r["parent_column"],
                    "parent": r["child"],
                    "parent_column": r["child_column"],
                }
            )

    nodes = [{"alias": "t0", "table": fact}]
    joins: list[str] = []
    selected = [f"t0.{_identifier(c)}" for c in profile[fact]["columns"]]
    reached: list[dict[str, Any]] = []
    used_columns = set(profile[fact]["columns"])
    joined_from: dict[str, str] = {}  # table -> the alias it was first joined from
    frontier = [("t0", fact, 0, (fact,))]
    while frontier:
        alias, current, depth, ancestors = frontier.pop(0)
        if depth >= MAX_JOIN_DEPTH:
            continue
        roles = Counter(r["parent"] for r in outgoing.get(current, []))
        for r in outgoing.get(current, []):
            target = r["parent"]
            # Never back to a table on the way here, and a table already joined from elsewhere says
            # the same thing again: it is joined twice only from one table by two columns (a role:
            # origin_country and destination_country are both `country`).
            if target in ancestors or joined_from.get(target, alias) != alias:
                continue
            joined_from[target] = alias
            new_alias = f"t{len(nodes)}"
            nodes.append({"alias": new_alias, "table": target})
            joins.append(
                f"LEFT JOIN {_identifier(target)} {new_alias} ON {alias}.{_identifier(r['child_column'])}"
                f" = {new_alias}.{_identifier(r['parent_column'])}"
            )
            for column in profile[target]["columns"]:
                if column == r["parent_column"]:
                    continue
                name = column
                if roles[target] > 1:  # the same table twice: every use says which role it plays
                    name = f"{r['child_column']}__{column}"
                elif name in used_columns:
                    name = f"{target}__{column}"
                used_columns.add(name)
                selected.append(f"{new_alias}.{_identifier(column)} AS {_identifier(name)}")
            reached.append({**r, "depth": depth + 1})
            frontier.append((new_alias, target, depth + 1, (*ancestors, target)))

    # What points AT the fact -- one-to-many -- would multiply its rows: named, not joined.
    children = []
    for r in incoming.get(fact, []):
        if r["cardinality"] != "many_to_one":
            continue
        children.append(
            {
                "table": r["child"],
                "through": f"{r['child']}.{r['child_column']} -> {fact}.{r['parent_column']}",
                "rows_per_fact_row": round(profile[r["child"]]["rows"] / max(profile[fact]["rows"], 1), 2),
            }
        )
    aggregated = []
    if aggregate_children:
        for child in children:
            rel = next(r for r in relationships if r["child"] == child["table"] and r["parent"] == fact)
            metrics = _numeric_columns(profile[child["table"]], {rel["child_column"]})
            alias = f"t{len(nodes) + len(aggregated)}"
            parts = [f"count(*) AS {_identifier(child['table'] + '__rows')}"]
            parts += [f"avg({_identifier(m)}) AS {_identifier(child['table'] + '__avg_' + m)}" for m in metrics]
            joins.append(
                f"LEFT JOIN (SELECT {_identifier(rel['child_column'])} AS k, {', '.join(parts)} "
                f"FROM {_identifier(child['table'])} GROUP BY 1) {alias} "
                f"ON t0.{_identifier(rel['parent_column'])} = {alias}.k"
            )
            for part_name in [child["table"] + "__rows"] + [child["table"] + "__avg_" + m for m in metrics]:
                selected.append(f"{alias}.{_identifier(part_name)}")
            aggregated.append(child["table"])
    sql = f"SELECT {', '.join(selected)}\nFROM {_identifier(fact)} t0\n" + "\n".join(joins)
    return {
        "fact": fact,
        "sql": sql.strip(),
        "tables_used": [n["table"] for n in nodes] + aggregated,
        "joined": [
            {
                "table": r["parent"],
                "on": f"{r['child']}.{r['child_column']} = {r['parent']}.{r['parent_column']}",
                "cardinality": r["cardinality"],
            }
            for r in reached
        ],
        "grain": f"one row per {fact} row: every join is many-to-one or one-to-one, so no row is multiplied",
        "to_aggregate_first": [c for c in children if c["table"] not in aggregated],
        "aggregated": aggregated,
    }


def relate(
    tables: dict[str, Path],
    *,
    memory_mb: int,
    threads: int,
    fact: str = "",
    aggregate_children: bool = False,
) -> dict[str, Any]:
    """Profile `tables`, find how they relate, and (with `fact`) plan the join."""
    import duckdb

    if len(tables) < 2:
        raise QueryRefused("Relating tables needs at least two (a folder with several csv/parquet/json files).")
    if len(tables) > MAX_TABLES:
        raise QueryRefused(f"{len(tables)} tables is more than the {MAX_TABLES} this relates at once; pass `tables`.")
    started = time.monotonic()
    con = duckdb.connect(":memory:")
    try:
        con.execute(f"SET threads={int(threads)}")
        con.execute(f"SET memory_limit={_quote(f'{memory_mb}MB')}")
        for name, path in tables.items():
            con.execute(_view_sql(name, path))
        profile = _profile(con, tables)
        relationships = find_relationships(con, tables, profile)
        plan = join_plan(fact, profile, relationships, aggregate_children=aggregate_children) if fact else None
    except duckdb.OutOfMemoryException as exc:
        raise QueryRefused(
            f"Relating these tables needed more than the {memory_mb:,} MB it was given; pass a larger memory_mb "
            "or relate fewer tables at once."
        ) from exc
    except duckdb.Error as exc:
        raise QueryRefused(f"A table could not be read: {str(exc).strip()}") from exc
    finally:
        con.close()
    related = {r["child"] for r in relationships} | {r["parent"] for r in relationships}
    return {
        "tables": [
            {
                "name": name,
                "file": tables[name].name,
                "rows": entry["rows"],
                "columns": len(entry["columns"]),
                "primary_key": primary_key(entry, name),
            }
            for name, entry in profile.items()
        ],
        "relationships": relationships,
        "unrelated_tables": [name for name in profile if name not in related],
        "join_plan": plan,
        "seconds": round(time.monotonic() - started, 2),
    }
