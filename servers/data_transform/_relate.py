"""relate_tables: which tables of a folder relate to which, and a join that keeps the grain."""

from __future__ import annotations

import logging
import sys
import zipfile
from pathlib import Path

_ROOT = str(Path(__file__).resolve().parents[2])
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from shared.file_utils import error_text, get_default_output_dir, hint_for_error, resolve_path
from shared.isolation import memory_budget_mb, worker_threads
from shared.progress import fail, info, ok
from shared.sql_query import FOLDER_SUFFIXES, QueryRefused, run_query
from shared.table_graph import MAX_TABLES, relate, table_name
from shared.version_control import snapshot_if_exists

logger = logging.getLogger(__name__)

MAX_RELATIONSHIPS_SHOWN = 100


def _refusal(error: str, hint: str, label: str) -> dict:
    return {
        "success": False,
        "op": "relate_tables",
        "error": error,
        "hint": hint,
        "progress": [fail(label, error.splitlines()[0][:200])],
        "token_estimate": 40,
    }


def _from_zip(archive: Path, progress: list[dict]) -> Path:
    """Unpack the tables of a zip beside it (in the output folder when one is set) and return the folder."""
    target = get_default_output_dir(str(archive)) / archive.stem
    target.mkdir(parents=True, exist_ok=True)
    written = 0
    with zipfile.ZipFile(archive) as zf:
        for member in zf.infolist():
            name = Path(member.filename).name  # a member's directories are dropped: no path can escape
            if member.is_dir() or Path(name).suffix.lower() not in FOLDER_SUFFIXES:
                continue
            out = target / name
            if out.exists() and out.stat().st_size == member.file_size:
                continue
            with zf.open(member) as src, out.open("wb") as dst:
                while chunk := src.read(1 << 20):
                    dst.write(chunk)
            written += 1
    progress.append(info("Unpacked the archive", f"{written} table file(s) into {target.name}/"))
    return target


def _collect(source: str, tables: dict[str, str] | None, progress: list[dict]) -> dict[str, Path]:
    found: dict[str, Path] = {}
    if source:
        path = resolve_path(source)
        if path.is_file() and path.suffix.lower() == ".zip":
            path = _from_zip(path, progress)
        if not path.is_dir():
            raise QueryRefused(f"source must be a folder of tables or a .zip of them, not {path.name!r}.")
        for file in sorted(path.iterdir()):
            if file.is_file() and file.suffix.lower() in FOLDER_SUFFIXES:
                name = table_name(file)
                if name in found:
                    raise QueryRefused(f"Two files are both the table {name!r}: {found[name].name} and {file.name}.")
                found[name] = file
    for name, location in (tables or {}).items():
        if name in found:
            raise QueryRefused(f"Table name {name!r} is used twice (once from source).")
        file = resolve_path(location)
        if not file.is_file():
            raise QueryRefused(f"Table {name!r}: file not found: {file.name}")
        found[name] = file
    return found


def relate_tables(
    source: str = "",
    tables: dict[str, str] | None = None,
    fact: str = "",
    aggregate_children: bool = False,
    output_path: str = "",
    memory_mb: int = 0,
) -> dict:
    progress: list[dict] = []
    try:
        found = _collect(source, tables, progress)
        if not found:
            return _refusal(
                "No tables to relate.",
                "Pass source (a folder of csv/parquet/json files, or a .zip of them) or tables ({name: path}).",
                "Nothing to relate",
            )
        if output_path and not fact:
            return _refusal(
                "output_path writes the joined table, which needs a fact table to join around.",
                "Pass fact (the table whose rows you want one of, e.g. the trips or the orders).",
                "output_path without fact",
            )
        if len(found) > MAX_TABLES:
            return _refusal(
                f"{len(found)} tables is more than the {MAX_TABLES} this relates at once.",
                "Pass `tables` with the ones you want.",
                "Too many tables",
            )
        budget = memory_mb if memory_mb > 0 else memory_budget_mb()
        threads = worker_threads()
        model = relate(found, memory_mb=budget, threads=threads, fact=fact, aggregate_children=aggregate_children)
        relationships = model["relationships"]
        progress.append(
            ok(
                f"Related {len(found)} tables",
                f"{len(relationships)} relationship(s) in {model['seconds']} s",
            )
        )

        notes: list[str] = []
        for r in relationships:
            if r["orphan_rate"] > 0:
                notes.append(
                    f"{r['orphan_rate']:.1%} of {r['child']}.{r['child_column']} has no match in "
                    f"{r['parent']}.{r['parent_column']}: a LEFT JOIN keeps those rows with empty parent columns."
                )
        for t in model["tables"]:
            if not t["primary_key"]:
                notes.append(f"{t['name']} has no single unique column: its rows are identified by a combination.")
        if model["unrelated_tables"]:
            notes.append(
                f"No relationship found for {', '.join(model['unrelated_tables'])}: nothing in its columns "
                "names another table's key. It can still be joined by hand with query_data."
            )

        result: dict = {
            "success": True,
            "op": "relate_tables",
            "tables": model["tables"],
            "relationships": relationships[:MAX_RELATIONSHIPS_SHOWN],
            "unrelated_tables": model["unrelated_tables"],
            "notes": notes[:20],
        }
        if len(relationships) > MAX_RELATIONSHIPS_SHOWN:
            result["relationships_total"] = len(relationships)
        plan = model["join_plan"]
        if plan is not None:
            result["join_plan"] = plan
            result["query_data_args"] = {
                "sql": plan["sql"],
                "tables": {n: str(found[n]) for n in plan["tables_used"] if n in found},
            }
            result["hint"] = (
                "join_plan.sql keeps one row per fact row. Run it with query_data (query_data_args is the call), "
                "or pass output_path here to write it; aggregate to_aggregate_first tables before joining them, "
                "or pass aggregate_children=true."
            )
        else:
            result["hint"] = "Pass fact (one of the tables) to get a join that keeps that table's rows as they are."

        if output_path and plan is not None:
            out = resolve_path(output_path)
            out.parent.mkdir(parents=True, exist_ok=True)
            backup = snapshot_if_exists(out)
            used = {n: found[n] for n in plan["tables_used"] if n in found}
            written = run_query(plan["sql"], tables=used, output=out, preview_rows=5, memory_mb=budget, threads=threads)
            fact_rows = next(t["rows"] for t in model["tables"] if t["name"] == fact)
            kept = written["rows_total"] == fact_rows
            result.update(
                {
                    "output_path": str(out),
                    "output_name": out.name,
                    "rows_written": written["rows_total"],
                    "columns_written": len(written["columns"]),
                    "grain_kept": kept,
                    "preview": written["rows"],
                }
            )
            if backup:
                result["backup"] = backup
            progress.append(
                ok(
                    "Joined table written",
                    f"{out.name}: {written['rows_total']:,} rows x {len(written['columns'])} columns",
                )
            )
            if not kept:
                result["success"] = False
                result["error"] = (
                    f"The join produced {written['rows_total']:,} rows from {fact_rows:,} {fact} rows: "
                    "a key that looked unique is not. The file was written; do not analyse it as one row per "
                    f"{fact}."
                )
        result["progress"] = progress
        result["token_estimate"] = len(str(result)) // 4
        return result
    except QueryRefused as exc:
        return _refusal(
            str(exc),
            "source is a folder (or .zip) of csv, tsv, parquet, json or jsonl files; tables maps names to files.",
            "Relate refused",
        )
    except Exception as exc:
        logger.exception("relate_tables error")
        return {
            "success": False,
            "op": "relate_tables",
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check the paths are absolute and the files readable."),
            "progress": [fail("Unexpected error", str(exc)[:200])],
            "token_estimate": 20,
        }
