# CLAUDE.md — MCP Data Analyst

This file defines how Claude Code must approach this project. Read it before
touching any file. Every rule here derives from `STANDARDS.md` — this document
applies those rules specifically to the data analytics domain.

---

## Project Purpose

This repository is a **local-first MCP server for data analytics**. It gives a
language model structured, surgical access to CSV/tabular datasets through a
small set of deterministic tools — without ever sending data to a cloud API.

The reference workflow is captured in `Data_Analytic_Lookup.ipynb`. Every tool
in this server is the MCP-callable equivalent of a cell in that notebook.

---

## What This Is (and Is Not)

| This IS | This is NOT |
|---|---|
| A deterministic function executor | An AI agent or chat assistant |
| A local-only data tool (no API keys) | A cloud analytics wrapper |
| A server designed for constrained hardware | A high-memory enterprise pipeline |
| A structured API the model calls with JSON | A script runner or notebook executor |

The model provides intelligence — deciding what to call and with what arguments.
This server provides execution — doing the operation reliably and returning a
structured result. Never cross that boundary.

---

## Architecture

### Three-Tier Split

This project must be split into three independent servers. Never mix tiers.

| Tier | Directory | Purpose | Tool count |
|---|---|---|---|
| Basic | `servers/data_basic/` | Load, inspect, filter, patch, restore | 6–8 tools |
| Medium | `servers/data_medium/` | Profiling, cleaning pipelines, aggregations | 5–7 tools |
| Advanced | `servers/data_advanced/` | Dashboard generation, visual export, profiling reports | 5–6 tools |

**Tier 1 must be fully self-contained.** A user doing simple tasks (inspect a
column, fill nulls, filter rows) must never need tier 2 or 3 loaded.

### Engine / Server Separation (Mandatory)

Every tier has exactly two logic files:

```
servers/data_basic/
├── server.py    ← thin MCP wrapper only; zero domain logic
├── engine.py    ← all pandas/polars/duckdb logic; zero MCP imports
└── pyproject.toml
```

**`engine.py` rules:**
- No `from mcp import ...` or `from fastmcp import ...` anywhere
- All domain logic lives here: file loading, transforms, stats, snapshots
- Every function is callable from a plain Python REPL with no MCP running
- All exceptions caught here; always returns a `dict`

**`server.py` rules:**
- Tool body is one line only: `return engine.function_name(param)`
- If a tool body exceeds two lines, move the logic to `engine.py`
- Import only `FastMCP` and `engine`

### Shared Utilities

```
shared/
├── version_control.py   # snapshot() and restore()
├── patch_validator.py   # validate op arrays before applying
├── file_utils.py        # path resolution, atomic writes
├── platform_utils.py    # MCP_CONSTRAINED_MODE, get_max_rows()
├── progress.py          # ok/fail/info/warn/undo helpers
└── receipt.py           # append_receipt(), read_receipt_log()
```

Never duplicate shared logic between tiers. Import from `shared/`.

---

## Notebook → MCP Tool Mapping

`Data_Analytic_Lookup.ipynb` is the source of truth for what analytics workflows
this server must support. Each notebook section maps to tools in a specific tier.

### Tier 1 — Basic Tools (from notebook sections 01–03)

| Notebook section | Tool name | What it does |
|---|---|---|
| 01.01 Importing Main Data | `load_dataset` | Load CSV into session; return schema + row count |
| 01.02 Importing Geo Data | `load_geo_dataset` | Load GeoJSON/shapefile; return geometry columns |
| 02.01 Selecting/Dropping Variables | `apply_patch` (op: `drop_column`) | Drop named columns from dataset |
| 02.02 Cleaning Text | `apply_patch` (op: `clean_text`) | Strip spaces, title-case headers/values |
| 02.03 Converting Data | `apply_patch` (op: `cast_column`) | Cast column to int/float/datetime/str |
| 03.01 Replacing Values | `apply_patch` (op: `replace_values`) | Replace specific values in a column |
| 03.02 Add New Variable | `apply_patch` (op: `add_column`) | Derive new column via math or mapping |
| 03.04 Handling Outliers | `apply_patch` (op: `cap_outliers`) | IQR or std-dev capping |
| 03.05 Handling Missing/Null | `apply_patch` (op: `fill_nulls`) | Fill with mean/median/mode/ffill/bfill/drop |
| 03.08 Handling Duplicates | `apply_patch` (op: `drop_duplicates`) | Drop duplicate rows |
| — | `inspect_dataset` | Return schema, row count, null counts, dtype map |
| — | `read_column_stats` | Stats for one column: mean/median/null/unique |
| — | `search_columns` | Find columns matching criteria (has_nulls, dtype, etc.) |
| — | `restore_version` | Restore file to a previous snapshot |
| — | `read_receipt` | Read operation history log for a file |

### Tier 2 — Medium Tools (from notebook sections 03–05)

| Notebook section | Tool name | What it does |
|---|---|---|
| 03.04 Outlier check | `check_outliers` | Run IQR/std-dev scan; return outlier summary |
| 03.05 Null detection | `scan_nulls_zeros` | Return all columns with nulls or zeros + counts |
| 04 Enriching Data | `enrich_with_geo` | Merge dataset with geo data on a location key |
| 05 Data Validation | `validate_dataset` | Check types, nulls, duplicates; return report |
| — | `compute_aggregations` | Group-by + agg (sum/mean/count) on specified cols |
| — | `run_cleaning_pipeline` | Apply ordered list of cleaning ops in one call |

### Tier 3 — Advanced Tools (from notebook sections 06–09)

| Notebook section | Tool name | What it does |
|---|---|---|
| 06.01 YData Profiling | `generate_profile_report` | Run ydata-profiling; save HTML report |
| 06.02 SweetViz | `generate_sweetviz_report` | Run SweetViz analysis; save HTML report |
| 06.03 AutoViz | `generate_autoviz_report` | Run AutoViz; save chart outputs |
| 08 Chart Drafting | `generate_chart` | Generate Plotly chart (bar/pie/line/scatter/geo/treemap) |
| 09 Create Dashboard | `generate_dashboard` | Render Streamlit app.py from dataset |

> **Note on charts:** `generate_chart` accepts `chart_type` as a string
> enum: `"bar"`, `"pie"`, `"line"`, `"scatter"`, `"geo"`, `"treemap"`,
> `"time_series"`, `"radius"`. Never create one tool per chart type.

---

## The Four-Tool Pattern

Every data modification task follows this loop. Design tools so the model is
guided through it naturally:

```
LOCATE  →  INSPECT  →  PATCH  →  VERIFY
```

### Applied to this project

```
Round 1 (LOCATE):   search_columns(file="sales.csv", has_nulls=True)
                    → {"columns": ["revenue"], "null_counts": {"revenue": 23}}

Round 2 (INSPECT):  read_column_stats(file="sales.csv", column="revenue")
                    → {"mean": 4200, "median": 3800, "null_count": 23}

Round 3 (PATCH):    apply_patch(file="sales.csv", ops=[
                        {"op": "fill_nulls", "column": "revenue", "strategy": "median"}
                    ])
                    → {"success": true, "applied": 1, "backup": "..."}

Round 4 (VERIFY):   read_column_stats(file="sales.csv", column="revenue")
                    → {"mean": 4190, "median": 3800, "null_count": 0}
```

---

## Surgical Read Protocol

**Never return more data than the model asked for.**

### Return size limits (enforced in `engine.py`, not by the model)

| Data type | Default limit | Constrained mode (`MCP_CONSTRAINED_MODE=1`) |
|---|---|---|
| DataFrame rows | 100 per call | 20 per call |
| DataFrame columns | 50 per call | 20 per call |
| Search results | 50 per call | 10 per call |

When a response is truncated, always include:

```python
{
    "truncated": True,
    "returned": 20,
    "total_available": 5000,
    "hint": "Use read_rows(file, start, end) to read specific row ranges."
}
```

Every response includes `"token_estimate": len(str(response)) // 4`.

### Read tool classes required

- **Index tool** — returns structure, never content: `inspect_dataset`
- **Search tool** — scans content, returns matching addresses: `search_columns`
- **Bounded read tool** — reads one node with a hard size cap: `read_column_stats`

---

## Tool Schema Rules

### Docstrings — 80-character hard limit

Every `@mcp.tool()` docstring must be ≤ 80 characters. These are selection cues
for the model, not human documentation. CI must assert this.

```python
# Good
"""Fill null values in a column. strategy: mean median mode ffill bfill drop."""

# Bad — too long, narrative style
"""This tool fills null values in the specified column of the dataset using the
given fill strategy (mean, median, mode, forward fill, backward fill, or drop)."""
```

### Parameter types allowed

```python
str      # paths, names, enum values (document valid values in docstring)
int      # indices, counts, limits, seeds
float    # ratios, thresholds
bool     # flags; always default to False for write-enabling flags
list[dict]   # op arrays only (apply_patch)
list[str]    # column lists, file lists
```

Never use `Optional[T]` (use `T = None`), `Union`, `Any`, `dict`, or Pydantic
models in tool signatures.

### Every write tool must have `dry_run: bool = False`

When `dry_run=True`, return exactly what would change without modifying the file:

```python
if dry_run:
    return {
        "success": True,
        "dry_run": True,
        "would_change": description_of_changes,
        "token_estimate": ...,
    }
```

---

## Return Value Contract

Every tool returns a `dict`. No exceptions. Never return a plain string, list,
`None`, or boolean.

### Required fields

| Field | Type | When |
|---|---|---|
| `"success"` | `bool` | Always |
| `"op"` | `str` | On success |
| `"error"` | `str` | On failure |
| `"hint"` | `str` | On failure — must name a specific tool or fix |
| `"backup"` | `str` | After any write — path to snapshot |
| `"progress"` | `list` | Always — use `shared/progress.py` helpers |
| `"dry_run"` | `bool` | When `dry_run=True` |
| `"token_estimate"` | `int` | Always — `len(str(response)) // 4` |
| `"truncated"` | `bool` | On bounded reads |

### Progress helpers — always use these, never construct dicts by hand

```python
from shared.progress import ok, fail, info, warn, undo

ok("Loaded sales.csv",       "5,000 rows × 12 cols")
ok("Filled nulls in revenue","23 cells → 3800.0")
fail("Column not found",     "Available: revenue, discount, date")
warn("Large dataset",        "Constrained mode: returning 20 rows")
```

Use `Path(x).name` in messages — never full absolute paths.

---

## Error Handling Contract

All exceptions are caught in `engine.py` and converted to error dicts. Never let
an exception propagate to `server.py`.

```python
def fill_nulls(file_path: str, column_name: str, strategy: str) -> dict:
    backup = None
    try:
        path = resolve_path(file_path)
        backup = snapshot(str(path))
        # ... do work ...
        return {"success": True, ...}
    except FileNotFoundError:
        return {
            "success": False,
            "error": f"File not found: {file_path}",
            "hint": "Check that file_path is absolute and the file exists.",
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "backup": backup,
            "hint": "Use restore_version to undo if a snapshot was taken.",
        }
```

### Hint field rules

The hint must complete "To fix this, ..." and must name a specific tool or value.

```python
# Bad
"hint": "Invalid input."

# Good
"hint": "Use inspect_dataset() first to verify column names and dtypes."
"hint": f"Available strategies: mean, median, mode, ffill, bfill, drop"
```

---

## State and Version Control

### The snapshot rule

Every write tool must snapshot the file before modifying it:

```python
from shared.version_control import snapshot
backup = snapshot(str(path))    # creates .mcp_versions/filename_TIMESTAMP.bak
```

Always return `"backup": backup` in the response.

### Companion files per dataset

```
sales_q3.csv
sales_q3.csv.mcp_state.json     ← schema cache + patch history
sales_q3.csv.mcp_receipt.json   ← operation receipt log
.mcp_versions/                  ← timestamped backups
```

### Receipt log

Every write appends to the receipt log via `shared/receipt.py`:

```python
from shared.receipt import append_receipt
append_receipt(file_path, tool="fill_nulls",
               args={"column": "revenue", "strategy": "median"},
               result="filled 23 nulls", backup=backup)
```

`append_receipt` must never raise — silently drop on failure.

---

## Token Budget

This server targets **8 GB VRAM / 9B model** as the primary constraint. On this
hardware, the effective context window is ~10,000–12,000 tokens.

| Budget item | Limit |
|---|---|
| All tool schemas combined | ≤ 700 tokens |
| Each tool docstring | ≤ 80 chars |
| Read tool responses | ≤ 500 tokens |
| Write confirmations | ≤ 150 tokens |

Set `MCP_CONSTRAINED_MODE=1` at install time on machines with ≤ 8 GB VRAM.
Always call `platform_utils.get_max_rows()` — never hardcode row limits.

---

## Domain Libraries

Use these libraries. Do not introduce alternatives without a documented reason.

| Purpose | Library |
|---|---|
| Tabular data (primary) | `polars` (preferred) or `pandas` |
| SQL-style queries | `duckdb` |
| Data profiling | `ydata-profiling` |
| Automated EDA | `sweetviz`, `autoviz` |
| Interactive charts | `plotly` |
| Static charts | `matplotlib`, `seaborn` |
| Geospatial | `geopandas` |
| Dashboard | `streamlit` |
| Tabular formatting | `tabulate` |
| Resource monitoring | `psutil` |

Pin Python to `3.11`. Use `uv` as the package manager. Never use `pip` or
`conda` in production. Never use `poetry` for new projects.

---

## stdout is Sacred

**Never write to stdout in any `engine.py` or `server.py` module.**
Any `print()` corrupts the MCP stdio channel.

```python
# Wrong — crashes the MCP protocol
print("Processing file...")

# Correct — all debug logs go to stderr
import logging, sys
logger = logging.getLogger(__name__)
logger.debug("Processing file...")
```

Configure in `server.py`:
```python
import sys, logging
logging.basicConfig(stream=sys.stderr, level=logging.WARNING)
```

---

## Testing Standards

Tests import `engine.py` directly. Never spin up an MCP server process in tests.

```python
from servers.data_basic.engine import fill_nulls, search_columns

def test_fill_nulls(tmp_path, csv_fixture):
    result = fill_nulls(str(csv_fixture), "revenue", "median")
    assert result["success"] is True
    assert result["filled"] == 23
    assert ".mcp_versions" in result["backup"]
    assert "progress" in result
    assert result["token_estimate"] > 0
```

### What to test for every write tool

1. Success — operation completes, `"success": True`
2. Content correct — read back and verify
3. Snapshot created — `.mcp_versions/` has new `.bak` file
4. Backup in response — `"backup"` key present
5. Dry run — `dry_run=True` returns `"would_change"` without modifying file
6. Progress present — `"progress"` array in response
7. Wrong file type — returns error dict with correct hint
8. File not found — returns error dict with correct hint
9. Column not found — returns error dict with available columns in hint
10. Constrained mode — `MCP_CONSTRAINED_MODE=1` enforces smaller limits

### Coverage requirements

| Module | Minimum coverage |
|---|---|
| `shared/` | 100% |
| `engine.py` per tier | ≥ 90% |
| All documented error conditions | Must be tested |

### Fixture categories required

- `simple` — clean CSV, minimal edge cases
- `messy` — nulls, type mismatches, duplicate rows, encoding issues
- `large` — enough rows to test truncation and constrained mode

---

## Development Checklist

### Before adding a new tool

- [ ] Does this tool belong in tier 1, 2, or 3? (use the decision tree)
- [ ] Does it follow LOCATE/INSPECT/PATCH/VERIFY? Or is it a read-only tool?
- [ ] Is the docstring ≤ 80 characters?
- [ ] Do all parameters use only allowed types?
- [ ] Does every write path call `snapshot()` and return `"backup"`?
- [ ] Does it have `dry_run: bool = False` if it writes anything?
- [ ] Does it use `platform_utils.get_max_rows()` for row limits?
- [ ] Does it include `"progress"` and `"token_estimate"` in the response?
- [ ] Are all exceptions caught in `engine.py`?
- [ ] Is there no `print()` anywhere in the call path?

### Before committing

- [ ] `engine.py` has zero MCP imports
- [ ] `server.py` tool bodies are one line each
- [ ] All new tools have tests covering success, failure, dry_run, and constrained mode
- [ ] CI passes on Linux, macOS, and Windows
- [ ] Docstring length CI check passes: `assert len(tool.__doc__) <= 80`

---

## Cross-Platform Rules

- Use `pathlib.Path` everywhere — never string concatenation for paths
- Never write to stdout (use `sys.stderr` for all logging)
- Use atomic file writes: write to temp file, then `shutil.move(tmp, target)`
- `.gitattributes` must set `* text=auto eol=lf` and `*.bat text eol=crlf`
- `install.sh` must use `#!/bin/sh` (POSIX, not bash)

---

## What Never to Do

- Do not call any paid API (OpenAI, Cohere, Google Cloud, AWS) from any tool
- Do not return raw DataFrame rows beyond the size limits in this document
- Do not create one tool per chart type — use `chart_type` string enum
- Do not add AI inference or "smart guessing" inside any engine function
- Do not mix tiers in one server
- Do not put domain logic in `server.py`
- Do not put MCP imports in `engine.py`
- Do not use `print()` anywhere in the server or engine
- Do not skip `dry_run` on write tools
- Do not skip `snapshot()` before any file modification
- Do not return `None`, plain `str`, or bare `list` from any tool
