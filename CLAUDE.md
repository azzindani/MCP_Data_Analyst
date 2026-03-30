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

- [x] Does this tool belong in tier 1, 2, or 3? (use the decision tree)
- [x] Does it follow LOCATE/INSPECT/PATCH/VERIFY? Or is it a read-only tool?
- [x] Is the docstring ≤ 80 characters?
- [x] Do all parameters use only allowed types?
- [x] Does every write path call `snapshot()` and return `"backup"`?
- [x] Does it have `dry_run: bool = False` if it writes anything?
- [x] Does it use `platform_utils.get_max_rows()` for row limits?
- [x] Does it include `"progress"` and `"token_estimate"` in the response?
- [x] Are all exceptions caught in `engine.py`?
- [x] Is there no `print()` anywhere in the call path?

### Before committing

- [x] `engine.py` has zero MCP imports
- [x] `server.py` tool bodies are one line each
- [x] All new tools have tests covering success, failure, dry_run, and constrained mode
- [x] CI passes on Linux, macOS, and Windows
- [x] Docstring length CI check passes: `assert len(tool.__doc__) <= 80`

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

---

## Tool Specifications

This section defines every tool extracted from `Data_Analytic_Lookup.ipynb`.
Each entry includes: tier, notebook source, signature, parameter contract,
return fields, and a per-tool development checklist.

Legend for checklist status: `[ ]` = not started · `[x]` = done

---

### TIER 1 — `servers/data_basic/`

---

#### Tool: `load_dataset`

| Field | Value |
|---|---|
| Tier | 1 — Basic |
| Notebook source | §01.01 Importing Main Data |
| File | `servers/data_basic/engine.py` |
| Writes to disk | No |
| Docstring (≤80 chars) | `"Load CSV file. Returns schema, row count, dtypes, null counts."` |

**Signature**

```python
def load_dataset(
    file_path: str,          # absolute path to CSV file
    encoding: str = "utf-8", # "utf-8", "ISO-8859-1", "latin1", "utf-16"
    separator: str = ",",    # delimiter character
    max_rows: int = 0,       # 0 = all rows; >0 = sample only
) -> dict
```

**Return fields**

```python
{
    "success": True,
    "op": "load_dataset",
    "file": "sales.csv",          # Path.name only
    "rows": 9648,
    "columns": 12,
    "dtypes": {"Revenue": "float64", "Region": "object", ...},
    "null_counts": {"Revenue": 0, "Region": 3},
    "unique_counts": {"Region": 5, "Product": 88},
    "sample": [...],              # first 2 rows as list[dict], always 2
    "encoding_used": "ISO-8859-1",
    "progress": [...],
    "token_estimate": 210,
}
```

**Error cases**

| Condition | `error` message | `hint` |
|---|---|---|
| File not found | `"File not found: sales.csv"` | `"Check file_path is absolute."` |
| Not a CSV | `"Expected .csv, got .xlsx"` | `"Use file_path pointing to a .csv file."` |
| Encoding fails | `"Cannot decode with utf-8"` | `"Try encoding='ISO-8859-1' or 'latin1'."` |
| Empty file | `"File is empty: sales.csv"` | `"Verify the file has header + data rows."` |

**Development checklist**

- [x] `engine.load_dataset` implemented — reads with polars/pandas, detects nulls, unique counts
- [x] `server.py` tool registered — one-line body only
- [x] Docstring ≤ 80 chars verified
- [x] Returns 2-row sample (hard cap, never configurable)
- [x] `encoding` and `separator` forwarded to read call
- [x] `max_rows > 0` triggers row sampling, adds `warn()` to progress
- [x] `token_estimate` present in all return paths
- [x] `progress` array populated with `ok()`/`warn()` helpers
- [x] Test: success with simple fixture — assert rows/columns/dtypes correct
- [x] Test: encoding=ISO-8859-1 reads accented characters correctly
- [x] Test: file not found → error dict with hint
- [x] Test: `.xlsx` file → error dict with hint
- [x] Test: empty CSV → error dict with hint
- [x] Test: `MCP_CONSTRAINED_MODE=1` with large file → warn in progress

---

#### Tool: `load_geo_dataset`

| Field | Value |
|---|---|
| Tier | 1 — Basic |
| Notebook source | §01.02 Importing Geo Data |
| Writes to disk | No |
| Docstring | `"Load GeoJSON or shapefile. Returns geometry columns and CRS."` |

**Signature**

```python
def load_geo_dataset(
    file_path: str,             # absolute path to .geojson or .shp
    rename_column: str = "",    # rename "name" → this value if provided
    keep_columns: list[str] = None,  # columns to keep; None = all
) -> dict
```

**Return fields**

```python
{
    "success": True,
    "op": "load_geo_dataset",
    "file": "us-states.geojson",
    "rows": 50,
    "columns": ["State", "geometry"],
    "crs": "EPSG:4326",
    "geometry_type": "MultiPolygon",
    "sample": [...],             # first 2 rows as list[dict], geometry as WKT string
    "progress": [...],
    "token_estimate": 145,
}
```

**Development checklist**

- [x] `engine.load_geo_dataset` implemented with `geopandas.read_file`
- [x] `rename_column` applies rename mapping before returning
- [x] `keep_columns` filters to only requested columns + geometry
- [x] Geometry column serialised as WKT string in sample (never raw object)
- [x] Test: valid GeoJSON → correct rows/columns/crs
- [x] Test: rename_column renames "name" → custom value
- [x] Test: file not found → error dict
- [x] Test: non-geo file (.csv) → error dict with hint

---

#### Tool: `inspect_dataset`

| Field | Value |
|---|---|
| Tier | 1 — Basic |
| Notebook source | §01.01 (`df.info()`, `df.nunique()`, `df.head()`) |
| Writes to disk | No |
| Docstring | `"Inspect dataset schema, dtypes, null counts, row/col totals."` |

**Signature**

```python
def inspect_dataset(
    file_path: str,
    include_sample: bool = False,   # include first 2 rows in response
) -> dict
```

**Return fields**

```python
{
    "success": True,
    "op": "inspect_dataset",
    "file": "sales.csv",
    "rows": 9648,
    "columns": 12,
    "column_names": ["Region", "State", "City", "Revenue", ...],
    "dtypes": {"Region": "object", "Revenue": "float64", ...},
    "null_counts": {"Region": 0, "Revenue": 23},
    "null_pct": {"Region": 0.0, "Revenue": 0.24},
    "unique_counts": {"Region": 5, "Product": 88},
    "numeric_columns": ["Revenue", "Units Sold", ...],
    "categorical_columns": ["Region", "State", "Product", ...],
    "datetime_columns": ["Order Date"],
    "sample": [...],             # only if include_sample=True
    "progress": [...],
    "token_estimate": 320,
}
```

**Development checklist**

- [x] `engine.inspect_dataset` reads file, computes all schema fields
- [x] Columns split into `numeric_columns` / `categorical_columns` / `datetime_columns`
- [x] `null_pct` computed as `null_count / rows * 100` rounded to 2dp
- [x] `include_sample=False` by default — never return rows unless asked
- [x] `token_estimate` capped: if response > 500 tokens, truncate `column_names` with truncation flag
- [x] Test: clean CSV → correct dtype classification
- [x] Test: messy CSV with mixed types → nulls and dtypes accurate
- [x] Test: `include_sample=True` → 2 rows in response
- [x] Test: file not found → error dict

---

#### Tool: `read_column_stats`

| Field | Value |
|---|---|
| Tier | 1 — Basic |
| Notebook source | §03.04 (distribution checks), §03.05 (null detection) |
| Writes to disk | No |
| Docstring | `"Stats for one column: mean median std min max nulls unique top."` |

**Signature**

```python
def read_column_stats(
    file_path: str,
    column: str,
) -> dict
```

**Return fields — numeric column**

```python
{
    "success": True,
    "op": "read_column_stats",
    "column": "Revenue",
    "dtype": "float64",
    "count": 9648,
    "null_count": 23,
    "null_pct": 0.24,
    "zero_count": 0,
    "mean": 4287.32,
    "median": 3800.0,
    "std": 1923.5,
    "min": 210.0,
    "max": 19500.0,
    "q1": 2400.0,
    "q3": 5800.0,
    "iqr": 3400.0,
    "outlier_count_iqr": 42,
    "outlier_count_std": 18,
    "progress": [...],
    "token_estimate": 175,
}
```

**Return fields — categorical column**

```python
{
    "success": True,
    "op": "read_column_stats",
    "column": "Region",
    "dtype": "object",
    "count": 9648,
    "null_count": 0,
    "unique_count": 5,
    "top_values": {"West": 2340, "South": 2180, "East": 1950, "North": 1800, "Other": 1378},
    "progress": [...],
    "token_estimate": 110,
}
```

**Development checklist**

- [x] `engine.read_column_stats` dispatches on dtype — numeric path vs categorical path
- [x] IQR and std outlier counts computed using notebook formulas (§03.04.01, §03.04.02)
- [x] `zero_count` always included for numeric columns
- [x] `top_values` limited to top 10 by frequency for categorical
- [x] Column not found → error dict listing available columns
- [x] Test: numeric column → all stat fields present and correct
- [x] Test: categorical column → top_values, unique_count correct
- [x] Test: datetime column → returns dtype + min/max date + null_count
- [x] Test: column not found → error with hint listing column names
- [x] Test: column with all nulls → graceful (no ZeroDivisionError)

---

#### Tool: `search_columns`

| Field | Value |
|---|---|
| Tier | 1 — Basic |
| Notebook source | §03.05.01 (zero detection), §03.05.02 (null detection) |
| Writes to disk | No |
| Docstring | `"Find columns by criteria: has_nulls dtype has_zeros name_contains."` |

**Signature**

```python
def search_columns(
    file_path: str,
    has_nulls: bool = False,        # return only columns with null_count > 0
    has_zeros: bool = False,        # numeric columns with zero values
    dtype: str = "",                # filter by dtype: "numeric", "object", "datetime"
    name_contains: str = "",        # case-insensitive substring match on column name
    min_null_pct: float = 0.0,      # only columns above this null percentage
) -> dict
```

**Return fields**

```python
{
    "success": True,
    "op": "search_columns",
    "matched": 3,
    "columns": ["Revenue", "Discount", "Units Sold"],
    "null_counts": {"Revenue": 23, "Discount": 0, "Units Sold": 7},
    "zero_counts": {"Revenue": 0, "Discount": 156, "Units Sold": 3},
    "dtypes": {"Revenue": "float64", "Discount": "float64", "Units Sold": "int64"},
    "truncated": False,
    "progress": [...],
    "token_estimate": 130,
}
```

**Development checklist**

- [x] All filter criteria composable (multiple can apply at once)
- [x] `dtype="numeric"` matches both `int64` and `float64` columns
- [x] `name_contains` is case-insensitive
- [x] Returns empty `columns: []` with `matched: 0` when nothing found — not an error
- [x] Truncated at `get_max_results()` — includes `truncated: true` flag when trimmed
- [x] Test: `has_nulls=True` returns only columns with nulls
- [x] Test: `has_zeros=True` returns only numeric columns with zeros
- [x] Test: `dtype="object"` returns only string columns
- [x] Test: `name_contains="date"` matches "Order Date", "Ship Date" case-insensitively
- [x] Test: no criteria → returns all columns (acts as column lister)
- [x] Test: constrained mode → max 10 results

---

#### Tool: `apply_patch`

| Field | Value |
|---|---|
| Tier | 1 — Basic |
| Notebook source | §02.01–02.03, §03.01–03.08 (all data transformation cells) |
| Writes to disk | Yes — snapshots before every write |
| Docstring | `"Apply ordered ops to a CSV. ops: see Op Reference below."` |

**Signature**

```python
def apply_patch(
    file_path: str,
    ops: list[dict],       # ordered list of operation dicts; see Op Reference
    dry_run: bool = False, # preview changes without writing
) -> dict
```

**Return fields**

```python
{
    "success": True,
    "op": "apply_patch",
    "applied": 3,
    "results": [
        {"op": "fill_nulls", "column": "Revenue", "filled": 23},
        {"op": "drop_duplicates", "dropped": 5},
        {"op": "cast_column", "column": "Order Date", "dtype": "datetime"},
    ],
    "backup": ".mcp_versions/sales_2026-03-27T10-00-00Z.bak",
    "progress": [...],
    "token_estimate": 180,
}
```

**Op Reference — all supported ops**

Every op dict must have `"op"` as the first key. All fields are validated
before any op is applied. Failure on op N rolls back to snapshot; ops 0..N-1
are undone.

---

**`drop_column`** — notebook §02.01

```python
{"op": "drop_column", "columns": ["col1", "col2"]}
```

| Param | Type | Description |
|---|---|---|
| `columns` | `list[str]` | Column names to remove |

Returns: `{"op": "drop_column", "dropped": ["col1", "col2"], "remaining": 10}`

---

**`clean_text`** — notebook §02.02.01–02.02.03

```python
{"op": "clean_text", "scope": "both"}
# scope: "headers" | "values" | "both"
```

Applies in order: strip whitespace → title-case headers → title-case string values.

Returns: `{"op": "clean_text", "scope": "both", "columns_affected": 7}`

---

**`cast_column`** — notebook §02.03.01–02.03.03

```python
{"op": "cast_column", "column": "Order Date", "dtype": "datetime"}
# dtype: "int" | "float" | "str" | "datetime"
```

Returns: `{"op": "cast_column", "column": "Order Date", "from": "object", "to": "datetime64[ns]", "failed": 0}`

---

**`replace_values`** — notebook §03.01

```python
{
    "op": "replace_values",
    "column": "Region",
    "mapping": {"west": "West", "W": "West"}
}
```

Returns: `{"op": "replace_values", "column": "Region", "replaced": 234}`

---

**`add_column`** — notebook §03.02.01–03.02.02

```python
# Math mode
{"op": "add_column", "name": "Profit Margin", "expr": "Profit / Revenue", "mode": "math"}

# Threshold mode — values below threshold → "Other"
{"op": "add_column", "name": "Top Region", "source": "Region", "mode": "threshold", "threshold": 1300}
```

`expr` supports `+`, `-`, `*`, `/` on existing numeric column names.
No `eval()` — parse expression tree manually for safety.

Returns: `{"op": "add_column", "name": "Profit Margin", "dtype": "float64", "null_count": 0}`

---

**`cap_outliers`** — notebook §03.04.01–03.04.02

```python
# IQR method
{"op": "cap_outliers", "column": "Revenue", "method": "iqr", "th1": 0.25, "th3": 0.75}

# Std-dev method
{"op": "cap_outliers", "column": "Revenue", "method": "std"}
```

Caps values at computed lower/upper bounds (does not drop rows).
`th1`/`th3` only used when `method="iqr"`.

Returns: `{"op": "cap_outliers", "column": "Revenue", "method": "iqr", "capped_upper": 12, "capped_lower": 3, "lower_limit": 150.0, "upper_limit": 9200.0}`

---

**`fill_nulls`** — notebook §03.05.03–03.05.05

```python
{
    "op": "fill_nulls",
    "column": "Revenue",
    "strategy": "median",
    "fill_zeros": false
}
# strategy: "mean" | "median" | "mode" | "ffill" | "bfill" | "drop"
# fill_zeros: if true, treat 0 as null before filling (notebook §02.03.04)
```

Returns: `{"op": "fill_nulls", "column": "Revenue", "strategy": "median", "filled": 23, "value_used": 3800.0}`

---

**`drop_duplicates`** — notebook §03.08

```python
{"op": "drop_duplicates"}                           # all columns
{"op": "drop_duplicates", "subset": ["customer_id", "date"]}  # specific columns
```

Returns: `{"op": "drop_duplicates", "dropped": 47, "remaining": 9601}`

---

**`apply_patch` development checklist**

- [x] `validate_ops()` called before any file modification — uses `shared/patch_validator.py`
- [x] `snapshot()` called once before first op; backup path returned in response
- [x] Ops applied sequentially; halt + return error on first failure
- [x] On failure: backup path included so caller can `restore_version`
- [x] `dry_run=True`: run all validation, compute `would_change` summary, no file write
- [x] `add_column` expr parser — no `eval()`, supports `*`, `/`, `+`, `-` only
- [x] `clean_text` handles `None`/NaN values in object columns without crashing
- [x] `cast_column` to datetime uses `pd.to_datetime(..., errors='coerce')` — never raises
- [x] `fill_nulls` with `fill_zeros=True` replaces 0 → NaN before filling
- [x] `cap_outliers` uses notebook IQR formula: `Q1 - 1.5*IQR` / `Q3 + 1.5*IQR`
- [x] Receipt log appended via `append_receipt()` after each successful patch
- [x] Test: single op success — file modified, backup created, receipt written
- [x] Test: multi-op success — all ops applied in order
- [x] Test: `drop_column` on non-existent column → error, no file change
- [x] Test: `cast_column` to int on non-numeric string → partial fail tracked in `failed`
- [x] Test: `fill_nulls` strategy=median on column with all nulls → graceful fallback
- [x] Test: `fill_nulls` fill_zeros=True → zeros treated as null
- [x] Test: `cap_outliers` IQR → capped counts correct vs manual calculation
- [x] Test: `add_column` math expr → new column values correct
- [x] Test: `add_column` threshold mode → low-freq values → "Other"
- [x] Test: `clean_text` scope=headers → column names title-cased
- [x] Test: `drop_duplicates` subset → only subset-based duplicates removed
- [x] Test: dry_run=True → `would_change` populated, file unchanged, no backup
- [x] Test: op 2 of 3 fails → file unchanged, backup still provided in response
- [x] Test: unknown op name → error before any write with hint listing allowed ops

---

#### Tool: `restore_version`

| Field | Value |
|---|---|
| Tier | 1 — Basic |
| Notebook source | Version control (no notebook equivalent — safety tool) |
| Writes to disk | Yes — overwrites working file from backup |
| Docstring | `"Restore file to a snapshot. timestamp from backup filename."` |

**Signature**

```python
def restore_version(
    file_path: str,
    timestamp: str = "",   # e.g. "2026-03-27T10-00-00Z"; "" = most recent backup
) -> dict
```

**Return fields**

```python
{
    "success": True,
    "op": "restore_version",
    "file": "sales.csv",
    "restored_from": ".mcp_versions/sales_2026-03-27T10-00-00Z.bak",
    "available_versions": [
        "sales_2026-03-27T10-00-00Z.bak",
        "sales_2026-03-27T09-45-00Z.bak",
    ],
    "progress": [...],
    "token_estimate": 95,
}
```

**Development checklist**

- [x] `timestamp=""` → restore most recent backup automatically
- [x] `available_versions` always listed in response (for reference)
- [x] Creates a new snapshot of current state before overwriting (undo of the undo)
- [x] Error if `.mcp_versions/` directory does not exist
- [x] Error if requested timestamp not found — list available timestamps in hint
- [x] Test: restore most recent → file content matches backup
- [x] Test: restore specific timestamp → correct backup applied
- [x] Test: no backups exist → error with hint to use `apply_patch` first
- [x] Test: timestamp not found → error listing available timestamps

---

#### Tool: `read_receipt`

| Field | Value |
|---|---|
| Tier | 1 — Basic |
| Notebook source | Audit trail (no notebook equivalent — tracking tool) |
| Writes to disk | No |
| Docstring | `"Read operation history log for a file. Returns receipt entries."` |

**Signature**

```python
def read_receipt(
    file_path: str,
    last_n: int = 10,   # return last N entries; 0 = all
) -> dict
```

**Return fields**

```python
{
    "success": True,
    "op": "read_receipt",
    "file": "sales.csv",
    "total_entries": 14,
    "returned": 10,
    "entries": [
        {
            "ts": "2026-03-27T10-00-00Z",
            "tool": "apply_patch",
            "args": {"ops": [{"op": "fill_nulls", "column": "Revenue"}]},
            "result": "filled 23 nulls",
            "backup": ".mcp_versions/sales_2026-03-27T10-00-00Z.bak"
        },
        ...
    ],
    "progress": [...],
    "token_estimate": 210,
}
```

**Development checklist**

- [x] Returns most recent entries first (descending timestamp)
- [x] `last_n=0` returns all entries — warn if > constrained mode limit
- [x] No receipt file yet → returns empty entries list, not an error
- [x] Test: receipt exists → entries returned in descending order
- [x] Test: `last_n=3` → exactly 3 most recent entries
- [x] Test: no receipt file → `{"entries": [], "total_entries": 0, "success": True}`

---

### TIER 2 — `servers/data_medium/`

---

#### Tool: `check_outliers`

| Field | Value |
|---|---|
| Tier | 2 — Medium |
| Notebook source | §03.04.01 IQR, §03.04.02 Std-Dev |
| Writes to disk | No |
| Docstring | `"Scan numeric columns for outliers. method: iqr std both."` |

**Signature**

```python
def check_outliers(
    file_path: str,
    columns: list[str] = None,   # None = all numeric columns
    method: str = "both",        # "iqr" | "std" | "both"
    th1: float = 0.25,           # IQR lower quantile threshold
    th3: float = 0.75,           # IQR upper quantile threshold
) -> dict
```

**Return fields**

```python
{
    "success": True,
    "op": "check_outliers",
    "method": "both",
    "scanned_columns": 5,
    "columns_with_outliers": 2,
    "results": {
        "Revenue": {
            "has_outliers_iqr": True,
            "outlier_count_iqr": 42,
            "lower_limit_iqr": 150.0,
            "upper_limit_iqr": 9200.0,
            "has_outliers_std": True,
            "outlier_count_std": 18,
            "lower_limit_std": -210.0,
            "upper_limit_std": 12300.0,
        },
        "Units Sold": {
            "has_outliers_iqr": False,
            "has_outliers_std": True,
            "outlier_count_std": 3,
        }
    },
    "truncated": False,
    "progress": [...],
    "token_estimate": 280,
}
```

**Development checklist**

- [ ] IQR formula matches notebook §03.04.01: `Q3 + 1.5*IQR` / `Q1 - 1.5*IQR`
- [ ] Std formula matches notebook §03.04.02: `mean ± 3*std`
- [ ] `columns=None` auto-selects all `int64`/`float64` columns
- [ ] Skips non-numeric columns silently (no error)
- [ ] Results truncated at `get_max_results()` with `truncated: true` flag
- [ ] Test: column with known outliers → counts match manual calculation
- [ ] Test: clean column → `has_outliers_iqr: false, has_outliers_std: false`
- [ ] Test: `method="iqr"` → only IQR fields in results (no std fields)
- [ ] Test: `columns=["Revenue"]` → only scans Revenue
- [ ] Test: column not found → error listing available columns

---

#### Tool: `scan_nulls_zeros`

| Field | Value |
|---|---|
| Tier | 2 — Medium |
| Notebook source | §03.05.01 Zero detection, §03.05.02 Null detection |
| Writes to disk | No |
| Docstring | `"Scan all columns for nulls and zeros. Returns counts and pcts."` |

**Signature**

```python
def scan_nulls_zeros(
    file_path: str,
    include_zeros: bool = True,
    min_count: int = 1,   # only report columns with at least this many nulls/zeros
) -> dict
```

**Return fields**

```python
{
    "success": True,
    "op": "scan_nulls_zeros",
    "total_rows": 9648,
    "clean_columns": 8,
    "flagged_columns": 4,
    "results": {
        "Revenue":    {"null_count": 23, "null_pct": 0.24, "zero_count": 0,  "zero_pct": 0.0},
        "Discount":   {"null_count": 0,  "null_pct": 0.0,  "zero_count": 156, "zero_pct": 1.62},
        "Units Sold": {"null_count": 7,  "null_pct": 0.07, "zero_count": 3,  "zero_pct": 0.03},
        "Region":     {"null_count": 3,  "null_pct": 0.03, "zero_count": null},
    },
    "suggested_actions": {
        "Revenue":  "apply_patch op=fill_nulls strategy=median",
        "Discount": "apply_patch op=fill_nulls fill_zeros=true strategy=mean",
    },
    "progress": [...],
    "token_estimate": 220,
}
```

**Development checklist**

- [ ] Detects NaN, None, and pandas `NA` as null for all column types
- [ ] Detects `0` and `"0"` as zero for numeric columns
- [ ] Object columns: detect `""`, `"-"`, `"N/A"`, `"null"` as null-like values
- [ ] `suggested_actions` provides a hint string per flagged column
- [ ] `zero_count` is `null` (not 0) for non-numeric columns in response
- [ ] Test: messy fixture → all null/zero counts correct
- [ ] Test: `include_zeros=False` → zero counts omitted from results
- [ ] Test: `min_count=5` → columns with fewer than 5 issues excluded
- [ ] Test: clean dataset → `flagged_columns: 0`, `results: {}`

---

#### Tool: `enrich_with_geo`

| Field | Value |
|---|---|
| Tier | 2 — Medium |
| Notebook source | §04 Enriching Data (geo merge, mismatch detection) |
| Writes to disk | Yes — saves enriched CSV |
| Docstring | `"Merge dataset with geo data on a location key. Saves result."` |

**Signature**

```python
def enrich_with_geo(
    file_path: str,
    geo_file_path: str,
    join_column: str,         # column in main dataset to join on
    geo_join_column: str,     # column in geo dataset to join on
    output_path: str = "",    # save path; "" = overwrite file_path
    dry_run: bool = False,
) -> dict
```

**Return fields**

```python
{
    "success": True,
    "op": "enrich_with_geo",
    "rows_before": 9648,
    "rows_after": 9648,
    "matched": 9601,
    "unmatched_main": ["Unknown State", "N/A"],
    "unmatched_geo": ["Puerto Rico", "District of Columbia"],
    "new_columns": ["geometry", "State_Code"],
    "output_file": "sales_enriched.csv",
    "backup": ".mcp_versions/sales_2026-03-27T10-00-00Z.bak",
    "progress": [...],
    "token_estimate": 195,
}
```

**Development checklist**

- [ ] Mismatch detection: print values in main not in geo and vice versa (notebook §04)
- [ ] Left join by default — no rows dropped from main dataset
- [ ] `unmatched_main` capped at 20 values in response
- [ ] Geometry column serialised as WKT string in output CSV
- [ ] `dry_run=True` → returns match stats without writing
- [ ] Snapshot of `file_path` taken before write
- [ ] Test: exact match → `unmatched_main: []`, all rows enriched
- [ ] Test: partial match → unmatched lists populated correctly
- [ ] Test: `join_column` not in dataset → error with hint
- [ ] Test: dry_run → no file written

---

#### Tool: `validate_dataset`

| Field | Value |
|---|---|
| Tier | 2 — Medium |
| Notebook source | §05 Data Validation |
| Writes to disk | No |
| Docstring | `"Validate dataset quality: types nulls duplicates ranges. Report."` |

**Signature**

```python
def validate_dataset(
    file_path: str,
    expected_dtypes: dict = None,   # {"col": "float64", ...} — pass as JSON string in practice
    max_null_pct: float = 5.0,      # flag columns exceeding this null %
    check_duplicates: bool = True,
) -> dict
```

**Return fields**

```python
{
    "success": True,
    "op": "validate_dataset",
    "passed": False,
    "score": 82,                     # 0–100 quality score
    "issues": [
        {"severity": "error",   "column": "Revenue",  "issue": "23 nulls (0.24%)"},
        {"severity": "warning", "column": "Discount", "issue": "156 zeros (1.62%)"},
        {"severity": "info",    "column": None,        "issue": "47 duplicate rows"},
    ],
    "dtype_mismatches": {"Order Date": {"expected": "datetime64", "actual": "object"}},
    "duplicate_count": 47,
    "null_summary": {"Revenue": 23, "Units Sold": 7},
    "progress": [...],
    "token_estimate": 260,
}
```

**Development checklist**

- [ ] Quality score formula: 100 - (null_pct_penalty + dup_penalty + type_penalty)
- [ ] `severity` levels: `"error"` (nulls > max_null_pct), `"warning"` (zeros), `"info"` (low impact)
- [ ] `expected_dtypes=None` skips dtype checking
- [ ] `passed=True` only when issues list is empty
- [ ] Test: clean dataset → `passed: true`, score near 100
- [ ] Test: messy fixture → all issue types reported correctly
- [ ] Test: `expected_dtypes` with mismatch → dtype issue in issues list
- [ ] Test: `check_duplicates=False` → no duplicate check in results

---

#### Tool: `compute_aggregations`

| Field | Value |
|---|---|
| Tier | 2 — Medium |
| Notebook source | §08.02–08.03 (groupby sum/count/mean in chart drafting) |
| Writes to disk | No |
| Docstring | `"Group by columns and aggregate. agg: sum mean count min max."` |

**Signature**

```python
def compute_aggregations(
    file_path: str,
    group_by: list[str],          # columns to group on
    agg_column: str,              # column to aggregate
    agg_func: str = "sum",        # "sum" | "mean" | "count" | "min" | "max"
    sort_desc: bool = True,       # sort result by agg value descending
    top_n: int = 0,               # 0 = all; >0 = return top N groups
) -> dict
```

**Return fields**

```python
{
    "success": True,
    "op": "compute_aggregations",
    "group_by": ["Region"],
    "agg_column": "Revenue",
    "agg_func": "sum",
    "groups": 5,
    "returned": 5,
    "result": [
        {"Region": "West",  "Revenue": 4823000.0},
        {"Region": "South", "Revenue": 4102000.0},
        ...
    ],
    "truncated": False,
    "progress": [...],
    "token_estimate": 145,
}
```

**Development checklist**

- [ ] `result` rows capped at `get_max_rows()` — truncated flag if exceeded
- [ ] `top_n > 0` applies before row cap (top_n takes priority)
- [ ] `agg_func="count"` works on any column type (not just numeric)
- [ ] `group_by` columns must all exist — error lists missing columns
- [ ] Test: single group-by sum → values match manual groupby
- [ ] Test: multi-column group-by → compound key rows returned
- [ ] Test: `top_n=3` → only 3 rows regardless of dataset size
- [ ] Test: `agg_func="count"` on categorical column → correct counts
- [ ] Test: group_by column not found → error with hint

---

#### Tool: `run_cleaning_pipeline`

| Field | Value |
|---|---|
| Tier | 2 — Medium |
| Notebook source | §02–§03 combined (full preprocessing sequence) |
| Writes to disk | Yes — single snapshot before pipeline starts |
| Docstring | `"Run ordered cleaning ops in one call. Single snapshot taken."` |

**Signature**

```python
def run_cleaning_pipeline(
    file_path: str,
    ops: list[dict],       # same op format as apply_patch
    dry_run: bool = False,
) -> dict
```

This tool is a higher-level wrapper over `apply_patch` that:
1. Takes a single snapshot before all ops
2. Applies each op sequentially (same engine functions as `apply_patch`)
3. Returns a consolidated summary with per-op results
4. Appends one receipt entry for the whole pipeline

The key difference from `apply_patch`: this tool is intended for multi-step
cleaning sequences that span multiple op types (clean_text + cast + fill_nulls
+ drop_duplicates) as a single atomic operation.

**Return fields**

```python
{
    "success": True,
    "op": "run_cleaning_pipeline",
    "total_ops": 4,
    "applied": 4,
    "summary": [
        {"op": "clean_text",      "columns_affected": 7},
        {"op": "cast_column",     "column": "Order Date", "from": "object", "to": "datetime64"},
        {"op": "fill_nulls",      "column": "Revenue", "filled": 23},
        {"op": "drop_duplicates", "dropped": 47},
    ],
    "backup": ".mcp_versions/sales_2026-03-27T10-00-00Z.bak",
    "progress": [...],
    "token_estimate": 195,
}
```

**Development checklist**

- [ ] One snapshot taken before pipeline, not per-op
- [ ] Internally delegates each op to same engine functions as `apply_patch`
- [ ] Failure on op N → restore from snapshot, report which op failed
- [ ] `dry_run=True` → simulate all ops, return `would_change` per op
- [ ] Single receipt log entry for the full pipeline (not one per op)
- [ ] Test: 4-op pipeline → all ops applied, one backup created
- [ ] Test: op 3 of 4 fails → file restored to pre-pipeline state
- [ ] Test: dry_run → `would_change` per op, no file modification
- [ ] Test: empty ops list → returns error ("at least one op required")

---

### TIER 3 — `servers/data_advanced/`

---

#### Tool: `generate_profile_report`

| Field | Value |
|---|---|
| Tier | 3 — Advanced |
| Notebook source | §06.01 YData Profiling |
| Writes to disk | Yes — HTML report |
| Docstring | `"Run ydata-profiling on dataset. Saves HTML report to disk."` |

**Signature**

```python
def generate_profile_report(
    file_path: str,
    output_path: str = "",        # "" = same dir as file, named {stem}_profile.html
    title: str = "",              # report title; "" = filename stem
    description: str = "",        # dataset description in report
    correlations: bool = True,    # compute pearson/spearman/kendall/phi_k
    minimal: bool = False,        # minimal=True skips heavy correlation compute
) -> dict
```

**Return fields**

```python
{
    "success": True,
    "op": "generate_profile_report",
    "report_path": "sales_profile.html",   # Path.name only
    "report_size_kb": 4210,
    "columns_profiled": 12,
    "rows": 9648,
    "correlations_included": True,
    "progress": [...],
    "token_estimate": 120,
}
```

**Development checklist**

- [ ] Memory check before running: `psutil.virtual_memory().available` — warn if < 2 GB
- [ ] `minimal=True` passes `minimal=True` to `ProfileReport` for fast execution
- [ ] `correlations=False` disables all correlation types
- [ ] Output path defaults to `{file_stem}_profile.html` in same directory as input
- [ ] Never return raw report content — return path only
- [ ] Redirect stdout during profiling (ydata prints to stdout) — capture to stderr
- [ ] Test: small fixture → report HTML file created on disk
- [ ] Test: `minimal=True` → completes faster, file still created
- [ ] Test: output_path specified → file created at that path
- [ ] Test: insufficient memory → warning in progress, minimal mode auto-fallback

---

#### Tool: `generate_sweetviz_report`

| Field | Value |
|---|---|
| Tier | 3 — Advanced |
| Notebook source | §06.02 SweetViz |
| Writes to disk | Yes — HTML report |
| Docstring | `"Run SweetViz EDA on dataset. Saves HTML report to disk."` |

**Signature**

```python
def generate_sweetviz_report(
    file_path: str,
    output_path: str = "",      # "" = {stem}_sweetviz.html
    target_column: str = "",    # optional target feature for analysis
) -> dict
```

**Return fields**

```python
{
    "success": True,
    "op": "generate_sweetviz_report",
    "report_path": "sales_sweetviz.html",
    "report_size_kb": 2840,
    "columns_analysed": 12,
    "target_column": "",
    "progress": [...],
    "token_estimate": 110,
}
```

**Development checklist**

- [ ] Suppress SweetViz browser auto-open: use `show_html(..., open_browser=False)`
- [ ] `target_column` passed to `sv.analyze(df, target_feat=target_column)`
- [ ] Redirect SweetViz stdout/stderr — use `contextlib.redirect_stdout`
- [ ] Test: basic report created on disk
- [ ] Test: `target_column="Revenue"` → report includes target analysis
- [ ] Test: target column not found → error with hint

---

#### Tool: `generate_autoviz_report`

| Field | Value |
|---|---|
| Tier | 3 — Advanced |
| Notebook source | §06.03 AutoViz |
| Writes to disk | Yes — chart files (HTML/SVG) |
| Docstring | `"Run AutoViz auto-EDA on dataset. Saves charts to output dir."` |

**Signature**

```python
def generate_autoviz_report(
    file_path: str,
    output_dir: str = "",       # "" = {file_dir}/autoviz_output/
    chart_format: str = "html", # "html" | "svg" | "bokeh"
    max_rows_analyzed: int = 0, # 0 = all; large datasets should sample
) -> dict
```

**Return fields**

```python
{
    "success": True,
    "op": "generate_autoviz_report",
    "output_dir": "autoviz_output",   # Path.name only
    "chart_files": ["Revenue_bar.html", "Region_pie.html", ...],
    "chart_count": 8,
    "rows_analyzed": 9648,
    "progress": [...],
    "token_estimate": 130,
}
```

**Development checklist**

- [ ] Redirect AutoViz stdout (it prints heavily) — capture to `/dev/null` or stderr
- [ ] `chart_format="bokeh"` fallback: try bokeh, except → fall back to html
- [ ] `max_rows_analyzed > 0` → sample dataset before passing to AutoViz
- [ ] Return list of created files — scan output directory after completion
- [ ] Test: html format → output dir created, HTML files present
- [ ] Test: `max_rows_analyzed=500` → sampled dataset used
- [ ] Test: output_dir specified → files in correct location

---

#### Tool: `generate_chart`

| Field | Value |
|---|---|
| Tier | 3 — Advanced |
| Notebook source | §08.03–08.11 (bar, pie, geo, time_series, treemap, scatter, radius) |
| Writes to disk | Yes — saves HTML chart file |
| Docstring | `"Generate Plotly chart. type: bar pie line scatter geo treemap radius."` |

**Signature**

```python
def generate_chart(
    file_path: str,
    chart_type: str,              # "bar"|"pie"|"line"|"scatter"|"geo"|"treemap"|"time_series"|"radius"
    value_column: str,            # numeric column to plot (y-axis / values)
    category_column: str = "",    # categorical column (x-axis / color / group)
    agg_func: str = "sum",        # "sum" | "mean" | "count"
    color_column: str = "",       # column for color encoding
    date_column: str = "",        # datetime column for time_series chart_type
    period: str = "M",            # time period: "Y" | "Q" | "M" | "W" | "D"
    hierarchy_columns: list[str] = None,  # treemap: ["Region","State","City"]
    geo_file_path: str = "",      # required for chart_type="geo"
    geo_join_column: str = "",    # join key in geo file
    output_path: str = "",        # "" = {stem}_{chart_type}.html
    title: str = "",              # "" = auto-generated from columns
    theme: str = "plotly_dark",   # plotly template name
) -> dict
```

**Chart type → parameter mapping**

| `chart_type` | Required params | Optional params |
|---|---|---|
| `"bar"` | `value_column`, `category_column` | `agg_func`, `color_column`, `title` |
| `"pie"` | `value_column`, `category_column` | `agg_func`, `title` |
| `"line"` | `value_column`, `category_column` | `color_column`, `agg_func`, `title` |
| `"scatter"` | `value_column`, `category_column` | `color_column`, `title` |
| `"geo"` | `value_column`, `category_column`, `geo_file_path`, `geo_join_column` | `agg_func`, `title` |
| `"treemap"` | `value_column`, `hierarchy_columns` | `title` |
| `"time_series"` | `value_column`, `date_column` | `period`, `color_column`, `title` |
| `"radius"` | `category_column`, `value_column` | `title` (radar/spider chart) |

**Return fields**

```python
{
    "success": True,
    "op": "generate_chart",
    "chart_type": "bar",
    "output_path": "sales_bar.html",
    "title": "Total Revenue by Region",
    "rows_plotted": 5,
    "progress": [...],
    "token_estimate": 110,
}
```

**Development checklist**

- [ ] Dispatch to internal `_render_{chart_type}()` function per type
- [ ] `agg_func` applied before plotting — uses same logic as `compute_aggregations`
- [ ] `time_series`: `date_column` cast to datetime if object; period applied via `dt.to_period()`
- [ ] `geo`: merges with geo file using geopandas before rendering `px.choropleth_mapbox`
- [ ] `radius`: renders `go.Scatterpolar` — requires `category_column` (group) + multiple `value_column` (comma-sep list)
- [ ] `theme` validated against plotly template list — default `plotly_dark` on error
- [ ] Chart saved as self-contained HTML (no CDN calls) — `fig.write_html(..., include_plotlyjs='cdn')` is forbidden
- [ ] `title=""` → auto-generate from `"{agg_func} of {value_column} by {category_column}"`
- [ ] Test: bar chart → HTML file created, title contains column names
- [ ] Test: pie chart → HTML file created
- [ ] Test: time_series with date column → period grouping applied correctly
- [ ] Test: treemap with hierarchy_columns → nested treemap rendered
- [ ] Test: geo chart → merges with geo file, choropleth created
- [ ] Test: unknown chart_type → error listing valid types
- [ ] Test: value_column not numeric for bar/scatter → error with hint

---

#### Tool: `generate_dashboard`

| Field | Value |
|---|---|
| Tier | 3 — Advanced |
| Notebook source | §09 Create Dashboard, §10 Run Dashboard |
| Writes to disk | Yes — writes `app.py` Streamlit file |
| Docstring | `"Generate Streamlit dashboard app.py from dataset. Run separately."` |

**Signature**

```python
def generate_dashboard(
    file_path: str,
    output_path: str = "",         # "" = {file_dir}/app.py
    title: str = "",               # dashboard page title; "" = filename stem
    chart_types: list[str] = None, # charts to include; None = auto-detect from dtypes
    geo_file_path: str = "",       # include map tab if provided
    theme: str = "plotly_dark",    # plotly chart theme
    dry_run: bool = False,
) -> dict
```

**What it generates**

The engine writes a self-contained `app.py` that:
1. `st.set_page_config()` with title and wide layout
2. Loads the dataset from `file_path` (hardcoded path — local only)
3. Sidebar filters for categorical columns (up to 3)
4. KPI metric cards for all numeric columns (`go.Indicator`)
5. Auto-detected charts based on column dtypes:
   - Numeric + categorical → bar chart
   - Datetime + numeric → time series
   - Two numeric → scatter
   - Categorical frequency → pie
   - Geo file provided → choropleth tab
6. `st.plotly_chart()` for each chart with `use_container_width=True`

**Return fields**

```python
{
    "success": True,
    "op": "generate_dashboard",
    "output_path": "app.py",
    "dashboard_title": "Sales Dashboard",
    "charts_included": ["bar", "time_series", "scatter", "pie"],
    "kpi_columns": ["Revenue", "Units Sold", "Profit"],
    "filter_columns": ["Region", "Product", "Category"],
    "run_command": "streamlit run app.py",
    "progress": [...],
    "token_estimate": 140,
}
```

**Development checklist**

- [ ] Generated `app.py` is syntactically valid Python — validate with `py_compile`
- [ ] Dataset path in `app.py` written as absolute path resolved from `file_path`
- [ ] No hardcoded CDN calls or internet dependencies in generated file
- [ ] `chart_types=None` → auto-detect: scan dtypes (numeric→bar, datetime→time_series, etc.)
- [ ] KPI cards use `go.Indicator` matching notebook §08.02 pattern
- [ ] Sidebar filters use `st.sidebar.multiselect()` for categorical columns
- [ ] `dry_run=True` → return `would_generate` summary without writing `app.py`
- [ ] `py_compile.compile(output_path)` called after write — error if generated code invalid
- [ ] Test: generates valid `app.py` — `py_compile` passes
- [ ] Test: `chart_types=["bar", "pie"]` → only those chart sections in output
- [ ] Test: `geo_file_path` provided → map tab included in generated code
- [ ] Test: `dry_run=True` → `app.py` not created
- [ ] Test: existing `app.py` overwritten — snapshot taken first
