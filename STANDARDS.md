# STANDARDS.md — General-Purpose MCP Server Development Standards

This document defines the complete standard for building open-source MCP
(Model Context Protocol) servers with local execution engines. It is derived from
real architectural decisions, real failure modes, and real hardware constraints
encountered while building production-grade MCP tooling across multiple domains:
document editing, data analytics, machine learning, media processing, system
automation, and more.

These standards apply to any MCP server regardless of domain. They are not optional
guidelines. They are the rules that determine whether a server works reliably on
constrained hardware with a local LLM — or at all.

---

## Table of Contents

1. [Why these standards exist](#1-why-these-standards-exist)
2. [The core mental model](#2-the-core-mental-model)
3. [MCP Primitives — Tools, Resources, and Prompts](#3-mcp-primitives--tools-resources-and-prompts)
4. [The self-hosted execution principle](#4-the-self-hosted-execution-principle)
5. [Language and runtime selection](#5-language-and-runtime-selection)
6. [Repository structure](#6-repository-structure)
7. [The three-tier split](#7-the-three-tier-split)
8. [Tool count discipline](#8-tool-count-discipline)
9. [The four-tool pattern](#9-the-four-tool-pattern)
10. [Surgical read protocol](#10-surgical-read-protocol)
11. [Tool schema design](#11-tool-schema-design)
12. [Tool annotations](#12-tool-annotations)
13. [The patch protocol](#13-the-patch-protocol)
14. [Engine and server separation](#14-engine-and-server-separation)
15. [Engine sub-module pattern](#15-engine-sub-module-pattern)
16. [Return value contract](#16-return-value-contract)
17. [Error handling contract](#17-error-handling-contract)
18. [Security considerations](#18-security-considerations)
19. [State and version control](#19-state-and-version-control)
20. [Token budget discipline](#20-token-budget-discipline)
21. [Hardware tiers and resource constraints](#21-hardware-tiers-and-resource-constraints)
22. [Progress output](#22-progress-output)
23. [Long-running operations and async](#23-long-running-operations-and-async)
24. [Live state and reload](#24-live-state-and-reload)
25. [Operation receipt log](#25-operation-receipt-log)
26. [Output generation pattern](#26-output-generation-pattern)
27. [Testing standards](#27-testing-standards)
28. [Cross-platform compatibility](#28-cross-platform-compatibility)
29. [Multi-client compatibility](#29-multi-client-compatibility)
30. [Transport modes](#30-transport-modes)
31. [Installation and distribution](#31-installation-and-distribution)
32. [Naming conventions](#32-naming-conventions)
33. [Dependency policy](#33-dependency-policy)
34. [CI/CD requirements](#34-cicd-requirements)
35. [Documentation requirements](#35-documentation-requirements)
36. [Smart domain inference pattern](#36-smart-domain-inference-pattern)
37. [What to never do](#37-what-to-never-do)
38. [Checklist — new server from scratch](#38-checklist--new-server-from-scratch)
39. [Checklist — new tool in existing server](#39-checklist--new-tool-in-existing-server)
40. [Domain reference table](#40-domain-reference-table)

---

## 1. Why These Standards Exist

Most MCP servers are built for cloud AI with large context windows, fast inference,
and the assumption of always-on internet. When those servers are used with local LLMs
on consumer hardware, they fail in predictable ways:

- Tool schemas consume 20–30% of the available context window before any work starts
- Tools return entire datasets when only a slice is needed
- A single "helpful" tool does too many things, confusing smaller models
- No version control means a bad tool call silently corrupts user data
- Tools rely on external APIs, leaking data and requiring paid subscriptions
- Install requires 15 manual steps, so nobody uses it

These standards exist to prevent all of these failures by design, not by luck.

The two target constraints that drive every decision in this document:

> **Constraint 1 (hardware):** A user with an 8GB GPU, running a 9B parameter local
> model, doing real work on real data, must be able to use these tools without hitting
> context limits, without corrupting data, and without needing a developer to set it up.

> **Constraint 2 (sovereignty):** All execution happens locally. No data leaves the
> user's machine. No API keys. No cloud subscriptions. No dependency on a third-party
> server being up. The tool runs on the user's hardware using the user's resources.

Every rule in this document traces back to one or both of these constraints.

---

## 2. The Core Mental Model

An MCP server is a **structured API that a language model calls with JSON arguments
and receives JSON results from**. It is not a chat assistant. It is not a script
runner. It is not an AI agent. It is a deterministic function executor.

The model provides intelligence — deciding what to call, in what order, with what
arguments. The MCP server provides execution — doing the operation reliably, returning
structured confirmation, never guessing.

```
Model's job:        understand intent, choose tools, generate arguments, decide next step
MCP server's job:   validate input, execute operation, return structured result
```

The server must never cross into the model's job. No AI inference inside tools. No
"smart" behavior that guesses what the user probably meant. No tools that decide
to do extra work beyond what was asked. Deterministic in, deterministic out.

This boundary is what makes the server testable, debuggable, and reliable on
hardware-constrained machines.

---

## 3. MCP Primitives — Tools, Resources, and Prompts

The MCP protocol defines three primitives: Tools, Resources, and Prompts. Use each
for what it is designed for — do not substitute one for another.

### Tools

**Tools** are the primary primitive. They are called by the model with arguments and
return structured JSON. Use tools for: all operations that read, transform, write, or
execute something in response to model intent.

Tools are the right choice for:
- Reading a file or database (result changes between calls)
- Applying a patch or transformation to data
- Running a computation or analysis
- Generating an output file

### Resources

**Resources** expose stable, re-readable context that the model can include in its
conversation without a tool call. Use resources for: schemas that never change between
calls, reference data the model needs repeatedly (e.g. a database schema, a list of
supported operations, a config file). Resources are read-only and stateless.

Do not use resources for data that changes between calls. If the data can change, it
must be a tool.

Example of a Resource that belongs in a data server:

```python
@mcp.resource("schema://{file_path}")
def get_schema_resource(file_path: str) -> str:
    """Expose dataset schema as a resource the model can read without a tool call."""
    return engine.get_schema_json(file_path)
```

Do not use Resources as a workaround for surgical read tools. If the data changes
between calls, it must be a tool.

### Prompts

**Prompts** are reusable prompt templates registered with the server. Use prompts
sparingly — only when you need to give the model a structured starting workflow that
users will invoke directly (e.g., a "clean this dataset" workflow prompt). Most
servers do not need prompts.

### Rule of thumb

```
Model needs to call it to do work          → Tool
Model needs to reference it for context    → Resource
User needs a starting workflow template    → Prompt
```

---

## 4. The Self-Hosted Execution Principle

This is the founding principle that distinguishes this standard from cloud-first MCP
servers.

### What it means

Every tool in every server must execute its core operation using local resources:
local CPU, local GPU, local RAM, local disk, local processes. The tool must not
require an internet connection to perform its primary function.

### What is allowed

- Reading from and writing to local files and databases
- Spawning local subprocesses (FFmpeg, Tesseract, LibreOffice, etc.)
- Using local GPU via CUDA/ROCm/Metal through standard libraries
- Communicating with services the user runs locally (local Postgres, local Docker,
  local MLflow, local Redis)
- Optional enhancement via network (e.g., downloading a model weight on first run)
  provided the tool degrades gracefully without it

### What is not allowed as a primary execution path

- Calling a third-party paid API to perform the core operation (OpenAI, Google Cloud
  Vision, AWS Textract, Cohere, etc.)
- Requiring OAuth or API key authentication to function
- Sending user data to an external server as part of normal tool execution
- Depending on a cloud service being online for the tool to succeed

### The test

Ask this question for every tool: **"Can this tool complete its primary operation
with the machine disconnected from the internet?"** If the answer is no, the tool
violates this standard.

### Exceptions

Network access is permitted for:
- One-time model/asset downloads on first run (with local caching)
- Tools explicitly scoped to network operations (web scraper, feed reader, sitemap
  crawler) where network access is the stated purpose
- Optional telemetry that is disabled by default

Document every exception clearly in the tool's docstring and in the README.

---

## 5. Language and Runtime Selection

### The rule: libraries dictate language, not preference

Choose the language that has the best library support for your domain. Not the
language you prefer. Not the language that is fashionable. The language where the
problem is already solved locally.

| Domain | Recommended language | Primary local libraries |
|---|---|---|
| Document editing (docx, xlsx, pptx) | Python | python-docx, openpyxl, python-pptx |
| PDF manipulation | Python | PyMuPDF, pdfplumber, reportlab |
| Data analytics | Python | polars, duckdb, pandas, ydata-profiling |
| Machine learning | Python | scikit-learn, XGBoost, LightGBM, FLAML |
| Deep learning / GPU | Python | PyTorch, ONNX Runtime |
| Image processing | Python | Pillow, OpenCV, scikit-image |
| Audio processing | Python | librosa, pydub |
| Video processing | Python | MoviePy, FFmpeg (subprocess) |
| OCR | Python | easyocr, surya, pytesseract |
| Browser automation | Python or TypeScript | playwright-python or playwright (Node) |
| Database (SQL) | Python or TypeScript | duckdb, sqlite3, psycopg2 |
| System / OS automation | Python or Go | psutil, subprocess; Go single binary |
| Web scraping | Python | playwright, BeautifulSoup, httpx |
| File system operations | Go or Rust | Single binary, no runtime, fastest |
| Web API wrappers | TypeScript | Best for JSON-heavy REST APIs |
| Geospatial | Python | geopandas, shapely, rasterio |
| Time series | Python | statsforecast, sktime, neuralprophet |
| Security tools | Python or Rust | cryptography, bandit, detect-secrets |

### Python setup

Pin Python to `3.12`. Use `uv` as the package manager. Never use pip directly in
production. Never use conda. Never use poetry for new projects.

```
# .python-version
3.12
```

```toml
# pyproject.toml
[project]
requires-python = ">=3.12"
```

Pin `fastmcp` version in `pyproject.toml`. The FastMCP API surface has changed
between major versions. Breaking changes in the tool registration API have caused
silent failures where tools appear to register but are never served.

```toml
[project]
dependencies = [
    "fastmcp>=2.0,<3.0",
    ...
]
```

### TypeScript setup

Use Node.js 20 LTS. Use `npm` with `package-lock.json` committed. Pin the
`@modelcontextprotocol/sdk` version.

### Go setup

Use Go 1.22+. Commit `go.sum`. Use `github.com/mark3labs/mcp-go`.

### Rust setup

Use Rust stable channel. Use the `rmcp` crate. Commit `Cargo.lock`.

---

## 6. Repository Structure

### Monorepo is the default for multi-server projects

If you are building more than one MCP server that share a domain or pipeline
(e.g., data profiler + data cleaner + statistical analysis, or Word + Excel +
PowerPoint), use a monorepo with a workspace. One repo, one lockfile, one CI
pipeline, one install script.

### Monorepo layout

```
{project-name}/
│
├── shared/                         # code shared by ALL servers — never duplicate
│   ├── __init__.py
│   ├── version_control.py          # snapshot / rollback
│   ├── patch_validator.py          # validate op arrays before applying
│   ├── file_utils.py               # path resolution, atomic writes, JSON helpers
│   ├── platform_utils.py           # OS detection, hardware mode flags
│   ├── progress.py                 # ok/fail/info/warn/undo progress helpers
│   └── receipt.py                  # operation receipt log
│
├── servers/
│   ├── {domain}_{tier}/            # e.g. data_basic, ml_basic, office_basic
│   │   ├── __init__.py
│   │   ├── server.py               # FastMCP setup + tool definitions (thin)
│   │   ├── engine.py               # pure domain logic (no MCP imports)
│   │   └── pyproject.toml
│   │
│   ├── {domain}_medium/
│   │   └── ...
│   │
│   └── {domain}_advanced/
│       └── ...
│
├── tests/
│   ├── fixtures/                   # real test data, committed to repo
│   ├── conftest.py
│   └── test_{server_name}.py
│
├── install/
│   ├── install.sh                  # Linux / macOS — POSIX sh compatible
│   ├── install.bat                 # Windows CMD
│   └── mcp_config_writer.py        # writes to AI client config files
│
├── pyproject.toml                  # root workspace
├── uv.lock
├── .python-version
├── .gitattributes
├── .editorconfig
├── CLAUDE.md
├── STANDARDS.md
└── README.md
```

### Single-server flat layout

```
{server-name}/
├── server.py
├── engine.py
├── shared/
├── tests/
│   ├── fixtures/
│   └── test_engine.py
├── install/
│   ├── install.sh
│   └── install.bat
├── pyproject.toml
├── uv.lock
└── README.md
```

---

## 7. The Three-Tier Split

Every MCP server targets a specific complexity tier. Never mix tiers in one server.
This is the most important structural decision because it directly controls how many
tools the local model has to reason about at once.

### Tier 1 — Basic (CRUD and direct operations)

Tools that read data and perform simple create/update/delete operations on individual
nodes. No complex transformations. No multi-step pipelines. No cross-element
operations.

**Examples across domains:**
- Documents: read paragraph, replace text, add row to table
- Data: load file, read column stats, filter rows, export slice
- ML: load dataset, inspect schema, run single model, get prediction
- System: read file, list processes, get CPU stats
- Media: read metadata, crop image, extract audio segment

**Tool count target: 6–8 tools.**
This tier must stand alone — a user doing simple tasks should never need to load
tier 2 or 3.

### Tier 2 — Medium (structured and pipeline operations)

Tools that perform multi-step structured operations — formulas, conditional logic,
template filling, batch transforms, pipeline stages.

**Examples across domains:**
- Documents: fill template placeholders, apply formula set, manage relationships
- Data: run profiling report, apply cleaning pipeline, compute aggregations
- ML: train with cross-validation, run feature engineering, compare models
- System: execute multi-step job, manage process groups, schedule tasks
- Media: transcode batch, apply filter chain, run OCR pipeline

**Tool count target: 5–7 tools.**
Can be loaded alongside tier 1. Total combined should not exceed 15 tools.

### Tier 3 — Advanced (layout, visual, export, optimization)

Tools that deal with visual layout, formatting, model optimization, export operations,
and complex cross-element interactions. These require more context to use correctly
and are rarely needed alongside tier 1 operations.

**Examples across domains:**
- Documents: set styles, create charts, export to PDF, manage layouts
- Data: generate interactive dashboard, export profiling report, build visualization
- ML: hyperparameter optimization, model export (ONNX, pickle), deployment package
- System: performance profiling, resource optimization, audit report
- Media: video rendering, style transfer, full pipeline export

**Tool count target: 5–6 tools.**
Load standalone — advanced tools are used in dedicated sessions.

### Decision tree for tier assignment

```
Does the tool read or write a single named node (row, cell, file, record, frame)?
  Yes → Tier 1

Does the tool apply a structured pipeline, rule, or multi-step transform?
  Yes → Tier 2

Does the tool change visual appearance, export format, or optimize a model/asset?
  Yes → Tier 3

Does the tool span all three concerns?
  → Split it into multiple tools, one per tier
```

---

## 8. Tool Count Discipline

### Hard limits by hardware target

| Target hardware | Max tools per server | Max tools loaded simultaneously |
|---|---|---|
| 4–6 GB VRAM (≤7B model) | 6 | 6 (one server only) |
| 8 GB VRAM (9B model) | 8 | 12 (tier 1 + tier 2) |
| 12–16 GB VRAM (14B model) | 10 | 16 (any two servers) |
| 24 GB+ VRAM (32B+ model) | 12 | 20 |

The general open-source target is **8 GB VRAM**. Design for 8 tools per server,
12 tools maximum loaded simultaneously.

### Why tool count matters more than you think

Every tool's JSON schema sits in the model's KV cache for the entire conversation.
At 8 GB VRAM with a 9B model, the KV cache budget is approximately 10,000–12,000
tokens. Each tool schema with docstring and 3–4 parameters consumes approximately
80–120 tokens. Ten tools consume roughly 1,000 tokens — ~8–10% of the entire context
window — before the model has seen a single byte of user data.

Beyond tokens, there is a cognitive cost: the model must choose from N tools on every
turn. At 8 tools, a 9B model makes correct selections reliably. At 15 tools, errors
increase. At 20 tools, the model frequently confuses similar tools.

**The rule: fewer tools, sharper tools.** A tool that does one thing precisely is
better than a tool that does three things approximately.

---

## 9. The Four-Tool Pattern

Every data or state-changing task follows this exact four-step loop. Encode this in
your tool design so the model is guided through it naturally.

```
LOCATE  →  INSPECT  →  PATCH  →  VERIFY
```

### Step 1: LOCATE

Find the node(s) that need changing without reading everything else.
Returns addresses, indices, IDs — zero actual content.

### Step 2: INSPECT

Read only the located node(s) to understand current state.
Returns one node in detail, bounded by size limits.

### Step 3: PATCH

Apply the targeted edit to only that node.
Returns a confirmation dict with what changed.

### Step 4: VERIFY

Read back only the edited node to confirm the change applied correctly.
Same read tool as Step 2 — model confirms result matches intent.

### Example — data analytics domain

```
User: "Fix the missing values in the revenue column of sales_q3.csv"

Round 1 (LOCATE):   search_columns(file="sales_q3.csv", has_nulls=True)
                    → {"columns": ["revenue", "discount"], "null_counts": {"revenue": 23}}

Round 2 (INSPECT):  read_column_stats(file="sales_q3.csv", column="revenue")
                    → {"mean": 4200, "median": 3800, "null_count": 23, "dtype": "float64"}

Round 3 (PATCH):    fill_nulls(file="sales_q3.csv", column="revenue", strategy="median")
                    → {"success": true, "filled": 23, "value_used": 3800, "backup": "..."}

Round 4 (VERIFY):   read_column_stats(file="sales_q3.csv", column="revenue")
                    → {"mean": 4190, "median": 3800, "null_count": 0, "dtype": "float64"}
```

### Example — machine learning domain

```
User: "Train a classifier on customer_churn.csv targeting the 'churned' column"

Round 1 (LOCATE):   inspect_dataset(file="customer_churn.csv")
                    → {"rows": 5000, "columns": 18, "target_candidates": ["churned", "active"]}

Round 2 (INSPECT):  read_column_profile(file="customer_churn.csv", column="churned")
                    → {"dtype": "bool", "class_balance": {"True": 0.23, "False": 0.77}}

Round 3 (PATCH):    train_classifier(file="customer_churn.csv", target="churned",
                                     model="xgboost", test_size=0.2)
                    → {"success": true, "accuracy": 0.87, "f1": 0.79, "model_path": "..."}

Round 4 (VERIFY):   read_model_report(model_path="...")
                    → {"confusion_matrix": [...], "feature_importance": [...]}
```

### Anti-patterns that violate this pattern

- A tool that searches AND reads AND edits in one call → split into three
- A tool that returns the full dataset so the model can "find what it needs"
- A tool that writes without returning what changed
- A tool that trains a model and also evaluates and also exports in one call

---

## 10. Surgical Read Protocol

### The fundamental rule

A tool that returns data must return **only the data the model asked for**. Not
surrounding context. Not related data "that might be useful". Not the full parent
structure for convenience.

This is the most important performance decision in the entire server.

### Mandatory tool classes for every domain

**Index tool** — returns structure without content:

```python
# Returns keys, addresses, counts, metadata — zero actual content
get_dataset_schema()        # column names + dtypes + row count, not values
get_model_summary()         # model type + params + metrics, not weights
get_document_outline()      # heading text + indices, not body text
list_processes()            # PIDs + names, not memory maps
list_image_metadata()       # filenames + dimensions + format, not pixels
```

**Search tool** — scans content, returns matching addresses:

```python
# Returns matches with addresses — caller reads only those
search_columns(has_nulls=True)          # column names, not full columns
search_rows(where="revenue > 10000")    # row indices, not full rows
search_files(pattern="*.csv")           # file paths, not file contents
search_logs(level="ERROR")              # line numbers, not surrounding lines
```

**Bounded read tool** — reads a specific address with a hard size limit:

```python
# Returns exactly one node or a bounded slice
read_rows(file, start, end)             # bounded row range
read_column_stats(file, column)         # stats for one column, not all rows
read_model_metrics(model_path)          # metrics dict, not full predictions
read_log_range(path, start, end)        # bounded lines, hard cap enforced
```

### Return size limits by data type

Enforce these limits in the engine, not in the model.

| Data type | Default limit | 8 GB mode limit | Enforcement |
|---|---|---|---|
| DataFrame rows | 100 per call | 20 per call | Error if exceeded |
| DataFrame columns | 50 per call | 20 per call | Truncate with warning |
| Search results | 50 per call | 10 per call | `max_results` parameter |
| Log lines | 100 per call | 50 per call | Error if exceeded |
| List items (generic) | 100 per call | 40 per call | Truncate with flag |
| JSON object depth | 5 levels | 3 levels | Flatten deeper structures |
| Text paragraphs | 50 per call | 20 per call | Error if exceeded |
| Image array pixels | Never raw | Never raw | Always return stats/path |
| Model weights | Never raw | Never raw | Always return path + summary |

Every tool response that was limited must include:

```python
{
    "truncated": True,
    "returned": 20,
    "total_available": 5000,
    "hint": "Use read_rows(file, start, end) to read specific row ranges."
}
```

### The token_estimate field

Every tool response must include a rough token count of its own output:

```python
response["token_estimate"] = len(str(response)) // 4
```

This is for the model to budget remaining context window capacity.

---

## 11. Tool Schema Design

### Docstring length — the 80-character rule

Every `@mcp.tool()` docstring must be 80 characters or fewer. These are sent to the
model on every turn. They are not documentation for humans — they are selection cues.

```python
# Good — 55 characters, unambiguous
"""Profile dataset columns. Returns stats and null counts."""

# Good — 61 characters
"""Train classifier on CSV. Returns accuracy, F1, model path."""

# Bad — 98 characters
"""This tool analyzes the dataset and returns detailed statistical information
about each column including mean, median, and missing value counts."""
```

Test this in CI: `assert len(tool.__doc__) <= 80`.

### Parameter naming

Parameters are lowercase `snake_case` nouns that describe what they contain.

```python
# Correct
file_path: str
column_name: str
model_path: str
target_column: str
max_results: int
test_size: float
dry_run: bool

# Wrong — verb-first
get_file_path: str
target_col: str       # abbreviation

# Wrong — camelCase
filePath: str
columnName: str
```

### Allowed parameter types

Only use these types in tool function signatures:

- `str` — paths, names, addresses, text, enum values as strings
- `int` — indices, counts, limits, seeds
- `float` — ratios, thresholds, percentages
- `bool` — flags with sensible defaults
- `list[dict]` — only for patch op arrays
- `list[str]` — only for column lists, file lists, label lists

Never use:

- `Optional[T]` — use `T = None` instead
- `Union[T, S]` — split into two tools or use a discriminated string
- `Any` — always type precisely
- `dict` — too vague; model hallucinates arbitrary keys
- Custom Pydantic models in tool signatures
- `Enum` — use `str` with valid values documented in the docstring

### Enum values in docstrings

```python
@mcp.tool()
def fill_nulls(
    file_path: str,
    column_name: str,
    strategy: str,       # "mean", "median", "mode", "ffill", "bfill", "drop"
    dry_run: bool = False,
) -> dict:
    """Fill null values in column. strategy: mean median mode ffill bfill drop."""
```

### The dry_run parameter

Every write tool must have `dry_run: bool = False`. When `True`, the tool returns
exactly what it would change without changing anything. This is the primary
trust-building feature for new users running destructive operations.

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

## 12. Tool Annotations

FastMCP and the MCP protocol support tool annotations that help AI clients display
and reason about tools correctly. Always set these.

```python
from fastmcp import FastMCP

mcp = FastMCP("server-name")

@mcp.tool(
    annotations={
        "readOnlyHint": True,       # does not modify any state
        "destructiveHint": False,   # does not destroy data
        "idempotentHint": True,     # safe to call multiple times
        "openWorldHint": False,     # does not interact with external services
    }
)
def read_column_stats(file_path: str, column: str) -> dict:
    """Stats for one column: mean median std min max nulls unique top."""
    return engine.read_column_stats(file_path, column)
```

### Annotation rules by tool type

| Tool type | readOnlyHint | destructiveHint | idempotentHint | openWorldHint |
|---|---|---|---|---|
| Read / inspect / search | True | False | True | False |
| Write / patch (with snapshot) | False | False | False | False |
| Delete / drop rows | False | True | False | False |
| Network / scrape / download | False | False | False | True |
| Export / generate HTML | False | False | True | False |

`destructiveHint=True` should trigger an extra confirmation prompt in most AI
clients. Use it for `drop_column`, `delete_rows`, `purge_versions`.

---

## 13. The Patch Protocol

### When to use a patch protocol

Any tool that modifies structured data should accept a **list of operations** rather
than a single operation when the task naturally involves multiple changes. A dataset
with 5 cleaning steps should be processed in one `apply_patch` call with 5 ops, not
5 separate tool calls.

### Standard op array format

```python
[
    {
        "op": "fill_nulls",
        "column": "revenue",
        "strategy": "median"
    },
    {
        "op": "drop_duplicates",
        "subset": ["customer_id", "date"]
    },
    {
        "op": "rename_column",
        "from": "rev",
        "to": "revenue_usd"
    }
]
```

Rules:
- `"op"` is always the first key
- All other fields are operation-specific required fields
- Maximum 50 ops per batch
- Ops are applied sequentially
- Validate entire array before applying any operation

### Op naming convention

Op names are `verb_noun` snake_case:

```
fill_nulls        drop_duplicates     rename_column
replace_text      set_cell            insert_row
train_model       export_report       apply_transform
```

Allowed verbs: `fill`, `drop`, `rename`, `replace`, `set`, `insert`, `delete`,
`add`, `update`, `move`, `train`, `export`, `apply`, `restore`

### Validation before execution

Always validate the entire op array **before** applying any operation:

```python
def apply_patch(file_path: str, ops: list[dict]) -> dict:
    valid, error = validate_ops(ops, ALLOWED_OPS)
    if not valid:
        return {"success": False, "error": error, "applied": 0}

    backup = snapshot(file_path)
    results = []

    for op in ops:
        result = _apply_single_op(file_path, op)
        results.append(result)
        if not result["success"]:
            return {
                "success": False,
                "error": f"Op {len(results)} failed: {result['error']}",
                "applied": len(results) - 1,
                "backup": backup,
                "hint": "Use restore_version to undo partial changes."
            }

    return {
        "success": True,
        "applied": len(ops),
        "backup": backup,
        "results": results,
        "token_estimate": ...,
    }
```

Stop on first failure. Never partially apply a batch and report success.

---

## 14. Engine and Server Separation

### The mandatory split

Every server has exactly two files for logic:

**`engine.py`** — pure domain logic, zero MCP imports:

```python
# engine.py imports
from pathlib import Path
from shared.version_control import snapshot
from shared.patch_validator import validate_ops
from shared.platform_utils import get_max_rows, is_constrained_mode
# domain library imports (polars, sklearn, Pillow, psutil, etc.)

# engine.py does NOT import:
# from mcp import ...
# from fastmcp import ...
```

**`server.py`** — thin MCP wrapper, zero domain logic:

```python
from mcp.server.fastmcp import FastMCP
from . import engine

mcp = FastMCP("server-name")

@mcp.tool()
def tool_name(param: str) -> dict:
    """Short description under 80 chars."""
    return engine.tool_name(param)    # one line only

def main() -> None:
    mcp.run()
```

**The rule:** Any line that touches domain data belongs in `engine.py`. Any line
that touches the MCP protocol belongs in `server.py`. If a tool body in `server.py`
is more than two lines, it has logic that belongs in `engine.py`.

### Why this split matters

1. **Testability** — tests import `engine.py` directly without starting an MCP
   server process
2. **Debuggability** — failures can be reproduced in a Python REPL with zero
   MCP overhead
3. **Reusability** — the same engine can power a CLI tool, a web API, or a
   notebook without changes
4. **Clarity** — `engine.py` contains only domain logic; `server.py` contains
   only tool definitions

---

## 15. Engine Sub-Module Pattern

When `engine.py` grows beyond ~400–500 lines, split it into focused sub-modules.
The engine entry point becomes a thin router.

### Sub-module layout

```
servers/data_advanced/
├── server.py              ← MCP wrapper (unchanged)
├── engine.py              ← thin router: imports from sub-modules, re-exports
├── _adv_io.py             ← file loading, export, format conversion
├── _adv_transform.py      ← data cleaning, patching, aggregations
├── _adv_analysis.py       ← statistics, outlier detection, profiling
├── _adv_charts.py         ← chart generation (bar, pie, line, scatter, etc.)
├── _adv_dashboard.py      ← dashboard HTML assembly
└── _adv_report.py         ← EDA report HTML assembly
```

### Sub-module naming rules

- Prefix with `_{domain_abbr}_` to avoid name collisions
- Group by what the code does, not by what tool calls it
- Sub-modules have zero MCP imports (same rule as `engine.py`)
- `engine.py` re-exports all public functions:

```python
# engine.py — thin router
from ._adv_io import load_file, export_data
from ._adv_transform import apply_patch, run_cleaning_pipeline
from ._adv_analysis import run_eda, check_outliers
from ._adv_charts import generate_chart, generate_multi_chart
from ._adv_dashboard import generate_dashboard

__all__ = [
    "load_file", "export_data",
    "apply_patch", "run_cleaning_pipeline",
    "run_eda", "check_outliers",
    "generate_chart", "generate_multi_chart",
    "generate_dashboard",
]
```

Tests still import from `engine.py` — sub-module structure is invisible to tests.

### Lazy imports for heavy dependencies

Sub-modules that depend on large libraries (torch, sklearn, transformers) should
import those libraries inside the function, not at module level. This avoids paying
the import cost when `engine.py` is loaded for a function that does not need that
library.

```python
# _adv_ml.py — import torch only when the function is called
def run_inference(model_path: str, input_path: str) -> dict:
    import torch   # lazy import — only loaded when this function runs
    ...
```

---

## 16. Return Value Contract

### Every tool returns a dict

No exceptions. Never return a plain string, list, `None`, or boolean.

```python
{"success": True, "op": "fill_nulls", ...}
{"success": False, "error": "...", "hint": "..."}
```

### Required fields in every response

| Field | Type | When required | Purpose |
|---|---|---|---|
| `"success"` | `bool` | Always | Model checks this first |
| `"op"` | `str` | On success | Confirms which operation ran |
| `"error"` | `str` | On failure | Human-readable failure reason |
| `"hint"` | `str` | On failure | Actionable recovery instruction |
| `"backup"` | `str` | After any write | Path to snapshot taken before write |
| `"progress"` | `list` | Always | Step-by-step execution log |
| `"dry_run"` | `bool` | When dry_run=True | Confirms simulation mode |
| `"token_estimate"` | `int` | Always | `len(str(response)) // 4` |
| `"truncated"` | `bool` | On bounded reads | Always explicit, never absent |

### What to include in write confirmations

```python
# Good — model can verify without a follow-up read
{
    "success": True,
    "op": "fill_nulls",
    "column": "revenue",
    "strategy": "median",
    "filled": 23,
    "value_used": 3800.0,
    "backup": ".mcp_versions/sales_q3_2026-03-25T14-30-00Z.bak",
    "progress": [
        {"icon": "✔", "msg": "Loaded sales_q3.csv", "detail": "5000 rows"},
        {"icon": "✔", "msg": "Filled nulls in revenue", "detail": "23 cells → 3800.0"},
        {"icon": "✔", "msg": "Saved sales_q3.csv", "detail": "snapshot created"},
    ],
    "token_estimate": 87
}
```

Never return raw data arrays, model weights, or full file contents from a write tool.
Write tools confirm the write. Read tools read.

---

## 17. Error Handling Contract

### Never raise exceptions to the caller

All exceptions are caught in `engine.py` and converted to error dicts.

```python
def train_classifier(file_path: str, target: str, model: str) -> dict:
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
    except MemoryError:
        return {
            "success": False,
            "error": "Insufficient RAM to load dataset.",
            "hint": "Use read_rows() with a bounded range or increase system RAM.",
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "backup": backup,
            "hint": "Use restore_version to undo if a snapshot was taken.",
        }
```

### Standard error message patterns by category

```python
# File errors
f"File not found: {path}"
f"Expected .csv file, got .{ext}"
f"File is locked by another process: {path}"

# Data errors
f"Column '{name}' not found. Available: {', '.join(columns)}"
f"Target column '{name}' has only 1 unique value — cannot train classifier"
f"Row index {n} out of range (0–{max_val})"

# Resource errors
f"Insufficient RAM: need ~{required_gb:.1f} GB, available ~{available_gb:.1f} GB"
f"GPU not available. Set device='cpu' to run on CPU."
f"Model file too large for available VRAM. Use quantized variant."

# Validation errors
f"Unknown op: '{op}'. Allowed: {', '.join(allowed_ops)}"
f"Required parameter missing: '{field}'"
f"Value '{val}' not in allowed values: {', '.join(allowed)}"

# Domain errors
f"Dataset has {rows} rows but need at least {min_rows} to train reliably"
f"Image resolution {w}x{h} exceeds processing limit {max_px}px"
```

### The hint field rules

The hint must complete the sentence "To fix this, ..." and must name a specific
tool to call or a specific value to check.

```python
# Bad hints
"hint": "Invalid input."
"hint": "Try again."

# Good hints
"hint": "Use inspect_dataset() first to verify column names and dtypes."
"hint": "Set device='cpu' if no GPU is available."
"hint": f"Available strategies: mean, median, mode, ffill, bfill, drop"
"hint": "Use read_rows(file, 0, 100) to preview data before patching."
```

---

## 18. Security Considerations

### Path traversal prevention

All file paths from tool parameters must be validated before use:

```python
from pathlib import Path

def resolve_path(file_path: str, allowed_extensions: tuple[str, ...] = ()) -> Path:
    """Resolve and validate a file path from tool input."""
    path = Path(file_path).resolve()

    # Reject paths that escape the user's home directory
    home = Path.home().resolve()
    try:
        path.relative_to(home)
    except ValueError:
        raise ValueError(f"Path outside allowed directory: {file_path}")

    if allowed_extensions and path.suffix.lower() not in allowed_extensions:
        raise ValueError(
            f"Extension {path.suffix!r} not allowed. Expected: {allowed_extensions}"
        )

    return path
```

Never use raw `file_path` strings in `open()`, `pd.read_csv()`, or subprocess calls
without resolving and validating first. Add `resolve_path()` to
`shared/file_utils.py`.

### Subprocess injection prevention

For servers that spawn subprocesses (FFmpeg, Tesseract, LibreOffice, etc.), never
pass user-provided strings directly into shell commands:

```python
# Wrong — shell injection risk
subprocess.run(f"ffmpeg -i {input_path} {output_path}", shell=True)

# Correct — argument list, no shell
subprocess.run(
    ["ffmpeg", "-i", str(input_path), str(output_path)],
    shell=False,
    capture_output=True,
    timeout=300,
)
```

Always set `shell=False`. Always pass an argument list. Always set `timeout`.
Always `capture_output=True` to avoid stdout leakage.

### Expression evaluation — no eval()

For servers that accept user-defined expressions (column calculations, filter
expressions, formula strings), never use `eval()` or `exec()`. Parse the expression
tree manually:

```python
# Wrong
result = eval(user_expression)

# Correct — parse and validate against allowed operations
ALLOWED_OPS = {"+", "-", "*", "/"}
result = _safe_eval_expr(user_expression, df.columns, ALLOWED_OPS)
```

Implement `_safe_eval_expr` in the engine using AST parsing with an allowlist of
operations.

### Sensitive data in responses

Never include connection strings, API keys, passwords, or full system paths in tool
responses. Use `Path(x).name` for file references in progress messages. Redact
credentials in error messages.

---

## 19. State and Version Control

### The snapshot rule

**Every tool that modifies persistent data must snapshot before writing.**

This applies to:
- Files on disk (datasets, documents, models, configs)
- Database records (include `before_state` in response)
- Generated outputs that took significant compute to produce

```python
# shared/version_control.py
from pathlib import Path
from datetime import datetime, timezone
import shutil

def snapshot(file_path: str) -> str:
    source = Path(file_path).resolve()
    if not source.exists():
        raise FileNotFoundError(f"Cannot snapshot: {source}")

    versions_dir = source.parent / ".mcp_versions"
    versions_dir.mkdir(exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    backup = versions_dir / f"{source.stem}_{ts}{source.suffix}.bak"
    shutil.copy2(source, backup)
    return str(backup)
```

### The companion state file pattern

For complex files, maintain a companion JSON state file:

```
sales_q3.csv
sales_q3.csv.mcp_state.json    ← companion
```

```json
{
    "version": 3,
    "file": "sales_q3.csv",
    "last_modified": "2026-03-25T14:30:00Z",
    "schema": {"revenue": "float64", "customer_id": "int64"},
    "known_stats": {"revenue": {"mean": 4190, "null_count": 0}},
    "patches": [
        {"version": 1, "ts": "2026-03-25T10:00Z", "ops": 2},
        {"version": 2, "ts": "2026-03-25T14:30Z", "ops": 1}
    ]
}
```

### Restore tool — mandatory in tier 1

Every tier 1 server must include `restore_version`:

```python
@mcp.tool()
def restore_version(file_path: str, timestamp: str) -> dict:
    """Restore file to a previous snapshot by timestamp."""
    return engine.restore_version(file_path, timestamp)
```

### Confirming explicit destructive writes

For operations with no undo (external API side effects, irreversible transforms),
add an explicit confirm parameter:

```python
def delete_all_nulls(file_path: str, confirm: bool = False) -> dict:
    if not confirm:
        return {
            "success": False,
            "error": "This operation permanently deletes rows. Pass confirm=True to proceed.",
            "hint": "Use dry_run=True first to preview what would be deleted.",
        }
```

---

## 20. Token Budget Discipline

### The VRAM → context window → token budget chain

```
GPU VRAM
  └→ Model weights occupy most of VRAM
       └→ Remaining VRAM = KV cache
            └→ KV cache size = effective context window
                 └→ Token budget per call = context / expected_turns
```

On 8 GB GPU with 9B model (Q4_K_M):
- Model weights: ~5.5 GB
- Available KV cache: ~1.7 GB
- Effective context: ~10,000–12,000 tokens
- Per-turn content budget: ~100–300 tokens after schema + history overhead

### Budget rules

1. Tool schemas: keep under 700 tokens total (≤8 tools, ≤80 char docstrings)
2. Read tool responses: under 500 tokens
3. Write confirmations: under 150 tokens
4. Never return raw arrays, pixel data, weight tensors, or full file contents

### The hardware mode flag

Every server reads a `MCP_CONSTRAINED_MODE` environment variable:

```python
# shared/platform_utils.py
import os

def is_constrained_mode() -> bool:
    return os.environ.get("MCP_CONSTRAINED_MODE", "0") == "1"

def get_max_rows() -> int:
    return 20 if is_constrained_mode() else 100

def get_max_results() -> int:
    return 10 if is_constrained_mode() else 50

def get_max_depth() -> int:
    return 3 if is_constrained_mode() else 5
```

The installer sets `MCP_CONSTRAINED_MODE=1` automatically on machines with ≤8 GB
VRAM. Never hardcode limits in engine functions — always call these helpers.

---

## 21. Hardware Tiers and Resource Constraints

### Design for constrained — test on standard — document for high-end

Write code for constrained hardware constraints and let users with more hardware
benefit automatically through the absence of artificial limits.

### Model and hardware reference

| VRAM | Model family | Quant | Context | Max tools |
|---|---|---|---|---|
| 4 GB | 3–4B models | Q4_K_M | ~5K tokens | 5 |
| 6 GB | 7B models | Q4_K_M | ~7K tokens | 6 |
| 8 GB | 9B models | Q3_K_S | ~12K tokens | 8 |
| 12 GB | 9B models | Q8_0 | ~20K tokens | 10 |
| 16 GB | 14B models | Q4_K_M | ~24K tokens | 12 |
| 24 GB | 32B models | Q4_K_M | ~32K tokens | 15 |

### Resource-aware execution

For compute-heavy domains (ML training, video processing, large dataset profiling),
tools must check available resources before starting and fail fast with a helpful
message:

```python
import psutil

def check_memory(required_gb: float) -> dict | None:
    available = psutil.virtual_memory().available / 1e9
    if available < required_gb:
        return {
            "success": False,
            "error": f"Need ~{required_gb:.1f} GB RAM, only {available:.1f} GB available.",
            "hint": "Use a row-sampled subset with read_rows(file, 0, 10000) first.",
        }
    return None
```

### The README hardware statement

Include this in every MCP server README:

> **A note on local execution and context limits:**
> This server runs entirely on your hardware — no data leaves your machine. On 8 GB
> VRAM with a 9B model, your effective context window is approximately 10,000–12,000
> tokens. This server is designed to stay within that budget through surgical read
> tools. For best results, run one focused task per session, then start a fresh chat.
> Fewer loaded tools means more context for your actual work.

---

## 22. Progress Output

### The rule

Every tool response includes a `"progress"` array. Never print to stdout.
All visible output goes in the progress array.

```python
{
    "success": True,
    "progress": [
        {"icon": "✔", "msg": "Loaded sales_q3.csv",           "detail": "5,000 rows × 12 cols"},
        {"icon": "✔", "msg": "Detected 23 nulls in revenue",  "detail": "0.46% of column"},
        {"icon": "✔", "msg": "Filled with median",            "detail": "value: 3800.0"},
        {"icon": "✔", "msg": "Saved sales_q3.csv",            "detail": "snapshot created"},
    ],
    "token_estimate": 95
}
```

### shared/progress.py helpers

```python
def ok(msg: str, detail: str = "") -> dict:
    return {"icon": "✔", "msg": msg, "detail": detail}

def fail(msg: str, detail: str = "") -> dict:
    return {"icon": "✘", "msg": msg, "detail": detail}

def info(msg: str, detail: str = "") -> dict:
    return {"icon": "→", "msg": msg, "detail": detail}

def warn(msg: str, detail: str = "") -> dict:
    return {"icon": "⚠", "msg": msg, "detail": detail}

def undo(msg: str, detail: str = "") -> dict:
    return {"icon": "↩", "msg": msg, "detail": detail}
```

Use these helpers. Never construct progress dicts by hand. Always use `Path(x).name`
in messages — never full paths.

---

## 23. Long-Running Operations and Async

### When async matters

For tools that invoke subprocesses (FFmpeg, model training, OCR on large files),
sync blocking is fine for operations under ~30 seconds. For longer operations, use
async:

```python
import asyncio
from fastmcp import FastMCP

mcp = FastMCP("video_basic")

@mcp.tool()
async def transcode_video(input_path: str, output_format: str) -> dict:
    """Transcode video to target format. Async for long operations."""
    return await engine.transcode_video_async(input_path, output_format)
```

For subprocess-based async:

```python
async def transcode_video_async(input_path: str, output_format: str) -> dict:
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-i", str(input_path), str(output_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=600)
    if proc.returncode != 0:
        return {
            "success": False,
            "error": stderr.decode(),
            "hint": "Check input file format and available disk space.",
        }
    return {"success": True, ...}
```

### Do not mix sync and async

If your server has any async tools, make the FastMCP instance async-compatible. Do
not mix `@mcp.tool()` (sync) and `async def` tools without verifying FastMCP version
compatibility.

### Progress for long operations

For tools that take more than a few seconds, include `"elapsed_seconds"` in the
response and intermediate `info()` entries in the progress array:

```python
progress = [
    info("Started transcoding", f"{input_path.name} → {output_format}"),
    info("Encoding video stream", "pass 1 of 2"),
    ok("Transcoding complete", f"elapsed: {elapsed:.1f}s"),
]
```

---

## 24. Live State and Reload

For servers that edit files that may be open in another application (documents,
spreadsheets, config files), implement `shared/live_edit.py`:

```python
def is_file_open(path: Path) -> bool: ...
def notify_reload(path: Path) -> dict: ...    # best-effort, never fails main op
```

Platform behavior:
- **macOS**: AppleScript triggers reload in Office apps
- **Windows**: Shadow file + optional COM via pywin32, graceful fallback
- **Linux**: Most apps poll the file; write normally

If reload is unavailable, return `info("File saved — reopen to see changes")` in the
progress array. Never fail the main operation because reload failed.

---

## 25. Operation Receipt Log

Every server that writes data maintains a persistent receipt log alongside modified
files:

```
sales_q3.csv
sales_q3.csv.mcp_receipt.json    ← receipt
```

```json
[
    {
        "ts": "2026-03-25T14:30:00Z",
        "tool": "fill_nulls",
        "args": {"column": "revenue", "strategy": "median"},
        "result": "filled 23 nulls",
        "backup": ".mcp_versions/sales_q3_2026-03-25T14-30-00Z.bak"
    }
]
```

### shared/receipt.py

```python
def append_receipt(file_path: str, tool: str, args: dict, result: str,
                   backup: str | None) -> None:
    """Append one record to receipt log. Never raises."""

def read_receipt_log(file_path: str) -> list[dict]:
    """Read full receipt log for a file."""
```

`append_receipt` must never raise — it wraps all I/O in try/except and silently
drops the record on failure rather than crashing the main operation.

Every tier 1 server includes a `read_receipt` tool:

```python
@mcp.tool()
def read_receipt(file_path: str) -> dict:
    """Read operation history for a file. Returns log entries."""
    return engine.read_receipt_log(file_path)
```

---

## 26. Output Generation Pattern

Some servers generate file outputs as their primary artifact: HTML reports, charts,
PDF exports, generated dashboards. This section defines the standard for these tools.

### The output generation contract

Output-generating tools must:
1. Accept an `output_path: str = ""` parameter — empty string means auto-generate
   beside the input file
2. Accept `open_after: bool = True` — auto-open in the default application after
   generating
3. Return `"output_path"` (absolute path) and `"output_name"` (filename only) in
   the response
4. Never stream raw bytes through the MCP channel — always write to disk and return
   the path

```python
@mcp.tool()
def generate_report(
    file_path: str,
    output_path: str = "",    # "" = auto-generate name beside input
    open_after: bool = True,  # open in browser/app after generation
    theme: str = "dark",      # "dark" | "light" | "device"
) -> dict:
    """Generate HTML report for dataset. Opens in browser."""
    return engine.generate_report(file_path, output_path, open_after, theme)
```

### Auto-naming convention

When `output_path=""`, derive the filename from the input:

```python
def _resolve_output_path(input_path: Path, suffix: str, output_path: str) -> Path:
    if output_path:
        return Path(output_path).resolve()
    return input_path.parent / f"{input_path.stem}_{suffix}.html"
```

Standard suffixes: `_eda`, `_dashboard`, `_report`, `_chart`, `_distribution`,
`_profile`.

### HTML output standards

For HTML outputs (charts, reports, dashboards):
- Use a shared `html_theme.py` module for CSS variables, viewport meta, and Plotly
  template
- Support dark/light/device themes via CSS custom properties
- Use `Plotly.js` loaded from local CDN or bundled — never a remote CDN in production
- All charts must be `responsive: true` in their Plotly layout config
- Wrap wide tables in `<div style="overflow-x:auto">` to prevent horizontal overflow
- Chart containers must have `overflow: hidden; max-width: 100%`

### Opening files after generation

```python
def _open_file(path: Path) -> None:
    """Open generated file in default app. Best-effort, never fails."""
    try:
        webbrowser.open(f"file://{path.resolve()}")
    except Exception:
        try:
            if sys.platform == "win32":
                subprocess.Popen(["start", str(path.resolve())], shell=True)
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(path.resolve())])
            else:
                subprocess.Popen(["xdg-open", str(path.resolve())])
        except Exception:
            pass   # non-critical; report was still generated
```

---

## 27. Testing Standards

### Test engine, not server

Tests import and call `engine.py` directly. Never spin up an MCP server process.

```python
# Correct
from servers.data_basic.engine import fill_nulls, search_columns

def test_fill_nulls(tmp_path, csv_fixture):
    f = tmp_path / "sales.csv"
    shutil.copy(csv_fixture, f)
    result = fill_nulls(str(f), "revenue", "median")
    assert result["success"] is True
    assert result["filled"] == 23
    assert ".mcp_versions" in result["backup"]
```

### Real fixture data — never purely synthetic

Test fixtures must represent real-world messiness: nulls, mixed types, irregular
formatting, edge-case values, encoding issues. A perfectly clean synthetic fixture
hides the bugs that matter.

Required fixture categories:
- `simple` — clean data, minimal edge cases
- `messy` — nulls, type mismatches, encoding issues, duplicate rows
- `large` — enough rows/records to test truncation and performance

### What to test for every write operation

1. **Success** — operation completes, `"success": True`
2. **Content correct** — read back the written node, verify content
3. **Snapshot created** — `.mcp_versions/` has a new `.bak` file
4. **Backup in response** — `"backup"` key present
5. **Dry run** — `dry_run=True` returns `"would_change"` without modifying file
6. **Progress present** — `"progress"` array in response
7. **Wrong file type** — error dict with correct hint
8. **File not found** — error dict with correct hint
9. **Index/column out of range** — error dict with available options in hint
10. **Constrained mode** — `MCP_CONSTRAINED_MODE=1` enforces smaller limits

### Coverage requirements

| Module | Minimum coverage |
|---|---|
| `shared/` | 100% |
| `engine.py` | ≥ 90% |
| Error paths | All documented conditions tested |
| Happy paths | All tools tested |

### CI must run on all three platforms

```yaml
strategy:
  matrix:
    os: [ubuntu-22.04, windows-latest, macos-13]
```

---

## 28. Cross-Platform Compatibility

### The path rule — pathlib everywhere

```python
# Wrong
path = base_dir + "/" + filename

# Correct
path = Path(base_dir) / filename
```

### Line endings

```
# .gitattributes
* text=auto eol=lf
*.bat text eol=crlf
*.cmd text eol=crlf
```

### Shell scripts — POSIX sh, not bash

`install.sh` must use `#!/bin/sh` and POSIX-compatible syntax.

### stdout is the MCP protocol channel

Never write to stdout in any engine or server module. Any `print()` statement
corrupts the MCP stdio channel.

```python
# Wrong
print("Processing file...")

# Correct — all logs go to stderr
import logging
logger = logging.getLogger(__name__)
logger.debug("Processing file...")
```

Configure in `server.py`:

```python
import sys, logging
logging.basicConfig(stream=sys.stderr, level=logging.WARNING)
```

### Windows long paths

```python
if sys.platform == "win32" and len(str(path)) > 200:
    path = Path("\\\\?\\" + str(path.resolve()))
```

### Atomic file writes

```python
import tempfile, shutil
with tempfile.NamedTemporaryFile(delete=False, suffix=suffix, dir=path.parent) as tmp:
    # write to tmp
    tmp_path = tmp.name
shutil.move(tmp_path, path)
```

---

## 29. Multi-Client Compatibility

### Config file locations by platform

| Client | Config (macOS) | Config (Windows) |
|---|---|---|
| LM Studio | `~/Library/Application Support/LM Studio/mcp.json` | `%APPDATA%\LM Studio\mcp.json` |
| Claude Desktop | `~/Library/Application Support/Claude/claude_desktop_config.json` | `%APPDATA%\Claude\claude_desktop_config.json` |
| Cursor | `~/.cursor/mcp.json` | `~/.cursor/mcp.json` |
| Windsurf | `~/.codeium/windsurf/mcp_config.json` | `~/.codeium/windsurf/mcp_config.json` |
| Cline | `~/Library/Application Support/Code/User/settings.json` | `%APPDATA%\Code\User\settings.json` |

The `mcp_config_writer.py` in `install/` handles all client differences.
Server code is identical regardless of client.

---

## 30. Transport Modes

### Every server supports two modes

**stdio** (default) — for local AI clients:

```
uv run --directory servers/my_server my-server
```

**HTTP** — for remote or multi-user access:

```
uv run --directory servers/my_server my-server --transport http --port 8765
```

```python
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--transport", choices=["stdio", "http"], default="stdio")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--auth-token", type=str, default=None)
    args = parser.parse_args()

    if args.transport == "http":
        mcp.run(transport="streamable-http", host="127.0.0.1",
                port=args.port, path="/mcp")
    else:
        mcp.run()
```

Auth tokens for HTTP mode must be generated by the installer (32+ random hex chars),
stored in config, never hardcoded in source.

---

## 31. Installation and Distribution

### The install experience standard

A user who has never used a terminal must be able to install and use the server:

1. Download the repo (zip or `git clone`)
2. Double-click `install.bat` (Windows) or run `./install.sh` (macOS/Linux)
3. Answer one menu question (which AI client)
4. Restart their AI client
5. Start using the tools

### install.sh checklist

```sh
#!/bin/sh
# 1. Check Python 3.12+
# 2. Check/install uv
# 3. uv sync
# 4. Detect hardware (VRAM) and set MCP_CONSTRAINED_MODE if ≤ 8GB
# 5. Ask which AI client platforms
# 6. python3 install/mcp_config_writer.py --servers "$selected" --platform "$platform"
# 7. echo "Done. Restart your AI client."
```

### mcp_config_writer.py rules

- Parse existing config with `json5` (handles comments and trailing commas)
- Append-only — never modify or remove existing entries
- Idempotent — safe to run twice
- Write with `json.dumps()` — strict valid JSON output
- Atomic write (temp file + rename)

---

## 32. Naming Conventions

### Server directories

```
servers/{domain}_{tier}/    # data_basic, ml_medium, office_advanced, media_basic
```

### Python identifiers

```python
# Functions — snake_case verb
read_dataset()
apply_patch()
train_classifier()

# Classes — PascalCase
PatchValidator
VersionControl

# Constants — UPPER_SNAKE_CASE
MAX_ROWS = 100
DEFAULT_TEST_SIZE = 0.2

# Private helpers — leading underscore
_apply_single_op()
_resolve_strategy()
```

### MCP tool function names — verb_noun

```
read_dataset        list_columns        search_rows
fill_nulls          drop_duplicates     rename_column
train_classifier    export_model        apply_patch
restore_version     read_receipt        inspect_dataset
```

Allowed verbs: `read`, `list`, `search`, `get`, `inspect`, `set`, `fill`, `drop`,
`rename`, `replace`, `insert`, `delete`, `add`, `update`, `move`, `train`, `export`,
`apply`, `restore`, `run`, `generate`

### Commit message format

```
{type}({scope}): {description}

Types:   feat | fix | test | docs | refactor | chore
Scope:   server name, shared, install, or root

feat(data_basic): add surgical column search tool
fix(shared): handle Windows long path in snapshot
test(ml_basic): add OOM failure test for large datasets
```

---

## 33. Dependency Policy

### Approved licenses

- MIT, Apache 2.0, BSD 2-Clause / 3-Clause, ISC, PSF

**Not permitted:** GPL / LGPL, commercial / proprietary, unlicensed

### Vetting checklist before adding any library

- License is in the approved list
- Last release within 12 months
- No known CVEs via `uv audit`
- Does not pull in an unnecessarily large dependency tree
- No existing approved alternative
- Pinned in `pyproject.toml` with minimum version (`>=x.y.z`)

### Prohibited libraries

```
win32com / pywin32     — Windows only
Spire.*                — commercial license
Aspose.*               — commercial license
Any cloud SDK used as primary execution engine (boto3 for ML, google-cloud-*, etc.)
```

---

## 34. CI/CD Requirements

### Required checks on every PR

```yaml
jobs:
  test:
    strategy:
      matrix:
        os: [ubuntu-22.04, windows-latest, macos-13]
    env:
      MCP_CONSTRAINED_MODE: "1"
      PYTHONPATH: "."
    steps:
      - uv sync --frozen
      - uv run ruff check .
      - uv run ruff format --check .
      - uv run pyright servers/ shared/
      - uv run pytest tests/ --cov=servers --cov=shared --cov-fail-under=90
      - python verify_tool_docstrings.py
```

Setting `PYTHONPATH: "."` is required so that `shared/` imports resolve correctly
when tests are run from the repo root. Without it, `from shared.progress import ok`
fails on CI even when it works locally.

`pyright` must be run on engine sub-modules too, not just the top-level `engine.py`.
When using the sub-module pattern (§15), ensure `pyright` covers the full
`servers/{name}/` directory, which includes `_adv_io.py`, `_adv_charts.py`, etc.

### verify_tool_docstrings.py

```python
import ast, pathlib, sys

errors = []
for f in pathlib.Path("servers").rglob("server.py"):
    tree = ast.parse(f.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            has_tool = any(
                (isinstance(d, ast.Attribute) and d.attr == "tool") or
                (isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute)
                 and d.func.attr == "tool")
                for d in node.decorator_list
            )
            if has_tool:
                doc = ast.get_docstring(node) or ""
                if len(doc) > 80:
                    errors.append(f"{f}:{node.lineno} {node.name}: {len(doc)} chars > 80")

if errors:
    print("\n".join(errors))
    sys.exit(1)
```

---

## 35. Documentation Requirements

### README.md required sections

1. One-line description — domain, tier, tool count
2. Prerequisites — Python version, uv, hardware requirements
3. Quick install — three commands or fewer
4. Platform setup — LM Studio, Claude Desktop, Cursor config snippets
5. Tool reference — table of tools with name, description, key parameters
6. Usage patterns — 2–3 example prompts showing the four-tool loop
7. Hardware requirements — VRAM table with recommendations
8. The hardware sovereignty statement (see section 21)
9. Contributing — how to add tools, link to STANDARDS.md
10. License

### CLAUDE.md — required in every repo

Every repository that an AI coding agent will work in must have a `CLAUDE.md`
containing:
- Project overview and goals
- Repository structure
- Architecture principles (engine/server split, tool count limits, surgical read,
  snapshot-before-write, self-hosted execution)
- Domain-specific tool design rules
- What the AI must never do
- Progress tracker with checkboxes

### In-code documentation

```python
# Tool docstrings — ≤ 80 chars, machine-readable selection cues
"""Fill null values in column. strategy: mean median mode ffill bfill drop."""

# Engine function docstrings — full human-readable explanation
def fill_nulls(file_path: str, column: str, strategy: str,
               dry_run: bool = False) -> dict:
    """
    Fill null values in the specified column using the given strategy.
    Snapshots the file before writing.
    Supports dry_run to preview changes without modifying the file.

    Returns:
        dict with "success", "filled" (count), "value_used", "backup",
        "progress", "token_estimate"
    Raises:
        Never — all exceptions caught and returned as error dicts
    """
```

---

## 36. Smart Domain Inference Pattern

When a tool operates over multiple columns or fields and needs to select the right
operation per item (e.g. sum vs mean vs max for numeric columns in a dashboard), do
not require the user to specify every detail. Implement smart inference with an
override escape hatch.

### The inference pattern

```python
# shared/column_utils.py — domain inference utilities
import re
import pandas as pd

_AGG_MEAN = frozenset({
    "rate", "ratio", "pct", "percent", "score", "avg",
    "average", "mean", "temperature", "density",
})
_AGG_MAX  = frozenset({"max", "maximum", "peak", "high", "ceiling"})
_AGG_MIN  = frozenset({"min", "minimum", "low", "lowest", "floor"})

def infer_agg(col: str, series: pd.Series | None = None) -> str:
    """
    Infer the best aggregation for a column.
    Priority: name-keyword match > [0,1] range heuristic > default sum.
    Returns: "sum" | "mean" | "max" | "min"
    """
    lower = col.lower()
    words = set(re.split(r"[^a-zA-Z]+", lower))
    if words & _AGG_MEAN: return "mean"
    if words & _AGG_MAX:  return "max"
    if words & _AGG_MIN:  return "min"

    # distribution heuristic: values in [0,1] are likely rates/proportions
    if series is not None:
        try:
            v = series.dropna()
            if len(v) > 0 and float(v.min()) >= 0.0 and float(v.max()) <= 1.0:
                return "mean"
        except Exception:
            pass
    return "sum"
```

### The override escape hatch

Always expose override parameters so the user can correct inference errors:

```python
@mcp.tool()
def generate_dashboard(
    file_path: str,
    agg_overrides: list[str] = None,  # e.g. ["revenue:sum", "rate:mean"]
    ...
) -> dict:
    """Generate dashboard with auto-detected chart types and aggregations."""
```

```python
def parse_agg_overrides(overrides: list[str] | None) -> dict[str, str]:
    """Parse ["col:agg", ...] into {col: agg} dict."""
    result = {}
    if not overrides:
        return result
    for item in overrides:
        if ":" in item:
            col, agg = item.split(":", 1)
            if agg.strip() in {"sum", "mean", "max", "min", "count"}:
                result[col.strip()] = agg.strip()
    return result
```

### Where to apply this pattern

- Dashboard chart type detection (bar vs time_series vs scatter)
- Aggregation function selection per numeric column
- Date format detection for datetime parsing
- Encoding detection for CSV loading
- Delimiter detection for CSV loading

The rule: **infer from evidence, expose an override, document what was inferred**.

Always include the inferred values in the tool response so the user can see what was
decided:

```python
{
    "success": True,
    "agg_used": {
        "revenue": "sum",
        "satisfaction_rate": "mean",
        "temperature": "mean",
    },
    ...
}
```

---

## 37. What to Never Do

These are absolute prohibitions. Any code that violates them is a defect.

1. **Print to stdout in any server or engine module.**
   Stdout is the MCP channel. Any print statement corrupts it.

2. **Return a plain string, list, None, or boolean from a tool.**
   Always return a dict.

3. **Write to data without calling `snapshot()` first.**
   No exceptions for "small changes" or "non-destructive edits".

4. **Swallow exceptions silently.**
   Every exception becomes an error dict with `"success": False`, `"error"`,
   and `"hint"`.

5. **Return full file contents or raw data arrays from a write tool.**
   Write tools confirm the write. Read tools read.

6. **Fall back to returning everything when a search finds nothing.**
   Return an empty list and a helpful hint.

7. **Exceed 10 tools in a single server.**
   Split into more servers at finer tier granularity.

8. **Put business logic in server.py.**
   Tool functions in `server.py` are one-liners calling `engine.py`.

9. **Hardcode token or size limits as magic numbers.**
   Always call `get_max_rows()`, `get_max_results()` from `platform_utils.py`.

10. **Use string concatenation for file paths.**
    Always use `pathlib.Path / operator`.

11. **Write a tool that both reads and writes in a single call.**
    LOCATE and INSPECT are separate from PATCH.

12. **Add a dependency without checking its license.**
    GPL dependencies are never permitted.

13. **Make the installer require terminal commands from the user.**
    Zero terminal interaction after download.

14. **Use a cloud API as the primary execution engine.**
    The self-hosted execution principle is non-negotiable.

15. **Return raw model weights, pixel arrays, or audio buffers.**
    Return paths, summaries, and stats. Never raw binary-equivalent data.

16. **Make a tool that cannot run offline.**
    Unless network access is the explicit stated purpose of the tool.

17. **Use `eval()` or `exec()` on user-provided input.**
    Parse expressions manually using AST with an operation allowlist.

18. **Pass user-provided strings into `subprocess.run()` with `shell=True`.**
    Always use argument lists and `shell=False`. Always set `timeout`.

19. **Use user-provided file paths without calling `resolve_path()` first.**
    Path traversal is a real attack vector even in local tools.

20. **Mix async and sync tool definitions without verifying framework compatibility.**
    Either all tools are sync or you have an async-aware server setup.

21. **Return `None` from an async tool.**
    Async engine functions must return a dict in all code paths.

22. **Load the entire engine module at import time for a server that only uses a few
    functions.**
    Sub-module imports in `engine.py` are acceptable — lazy import sub-modules that
    have heavy dependencies (torch, sklearn) only when the function is called.

---

## 38. Checklist — New Server from Scratch

### Discovery
- [ ] Define the domain clearly
- [ ] Define the tier (Basic / Medium / Advanced)
- [ ] List all tools — count before writing any code
- [ ] Confirm tool count ≤ 10 (target 6–8)
- [ ] Identify the surgical read tools (`search_*`, `inspect_*`, `list_*`)
- [ ] Identify write tools — confirm each has a snapshot before write
- [ ] Choose language based on local library availability
- [ ] Confirm all required libraries have approved licenses
- [ ] Verify every tool can run offline (self-hosted execution test)

### Setup
- [ ] Create `servers/{domain}_{tier}/`
- [ ] Create `__init__.py`
- [ ] Create `pyproject.toml` with `requires-python = ">=3.12"` and `shared` dependency
- [ ] Pin `fastmcp>=2.0,<3.0` in `pyproject.toml`
- [ ] Add to workspace `pyproject.toml` members list
- [ ] `uv sync` — no errors

### Shared modules
- [ ] `shared/version_control.py` — tested
- [ ] `shared/patch_validator.py` — tested
- [ ] `shared/file_utils.py` — includes `resolve_path()` with path traversal check
- [ ] `shared/platform_utils.py` — `is_constrained_mode()` and limit helpers
- [ ] `shared/progress.py` — ok/fail/info/warn/undo helpers
- [ ] `shared/receipt.py` — append_receipt / read_receipt_log

### Engine
- [ ] `engine.py` with zero MCP imports
- [ ] Surgical read tools implemented first
- [ ] Every tool returns dict with `"success"` as first key
- [ ] Every write tool calls `snapshot()` before writing
- [ ] Every write tool includes `"backup"` in return dict
- [ ] Every write tool includes `"dry_run"` path
- [ ] Every tool response includes `"progress"` array
- [ ] Every tool response includes `"token_estimate"`
- [ ] Bounded reads use `get_max_*()` helpers
- [ ] No `print()` statements anywhere
- [ ] `restore_version` delegates to `shared.version_control`
- [ ] All file path inputs validated through `resolve_path()` before use
- [ ] No `eval()` or `exec()` on user-provided input
- [ ] Subprocess calls use argument lists with `shell=False` and `timeout`

### Server
- [ ] `server.py` with FastMCP setup
- [ ] One `@mcp.tool()` per tool, each body is `return engine.func(params)`
- [ ] Every docstring ≤ 80 characters
- [ ] All parameters typed with allowed types
- [ ] Tool annotations set (`readOnlyHint`, `destructiveHint`, etc.)
- [ ] `--transport` and `--port` CLI args in `main()`
- [ ] `project.scripts` entry in `pyproject.toml`

### Tests
- [ ] `tests/fixtures/` with real data (simple + messy + large)
- [ ] `tests/test_{server_name}.py`
- [ ] Test every tool: success case
- [ ] Test every tool: file-not-found error
- [ ] Test every write tool: snapshot created
- [ ] Test every write tool: `"backup"` in response
- [ ] Test every write tool: `dry_run=True` does not modify file
- [ ] Test every bounded read: truncation at limit
- [ ] Test `restore_version`: file reverts correctly
- [ ] Run with `MCP_CONSTRAINED_MODE=1`: limits enforced
- [ ] `uv run pytest` — all pass
- [ ] `uv run pyright servers/{name}/` — no errors (covers sub-modules too)
- [ ] `uv run ruff check servers/{name}/` — no errors

### Distribution
- [ ] Add to `install/install.sh` menu
- [ ] Add to `install/install.bat` menu
- [ ] Add to `install/mcp_config_writer.py` registry
- [ ] Test install on clean machine or VM

### Review
- [ ] `verify_tool_docstrings.py` — all ≤ 80 chars
- [ ] Manual test in LM Studio (9B model) — four-tool loop works
- [ ] Manual test in Claude Desktop — tools appear and execute
- [ ] 10-step task test — context window not exceeded
- [ ] Update `CLAUDE.md` progress tracker

---

## 39. Checklist — New Tool in Existing Server

- [ ] Server tool count will not exceed 10 after adding
- [ ] Tool name follows `verb_noun` snake_case convention
- [ ] Verb is in the approved verb list
- [ ] Write engine function in `engine.py` first (no MCP imports)
- [ ] Engine function returns dict with `"success"` as first key
- [ ] Engine function calls `resolve_path()` as first operation on any file path
- [ ] Engine function validates file extension after resolve
- [ ] Engine function calls `snapshot()` if it writes
- [ ] Engine function includes `"backup"` in write responses
- [ ] Engine function includes `"dry_run"` path if it writes
- [ ] Engine function calls `append_receipt()` after every write
- [ ] Engine function includes `"progress"` array
- [ ] Engine function includes `"token_estimate"`
- [ ] Engine function catches all exceptions → error dict
- [ ] Error dict includes `"hint"` with actionable recovery
- [ ] Engine function uses `get_max_*()` helpers for bounded returns
- [ ] No `print()` statements
- [ ] No `eval()` or `exec()` on user-provided input
- [ ] Subprocess calls use `shell=False` and `timeout` (if applicable)
- [ ] Tool can run offline (self-hosted execution principle)
- [ ] Add `@mcp.tool()` in `server.py` calling engine function
- [ ] Tool docstring ≤ 80 characters
- [ ] Tool annotations set (`readOnlyHint`, `destructiveHint`, etc.)
- [ ] All parameters have allowed type annotations
- [ ] Optional parameters have primitive defaults
- [ ] Add success test
- [ ] Add file-not-found / missing data failure test
- [ ] Add snapshot-created test (write tools)
- [ ] Add `dry_run=True` test (write tools)
- [ ] Add progress array test
- [ ] `uv run pytest tests/test_{server_name}.py` — all pass
- [ ] `uv run ruff check servers/{name}/`
- [ ] `uv run pyright servers/{name}/`
- [ ] `verify_tool_docstrings.py` — ≤ 80 chars confirmed

---

## 40. Domain Reference Table

This table maps domains to their local execution engines, giving a quick reference
for building new servers that comply with the self-hosted execution principle.

| Domain | Server name pattern | Primary local engine | Cloud alternative replaced |
|---|---|---|---|
| Document editing | `office_basic` | python-docx, openpyxl, python-pptx | Google Docs API, Office 365 API |
| PDF processing | `pdf_basic` | PyMuPDF, pdfplumber, reportlab | Adobe API, AWS Textract |
| Data profiling | `data_basic` | ydata-profiling, sweetviz, polars | DataRobot, AWS Glue |
| Data cleaning | `data_medium` | pyjanitor, great_expectations, polars | Trifacta, OpenRefine cloud |
| SQL analytics | `sql_basic` | DuckDB, SQLite | BigQuery, Snowflake, Redshift |
| Statistical analysis | `stats_basic` | scipy, pingouin, statsmodels | SAS, SPSS cloud |
| Time series | `timeseries_basic` | statsforecast, sktime, neuralprophet | AWS Forecast, Azure Time Series |
| AutoML | `ml_basic` | FLAML, autosklearn, TPOT | Google AutoML, Azure AutoML |
| Model training | `ml_medium` | scikit-learn, XGBoost, LightGBM | SageMaker, Vertex AI |
| Deep learning | `dl_basic` | PyTorch, ONNX Runtime | OpenAI fine-tune, Cohere |
| Model evaluation | `ml_advanced` | SHAP, LIME, evidently, yellowbrick | Fiddler, Arize |
| MLflow tracking | `mlops_basic` | MLflow (local server) | MLflow cloud, W&B |
| OCR | `ocr_basic` | easyocr, surya, pytesseract | Google Vision, AWS Textract |
| Image processing | `image_basic` | Pillow, OpenCV, scikit-image | Google Vision, Cloudinary |
| Audio processing | `audio_basic` | librosa, pydub, FFmpeg | AWS Transcribe, Azure Speech |
| Video processing | `video_basic` | MoviePy, FFmpeg (subprocess) | AWS MediaConvert, Mux |
| Web scraping | `web_basic` | Playwright, BeautifulSoup, httpx | Apify, ScrapingBee |
| System monitoring | `sys_basic` | psutil, py-cpuinfo, GPUtil | Datadog, New Relic |
| Log analysis | `log_basic` | regex engine, structlog | Splunk, Papertrail |
| Docker management | `docker_basic` | docker-py SDK (local daemon) | Docker Hub API, AWS ECS |
| Secret scanning | `security_basic` | detect-secrets, bandit, pip-audit | Snyk, GitHub Advanced Security |
| Geospatial | `geo_basic` | geopandas, shapely, rasterio | Google Maps API, ArcGIS |
| Report generation | `report_basic` | HTML/CSS/JS engine, Playwright PDF | Tableau, PowerBI |
| Local search | `search_basic` | MeiliSearch (local), Whoosh, tantivy | Algolia, Elasticsearch cloud |

---

*Version: 3.0*
*Derived from: MCP_Microsoft_Office STANDARDS.md v1.1, expanded for general-purpose
self-hosted MCP server development.*
*This document should be linked from every MCP server project's README and CLAUDE.md.*
*When these standards conflict with a specific project's CLAUDE.md, the project's
CLAUDE.md takes precedence for that project. These are the defaults.*
