# MCP Data Analyst

A self-hosted MCP server that gives local LLMs structured access to CSV/tabular data analysis tools. No cloud APIs, no API keys — everything runs on your machine.

**Release [`v0.3.0`](https://github.com/azzindani/MCP_Data_Analyst/releases/tag/v0.3.0)** — source only. No wheel and no container image are published: install from the tag with the bundled installer, or build the image yourself from the `Dockerfile` in this repo.

## Features

- **One endpoint, eight tools** — `/mcp` serves the whole surface as eight domain tools: `data_inspect`, `data_edit`, `data_reshape`, `data_stats`, `data_chart`, `data_report`, `data_ingest`, `data_workspace`. Each takes an `action` (one of the 69 tools below, by its own name) and an `args` object whose every property says which actions take it. Same validation and answers as the tiers; the tier endpoints below keep serving for small local models and existing connections
- **69 tools** across 7 servers: workspace (6), basic (9), medium (7), transform (12), statistics (12), visual (13), ingest (10) — no name listed twice. Four older medium tools are retired: `extended_stats`, `statistical_tests`, `filter_rows` and `compute_aggregations` are no longer listed, still answer as before, and each answer names the tool that replaced it
- **LOCATE → INSPECT → PATCH → VERIFY** workflow for surgical data edits
- **Automatic version control** — every write is snapshotted and fully restorable (Windows-safe: collision-proof timestamps)
- **Operation receipt logging** — full audit trail of all modifications
- **Constrained mode** — reduces row/result limits for lower-memory machines
- **Project workspace management** — named aliases, saved pipeline templates, stage-tracked files
- **51 `apply_patch` ops** — original, filtering, numeric transforms (Box-Cox, Yeo-Johnson), encoding, temporal, structural
- **Full statistics suite** — OLS/logistic regression, 17 statistical tests, STL decomposition, ACF/PACF, ADF stationarity, MoM/QoQ/YoY period comparison
- **ydata-profiler quality reports** — alerts panel, Spearman + Pearson correlations, missing value matrix, per-column distribution charts
- **Interactive dashboards** — KPI sparklines, trend indicators, violin plots, geo maps, filter controls
- **Geo visualization** — scatter maps (lat/lon), choropleth (country/state), zero external data needed
- **3D charts** — scatter_3d and surface plots
- **Chart customization** — post-generate edits to titles, labels, colours, annotations on saved HTML
- **Composable dashboards** — `generate_dashboard(spec=…)` builds from a declarative JSON spec, the page embeds the spec it was built from, and `customize_dashboard` edits that spec instead of regenerating from prose
- **Multi-source dashboards** — `sources=[…]` renders extra files as tabs beside the primary one, with server-side exact totals
- **Target-aware profiling** — `run_eda(target_column=…)` ranks every column by its relation to the target; `compare_to=…` measures PSI / total-variation drift against a baseline and fills the fourth quality component
- **Leakage detection** — a feature that already contains the outcome is named with its evidence: single-feature separation, missingness that tracks the target, and post-outcome column names. Suspects, never verdicts, and kept out of the quality score
- **Depth control** — `mode="minimal" | "standard" | "full"`, `sample_n`, and per-section `include` overrides, so a loop pays for a summary and a frontier model can ask for everything
- **Lineage sidecars** — every derived file gets a `.mcp_lineage.json` naming the op, the source, and the row/column counts either side; chained by reference so a filtered-then-reshaped file can be traced back
- **Executable insights** — `insights.json` beside every report, each finding carrying the tool call that acts on it
- **Enriched Excel export** — README sheet, frozen header, autofilter, number formats and column validation on `export_data`
- **Light / dark / device theme** — all HTML outputs accept `theme: "dark" | "light" | "device"`
- **Mobile-responsive HTML** — viewport meta + CSS breakpoints on every report
- **Modular architecture** — each engine split into focused sub-modules, all under 1 000 lines

## Important: File Path Only

> **Do not attach files via the LM Studio attachment button.**
>
> LM Studio will RAG-chunk any attached file and send fragments to the model — the MCP tools will never see the actual data. This MCP works exclusively through **absolute file paths**.
>
> Always tell the model where the file lives on disk:
> ```
> Analyze C:\Users\you\data\sales.csv
> ```
> The model will pass that path directly to the MCP tools. Attachment-based workflows are not supported and will silently produce wrong results.

## Quick Install (LM Studio)

> **Tested on Windows 11** with LM Studio 0.4.x and uv 0.5+.

### Requirements

- **Git** — `git --version`
- **Python 3.14 or higher** — `python --version`
- **uv** — `uv --version` ([install guide](https://docs.astral.sh/uv/getting-started/installation/))
- **LM Studio** with a model that supports tool calling (Gemma 4, Qwen 3.5, etc.)

### Platform Support

| Platform | Status |
|---|---|
| Windows | Tested — real-world verified (Windows 11) |
| macOS | Untested — CI/CD pipeline passes |
| Linux | Untested — CI/CD pipeline passes |

> Real-world usage has only been verified on Windows. macOS and Linux are supported by design and pass the automated CI pipeline, but have not been tested by hand. Reports from non-Windows users are welcome.

### First Run

The first launch clones the repo and installs dependencies (~2-5 minutes). Subsequent launches are instant.

> **Pre-install recommended:** To avoid the 60-second LM Studio connection timeout on first launch, run this once in PowerShell before connecting:
> ```powershell
> $d = Join-Path $env:USERPROFILE '.mcp_servers\MCP_Data_Analyst'
> $g = Join-Path $d '.git'
> if (!(Test-Path $g)) { if (Test-Path $d) { Remove-Item -Recurse -Force $d }; git clone https://github.com/azzindani/MCP_Data_Analyst.git $d --quiet }
> Set-Location "$d\servers\data_basic"; uv sync
> Set-Location "$d\servers\data_medium"; uv sync
> Set-Location "$d\servers\data_workspace"; uv sync
> Set-Location "$d\servers\data_transform"; uv sync
> Set-Location "$d\servers\data_statistics"; uv sync
> Set-Location "$d\servers\data_visual"; uv sync
> Set-Location "$d\servers\data_ingest"; uv sync
> ```
> If you skip this step and LM Studio times out, press **Restart** in the MCP Servers panel — it will reconnect and complete the install immediately.

### Steps

1. Open LM Studio → **Developer** tab (`</>` icon) or you can find via **Integrations**
2. Find **mcp.json** or **Edit mcp.json** → click to open
3. Paste this config:

```json
{
  "mcpServers": {
    "data_analyst_basic": {
      "command": "powershell",
      "args": [
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        "$d = Join-Path $env:USERPROFILE '.mcp_servers\\MCP_Data_Analyst'; $g = Join-Path $d '.git'; if (!(Test-Path $g)) { if (Test-Path $d) { Remove-Item -Recurse -Force $d }; git clone https://github.com/azzindani/MCP_Data_Analyst.git $d --quiet } else { Set-Location $d; git fetch origin --quiet; git reset --hard FETCH_HEAD --quiet }; Set-Location (Join-Path $d 'servers\\data_basic'); uv sync --quiet; uv run python server.py"
      ],
      "env": { "MCP_CONSTRAINED_MODE": "0" },
      "timeout": 600000
    },
    "data_analyst_medium": {
      "command": "powershell",
      "args": [
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        "$d = Join-Path $env:USERPROFILE '.mcp_servers\\MCP_Data_Analyst'; $g = Join-Path $d '.git'; if (!(Test-Path $g)) { if (Test-Path $d) { Remove-Item -Recurse -Force $d }; git clone https://github.com/azzindani/MCP_Data_Analyst.git $d --quiet } else { Set-Location $d; git fetch origin --quiet; git reset --hard FETCH_HEAD --quiet }; Set-Location (Join-Path $d 'servers\\data_medium'); uv sync --quiet; uv run python server.py"
      ],
      "env": { "MCP_CONSTRAINED_MODE": "0" },
      "timeout": 600000
    },
    "data_analyst_workspace": {
      "command": "powershell",
      "args": [
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        "$d = Join-Path $env:USERPROFILE '.mcp_servers\\MCP_Data_Analyst'; $g = Join-Path $d '.git'; if (!(Test-Path $g)) { if (Test-Path $d) { Remove-Item -Recurse -Force $d }; git clone https://github.com/azzindani/MCP_Data_Analyst.git $d --quiet } else { Set-Location $d; git fetch origin --quiet; git reset --hard FETCH_HEAD --quiet }; Set-Location (Join-Path $d 'servers\\data_workspace'); uv sync --quiet; uv run python server.py"
      ],
      "env": { "MCP_CONSTRAINED_MODE": "0" },
      "timeout": 600000
    },
    "data_analyst_transform": {
      "command": "powershell",
      "args": [
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        "$d = Join-Path $env:USERPROFILE '.mcp_servers\\MCP_Data_Analyst'; $g = Join-Path $d '.git'; if (!(Test-Path $g)) { if (Test-Path $d) { Remove-Item -Recurse -Force $d }; git clone https://github.com/azzindani/MCP_Data_Analyst.git $d --quiet } else { Set-Location $d; git fetch origin --quiet; git reset --hard FETCH_HEAD --quiet }; Set-Location (Join-Path $d 'servers\\data_transform'); uv sync --quiet; uv run python server.py"
      ],
      "env": { "MCP_CONSTRAINED_MODE": "0" },
      "timeout": 600000
    },
    "data_analyst_statistics": {
      "command": "powershell",
      "args": [
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        "$d = Join-Path $env:USERPROFILE '.mcp_servers\\MCP_Data_Analyst'; $g = Join-Path $d '.git'; if (!(Test-Path $g)) { if (Test-Path $d) { Remove-Item -Recurse -Force $d }; git clone https://github.com/azzindani/MCP_Data_Analyst.git $d --quiet } else { Set-Location $d; git fetch origin --quiet; git reset --hard FETCH_HEAD --quiet }; Set-Location (Join-Path $d 'servers\\data_statistics'); uv sync --quiet; uv run python server.py"
      ],
      "env": { "MCP_CONSTRAINED_MODE": "0" },
      "timeout": 600000
    },
    "data_analyst_visual": {
      "command": "powershell",
      "args": [
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        "$d = Join-Path $env:USERPROFILE '.mcp_servers\\MCP_Data_Analyst'; $g = Join-Path $d '.git'; if (!(Test-Path $g)) { if (Test-Path $d) { Remove-Item -Recurse -Force $d }; git clone https://github.com/azzindani/MCP_Data_Analyst.git $d --quiet } else { Set-Location $d; git fetch origin --quiet; git reset --hard FETCH_HEAD --quiet }; Set-Location (Join-Path $d 'servers\\data_visual'); uv sync --quiet; uv run python server.py"
      ],
      "env": { "MCP_CONSTRAINED_MODE": "0" },
      "timeout": 600000
    },
    "data_analyst_ingest": {
      "command": "powershell",
      "args": [
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        "$d = Join-Path $env:USERPROFILE '.mcp_servers\\MCP_Data_Analyst'; $g = Join-Path $d '.git'; if (!(Test-Path $g)) { if (Test-Path $d) { Remove-Item -Recurse -Force $d }; git clone https://github.com/azzindani/MCP_Data_Analyst.git $d --quiet } else { Set-Location $d; git fetch origin --quiet; git reset --hard FETCH_HEAD --quiet }; Set-Location (Join-Path $d 'servers\\data_ingest'); uv sync --quiet; uv run python server.py"
      ],
      "env": { "MCP_CONSTRAINED_MODE": "0" },
      "timeout": 600000
    }
  }
}
```

4. Wait for the blue dot next to each server
5. Start chatting — the model will see all 69 tools

### macOS / Linux

Replace the `"command"` and `"args"` in each entry with the bash equivalent:

```json
{
  "mcpServers": {
    "data_analyst_basic": {
      "command": "bash",
      "args": [
        "-c",
        "d=\"$HOME/.mcp_servers/MCP_Data_Analyst\"; if [ ! -d \"$d/.git\" ]; then rm -rf \"$d\"; git clone https://github.com/azzindani/MCP_Data_Analyst.git \"$d\" --quiet; else cd \"$d\" && git fetch origin --quiet && git reset --hard FETCH_HEAD --quiet; fi; cd \"$d/servers/data_basic\"; uv sync --quiet; uv run python server.py"
      ],
      "env": { "MCP_CONSTRAINED_MODE": "0" },
      "timeout": 600000
    },
    "data_analyst_medium": {
      "command": "bash",
      "args": [
        "-c",
        "d=\"$HOME/.mcp_servers/MCP_Data_Analyst\"; if [ ! -d \"$d/.git\" ]; then rm -rf \"$d\"; git clone https://github.com/azzindani/MCP_Data_Analyst.git \"$d\" --quiet; else cd \"$d\" && git fetch origin --quiet && git reset --hard FETCH_HEAD --quiet; fi; cd \"$d/servers/data_medium\"; uv sync --quiet; uv run python server.py"
      ],
      "env": { "MCP_CONSTRAINED_MODE": "0" },
      "timeout": 600000
    },
    "data_analyst_workspace": {
      "command": "bash",
      "args": [
        "-c",
        "d=\"$HOME/.mcp_servers/MCP_Data_Analyst\"; if [ ! -d \"$d/.git\" ]; then rm -rf \"$d\"; git clone https://github.com/azzindani/MCP_Data_Analyst.git \"$d\" --quiet; else cd \"$d\" && git fetch origin --quiet && git reset --hard FETCH_HEAD --quiet; fi; cd \"$d/servers/data_workspace\"; uv sync --quiet; uv run python server.py"
      ],
      "env": { "MCP_CONSTRAINED_MODE": "0" },
      "timeout": 600000
    },
    "data_analyst_transform": {
      "command": "bash",
      "args": [
        "-c",
        "d=\"$HOME/.mcp_servers/MCP_Data_Analyst\"; if [ ! -d \"$d/.git\" ]; then rm -rf \"$d\"; git clone https://github.com/azzindani/MCP_Data_Analyst.git \"$d\" --quiet; else cd \"$d\" && git fetch origin --quiet && git reset --hard FETCH_HEAD --quiet; fi; cd \"$d/servers/data_transform\"; uv sync --quiet; uv run python server.py"
      ],
      "env": { "MCP_CONSTRAINED_MODE": "0" },
      "timeout": 600000
    },
    "data_analyst_statistics": {
      "command": "bash",
      "args": [
        "-c",
        "d=\"$HOME/.mcp_servers/MCP_Data_Analyst\"; if [ ! -d \"$d/.git\" ]; then rm -rf \"$d\"; git clone https://github.com/azzindani/MCP_Data_Analyst.git \"$d\" --quiet; else cd \"$d\" && git fetch origin --quiet && git reset --hard FETCH_HEAD --quiet; fi; cd \"$d/servers/data_statistics\"; uv sync --quiet; uv run python server.py"
      ],
      "env": { "MCP_CONSTRAINED_MODE": "0" },
      "timeout": 600000
    },
    "data_analyst_visual": {
      "command": "bash",
      "args": [
        "-c",
        "d=\"$HOME/.mcp_servers/MCP_Data_Analyst\"; if [ ! -d \"$d/.git\" ]; then rm -rf \"$d\"; git clone https://github.com/azzindani/MCP_Data_Analyst.git \"$d\" --quiet; else cd \"$d\" && git fetch origin --quiet && git reset --hard FETCH_HEAD --quiet; fi; cd \"$d/servers/data_visual\"; uv sync --quiet; uv run python server.py"
      ],
      "env": { "MCP_CONSTRAINED_MODE": "0" },
      "timeout": 600000
    },
    "data_analyst_ingest": {
      "command": "bash",
      "args": [
        "-c",
        "d=\"$HOME/.mcp_servers/MCP_Data_Analyst\"; if [ ! -d \"$d/.git\" ]; then rm -rf \"$d\"; git clone https://github.com/azzindani/MCP_Data_Analyst.git \"$d\" --quiet; else cd \"$d\" && git fetch origin --quiet && git reset --hard FETCH_HEAD --quiet; fi; cd \"$d/servers/data_ingest\"; uv sync --quiet; uv run python server.py"
      ],
      "env": { "MCP_CONSTRAINED_MODE": "0" },
      "timeout": 600000
    }
  }
}
```

## Available Tools

### One endpoint: eight domain tools at `/mcp`

For a capable model, connect `/mcp` instead of the seven tiers: eight tools
instead of 69. `action` names a tool below; `args` holds its arguments.

```json
{"action": "statistical_test",
 "args": {"file_path": "Ad_Data.csv", "test": "t_test", "column_a": "clicks", "group_column": "campaign_platform"}}
```

| Tool | Actions |
|---|---|
| `data_inspect` | load_dataset, load_geo_dataset, inspect_dataset, read_column_stats, search_columns, sample_data, auto_detect_schema, validate_dataset, scan_nulls_zeros, read_receipt |
| `data_edit` | apply_patch, list_patch_ops, run_cleaning_pipeline, smart_impute, feature_engineering, list_derive_ops, filter_dataset, enrich_with_geo, run_chain, restore_version |
| `data_reshape` | reshape_dataset, aggregate_dataset, pivot_table, merge_datasets, concat_datasets, resample_timeseries, export_data |
| `data_stats` | extended_stats, statistical_test, check_outliers, correlation_analysis, lag_correlation, regression_analysis, time_series_analysis, period_comparison, cohort_analysis, detect_anomalies, analyze_text_column, compare_datasets |
| `data_chart` | generate_chart, generate_distribution_plot, generate_correlation_heatmap, generate_pairwise_plot, generate_multi_chart, generate_geo_map, generate_3d_chart, customize_chart, cross_tabulate, value_counts |
| `data_report` | run_eda, generate_auto_profile, generate_dashboard, customize_dashboard |
| `data_ingest` | list_sheets, extract_sheet, extract_all_sheets, detect_tables, extract_table, normalize_headers, trim_empty, promote_header, flatten_merged_cells, convert_file |
| `data_workspace` | create_workspace, open_workspace, register_workspace_file, list_workspace_files, save_workspace_pipeline, run_workspace_pipeline |

An action asked of the wrong tool is pointed at the right one; an argument
the action does not take is refused by name.

### The tiers

### Tier — Ingest (10 tools)

Parse and normalize spreadsheets before analysis. Handles multi-sheet Excel/ODS files, multiple tables on a single sheet, merged cells, and format conversion. All write tools include `dry_run` and create `.mcp_versions/` snapshots.

| Tool | Purpose |
|---|---|
| `list_sheets` | List all sheets in an xlsx/ods file with row and col counts |
| `extract_sheet` | Extract one sheet to CSV; `sheet` accepts name or 0-based index |
| `extract_all_sheets` | Batch-extract every sheet to separate CSVs |
| `detect_tables` | Detect separate tables on a single sheet (blank-row/col gap detection) |
| `extract_table` | Extract one detected table by index to CSV |
| `normalize_headers` | Strip whitespace, lowercase, deduplicate column names |
| `trim_empty` | Drop fully-empty leading/trailing rows and columns |
| `promote_header` | Make row N the header; drop rows above it |
| `flatten_merged_cells` | Forward-fill merged cell regions in xlsx → CSV |
| `convert_file` | Convert between xlsx / ods / csv / json / parquet |

---

### Tier 0 — Workspace (6 tools)

Manage named workspaces, file aliases, and reusable cleaning pipelines. Every successful response includes `context` (op, summary, timestamp) and `handover` (next suggested tools, carry-forward params) so the LLM can chain tools across servers without losing state.

| Tool | Purpose |
|---|---|
| `create_workspace` | Create workspace with `data/raw`, `data/working`, `reports`, `pipelines` dirs |
| `open_workspace` | Open workspace — returns file aliases, saved pipelines, pipeline history |
| `register_workspace_file` | Add a CSV with an alias and stage (raw/working/trial/output); `handover` carries `workspace:name/alias` forward |
| `list_workspace_files` | List all registered files; filter by stage |
| `save_workspace_pipeline` | Save a named list of `apply_patch` op dicts; `handover` suggests `run_workspace_pipeline` next |
| `run_workspace_pipeline` | Execute a saved pipeline on an input alias, producing a new output alias |

Files can be referenced anywhere via `workspace:name/alias` syntax — all tools resolve aliases automatically.

---

### Tier 1 — Basic (9 tools)

| Tool | Purpose |
|---|---|
| `load_dataset` | Load CSV with auto-encoding detection |
| `load_geo_dataset` | Load GeoJSON/shapefile, return geometry info |
| `inspect_dataset` | Full schema inspection: dtypes, nulls, column classification |
| `read_column_stats` | Stats for one column: mean, median, outliers, top values |
| `search_columns` | Find columns by criteria: has_nulls, dtype, name_contains |
| `apply_patch` | **51 ops** across 6 categories — see table below |
| `restore_version` | Restore a file to any previous snapshot |
| `read_receipt` | Read the operation history log for a file |
| `list_patch_ops` | List all available `apply_patch` ops; filter by category |

#### `apply_patch` op categories (51 ops total)

| Category | Ops |
|---|---|
| **original** (13) | `fill_nulls`, `drop_duplicates`, `clean_text`, `cast_column`, `replace_values`, `add_column`, `cap_outliers`, `drop_column`, `normalize`, `label_encode`, `extract_regex`, `date_diff`, `rank_column` |
| **filtering** (9) | `sort`, `filter_isin`, `filter_not_isin`, `filter_between`, `filter_date_range`, `filter_regex`, `filter_quantile`, `filter_top_n`, `dedup_subset` |
| **numeric** (11) | `log_transform`, `sqrt_transform`, `boxcox_transform`, `yeojohnson_transform`, `robust_scale`, `winsorize`, `bin_column`, `qbin_column`, `clip_values`, `round_values`, `abs_values` |
| **encoding** (3) | `ordinal_encode`, `binary_encode`, `frequency_encode` |
| **temporal** (7) | `lag`, `lead`, `diff`, `pct_change`, `rolling_agg`, `ewm`, `cumulative` |
| **structural** (8) | `column_math`, `conditional_assign`, `split_column`, `combine_columns`, `regex_replace`, `str_slice`, `concat_file`, `melt` |

### Tier 2 — Medium (7 tools, 4 retired)

| Tool | Purpose |
|---|---|
| `compute_aggregations` | *Retired: use `aggregate_dataset` (transform), `mode='groupby'`.* Unlisted; still answers |
| `cross_tabulate` | Contingency tables — saves heatmap HTML |
| `pivot_table` | Multi-dimensional pivot tables |
| `value_counts` | Frequency tables — saves bar chart HTML |
| `filter_rows` | *Retired: use `filter_dataset` (transform).* Unlisted; still answers |
| `sample_data` | Random/head/tail sampling |
| `statistical_tests` | *Retired: use `statistical_test` (statistics) — 17 tests, alpha, effect size, post-hoc.* Unlisted; still answers |
| `analyze_text_column` | Character length stats, word frequency top-N, pattern detection (email, URL, phone, number) |
| `detect_anomalies` | IQR + z-score row flagging — adds `_anomaly_score` column, saves annotated CSV |
| `compare_datasets` | Schema diff, dtype changes, row count diff, null/mean delta between two CSVs |
| `extended_stats` | *Retired here: the statistics server serves the same tool.* Unlisted; still answers |

Chart-producing medium tools accept `theme: "dark" | "light" | "device"`, `output_path`, and `open_after`.

### Tier 2 — Transform (11 tools)

Focused transformation server — richer filtering, reshaping, and aggregation than the basic tier.

| Tool | Purpose |
|---|---|
| `filter_dataset` | Filter rows by 18 condition types (equals, isin, between, regex, date_range, quantile_between, starts_with, ends_with, …) + optional sort. A condition may compare against `other_column` instead of `value` |
| `reshape_dataset` | Reshape data: `pivot`, `melt`, `split_column`, `combine_columns`, `transpose` |
| `aggregate_dataset` | Aggregate: `groupby`, `crosstab`, `value_counts`, `describe`, `window` |
| `resample_timeseries` | Resample time series (D/W/M/Q/Y/H). `agg_func`: `sum mean count min max median std first last`. `dayfirst`: `auto` (default), `true`, `false` |
| `merge_datasets` | Merge two datasets; a key is picked unasked only when it identifies rows |
| `concat_datasets` | Stack multiple CSVs. `direction`: `rows`, or `columns` (side by side — needs equal row counts) |
| `smart_impute` | Auto-impute: numeric→median, datetime→ffill, categorical→mode |
| `run_cleaning_pipeline` | Multi-op cleaning with single snapshot + rollback |
| `feature_engineering` | `features`: `bins date_parts one_hot text_length` — or add named columns with `derive` (see below). `one_hot` is capped at 10 distinct values per column and 5 columns per call; skipped columns come back in `one_hot_skipped` with a reason each |
| `list_derive_ops` | The `derive` grammar for `feature_engineering`: every op with its required and optional keys, and a worked example. Omit `op` for all of them |
| `enrich_with_geo` | Merge dataset with geo data on a location key |
| `run_chain` | A whole job in one call: named steps that load files, run ops, compute a value, join, group and write -- checked whole before any file is read, and nothing written until every step ran (see below) |

#### Chains — `run_chain(steps=[...])`

One call for a job that used to take one call per step, each re-reading the
file the last one wrote. Every step has an `id` and ONE action; it reads the
table above it, or any earlier step named in `from`.

| Action | Step | Makes |
|---|---|---|
| `load` | `{"id": "orders", "load": "orders.csv"}` | a table from a CSV |
| `ops` | `{"ops": [...], "fallback": [...]}` | the table after the ops: every `apply_patch` op, plus `{"op": "filter", "where": "<formula>"}`, `{"op": "derive", "name": "net", "expr": "<formula>"}`, `{"op": "impute", "columns": [...]}` (smart_impute's fill: median, the value before, or mode) and `{"op": "for_each", "columns": [...], "do": [...]}`; `fallback` runs instead when an op fails |
| `scalar` | `{"id": "p95", "scalar": "percentile(net, 95)"}` | one value, read by later formulas as `$p95` |
| `join` | `{"join": ["clean", "cust"], "on": "customer_id", "how": "left"}` | the joined table (refused before it is built when it would not fit) |
| `group_by` | `{"group_by": ["region"], "agg": {"revenue": "sum(net)", "big": "count_if(net > $p95)"}}` | one row per group |
| `write` | `{"write": "region_summary.csv"}` | the table, saved as CSV (a file it replaces is snapshotted first) |
| `param` | `{"id": "min_amount", "param": 50}` | a value with a default, read as `$min_amount` -- also inside a `load`/`write` path |
| `call` | `{"call": "clean.chain.json", "args": {"min_amount": 100}, "tables": {"orders": "raw"}}` | the last table of a saved chain, run with its params set by `args` and any of its `load` steps handed a table from this chain by `tables` |

`for_each` repeats its `do` ops once per column, expanded before anything runs:
`$column` (or the name given by `as`) is the column -- backticked inside a
formula, as-is in any other field, `${column}` inside longer text such as
`"name": "${column}_log"`. A failure names the column it was for.

Formulas are the `add_column` language (precedence, parentheses, comparisons,
`and`/`or`, `if_else`, `coalesce`, ...). Aggregates: `sum mean median min max
std count count_distinct count_if percentile first last`, combined with
arithmetic (`sum(clicks) / sum(impressions) * 100`). `on_error: "skip"` lets a
step fail without stopping the chain; `until` runs the steps up to one id;
`dry_run` runs everything in memory and returns each step's rows, columns and
three sample rows. The whole chain -- every id, reference, op field and
`$name`, and every chain it calls -- is checked before any file is read, and
nothing is written until every step has run.

`save_as="clean.chain.json"` saves the chain once it has run: a function whose
`param` steps are its arguments. Another chain `call`s it by that path. Calls
nest five deep, a chain that calls itself is refused, and the steps of a whole
call tree are capped at 200.

`export_pandas=true` returns the chain as a pandas script (`pandas` in the
answer; a dry run exports too): the same steps, `$names` as variables, a
called chain as a Python function of its params, and nothing written until
every step ran. It carries small copies of the chain's rules -- arithmetic on
floats, element-wise `and`, a missing value in a filter read as false -- so it
writes the same files; the tests run every exported script and compare them
byte for byte. It covers `filter`, `derive`, `drop_column`, `sort`,
`drop_duplicates`, `dedup_subset`, `filter_isin`, `filter_not_isin`,
`filter_between`, `filter_top_n`, `clip_values`, `round_values`, `abs_values`
and `fill_nulls`; any other op is refused by name in `pandas_refused`.

#### Derived columns — `feature_engineering(derive=[...])`

Aggregation tools group by columns that are already in the file. `derive` adds
named ones first, so "tonnage by year" works when the file only carries the
integer `199907`. Specs apply in order, so a later one can read an earlier one's
output, and the whole list is refused on the first bad spec rather than writing
a half-derived frame.

```json
[
  {"name": "year",  "op": "text",       "column": "Activity Period", "how": "slice", "start": 0, "stop": 4, "as": "int"},
  {"name": "start", "op": "parse_date", "column": "Activity Period Start Date"},
  {"name": "month", "op": "date_part",  "column": "start", "part": "month"},
  {"name": "share", "op": "arith",      "column": "tons", "how": "div", "other": "total"},
  {"name": "code",  "op": "compare",    "column": "Operating Airline", "how": "ne", "other": "Published Airline"}
]
```

| `op` | `how` / `part` |
|---|---|
| `parse_date` | optional `format`, `dayfirst` |
| `date_part` | `year month day quarter weekday week yearmonth date` |
| `arith` | `add sub mul div floordiv mod` — against `other` (a column) or `value` |
| `compare` | `eq ne gt gte lt lte` — against `other` or `value` |
| `text` | `upper lower strip len slice combine` |

Every op takes an optional `"as": "int" | "float" | "str" | "bool"` cast.

#### Date orientation

Date parsing detects day-first vs month-first from the column itself: a value
above 12 in either field settles it, and so does a field that stays constant
while the series spans years (monthly data written `01-07-1999`). Where the data
is genuinely ambiguous the response says so in `progress` rather than guessing
silently. `time_series_analysis`, `period_comparison`, `cohort_analysis`,
`lag_correlation` and `resample_timeseries` take
`dayfirst: "auto" | "true" | "false"` to settle it explicitly. Those three are the
whole vocabulary and anything else is refused by name: `dayfirst="yes"` used to
be accepted as day-first and `dayfirst="banana"` fell through to auto-detect,
so a typo silently chose a date interpretation and the response said nothing.

---

### Tier 3 — Statistics (12 tools)

| Tool | Purpose |
|---|---|
| `statistical_test` | 17 test types: shapiro_wilk, ks, anderson, t_test, paired_t_test, one_sample_t, anova, chi_square, fisher, mann_whitney, wilcoxon, kruskal, levene, pearson, spearman, kendall, proportion_z — includes effect sizes (Cohen's d, η², Cramér's V) |
| `regression_analysis` | OLS or logistic regression via statsmodels — returns coefficients, p-values, R², RMSE, AIC, BIC, VIF, normality diagnostics, and insight summary |
| `period_comparison` | MoM / QoQ / YoY comparison — returns delta, pct_change, direction per metric |
| `time_series_analysis` | Trend + seasonality + rolling stats + exponential-smoothing forecast + **STL decomposition** + **ACF/PACF** + **ADF stationarity test** |
| `correlation_analysis` | Correlation matrix (Pearson/Spearman/Kendall) + top N pairs |
| `lag_correlation` | Lead-lag cross-correlation of two columns across a lag sweep — resamples to a regular grid, correlates at every lag, reports the peak, its lag, and a Bonferroni-adjusted p-value. `lag +k` means x leads y by k periods |
| `cohort_analysis` | Cohort retention matrix with auto-detected identifiers |
| `extended_stats` | Deep stats: skewness, kurtosis, percentiles, CI, MAD, CV |
| `check_outliers` | IQR/std outlier scan |
| `scan_nulls_zeros` | Null/zero detection + suggested fixes |
| `validate_dataset` | Data quality score 0–100 |
| `auto_detect_schema` | Smart column type inference with cleaning suggestions |

---

### Tier 3 — Visual (13 tools)

| Tool | Purpose |
|---|---|
| `run_eda` | Fast EDA: stats, nulls, correlations, outliers — saves HTML. `target_column` adds an association ranking and a leakage panel; `compare_to` adds drift; `mode` / `sample_n` / `include` control depth |
| `generate_auto_profile` | Full column profile: per-column charts, correlation network, quality dashboard |
| `generate_dashboard` | Interactive HTML dashboard: KPI cards, sparklines, violin plots, geo maps. Accepts a declarative `spec`, and `sources=[…]` for extra files as tabs. Every card is a panel in the page's own `_PANELS` document -- its columns, aggregate, title and style (colour, caps, bins, moving-average window, layout) -- drawn by one renderer over the theme; a `device` page follows the reader's light/dark setting |
| `customize_dashboard` | Edit a saved dashboard's embedded spec and re-render — a JSON change, not a described-in-prose rebuild. `ops` edit one panel at a time (`set_panel`, `add_panel`, `remove_panel`, `move_panel`); `dry_run` checks the edit and writes nothing |
| `generate_chart` | 13 chart types: bar, pie, line, scatter, geo, treemap, radius, time_series, sunburst, waterfall, funnel, parallel_coords, sankey |
| `generate_geo_map` | Scatter map (lat/lon) or choropleth (country/state) — auto-detected |
| `generate_3d_chart` | 3D scatter or surface chart |
| `generate_distribution_plot` | Histogram + box plot for numeric columns |
| `generate_correlation_heatmap` | Interactive Pearson/Spearman heatmap |
| `generate_pairwise_plot` | Scatter matrix for numeric columns |
| `generate_multi_chart` | Multi-variable bar/line chart (2+ metrics) |
| `export_data` | Export to CSV, Excel, or JSON. The workbook carries a README sheet, frozen header, autofilter, number formats and column validation |
| `customize_chart` | Post-generate edits to an existing HTML chart: title, axis labels, colour scheme, annotations, value labels, dimensions. `ops` edit the figure by path over an allow-list -- `{op: set, path: "layout.yaxis.type", value: "log"}`, `data[N]` or `data[*]` for traces (type bar/scatter, mode, colours, line width and dash, `yaxis: "y2"` for a right-hand axis) -- and `{op: reference_line, axis, value, label}`; anything outside the list is refused by name. `dry_run` writes nothing |

#### New chart types in `generate_chart`

| Type | Use case |
|---|---|
| `sunburst` | Hierarchical part-of-whole (requires `hierarchy_columns`) |
| `waterfall` | Running total / delta analysis (financial, budget) |
| `funnel` | Conversion / drop-off stages sorted descending |
| `parallel_coords` | Compare all numeric columns across rows (colored by value) |
| `sankey` | Flow between source and target categories (requires `color_column` as target) |

### Geo Map (`generate_geo_map`)

Auto-detects the right map type from your data:

| Data columns | Map type | Notes |
|---|---|---|
| `lat`/`latitude` + `lon`/`longitude` | Scatter map | No external data; uses Plotly's Natural Earth projection |
| `country`/`iso3`/`iso_code` | Choropleth (world) | Auto-detects ISO-3 codes vs country names |
| `state`/`state_code`/`state_abbr` | Choropleth (USA) | 2-letter US state codes → `USA-states` mode |

The `generate_dashboard` tool also auto-inserts geo charts when it detects these column patterns.

### Theme options (all HTML outputs)

| Value | Behaviour |
|---|---|
| `"dark"` | GitHub-style dark palette, Plotly dark template (default) |
| `"light"` | Light palette, Plotly white template |
| `"device"` | Auto-detects system `prefers-color-scheme`, switches at runtime via JS |

## Report Highlights

### `run_eda`
- **Alerts panel**: auto-detects CONSTANT columns, HIGH NULLS, ZEROS, HIGH CARDINALITY, IMBALANCED, SKEWED, OUTLIERS, HIGH CORR, DUPLICATES
- **Pearson + Spearman** correlation heatmaps
- **Missing value matrix**: Plotly heatmap showing WHERE data is absent (up to 300 sampled rows)
- **Zero counts** in column summary table
- Data sample (first 5 rows), outlier table, key insights
- **Quality breakdown**: `{completeness, validity, uniqueness, drift}` behind the 0–100 score. `drift` is reported as `None` rather than invented — it needs a baseline, and a component scored 100 because nothing was measured would be a different kind of lie. Pass `compare_to` and it becomes a number
- **Target leakage panel** (with `target_column`): names any feature that may already contain the outcome, with the evidence for each — how well it separates the classes alone, whether its *missingness* tracks the target, and whether it is named like a post-outcome field. The last is labelled a hint, because nothing was measured for it. Suspects never move the quality score: a number that changes depending on whether you named a target would be a number about the question, not the data
- **Depth**: `mode="minimal"` skips the correlation matrices, the outlier scan and the HTML page; `mode="full"` returns every correlation pair. `standard` is exactly what the tool did before the parameter existed
- **Honest sampling**: with `sample_n`, the response carries `was_sampled`, `sample_n` and `rows_total`, and the page header carries `rows_plotted / rows_total / was_sampled / data_hash`

### `generate_auto_profile`
All of `run_eda` plus:
- Per-column distribution charts (histogram + box for numeric, bar for categorical with percentage bars)
- Correlation network graph (force-directed layout for pairs with |r| > 0.5)
- Data quality dashboard (completeness bars per column)
- Summary statistics table (mean, median, std, Q1, Q3, skew, kurtosis, outliers, zeros)
- Actionable recommendations

### `generate_dashboard`
- **KPI sparklines**: 30-point mini trend chart on each metric card
- **Trend indicators**: ↑ / ↓ / → based on first-half vs second-half mean comparison
- **Data quality card**: overall quality score (0–100)
- **Violin plots**: distribution + outliers for numeric columns
- **Responsive filter bar**: pills, searchable dropdowns, number ranges and date ranges, with Clear Filters. A line under the bar says what is filtered, and the tab's session keeps each page's own filters
- **Declarative spec**: pass `spec={title, theme, layout, kpis, filters, interactions}` and the engine renders it rather than improvising a layout. The page embeds the spec it was built from, so the next agent can read it, change one panel, and re-render through `customize_dashboard`
- **Panel style and place**: a `layout` panel is `{chart, cols, agg, title, style, place}`. `style` takes only the fields its chart draws -- bar: `color colors top_n sort value_labels y_scale format prefix suffix`; line: `color accent ma legend y_scale format prefix suffix`; pie: `palette colors top_n legend`; scatter: `color accent legend y_scale`; histogram: `color accent bins`; box: `palette colors top_n y_scale format prefix suffix`; geo_scatter: `color`; choropleth: `colorscale format prefix suffix` -- and anything else is refused by name. `place: {span: 1-12, height: px}` puts the page on a 12-column grid (one column on a phone). The page's own `style: {palette, colors}` sets the palette and a colour per category value that every panel uses, so "North" is one colour everywhere; a panel's `colors` wins over the page's. Sequential colour scales draw the largest value darkest
- **Panels that are not plots**: `{chart: "section", title}` is a heading across the grid; `{chart: "text", title, text}` a note, escaped, in its own words; `{chart: "kpi", cols: {value}, agg, style: {color, format, prefix, suffix}}` one headline number; `{chart: "table", cols: {category, value}, agg, style: {top_n, sort, format, prefix, suffix}}` the top groups and their aggregate. The KPI and the table are computed in the page from the filtered rows, like every chart. `tabs` group any of them, and the tab bar sits above the cards it switches
- **Filters as controls, defaults and scopes**: a `filters` entry is a column name or `{column, control, default, scope}`. `control` is `pills` or `dropdown` for a list of values, `range` for numbers, `date_range` for a date column (its days, YYYY-MM-DD). `default` is what the page opens on: a list of values, or `{min, max}` with either bound optional. `scope` is `"page"` (the default) or the layout slots the filter narrows; the KPI row, the row count, the rows table and the export follow the page's filters only. A control that does not fit its column, a value the column does not hold, or defaults that would open the page or a panel on no rows are refused by name
- **Panel edits**: `customize_dashboard(path, ops=[...])` edits the layout in place. `set_panel {slot, <fields>}` replaces the fields it names (null removes one); `cols`, `style` and `place` change key by key. `add_panel {panel, at, tab}`, `remove_panel {slot}` and `move_panel {slot, to}` do what they say. Tabs and filter scopes follow the panels they name, and an edit that would leave a tab or a scoped filter with no panel is refused by name. `changes` apply first, then the ops. A detected page's slots are chart kinds, not panels: hand its `spec.layout` back as `changes={'layout': ...}` to make it editable
- **Multi-source tabs**: `sources=["chargedoff.csv", "anomalies.csv"]` renders each as its own tab. Row counts and summaries are computed server-side over the whole file, so a tab's totals are exact even when its table is paged
- **`embed_rows`**: caps how many rows are embedded in the page. It defaults to every row on purpose — the KPI cards and chart heights are computed in the browser from the embedded rows, so a default cap would silently divide every number on the dashboard. The saving is small in any case: on a 38,576-row file, dropping from 4,000 to 500 embedded rows saved 95 KB of a 4.9 MB page, because the weight is Plotly, not data
- **Honest embedding**: the header always carries `rows_embedded` and `was_sampled`

## Usage Examples

### Load and inspect a dataset

```
Load the file C:\data\sales.csv and tell me about its schema
```

### Find problem columns

```
Search for columns in C:\data\sales.csv that have null values
```

### Get column statistics

```
Show me the statistics for the Revenue column in C:\data\sales.csv
```

### Clean data

```
Fill null values in the Revenue column of C:\data\sales.csv using the median strategy
```

### Full cleaning workflow

```
Analyze C:\data\messy.csv for issues, then clean it up — fill nulls, remove duplicates, and standardize text
```

### Run a full data profile (ydata-profiler style)

```
Generate a comprehensive profile of C:\data\sales.csv
```

### Quick EDA with alerts

```
Run EDA on C:\data\sales.csv and highlight any data quality issues
```

### Interactive dashboard

```
Generate a dashboard for C:\data\sales.csv in light theme
```

### Statistical analysis

```
Run statistical tests on C:\data\sales.csv to compare Revenue across Regions
```

### Time series analysis

```
Analyze the time series trends in C:\data\sales.csv
```

### Undo a change

```
Restore C:\data\sales.csv to the previous version
```

### Project workflow

```
Create a project called "q3_analysis", register C:\data\sales_raw.csv as alias "raw_sales",
then save a pipeline that fills nulls in Revenue and drops duplicates
```

### Run a saved pipeline

```
Run the "clean_revenue" pipeline on the "raw_sales" alias and save the output as "clean_sales"
```

### Statistical analysis (new)

```
Run an OLS regression on C:\data\sales.csv with Revenue as the target and Units, Discount as predictors
```

```
Compare Revenue month-over-month in C:\data\monthly.csv
```

## Configuration

### Constrained Mode

For lower-memory machines, set `MCP_CONSTRAINED_MODE=1` in the `env` section of `mcp.json`. This reduces:
- DataFrame rows returned: 100 → 20
- Search results: 50 → 10
- Column limits: 50 → 20

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `MCP_CONSTRAINED_MODE` | `0` | Set to `1` for low-memory machines |
| `MCP_OUTPUT_DIR` | `~/Downloads` | Where generated charts/reports/CSVs land by default |
| `MCP_PUBLIC_BASE_URL` | _(unset)_ | Public URL serving `MCP_OUTPUT_DIR`; adds `public_url` to results |
| `MCP_FETCH_URLS` | `0` | `1` lets any `file_path` argument be an `http(s)` URL |
| `MCP_FETCH_ALLOW_PRIVATE` | `0` | `1` permits fetching hosts on private/loopback addresses |
| `MCP_MAX_FETCH_MB` | `100` | Size cap for a fetched URL |
| `MCP_MAX_INLINE_MB` | `10` | Size cap for a file sent inline, as a `data:` URI where a path goes |
| `MCP_MAX_UPLOAD_MB` | `100` | Size cap for a whole file sent inline in parts, or through an upload URL |
| `MCP_UPLOAD_URLS` | `0` | `1` (with `MCP_UPLOAD_BASE_URL`) hands a caller-side path a single-use upload URL |
| `MCP_UPLOAD_BASE_URL` | _(unset)_ | This server's public origin, which upload URLs are built on |
| `MCP_UPLOAD_SECRET` | _(per process)_ | Key upload URLs are signed with; set it to keep them valid across a restart |
| `MCP_MAX_MERGE_MB` | `256` | Largest table `merge_datasets` will build in memory; a bigger join is refused before it runs |

### Hybrid local + remote file handling

The same engine serves a local stdio install and a self-hosted HTTP endpoint,
but over HTTP the caller shares no filesystem with the server: a server-local
output path means nothing to it, and it may hold a link rather than a path.
Three opt-in variables close that gap without changing local behaviour — all
are unset by default, so a local install stays offline and writes to
`~/Downloads` exactly as before:

- **`MCP_OUTPUT_DIR`** — bind-mount a directory here and every generated file
  defaults into it, so outputs land somewhere you can actually see. It
  outranks the input file's own directory; an explicit `output_path` still
  wins over both.
- **`MCP_PUBLIC_BASE_URL`** — the public URL that serves that directory. Every
  produced file then comes back with a `public_url` you can open or hand on.
- **`MCP_FETCH_URLS=1`** — `load_dataset("https://…/sales.csv")`, and every
  other path argument, accepts a link; it is downloaded into
  `MCP_OUTPUT_DIR/inbox` before the tool runs. Hosts resolving to
  loopback/link-local/private addresses are refused, redirects included,
  unless `MCP_FETCH_ALLOW_PRIVATE=1`.
  A Google Drive, Docs, Dropbox, GitHub or GitLab share link is read as the
  file it points to, and a web page served where a file was asked for (a link
  that is not public answers with a sign-in page) is refused, not parsed. A path
  from the caller's side -- a chat's sandbox such as `/mnt/user-data/…` -- is
  refused by name, with the ways to bring the file here.
- **Inline files** — where a path goes, a file's bytes may go instead:
  `data:text/csv;name=sales.csv;base64,<bytes>` is saved to
  `MCP_OUTPUT_DIR/inbox/sales.csv` before the tool runs, and the tool reads
  that path. It is for a caller whose file sits in its own sandbox (a
  claude.ai upload) with no link to give. Every byte is model output, so it
  is capped at `MCP_MAX_INLINE_MB` (default 10); the same bytes sent twice
  are one file, and a taken name is never overwritten.
  A bigger file goes in parts: add `part=2/5;sha256=<of the whole file>` to
  each. The tool answers `tool_ran: false` with the parts still missing until
  the last lands, then runs on the joined, checked file (`MCP_MAX_UPLOAD_MB`,
  default 100; an upload left unfinished for an hour is dropped).
- Upload URLs are off by default. With `MCP_UPLOAD_URLS=1` and `MCP_UPLOAD_BASE_URL`
  (this server's public origin), the refusal for a path on the caller's side
  carries a URL minted for that file: `curl -T <file> '<url>'` from the sandbox
  writes it to `MCP_OUTPUT_DIR/inbox/`, and the answer is the path to pass. The
  bytes never pass through the model. A URL writes one file, once, within 15
  minutes, up to `MCP_MAX_UPLOAD_MB`, under the name fixed when it was minted; a
  forged, expired or spent one writes nothing. The route takes no API key -- its
  signed token (`MCP_UPLOAD_SECRET`, else a key made per process) is the
  credential -- so turning it on is the operator's decision.

Tools that create a file also accept `return_content=True`, which embeds the
file's bytes as `content_base64` for callers that have neither a shared
filesystem nor access to the public URL.

## Deployment

| Mode | Best for | Transport | Auth |
|---|---|---|---|
| **Local stdio** (default, above) | LM Studio / Claude Code on your machine | stdio | none |
| **Local Docker / HTTP** | Testing, or one other machine on your LAN | HTTP | optional |
| **VPS Docker** | Remote MCP clients (claude.ai, hosted harnesses) | HTTP | **required** |

Of the 9 `servers/` dirs, 7 are deployed: `data_basic`, `data_medium`,
`data_statistics`, `data_transform`, `data_visual`, `data_workspace`,
`data_ingest`. `data_advanced` is a retired stub (zero tools — superseded by
`data_visual`) and `data_project` is a redirect alias of `data_workspace`
(same process); neither is deployed separately.

Each sub-server keeps its own stdio server for local LM Studio "add one
sub-server" installs. For Docker/remote deployment all 7 run as separate MCP
endpoints inside **one process** (`unified_server.py`) on **one port** —
pandas/numpy/scipy/matplotlib load once instead of seven times (~260 MiB vs
~1.1 GiB idle), and all 7 still share one bearer-token set.

### HTTP transport (no Docker)

```bash
uv run python unified_server.py --port 8810
curl http://localhost:8810/health            # {"status":"ok","version":"0.2.2","sub_servers":[...]}
curl http://localhost:8810/basic/health      # per-sub-server health
```

### Docker

```bash
docker compose up -d --build
curl http://localhost:8810/health            # aggregate
curl http://localhost:8810/basic/mcp         # data_basic
curl http://localhost:8810/ingest/mcp        # data_ingest
```

With auth (**required** for any publicly reachable deploy — this is how the
production `data.casava.space` endpoint runs):

```bash
echo "DA_API_KEY=$(openssl rand -hex 24)" > .env   # gitignored, auto-loaded by docker-compose.yml
docker compose up -d --build
```

For multiple named clients instead of one shared key (Folio-style):

```bash
cp tokens.example.json tokens.json   # edit: replace placeholders with `openssl rand -hex 32`
DA_TOKENS_FILE=/path/to/tokens.json docker compose up -d --build
```

`/<name>/mcp` requires `Authorization: Bearer <token>` once any of
`DA_TOKENS_FILE` / `DA_TOKENS` / `DA_API_KEY` is set; `/health` and
`/version` (aggregate and per-sub-server) stay unauthenticated.

### Deployment environment variables

| Variable | Default | Description |
|---|---|---|
| `DA_HOST` | `0.0.0.0` | Bind address for the unified server |
| `DA_PORT` | `8810` | Port for the unified server (all 7 sub-servers) |
| `DA_TOKENS_FILE` | unset | JSON file of named bearer tokens (`{"name": "token"}`) — highest priority, shared across all sub-servers |
| `DA_TOKENS` | unset | Inline `"name:token,name2:token2"` |
| `DA_API_KEY` | unset | Single shared bearer token |

### Remote testing (Cloudflare Quick Tunnel)

Same idea as `azzindani/Folio`'s `launch.sh`: bring the Docker deployment up
and expose it at an ephemeral `*.trycloudflare.com` URL — no VPS, no DNS, no
account — so all 7 sub-servers are reachable from any MCP-compatible harness
for a quick remote smoke test.

```bash
./launch_tunnel.sh          # docker compose up -d --build, then tunnel
./launch_tunnel.sh stop     # tear the tunnel down (container keeps running)
```

Not for production: Quick Tunnels are unauthenticated at the transport layer.
Set `DA_API_KEY` or `DA_TOKENS_FILE` before tunneling so `/<name>/mcp` still
requires a bearer token even while it's publicly reachable.

### Remote smoke test (`remote_smoke_test.sh`)

Run in CI against a container (the `e2e` job) and by hand against the
deployment. `pytest` itself stays offline. Exercises a running HTTP endpoint: auth enforcement plus a real
handwritten-prompt-style call for **all 73 tools** (69 listed, 4 retired) across all 7 sub-servers
(basic, medium, statistics, transform, visual, workspace, ingest), against
real generated fixtures (a 200-row sales CSV, a region-population CSV, a real
GeoJSON, and a real messy multi-sheet `.xlsx` with merged cells), chaining
real outputs between calls. This is what caught the cross-sub-server
`sys.modules` collision breaking `run_workspace_pipeline` in the unified
Docker deployment.

```bash
./remote_smoke_test.sh                      # reads DA_API_KEY from .env, targets data.casava.space
DOMAIN=http://localhost:8810 ./remote_smoke_test.sh   # test a different target
CONTAINER=mcp-data-analyst ./remote_smoke_test.sh      # override container name
```

## Uninstall

**Step 1:** Remove from LM Studio
1. Open LM Studio → Developer tab (`</>`)
2. Delete all `data_analyst_*` entries (`workspace`, `basic`, `medium`, `transform`, `statistics`, `visual`, `ingest`) from MCP Servers
3. Restart LM Studio

**Step 2:** Delete installed files
```cmd
rmdir /s /q %USERPROFILE%\.mcp_servers\MCP_Data_Analyst
```

Or run the uninstall script:
```cmd
%USERPROFILE%\.mcp_servers\MCP_Data_Analyst\install\uninstall.bat
```

## Architecture

```
MCP_Data_Analyst/
├── servers/
│   ├── data_workspace/      ← T0: workspace management (6 tools)
│   │   ├── server.py        ← thin MCP wrapper; exposes workspace: tool names
│   │   └── engine.py        ← create/open/register/list/save/run + context+handover
│   ├── data_basic/          ← T1: load, inspect, patch, restore (9 tools)
│   │   ├── server.py        ← thin MCP wrapper (zero domain logic)
│   │   ├── engine.py        ← public API + list_patch_ops
│   │   └── _patch_ops.py    ← 51 apply_patch operations
│   ├── data_medium/         ← T2: aggregation, anomaly, text, comparison (7 tools, 4 retired)
│   │   ├── server.py
│   │   ├── engine.py
│   │   ├── _med_helpers.py
│   │   ├── _med_inspect.py
│   │   ├── _med_transform.py
│   │   ├── _med_analysis.py
│   │   └── _med_report.py   ← aggregations, cross-tab, pivot
│   ├── data_transform/      ← T2: richer filter/reshape/aggregate (10 tools)
│   │   ├── server.py
│   │   └── engine.py        ← filter_dataset, reshape_dataset, aggregate_dataset
│   ├── data_statistics/     ← T3: full statistics suite (12 tools)
│   │   ├── server.py
│   │   ├── engine.py
│   │   ├── _stats_tests.py  ← 17 statistical tests + effect sizes
│   │   ├── _stats_regression.py ← OLS + logistic regression
│   │   └── _stats_comparative.py← period comparison (MoM/QoQ/YoY)
│   ├── data_project/        ← redirect to data_workspace (backward compat)
│   ├── data_advanced/       ← engine only (no active server; used by data_visual)
│   │   ├── engine.py
│   │   ├── _adv_eda.py
│   │   ├── _adv_profile.py
│   │   ├── _adv_charts.py
│   │   ├── _adv_gencharts.py← 13 chart types, geo_map, 3d_chart
│   │   └── _adv_dashboard.py
│   ├── data_visual/         ← T3: EDA + dashboards + charts + customization (12 tools)
│   │   ├── server.py
│   │   ├── engine.py        ← re-exports data_advanced + customize_chart
│   │   └── _adv_customize.py← post-generate chart editing
│   └── data_ingest/         ← Ingest: xlsx/ods parsing, multi-table, normalization (10 tools)
│       ├── server.py
│       └── engine.py        ← list_sheets, extract_*, detect_tables, normalize_*, convert_file
├── shared/                  ← Ring-2 utilities (no MCP imports)
│   ├── version_control.py   ← snapshot() / restore() / list_versions()
│   ├── patch_validator.py   ← validate op arrays before apply
│   ├── file_utils.py        ← path resolution (project: aliases), atomic writes
│   ├── project_utils.py     ← project manifest CRUD, alias resolution
│   ├── platform_utils.py    ← MCP_CONSTRAINED_MODE, get_max_rows()
│   ├── progress.py          ← ok/fail/info/warn/undo helpers
│   ├── receipt.py           ← append_receipt() / read_receipt_log()
│   ├── html_layout.py       ← output path priority, HTML helpers
│   └── html_theme.py        ← CSS vars, Plotly templates, responsive HTML
├── install/
│   ├── run_server.bat       ← Windows launcher
│   └── uninstall.bat        ← Windows uninstaller
└── tests/
    ├── conftest.py
    ├── test_engine_basic.py     ← 124 tests (unit + e2e + four-tool pattern)
    ├── test_engine_medium.py    ← tests including STL/ACF/ADF
    ├── test_engine_advanced.py
    ├── test_engine_project.py   ← 26 tests (full project workflow e2e)
    ├── test_workspace_server.py ← 26 tests (context+handover contract)
    ├── test_engine_transform.py ← 35 tests (filter/reshape/aggregate)
    ├── test_engine_statistics.py← 39 tests (regression, stat tests, period comparison)
    ├── test_engine_ingest.py    ← 96 tests (list/extract/detect/normalize/convert)
    ├── test_a_*.py … test_the_*.py ← one file per defect found, named for the defect
    ├── test_shared.py
    ├── verify_tool_docstrings.py← CI gate: all @mcp.tool() docstrings ≤ 80 chars
    └── verify_output_paths.py   ← CI gate: output path priority contract
```

## Development

### Local Testing

```bash
# Install all dependencies from root (single lockfile)
uv sync

# Run all 2450 tests
uv run pytest tests/ -q --tb=short

# Run in constrained mode
MCP_CONSTRAINED_MODE=1 uv run pytest tests/ -q --tb=short

# Format → lint → type-check → verify docstrings → test (full CI sequence)
uv run ruff format servers/ shared/ tests/ --exclude "**/.venv/**"
uv run ruff check servers/ shared/ tests/ --exclude "**/.venv/**"
uv run pyright servers/ shared/
uv run python tests/verify_tool_docstrings.py
uv run python tests/verify_output_paths.py
uv run pytest tests/ -q --tb=short
```

### Run a single server locally

```bash
# Each server has its own venv — cd in, sync, then run server.py directly
cd servers/data_basic && uv sync && uv run python server.py
cd servers/data_medium && uv sync && uv run python server.py
cd servers/data_workspace && uv sync && uv run python server.py
cd servers/data_transform && uv sync && uv run python server.py
cd servers/data_statistics && uv sync && uv run python server.py
cd servers/data_visual && uv sync && uv run python server.py
cd servers/data_ingest && uv sync && uv run python server.py
```

## License

MIT
