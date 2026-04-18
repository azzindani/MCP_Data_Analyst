# MCP Data Analyst

A self-hosted MCP server that gives local LLMs structured access to CSV/tabular data analysis tools. No cloud APIs, no API keys — everything runs on your machine.

## Features

- **59 tools** across 6 servers: workspace (6), basic (9), medium (11), transform (10), statistics (11), visual (12)
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
- **Python 3.12 or higher** — `python --version`
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
> Set-Location $d; uv sync
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
        "$d = Join-Path $env:USERPROFILE '.mcp_servers\\MCP_Data_Analyst'; $g = Join-Path $d '.git'; if (!(Test-Path $g)) { if (Test-Path $d) { Remove-Item -Recurse -Force $d }; git clone https://github.com/azzindani/MCP_Data_Analyst.git $d --quiet } else { Set-Location $d; git fetch origin --quiet; git reset --hard FETCH_HEAD --quiet }; uv sync --quiet; uv run python -m servers.data_workspace.server"
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
        "$d = Join-Path $env:USERPROFILE '.mcp_servers\\MCP_Data_Analyst'; $g = Join-Path $d '.git'; if (!(Test-Path $g)) { if (Test-Path $d) { Remove-Item -Recurse -Force $d }; git clone https://github.com/azzindani/MCP_Data_Analyst.git $d --quiet } else { Set-Location $d; git fetch origin --quiet; git reset --hard FETCH_HEAD --quiet }; uv sync --quiet; uv run python -m servers.data_transform.server"
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
        "$d = Join-Path $env:USERPROFILE '.mcp_servers\\MCP_Data_Analyst'; $g = Join-Path $d '.git'; if (!(Test-Path $g)) { if (Test-Path $d) { Remove-Item -Recurse -Force $d }; git clone https://github.com/azzindani/MCP_Data_Analyst.git $d --quiet } else { Set-Location $d; git fetch origin --quiet; git reset --hard FETCH_HEAD --quiet }; uv sync --quiet; uv run python -m servers.data_statistics.server"
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
        "$d = Join-Path $env:USERPROFILE '.mcp_servers\\MCP_Data_Analyst'; $g = Join-Path $d '.git'; if (!(Test-Path $g)) { if (Test-Path $d) { Remove-Item -Recurse -Force $d }; git clone https://github.com/azzindani/MCP_Data_Analyst.git $d --quiet } else { Set-Location $d; git fetch origin --quiet; git reset --hard FETCH_HEAD --quiet }; uv sync --quiet; uv run python -m servers.data_visual.server"
      ],
      "env": { "MCP_CONSTRAINED_MODE": "0" },
      "timeout": 600000
    }
  }
}
```

4. Wait for the blue dot next to each server
5. Start chatting — the model will see all 59 tools

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
        "d=\"$HOME/.mcp_servers/MCP_Data_Analyst\"; if [ ! -d \"$d/.git\" ]; then rm -rf \"$d\"; git clone https://github.com/azzindani/MCP_Data_Analyst.git \"$d\" --quiet; else cd \"$d\" && git fetch origin --quiet && git reset --hard FETCH_HEAD --quiet; fi; cd \"$d\"; uv sync --quiet; uv run python -m servers.data_workspace.server"
      ],
      "env": { "MCP_CONSTRAINED_MODE": "0" },
      "timeout": 600000
    },
    "data_analyst_transform": {
      "command": "bash",
      "args": [
        "-c",
        "d=\"$HOME/.mcp_servers/MCP_Data_Analyst\"; if [ ! -d \"$d/.git\" ]; then rm -rf \"$d\"; git clone https://github.com/azzindani/MCP_Data_Analyst.git \"$d\" --quiet; else cd \"$d\" && git fetch origin --quiet && git reset --hard FETCH_HEAD --quiet; fi; cd \"$d\"; uv sync --quiet; uv run python -m servers.data_transform.server"
      ],
      "env": { "MCP_CONSTRAINED_MODE": "0" },
      "timeout": 600000
    },
    "data_analyst_statistics": {
      "command": "bash",
      "args": [
        "-c",
        "d=\"$HOME/.mcp_servers/MCP_Data_Analyst\"; if [ ! -d \"$d/.git\" ]; then rm -rf \"$d\"; git clone https://github.com/azzindani/MCP_Data_Analyst.git \"$d\" --quiet; else cd \"$d\" && git fetch origin --quiet && git reset --hard FETCH_HEAD --quiet; fi; cd \"$d\"; uv sync --quiet; uv run python -m servers.data_statistics.server"
      ],
      "env": { "MCP_CONSTRAINED_MODE": "0" },
      "timeout": 600000
    },
    "data_analyst_visual": {
      "command": "bash",
      "args": [
        "-c",
        "d=\"$HOME/.mcp_servers/MCP_Data_Analyst\"; if [ ! -d \"$d/.git\" ]; then rm -rf \"$d\"; git clone https://github.com/azzindani/MCP_Data_Analyst.git \"$d\" --quiet; else cd \"$d\" && git fetch origin --quiet && git reset --hard FETCH_HEAD --quiet; fi; cd \"$d\"; uv sync --quiet; uv run python -m servers.data_visual.server"
      ],
      "env": { "MCP_CONSTRAINED_MODE": "0" },
      "timeout": 600000
    }
  }
}
```

## Available Tools

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

### Tier 2 — Medium (11 tools)

| Tool | Purpose |
|---|---|
| `compute_aggregations` | Group-by aggregation (sum/mean/count/min/max) |
| `cross_tabulate` | Contingency tables — saves heatmap HTML |
| `pivot_table` | Multi-dimensional pivot tables |
| `value_counts` | Frequency tables — saves bar chart HTML |
| `filter_rows` | Filter by 8 condition types (equals, contains, gt, lt, gte, lte, not_null, is_null) |
| `sample_data` | Random/head/tail sampling |
| `statistical_tests` | Auto-select: t-test, ANOVA, chi-square, correlation |
| `analyze_text_column` | Character length stats, word frequency top-N, pattern detection (email, URL, phone, number) |
| `detect_anomalies` | IQR + z-score row flagging — adds `_anomaly_score` column, saves annotated CSV |
| `compare_datasets` | Schema diff, dtype changes, row count diff, null/mean delta between two CSVs |
| `extended_stats` | Deep stats: skewness, kurtosis, percentiles, CI, MAD, CV, distribution fit |

Chart-producing medium tools accept `theme: "dark" | "light" | "device"`, `output_path`, and `open_after`.

### Tier 2 — Transform (10 tools)

Focused transformation server — richer filtering, reshaping, and aggregation than the basic tier.

| Tool | Purpose |
|---|---|
| `filter_dataset` | Filter rows by 18 condition types (equals, isin, between, regex, date_range, quantile_between, starts_with, ends_with, …) + optional sort |
| `reshape_dataset` | Reshape data: `pivot`, `melt`, `split_column`, `combine_columns`, `transpose` |
| `aggregate_dataset` | Aggregate: `groupby`, `crosstab`, `value_counts`, `describe`, `window` |
| `resample_timeseries` | Resample time series (D/W/M/Q/Y/H) |
| `merge_datasets` | Merge two datasets with auto-detected join keys |
| `concat_datasets` | Stack multiple CSVs vertically or horizontally |
| `smart_impute` | Auto-impute: numeric→median, datetime→ffill, categorical→mode |
| `run_cleaning_pipeline` | Multi-op cleaning with single snapshot + rollback |
| `feature_engineering` | Auto-create date parts, bins, log transforms, one-hot features |
| `enrich_with_geo` | Merge dataset with geo data on a location key |

---

### Tier 3 — Statistics (11 tools)

| Tool | Purpose |
|---|---|
| `statistical_test` | 17 test types: shapiro_wilk, ks, anderson, t_test, paired_t_test, one_sample_t, anova, chi_square, fisher, mann_whitney, wilcoxon, kruskal, levene, pearson, spearman, kendall, proportion_z — includes effect sizes (Cohen's d, η², Cramér's V) |
| `regression_analysis` | OLS or logistic regression via statsmodels — returns coefficients, p-values, R², RMSE, AIC, BIC, VIF, normality diagnostics, and insight summary |
| `period_comparison` | MoM / QoQ / YoY comparison — returns delta, pct_change, direction per metric |
| `time_series_analysis` | Trend + seasonality + rolling stats + exponential-smoothing forecast + **STL decomposition** + **ACF/PACF** + **ADF stationarity test** |
| `correlation_analysis` | Correlation matrix (Pearson/Spearman/Kendall) + top N pairs |
| `cohort_analysis` | Cohort retention matrix with auto-detected identifiers |
| `extended_stats` | Deep stats: skewness, kurtosis, percentiles, CI, MAD, CV |
| `check_outliers` | IQR/std outlier scan |
| `scan_nulls_zeros` | Null/zero detection + suggested fixes |
| `validate_dataset` | Data quality score 0–100 |
| `auto_detect_schema` | Smart column type inference with cleaning suggestions |

---

### Tier 3 — Visual (12 tools)

| Tool | Purpose |
|---|---|
| `run_eda` | Fast EDA: stats, nulls, correlations, outliers — saves HTML |
| `generate_auto_profile` | Full column profile: per-column charts, correlation network, quality dashboard |
| `generate_dashboard` | Interactive HTML dashboard: KPI cards, sparklines, violin plots, geo maps |
| `generate_chart` | 13 chart types: bar, pie, line, scatter, geo, treemap, radius, time_series, sunburst, waterfall, funnel, parallel_coords, sankey |
| `generate_geo_map` | Scatter map (lat/lon) or choropleth (country/state) — auto-detected |
| `generate_3d_chart` | 3D scatter or surface chart |
| `generate_distribution_plot` | Histogram + box plot for numeric columns |
| `generate_correlation_heatmap` | Interactive Pearson/Spearman heatmap |
| `generate_pairwise_plot` | Scatter matrix for numeric columns |
| `generate_multi_chart` | Multi-variable bar/line chart (2+ metrics) |
| `export_data` | Export to CSV, Excel, or JSON |
| `customize_chart` | Post-generate edits to an existing HTML chart: title, axis labels, colour scheme, annotations, value labels, dimensions |

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
- **Responsive filter bar**: multi-select dropdowns with Clear All

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

## Uninstall

**Step 1:** Remove from LM Studio
1. Open LM Studio → Developer tab (`</>`)
2. Delete all `data_analyst_*` entries (`workspace`, `basic`, `medium`, `transform`, `statistics`, `visual`) from MCP Servers
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
│   ├── data_medium/         ← T2: aggregation, anomaly, text, comparison (11 tools)
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
│   ├── data_statistics/     ← T3: full statistics suite (11 tools)
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
│   └── data_visual/         ← T3: EDA + dashboards + charts + customization (12 tools)
│       ├── server.py
│       ├── engine.py        ← re-exports data_advanced + customize_chart
│       └── _adv_customize.py← post-generate chart editing
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
    ├── test_engine_project.py   ← 21 tests (full project workflow e2e)
    ├── test_workspace_server.py ← 26 tests (context+handover contract)
    ├── test_engine_transform.py ← 32 tests (filter/reshape/aggregate)
    ├── test_engine_statistics.py← 39 tests (regression, stat tests, period comparison)
    ├── test_shared.py
    ├── verify_tool_docstrings.py← CI gate: all @mcp.tool() docstrings ≤ 80 chars
    └── verify_output_paths.py   ← CI gate: output path priority contract
```

## Development

### Local Testing

```bash
# Install all dependencies from root (single lockfile)
uv sync

# Run all 561 tests
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
# Tier 1 — Basic (standalone, own venv)
cd servers/data_basic && uv sync && uv run python server.py

# Tier 2+ — run from repo root (shared deps via root pyproject.toml)
uv run python -m servers.data_workspace.server
uv run python -m servers.data_transform.server
uv run python -m servers.data_statistics.server
uv run python -m servers.data_visual.server
```

## License

MIT
