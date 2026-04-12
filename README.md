# MCP Data Analyst

A self-hosted MCP server that gives local LLMs structured access to CSV/tabular data analysis tools. No cloud APIs, no API keys — everything runs on your machine.

## Features

- **41 tools** across 3 tiers: basic (8), medium (22), advanced (11)
- **LOCATE → INSPECT → PATCH → VERIFY** workflow for surgical data edits
- **Automatic version control** — every write is snapshotted and fully restorable
- **Operation receipt logging** — full audit trail of all modifications
- **Constrained mode** — reduces row/result limits for lower-memory machines
- **ydata-profiler quality reports** — alerts panel, Spearman + Pearson correlations, missing value matrix, per-column distribution charts
- **Interactive dashboards** — KPI sparklines, trend indicators, violin plots, geo maps, auto-detected charts
- **Geo visualization** — scatter maps (lat/lon), choropleth (country/state), zero external data needed
- **3D charts** — scatter_3d and surface plots
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
> Set-Location "$d\servers\data_advanced"; uv sync
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
    "data_analyst_advanced": {
      "command": "powershell",
      "args": [
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        "$d = Join-Path $env:USERPROFILE '.mcp_servers\\MCP_Data_Analyst'; $g = Join-Path $d '.git'; if (!(Test-Path $g)) { if (Test-Path $d) { Remove-Item -Recurse -Force $d }; git clone https://github.com/azzindani/MCP_Data_Analyst.git $d --quiet } else { Set-Location $d; git fetch origin --quiet; git reset --hard FETCH_HEAD --quiet }; Set-Location (Join-Path $d 'servers\\data_advanced'); uv sync --quiet; uv run python server.py"
      ],
      "env": { "MCP_CONSTRAINED_MODE": "0" },
      "timeout": 600000
    }
  }
}
```

4. Wait for the blue dot next to each server
5. Start chatting — the model will see all 41 tools

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
    "data_analyst_advanced": {
      "command": "bash",
      "args": [
        "-c",
        "d=\"$HOME/.mcp_servers/MCP_Data_Analyst\"; if [ ! -d \"$d/.git\" ]; then rm -rf \"$d\"; git clone https://github.com/azzindani/MCP_Data_Analyst.git \"$d\" --quiet; else cd \"$d\" && git fetch origin --quiet && git reset --hard FETCH_HEAD --quiet; fi; cd \"$d/servers/data_advanced\"; uv sync --quiet; uv run python server.py"
      ],
      "env": { "MCP_CONSTRAINED_MODE": "0" },
      "timeout": 600000
    }
  }
}
```

## Available Tools

### Tier 1 — Basic (8 tools)

| Tool | Purpose |
|---|---|
| `load_dataset` | Load CSV with auto-encoding detection |
| `load_geo_dataset` | Load GeoJSON/shapefile, return geometry info |
| `inspect_dataset` | Full schema inspection: dtypes, nulls, column classification |
| `read_column_stats` | Stats for one column: mean, median, outliers, top values |
| `search_columns` | Find columns by criteria: has_nulls, dtype, name_contains |
| `apply_patch` | **13 ops**: fill_nulls, drop_duplicates, clean_text, cast_column, add_column, cap_outliers, replace_values, drop_column, **normalize**, **label_encode**, **extract_regex**, **date_diff**, **rank_column** |
| `restore_version` | Restore a file to any previous snapshot |
| `read_receipt` | Read the operation history log for a file |

#### New `apply_patch` ops

| Op | Description |
|---|---|
| `normalize` | Min-max or z-score scale a numeric column (`method: "minmax"\|"zscore"`) |
| `label_encode` | Encode categorical column to 0-based integers; returns `encoding` mapping |
| `extract_regex` | Extract a regex capture group into a new column |
| `date_diff` | Compute difference between two date columns in days/months/years |
| `rank_column` | Rank rows by a numeric column (dense, min, max, average, first) |

### Tier 2 — Medium (22 tools)

| Tool | Auto-Detect | Purpose |
|---|---|---|
| `check_outliers` | Numeric | IQR/std outlier scan — saves outlier box plot HTML |
| `scan_nulls_zeros` | Type-aware | Null/zero detection + suggested fixes — saves bar chart HTML |
| `enrich_with_geo` | — | Merge dataset with geo data on a location key |
| `validate_dataset` | Dtype | Data quality scoring 0–100 |
| `compute_aggregations` | — | Group-by aggregation (sum/mean/count/min/max) |
| `run_cleaning_pipeline` | — | Multi-op cleaning with single snapshot + rollback |
| `correlation_analysis` | Numeric | Correlation matrix + top N pairs — saves heatmap HTML |
| `cross_tabulate` | — | Contingency tables — saves heatmap HTML |
| `pivot_table` | — | Multi-dimensional pivot tables |
| `value_counts` | — | Frequency tables — saves bar chart HTML |
| `filter_rows` | — | Filter by 8 condition types (equals, contains, gt, lt, gte, lte, not_null, is_null) |
| `sample_data` | — | Random/head/tail sampling |
| `auto_detect_schema` | Full | Smart column type inference with cleaning suggestions |
| `smart_impute` | Type→strategy | Auto-impute: numeric→median, datetime→ffill, categorical→mode |
| `merge_datasets` | Join keys | Merge two datasets with auto-detect join keys and mismatch report |
| `feature_engineering` | Date/numeric/text | Auto-create features: date parts, bins, log transforms, one-hot |
| `statistical_tests` | Test selection | Auto-select: t-test, ANOVA, chi-square, correlation |
| `time_series_analysis` | Date column | Auto-detect date, trend, seasonality, rolling stats — saves line chart HTML |
| `cohort_analysis` | Cohort/date | Auto-detect cohort identifiers, build retention matrix — saves heatmap HTML |

| `analyze_text_column` | — | Character length stats, word frequency top-N, pattern detection (email, URL, phone, number) |
| `detect_anomalies` | Numeric | IQR + z-score row flagging — adds `_anomaly_score` column, saves annotated CSV |
| `compare_datasets` | — | Schema diff, dtype changes, row count diff, null/mean delta between two CSVs |

All chart-producing medium tools accept `theme: "dark" | "light" | "device"`, `output_path`, and `open_after`.

### Tier 3 — Advanced (11 tools)

| Tool | Purpose |
|---|---|
| `run_eda` | EDA report: alerts panel, data sample, Pearson + Spearman correlations, missing value matrix, zero stats, outliers, insights |
| `generate_auto_profile` | Full column profile: per-column charts, both correlation methods, alerts, missing matrix, data sample (head + tail), recommendations |
| `generate_dashboard` | Interactive HTML dashboard: KPI sparklines + trend arrows, auto-detected charts, violin plots, geo maps, filter controls |
| `generate_geo_map` | Geo map: auto-detects lat/lon → scatter map, or country/state column → choropleth. No external data needed |
| `generate_3d_chart` | 3D scatter or surface chart (`type: "scatter_3d"\|"surface"`) |
| `generate_distribution_plot` | Histogram + box plot for numeric columns |
| `generate_correlation_heatmap` | Interactive Pearson/Spearman heatmap |
| `generate_pairwise_plot` | Scatter matrix for numeric columns |
| `generate_multi_chart` | Multi-variable bar/line charts (2+ metrics) |
| `generate_chart` | **13 chart types**: bar, pie, line, scatter, geo, treemap, time_series, radius, **sunburst**, **waterfall**, **funnel**, **parallel_coords**, **sankey** |
| `export_data` | Export to CSV, Excel, or JSON |

All 11 advanced tools accept `theme: "dark" | "light" | "device"` and `open_after`.

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
2. Delete `data_analyst_basic`, `data_analyst_medium`, `data_analyst_advanced` from MCP Servers
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
│   ├── data_basic/
│   │   ├── server.py        ← thin MCP wrapper (zero domain logic)
│   │   ├── engine.py        ← public API re-exports (<900 lines)
│   │   ├── _patch_ops.py    ← all apply_patch operations
│   │   └── pyproject.toml
│   ├── data_medium/
│   │   ├── server.py
│   │   ├── engine.py        ← public API re-exports (<100 lines)
│   │   ├── _med_helpers.py  ← shared helpers
│   │   ├── _med_inspect.py  ← inspection + detection tools
│   │   ├── _med_transform.py← transformation tools
│   │   ├── _med_analysis.py ← analysis + stats tools
│   │   ├── _med_report.py   ← reporting + aggregation tools
│   │   └── pyproject.toml
│   └── data_advanced/
│       ├── server.py
│       ├── engine.py        ← public API re-exports (<50 lines)
│       ├── _adv_helpers.py  ← shared helpers + geo detection
│       ├── _adv_eda.py      ← run_eda
│       ├── _adv_profile.py  ← generate_auto_profile
│       ├── _adv_charts.py   ← distribution, correlation, pairwise, multi, export
│       ├── _adv_gencharts.py← generate_chart (13 types), geo_map, 3d_chart
│       ├── _adv_dashboard.py← generate_dashboard
│       └── pyproject.toml
├── shared/
│   ├── version_control.py   ← snapshot() and restore()
│   ├── patch_validator.py   ← validate op arrays
│   ├── file_utils.py        ← path resolution, atomic writes
│   ├── platform_utils.py    ← constrained mode, row limits
│   ├── progress.py          ← ok/fail/info/warn helpers
│   ├── receipt.py           ← operation receipt logging
│   └── html_theme.py        ← CSS vars, Plotly templates, responsive HTML helpers
├── install/
│   ├── run_server.bat       ← Windows launcher
│   └── uninstall.bat        ← Windows uninstaller
└── tests/
    ├── conftest.py
    ├── test_engine_basic.py
    ├── test_engine_medium.py
    └── test_engine_advanced.py
```

## Development

### Local Testing

```bash
# Install root dev dependencies
uv sync --group dev

# Run tests (Windows)
python -m pytest tests/ -v

# Run tests (Linux/macOS CI)
uv sync --group dev
cd servers/data_advanced && uv sync --dev && cd ../..
PYTHONPATH=. servers/data_advanced/.venv/bin/python -m pytest tests/ -v --tb=short

# Lint
uvx ruff check servers/ shared/ tests/ --exclude "**/.venv/**"
```

### Run a single tier server locally

```bash
cd servers/data_basic
uv sync
uv run python server.py
```

## License

MIT
