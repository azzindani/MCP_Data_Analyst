# MCP Data Analyst

A self-hosted MCP server that gives local LLMs structured access to CSV/tabular data analysis tools. No cloud APIs, no API keys — everything runs on your machine.

## Features

- **36 tools** across 3 tiers: basic, medium, and advanced
- **LOCATE → INSPECT → PATCH → VERIFY** workflow for surgical data edits
- **Automatic version control** — every write is snapshotted and fully restorable
- **Operation receipt logging** — full audit trail of all modifications
- **Constrained mode** — safe for machines with ≤8 GB VRAM
- **ydata-profiler quality reports** — alerts panel, Spearman + Pearson correlations, missing value matrix, per-column distribution charts
- **Interactive dashboards** — KPI sparklines, trend indicators, violin plots, auto-detected charts
- **Light / dark / device theme** — all HTML outputs accept `theme: "dark" | "light" | "device"`
- **Mobile-responsive HTML** — viewport meta + CSS breakpoints on every report

## Quick Install (LM Studio)

> **Tested on Windows 11** with LM Studio 0.4.x and uv 0.5+.

1. Open LM Studio → **Developer** tab (`</>` icon)
2. Scroll to **MCP Servers** → click **Add Server**
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
        "$d = Join-Path $env:USERPROFILE '.mcp_servers\\MCP_Data_Analyst'; if (!(Test-Path $d)) { git clone https://github.com/azzindani/MCP_Data_Analyst.git $d } else { Set-Location $d; git pull --quiet }; Set-Location (Join-Path $d 'servers\\data_basic'); uv run python server.py"
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
        "$d = Join-Path $env:USERPROFILE '.mcp_servers\\MCP_Data_Analyst'; if (!(Test-Path $d)) { git clone https://github.com/azzindani/MCP_Data_Analyst.git $d } else { Set-Location $d; git pull --quiet }; Set-Location (Join-Path $d 'servers\\data_medium'); uv run python server.py"
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
        "$d = Join-Path $env:USERPROFILE '.mcp_servers\\MCP_Data_Analyst'; if (!(Test-Path $d)) { git clone https://github.com/azzindani/MCP_Data_Analyst.git $d } else { Set-Location $d; git pull --quiet }; Set-Location (Join-Path $d 'servers\\data_advanced'); uv run python server.py"
      ],
      "env": { "MCP_CONSTRAINED_MODE": "0" },
      "timeout": 600000
    }
  }
}
```

4. Restart LM Studio
5. Wait for the green dot next to each server
6. Start chatting — the model will see all 36 tools

### First Run

The first launch clones the repo and installs dependencies (~2-5 minutes). Subsequent launches are instant.

### Requirements

- **Git** — `git --version`
- **uv** — `uv --version` ([install guide](https://docs.astral.sh/uv/getting-started/installation/))
- **Python 3.12** (auto-managed by uv)
- **LM Studio** with a model that supports tool calling (Qwen 2.5, Llama 3.1, etc.)

## Available Tools

### Tier 1 — Basic (8 tools)

| Tool | Purpose |
|---|---|
| `load_dataset` | Load CSV with auto-encoding detection |
| `load_geo_dataset` | Load GeoJSON/shapefile, return geometry info |
| `inspect_dataset` | Full schema inspection: dtypes, nulls, column classification |
| `read_column_stats` | Stats for one column: mean, median, outliers, top values |
| `search_columns` | Find columns by criteria: has_nulls, dtype, name_contains |
| `apply_patch` | 8 ops: fill_nulls, drop_duplicates, clean_text, cast_column, add_column, cap_outliers, replace_values, drop_column |
| `restore_version` | Restore a file to any previous snapshot |
| `read_receipt` | Read the operation history log for a file |

### Tier 2 — Medium (19 tools)

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

All 7 chart-producing medium tools accept `theme: "dark" | "light" | "device"`, `output_path`, and `open_after`.

### Tier 3 — Advanced (9 tools)

| Tool | Purpose |
|---|---|
| `run_eda` | EDA report: alerts panel, data sample, Pearson + Spearman correlations, missing value matrix, zero stats, outliers, insights |
| `generate_auto_profile` | Full column profile: per-column charts, both correlation methods, alerts, missing matrix, data sample (head + tail), recommendations |
| `generate_dashboard` | Interactive HTML dashboard: KPI sparklines + trend arrows, auto-detected charts, violin plots, filter controls |
| `generate_distribution_plot` | Histogram + box plot for numeric columns |
| `generate_correlation_heatmap` | Interactive Pearson/Spearman heatmap |
| `generate_pairwise_plot` | Scatter matrix for numeric columns |
| `generate_multi_chart` | Multi-variable bar/line charts (2+ metrics) |
| `generate_chart` | 8 chart types: bar, pie, line, scatter, geo, treemap, time_series, radius |
| `export_data` | Export to CSV, Excel, or JSON |

All 9 advanced tools accept `theme: "dark" | "light" | "device"` and `open_after`.

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
Search for columns in sales.csv that have null values
```

### Get column statistics

```
Show me the statistics for the Revenue column in sales.csv
```

### Clean data

```
Fill null values in the Revenue column of sales.csv using the median strategy
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
Run EDA on sales.csv and highlight any data quality issues
```

### Interactive dashboard

```
Generate a dashboard for sales.csv in light theme
```

### Statistical analysis

```
Run statistical tests on sales.csv to compare Revenue across Regions
```

### Time series analysis

```
Analyze the time series trends in sales.csv
```

### Undo a change

```
Restore sales.csv to the previous version
```

## Configuration

### Constrained Mode

For machines with ≤8 GB VRAM, set `MCP_CONSTRAINED_MODE=1` in the `env` section of `mcp.json`. This reduces:
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
│   │   ├── server.py      ← thin MCP wrapper (zero domain logic)
│   │   ├── engine.py      ← all pandas logic (zero MCP imports)
│   │   └── pyproject.toml
│   ├── data_medium/
│   │   ├── server.py
│   │   ├── engine.py
│   │   └── pyproject.toml
│   └── data_advanced/
│       ├── server.py
│       ├── engine.py
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
