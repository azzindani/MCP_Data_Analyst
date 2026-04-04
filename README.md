# MCP Data Analyst

A self-hosted MCP server that gives local LLMs structured access to CSV/tabular data analysis tools. No cloud APIs, no API keys — everything runs on your machine.

## Features

- **43 tools** across 3 tiers: basic, medium, and advanced
- **11 auto-detection tools** with nested logic for smart column inference
- **LOCATE → INSPECT → PATCH → VERIFY** workflow for surgical data edits
- **Automatic version control** — every change is snapshotted and restorable
- **Operation receipt logging** — full audit trail of all modifications
- **Constrained mode** — safe for machines with ≤8 GB VRAM
- **Fast EDA** — lightweight reports that run in seconds, not minutes

## Quick Install (LM Studio)

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
6. Start chatting — the model will see all 43 tools

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
| `apply_patch` | 8 transformation ops: fill_nulls, drop_duplicates, clean_text, cast_column, add_column, cap_outliers, replace_values, drop_column |
| `restore_version` | Restore a file to a previous snapshot |
| `read_receipt` | Read the operation history log for a file |

### Tier 2 — Medium (19 tools)
| Tool | Auto-Detect | Purpose |
|---|---|---|
| `check_outliers` | Numeric | IQR/std outlier scan |
| `scan_nulls_zeros` | Type-aware | Null/zero detection + suggested fixes |
| `enrich_with_geo` | — | Merge dataset with geo data |
| `validate_dataset` | Dtype | Quality scoring (0-100) |
| `compute_aggregations` | — | Group-by aggregation (sum/mean/count) |
| `run_cleaning_pipeline` | — | Multi-op cleaning with rollback |
| `correlation_analysis` | Numeric | Correlation matrix + top N pairs |
| `cross_tabulate` | — | Contingency tables between categories |
| `pivot_table` | — | Multi-dimensional pivot tables |
| `value_counts` | — | Frequency tables with percentages |
| `filter_rows` | — | Filter by 8 condition types (equals, contains, gt, lt, etc.) |
| `sample_data` | — | Random/head/tail sampling |
| **`auto_detect_schema`** | ✅ Full | Smart column type inference with cleaning suggestions |
| **`smart_impute`** | ✅ Type→strategy | Auto-impute: numeric→median, datetime→ffill, categorical→mode |
| **`merge_datasets`** | ✅ Join keys | Merge two datasets with auto-detect join keys |
| **`feature_engineering`** | ✅ Date/numeric/text | Auto-create features: date parts, bins, log transforms, one-hot |
| **`statistical_tests`** | ✅ Test selection | Auto-select: t-test, ANOVA, chi-square, correlation |
| **`time_series_analysis`** | ✅ Date column | Auto-detect date, compute trend, seasonality, rolling stats |
| **`cohort_analysis`** | ✅ Cohort/date | Auto-detect cohort identifiers, build retention matrix |

### Tier 3 — Advanced (16 tools)
| Tool | Auto-Detect | Purpose |
|---|---|---|
| `run_eda` | Column analysis | Fast EDA HTML report (stats, nulls, correlations, outliers) |
| `generate_distribution_plot` | Numeric | Histogram + box plot for numeric columns |
| `generate_multi_chart` | — | Multi-variable bar/line charts (2+ metrics) |
| `generate_chart` | — | 8 chart types: bar, pie, line, scatter, geo, treemap, time_series, radius |
| `generate_dashboard` | Dtype scanning | Auto-generate Streamlit dashboard |
| `generate_correlation_heatmap` | Numeric | Interactive correlation heatmap |
| `generate_pairwise_plot` | Numeric | Scatter matrix for numeric columns |
| `export_data` | — | Export to CSV, JSON, or Excel |
| **`rfm_analysis`** | ✅ Customer/date/monetary | RFM segmentation for customer data |
| **`auto_chart_recommendation`** | ✅ Column types | Recommend best chart type for any column pair |
| **`generate_insights_report`** | ✅ Full analysis | Auto-generate text insights from data patterns |
| **`anomaly_detection`** | ✅ Numeric columns | Z-score or IQR anomaly detection |
| **`segmentation_analysis`** | ✅ Clustering features | K-means customer/data segmentation |

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

### Auto-detect schema

```
Auto-detect the schema of sales.csv and suggest cleaning actions
```

### Smart imputation

```
Smart impute missing values in sales.csv using appropriate strategies for each column type
```

### Statistical analysis

```
Run statistical tests on sales.csv to compare Revenue across Regions
```

### Time series analysis

```
Analyze the time series trends in sales.csv
```

### RFM segmentation

```
Run RFM analysis on customers.csv to segment customers
```

### Anomaly detection

```
Detect anomalies in sales.csv using the IQR method
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
│   └── receipt.py           ← operation receipt logging
├── install/
│   ├── run_server.bat       ← Windows launcher
│   └── uninstall.bat        ← Windows uninstaller
└── tests/
    ├── conftest.py
    ├── test_engine_basic.py
    ├── test_engine_medium.py
    └── test_shared.py
```

## Development

### Local Testing

```bash
cd servers/data_basic
uv sync
uv run python server.py
```

### Run Test Suite

```bash
cd servers/data_basic
uv run pytest tests/ -v
```

## License

MIT
