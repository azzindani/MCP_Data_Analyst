# MCP Data Analyst

A self-hosted MCP server that gives local LLMs structured access to CSV/tabular data analysis tools. No cloud APIs, no API keys — everything runs on your machine.

## Features

- **8 built-in tools** for data loading, inspection, cleaning, and transformation
- **LOCATE → INSPECT → PATCH → VERIFY** workflow for surgical data edits
- **Automatic version control** — every change is snapshotted and restorable
- **Operation receipt logging** — full audit trail of all modifications
- **Constrained mode** — safe for machines with ≤8 GB VRAM

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
5. Wait for the green dot next to `data_analyst_basic`
6. Start chatting — the model will see 8 new tools

### First Run

The first launch clones the repo and installs dependencies (~2-5 minutes). Subsequent launches are instant.

### Requirements

- **Git** — `git --version`
- **uv** — `uv --version` ([install guide](https://docs.astral.sh/uv/getting-started/installation/))
- **Python 3.12** (auto-managed by uv)
- **LM Studio** with a model that supports tool calling (Qwen 2.5, Llama 3.1, etc.)

## Available Tools

| Tool | Purpose |
|---|---|
| `load_dataset` | Load a CSV file, return schema and row count |
| `load_geo_dataset` | Load GeoJSON/shapefile, return geometry info |
| `inspect_dataset` | Full schema inspection: dtypes, nulls, column classification |
| `read_column_stats` | Stats for one column: mean, median, outliers, top values |
| `search_columns` | Find columns by criteria: has_nulls, dtype, name_contains |
| `apply_patch` | Apply data transformations: fill_nulls, drop_duplicates, clean_text, cast_column, add_column, cap_outliers, replace_values, drop_column |
| `restore_version` | Restore a file to a previous snapshot |
| `read_receipt` | Read the operation history log for a file |

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

Delete the installed repo:
```cmd
rmdir /s /q %USERPROFILE%\.mcp_servers\MCP_Data_Analyst
```

Then remove the `data_analyst_basic` entry from your `mcp.json`.

## Architecture

```
MCP_Data_Analyst/
├── servers/data_basic/
│   ├── server.py      ← thin MCP wrapper (zero domain logic)
│   ├── engine.py      ← all pandas logic (zero MCP imports)
│   └── pyproject.toml
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

### Interactive Notebook Test

```bash
jupyter notebook test_tier1.ipynb
```

## License

MIT
