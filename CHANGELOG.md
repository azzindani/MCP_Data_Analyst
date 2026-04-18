# Changelog

All notable changes to this project will be documented in this file.

---

## [0.1.0] — 2026-04-18

### Initial release

MCP Data Analyst v0.1.0 is the first production-ready release of a local-first
MCP server for data analytics. It provides a language model with structured,
surgical access to CSV/tabular datasets through 84 deterministic tools across
7 servers — without sending any data to a cloud API.

---

### Servers

| Server | Tier | Tools | Purpose |
|---|---|---|---|
| `data_workspace` | T0 | 6 | Workspace management — create, open, register files, save and run pipelines |
| `data_basic` | T1 | 9 | Load, inspect, patch, restore — the core four-tool loop |
| `data_medium` | T2 | 11 | Aggregation, anomaly detection, text analysis, dataset comparison |
| `data_transform` | T2 | 10 | Rich filtering (18 condition types), reshape, aggregation, merging |
| `data_statistics` | T3 | 11 | Regression, 17 statistical tests, STL decomposition, MoM/QoQ/YoY |
| `data_visual` | T3 | 12 | EDA reports, 13 chart types, geo maps, 3D charts, dashboards, customization |

---

### Key features

#### Four-tool workflow
Every data modification task follows a guided `LOCATE → INSPECT → PATCH → VERIFY`
loop. Tools are designed so the model naturally advances through each stage.

#### Version control and audit trail
- Every write tool snapshots the file before modifying it into `.mcp_versions/`
  with Windows-safe collision-proof timestamps.
- Every write appends to a per-file receipt log (`*.mcp_receipt.json`) capturing
  the tool name, arguments, result, and backup path.
- `restore_version` recovers any snapshot atomically.

#### 51 `apply_patch` operations
Six categories of in-place column transformations:
- **Original** (13): fill nulls, cast type, rename, drop, clip, round, replace, …
- **Filtering** (9): drop duplicates, filter by value/range/regex, …
- **Numeric** (11): log, sqrt, normalize, standardize, Box-Cox, Yeo-Johnson, …
- **Encoding** (3): one-hot, ordinal, label encoding
- **Temporal** (7): parse dates, extract components, shift, …
- **Structural** (8): melt, pivot, split/combine columns, reorder, …

#### Statistics suite
- 17 statistical tests with effect sizes (Cohen's d, η², Cramér's V):
  Shapiro-Wilk, K-S, Anderson-Darling, t-tests, ANOVA, chi-square, Fisher,
  Mann-Whitney, Wilcoxon, Kruskal-Wallis, Levene, Pearson/Spearman/Kendall,
  proportion z-test.
- OLS and logistic regression via statsmodels.
- STL decomposition, ACF/PACF, ADF stationarity test.
- Period comparison: MoM, QoQ, YoY with optional group-by.

#### Visualization
- 13 chart types: bar, line, scatter, pie, treemap, sunburst, waterfall, funnel,
  geo, radius, time_series, parallel_coords, sankey.
- Geo maps: auto-detects scatter map (lat/lon) or choropleth (country/state).
- 3D charts: scatter_3d and surface.
- Interactive HTML dashboards with KPI sparklines, trend indicators (↑↓→),
  violin plots, geo maps, and a responsive filter bar.
- `customize_chart` for post-generate edits (title, axis labels, color scheme,
  annotations, value labels, dimensions) without regenerating the chart.
- Dark / light / device-adaptive theme on every HTML output.

#### Workspace management
Named workspaces with file aliases, pipeline templates, and stage tracking
(raw → working → trial → output). Supports `workspace:name/alias` syntax and
the legacy `project:name/alias` prefix for backward compatibility.

#### Multi-server handover protocol
Every tool response includes a `handover` block with `workflow_step`,
`suggested_next`, and `carry_forward` so the model can chain tools across
servers without losing context. Cross-MCP routing hints cover data, ml, office,
fs, and search domains.

#### Constrained mode
Set `MCP_CONSTRAINED_MODE=1` to halve all response sizes for low-memory or
small-context-window environments (rows 100→20, search results 50→10,
columns 50→20).

#### Dry run on all write tools
Every write tool accepts `dry_run: bool = False`. When `True`, it returns a
`would_change` description without touching the file.

---

### Shared utilities

`shared/` provides 12 modules consumed by all servers:

| Module | Purpose |
|---|---|
| `version_control.py` | Atomic snapshot and restore |
| `receipt.py` | Per-file JSON operation audit trail |
| `patch_validator.py` | Validates op arrays before execution |
| `workspace_utils.py` | Workspace manifest CRUD and alias resolution |
| `file_utils.py` | Path resolution and atomic file writes |
| `html_layout.py` | Output path priority, HTML helpers |
| `html_theme.py` | CSS variables, Plotly templates, responsive meta |
| `column_utils.py` | Column classification helpers |
| `handover.py` | Cross-MCP handover context builder |
| `platform_utils.py` | `MCP_CONSTRAINED_MODE` and memory-aware row limits |
| `progress.py` | `ok` / `fail` / `info` / `warn` / `undo` status helpers |
| `project_utils.py` | Project manifest I/O |

---

### Requirements

| Item | Version |
|---|---|
| Python | 3.12.x |
| Package manager | uv ≥ 0.5 |
| fastmcp | ≥ 2.0, < 3.0 |
| pandas | ≥ 2.2 |
| geopandas | ≥ 1.0 |
| plotly | ≥ 5.0 |
| scipy | ≥ 1.10 |
| statsmodels | ≥ 0.14 |

---

### Testing

561 tests across 9 modules covering success paths, error paths, dry run,
constrained mode, snapshot creation, and end-to-end four-tool workflows.
CI gates enforce per-tool docstring length ≤ 80 characters and output path
priority contracts.
