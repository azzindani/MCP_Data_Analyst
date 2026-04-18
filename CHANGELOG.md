# Changelog

All notable changes to this project will be documented in this file.

---

## [0.1.0] — 2026-04-18

### Initial release

MCP Data Analyst v0.1.0 is the first production-ready release of a local-first
MCP server for data analytics. It gives a language model structured, surgical
access to CSV/tabular datasets through 59 deterministic tools across 6 servers —
without sending any data to a cloud API.

---

### Servers

| Server | Tier | Tools | Purpose |
|---|---|---|---|
| `data_workspace` | T0 | 6 | Workspace management — named workspaces, file aliases, pipeline templates |
| `data_basic` | T1 | 9 | Load, inspect, patch, restore — the core four-tool loop |
| `data_medium` | T2 | 11 | Aggregation, pivot, anomaly detection, text analysis, dataset comparison |
| `data_transform` | T2 | 10 | Rich filtering (18 condition types), reshape, merge, resample |
| `data_statistics` | T3 | 11 | Regression, 17 statistical tests, STL decomposition, MoM/QoQ/YoY |
| `data_visual` | T3 | 12 | EDA, 13 chart types, geo maps, 3D charts, dashboards, chart customization |

---

### Key features

#### Four-tool workflow
Every data modification task follows a guided `LOCATE → INSPECT → PATCH → VERIFY`
loop. Tools are designed so the model naturally advances through each stage.

#### Version control and audit trail
- Every write tool snapshots the file before modifying it into `.mcp_versions/`
  with collision-proof timestamps (Windows-safe).
- Every write appends to a per-file receipt log (`*.mcp_receipt.json`) capturing
  the tool name, arguments, result, and backup path.
- `restore_version` recovers any snapshot atomically.

#### 51 `apply_patch` operations
Six categories of in-place column transformations callable from a single tool:

| Category | Count | Examples |
|---|---|---|
| Original | 13 | fill_nulls, cast_column, replace_values, cap_outliers, rank_column |
| Filtering | 9 | sort, filter_isin, filter_between, filter_date_range, filter_quantile |
| Numeric | 11 | log_transform, boxcox_transform, yeojohnson_transform, robust_scale, winsorize |
| Encoding | 3 | ordinal_encode, binary_encode, frequency_encode |
| Temporal | 7 | lag, lead, diff, pct_change, rolling_agg, ewm, cumulative |
| Structural | 8 | column_math, conditional_assign, split_column, melt, concat_file |

#### Statistics suite
- 17 statistical tests with effect sizes (Cohen's d, η², Cramér's V):
  Shapiro-Wilk, K-S, Anderson-Darling, t-tests, ANOVA, chi-square, Fisher,
  Mann-Whitney, Wilcoxon, Kruskal-Wallis, Levene, Pearson/Spearman/Kendall,
  proportion z-test.
- OLS and logistic regression via statsmodels (coefficients, p-values, R², AIC, BIC, VIF).
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
(raw → working → trial → output). Any tool accepts `workspace:name/alias` in
place of a file path — all servers resolve aliases automatically.

#### Multi-server handover protocol
Every tool response includes a `handover` block with `workflow_step`,
`suggested_next`, and `carry_forward` so the model can chain tools across
servers without losing context.

#### Constrained mode
Set `MCP_CONSTRAINED_MODE=1` to reduce all response sizes for low-memory or
small-context-window environments (rows 100→20, search results 50→10,
columns 50→20).

#### Dry run on all write tools
Every write tool accepts `dry_run: bool = False`. When `True`, it returns a
`would_change` description without touching the file.

---

### Shared utilities

`shared/` provides ring-2 modules (no MCP imports) consumed by all servers:

| Module | Purpose |
|---|---|
| `version_control.py` | Atomic snapshot and restore |
| `receipt.py` | Per-file JSON operation audit trail |
| `patch_validator.py` | Validates op arrays before execution |
| `project_utils.py` | Workspace manifest CRUD and alias resolution |
| `file_utils.py` | Path resolution and atomic file writes |
| `html_layout.py` | Output path priority, HTML helpers |
| `html_theme.py` | CSS variables, Plotly templates, responsive meta |
| `handover.py` | Cross-MCP handover context builder |
| `platform_utils.py` | `MCP_CONSTRAINED_MODE` and memory-aware row limits |
| `progress.py` | `ok` / `fail` / `info` / `warn` / `undo` status helpers |

---

### Requirements

| Item | Version |
|---|---|
| Python | 3.12+ |
| Package manager | uv ≥ 0.5 |
| fastmcp | ≥ 2.0, < 3.0 |
| pandas | ≥ 2.2 |
| polars | ≥ 0.20 |
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
