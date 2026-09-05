# Changelog

All notable changes to this project will be documented in this file.

---

## [Unreleased]

Fifteen commits since `0.2.2`, almost all of them driven by a tool user's
written review of a 38,576-row credit-risk sweep. The review's method was to
open every artifact the tools produced and check it against what the response
claimed — which is why most of what follows is a tool that succeeded while
saying something its own output did not support.

### Added — the review's asks

- **Composable dashboards.** `generate_dashboard(spec=…)` renders a declarative
  JSON spec instead of a fixed template, the page embeds the spec it was built
  from, and `customize_dashboard` edits that spec and re-renders. The review's
  words were "customisation = small JSON edit, not full rebuild".
- **Multi-source dashboards.** `sources=[…]` renders extra files as tabs.
  Summaries and row counts are computed server-side over the whole file, so a
  tab's totals are exact even when its table is paged.
- **Target-aware, comparison-aware EDA.** `run_eda(target_column=…)` ranks every
  column by its relation to the target, naming the measure per dtype pair — an
  AUC of 0.75 and a Cramer's V of 0.75 are not the same claim. `compare_to=…`
  reports schema differences and PSI / total-variation drift, which is also what
  finally makes the quality score's fourth component computable.
- **Leakage detection.** With a target named, `run_eda` reports any feature that
  may already contain the outcome, with the evidence: how well it separates the
  classes alone, whether its *missingness* tracks the target, and whether it is
  named like a post-outcome field. The last is labelled a hint because nothing
  was measured for it. Suspects, never verdicts — and deliberately kept out of
  `alerts` and out of the quality score, so one file cannot score two ways
  depending on whether the caller happened to name a target. `shared/leakage.py`
  is byte-identical with the copy in MCP_Machine_Learning, with a test asserting
  it.
- **Depth control.** `mode="minimal" | "standard" | "full"`, `sample_n`, and
  per-section `include` overrides. `standard` is exactly what the tool did
  before the parameter existed, so a caller who passes nothing gets yesterday's
  answer.
- **Lineage sidecars.** Every derived file gets a `.mcp_lineage.json` naming the
  op, the source, and the row and column counts either side. Chained by
  reference rather than by inlining, so a filtered-then-reshaped file traces
  back without each step restating the one before it. In-place writes get none:
  there is no "derived from" when the file is its own source.
- **Executable insights.** `insights.json` beside every report, each finding
  carrying the tool call that acts on it — `{tool, server, args}`, checked
  against the servers' own tool definitions. `HIGH CARDINALITY` deliberately
  carries no action, because dropping, encoding and binning are three different
  decisions and a tool that picks one is guessing.
- **Enriched Excel export.** README sheet, frozen header, autofilter, number
  formats and column validation. Measured at ~11% over a plain `to_excel` on a
  38,576-row file.
- **Sampled distribution plots.** 5,000 points by default with skew and kurtosis
  printed on the chart. The statistics come from every row; only the points are
  sampled, and the header says so.

### Fixed

- **Four servers wrote one receipt file and no two could read it.** One format,
  one reader.
- **The receipt log held two entries after twenty calls.** It was never broken —
  it records mutations — but nothing said so, and a file called
  `.mcp_receipt.json` invites exactly one reading. The scope is now declared in
  the file, in the response, and in `RECEIPT_SCOPE`; entries carry an argument
  hash, a fingerprint either side, and a duration.
- **One file, two quality scores.** `run_eda` said 77 and the ML sibling said 53
  for the same data, each having been fixed once already for disagreeing with
  something else. `shared/quality.py` is now one file, byte-identical across
  both repos, and the score arrives with its parts.
- **`customize_dashboard` could not find its own data** in the deployed layout.
- **The docstring gate measured the first line and claimed to measure the
  docstring**, so a long second line sailed through a cap meant to protect every
  client's `tools/list`.
- **A constant column reported `skew 0.00`** — pandas returns 0.0 for
  zero-variance skew, and "perfectly symmetric" is a claim about a distribution
  made about a column that has none.
- **Two smoke assertions matched a quote the wire never carries.** A tool result
  arrives as an escaped JSON string, so `"README"` reads `\"README\"`; both
  assertions had been silently matching nothing.

### Changed

- Counted **71 tools** (visual is now 13 with `customize_dashboard`). The README
  had said 70, and its smoke-test section had said 69.

---

## [0.2.2] — 2026-09-01

Source-only release: no wheel and no container image are published. Build the
image from the `Dockerfile` here, or install from the tag.

123 commits since `0.2.1`, most of them defects found by driving all 70 tools
through a harness and reading what came back — not by a failing test. The
common shape: a tool that succeeded and reported something its own output did
not support.

### Changed

- **Python 3.14** throughout, and **off third-party `fastmcp` 2.x onto the
  official `mcp` SDK** (`mcp.server.fastmcp.FastMCP`), including the unified
  server's mount and `Host` handling.
- **Remote deployment**: OAuth 2.0 bridge for claude.ai's Custom Connector, a
  shared output directory with `public_url` on every produced file, URL inputs,
  and `return_content` on the file-producing tools.
- **`remote_smoke_test.sh` runs in CI**, against a container rather than the
  deployment, with the shared directory its assertions need.

### Added

- `lag_correlation` — a delayed effect no longer reads as no effect.
- `check_outliers` flags the anomalous rows it always claimed to flag; the
  time-series tool draws the forecast it already computed; 3D charts can label
  their third axis.

### Fixed — claims a tool's own numbers did not support

- A verdict reported where no test produced one, and a post-hoc verdict with no
  p-value behind it.
- A KS test fitted its reference normal to the wrong sample; a paired test read
  offset pairs instead of row *i* against row *i*.
- A filter silently widened to a dtype group; a filter that was never applied;
  a pivot summing what cannot be summed.
- `auto_detect_schema` now says how much of the file it looked at (it samples).
- A failed op kept — and offered to restore — a snapshot of a file it never
  wrote; a no-op write left a snapshot behind.
- `Infinity` and `NaN` were being sent as JSON, which they are not.

### Fixed — charts, which only showed their defects when rendered

- Plotly went back inside the page it draws, so an artifact opens with no
  sibling file and no network.
- Dashboards stopped charting columns they had just called useless and stopped
  scoring a flawed dataset 100/100.
- Per-metric scales, so a small series no longer vanishes; captions naming
  their own column; titles where a reader looks.

---

## [0.2.0] — 2026-04-27

### New: `data_ingest` server — spreadsheet ingestion tier (10 tools)

Adds a dedicated ingestion tier for real-world spreadsheet workflows. Handles
multi-sheet Excel/ODS files, multiple tables on a single sheet, merged cells,
header normalization, and file format conversion.

| Tool | Purpose |
|---|---|
| `list_sheets` | List all sheets in xlsx/ods with row and col counts |
| `extract_sheet` | Extract one sheet to CSV; accepts name or 0-based index |
| `extract_all_sheets` | Batch-extract every sheet to separate CSVs |
| `detect_tables` | Blank-gap detection — finds separate tables on a single sheet |
| `extract_table` | Extract one detected table by index to CSV |
| `normalize_headers` | Strip whitespace, lowercase, deduplicate column names |
| `trim_empty` | Drop fully-empty leading/trailing rows and columns |
| `promote_header` | Make row N the header; drop rows above it |
| `flatten_merged_cells` | Forward-fill merged cell regions in xlsx → CSV |
| `convert_file` | Convert between xlsx / ods / csv / json / parquet |

**New dependencies:** `openpyxl>=3.1`, `odfpy>=1.4`, `pyarrow>=15.0`

**Tests:** 93 new tests; total 654 passing.

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
| Python | 3.14+ |
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
