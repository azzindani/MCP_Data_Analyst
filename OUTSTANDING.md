# Outstanding Implementation List

Generated from the MCP Data Analyst — Next Generation Architecture Plan.

Status legend: ✅ Done · ❌ Missing · 🔄 Partial

---

## Current State Summary

| Server | Status | Tools | Notes |
|---|---|---|---|
| `data_basic` | ✅ Done | 8 tools + 49 patch ops | Missing 2 ops + `list_patch_ops` |
| `data_medium` | 🔄 Transitional | 25 tools | Will be split into T2 + T3 |
| `data_advanced` | 🔄 Transitional | 11 tools | Will become `data_visual` (T4) |
| `data_project` | ❌ Missing | — | New T0 layer |
| `data_transform` | ❌ Missing | — | New T2, carved from `data_medium` |
| `data_statistics` | ❌ Missing | — | New T3, carved from `data_medium` + new tools |
| `data_visual` | ❌ Missing | — | Rename of `data_advanced` + `customize_chart` |

---

## Phase 1 — Foundation Gaps

### T1 `data_basic` — Patch Op Additions

| Item | Status | Notes |
|---|---|---|
| `boxcox_transform` op | ❌ Missing | Via scipy; store lambda in receipt |
| `yeojohnson_transform` op | ❌ Missing | Works on negatives; via scipy |
| `list_patch_ops()` tool | ❌ Missing | Returns full op catalog on demand |

All other 49 patch ops are ✅ implemented.

### T2 `data_transform` — New Server (10 Tools)

| Tool | Status | Source |
|---|---|---|
| `filter_dataset` | ❌ Missing | Upgraded from `filter_rows` (data_medium) |
| `reshape_dataset` | ❌ Missing | New — pivot/melt/split_column/combine/transpose |
| `aggregate_dataset` | ❌ Missing | Unified — absorbs compute_aggregations + cross_tabulate + value_counts + pivot_table |
| `resample_timeseries` | ❌ Missing | Exists in data_medium engine; needs its own server |
| `merge_datasets` | ❌ Missing | Move from data_medium |
| `concat_datasets` | ❌ Missing | Exists in data_medium engine; needs its own server |
| `smart_impute` | ❌ Missing | Move from data_medium |
| `run_cleaning_pipeline` | ❌ Missing | Move from data_medium |
| `feature_engineering` | ❌ Missing | Move from data_medium |
| `enrich_with_geo` | ❌ Missing | Move from data_medium |

### T3 `data_statistics` — New Server (11 Tools)

| Tool | Status | Source |
|---|---|---|
| `extended_stats` | ❌ Missing | Exists in data_medium; needs `mode` param + own server |
| `validate_dataset` | ❌ Missing | Move from data_medium |
| `auto_detect_schema` | ❌ Missing | Move from data_medium |
| `check_outliers` | ❌ Missing | Move from data_medium (merge with detect_anomalies) |
| `scan_nulls_zeros` | ❌ Missing | Move from data_medium |
| `correlation_analysis` | ❌ Missing | Move from data_medium |
| `statistical_test` | ❌ Missing | **New** — replaces statistical_tests; all 17 test types + effect sizes + post-hoc |
| `regression_analysis` | ❌ Missing | **New** — OLS + logistic via statsmodels |
| `time_series_analysis` | ❌ Missing | Move from data_medium + STL/ACF/ADF enhancements |
| `period_comparison` | ❌ Missing | **New** — MoM/QoQ/YoY comparison |
| `cohort_analysis` | ❌ Missing | Move from data_medium |

### T4 `data_visual` — New Server (12 Tools)

| Tool | Status | Source |
|---|---|---|
| `run_eda` | ❌ Missing | Move from data_advanced |
| `generate_auto_profile` | ❌ Missing | Move from data_advanced |
| `generate_distribution_plot` | ❌ Missing | Move from data_advanced |
| `generate_correlation_heatmap` | ❌ Missing | Move from data_advanced |
| `generate_pairwise_plot` | ❌ Missing | Move from data_advanced |
| `generate_chart` | ❌ Missing | Move from data_advanced |
| `generate_geo_map` | ❌ Missing | Move from data_advanced |
| `generate_3d_chart` | ❌ Missing | Move from data_advanced |
| `generate_dashboard` | ❌ Missing | Move from data_advanced |
| `generate_multi_chart` | ❌ Missing | Move from data_advanced |
| `export_data` | ❌ Missing | Move from data_advanced |
| `customize_chart` | ❌ Missing | **New** — modify existing Plotly chart without regenerating |

---

## Phase 2 — Analysis Power

### New Statistical Tools (T3)

| Item | Status | Notes |
|---|---|---|
| `statistical_test` — 17 test types | ❌ Missing | shapiro_wilk, ks, anderson, t_test, paired_t_test, one_sample_t, anova, chi_square, fisher, mann_whitney, wilcoxon, kruskal, levene, pearson, spearman, kendall, proportion_z |
| Effect sizes per test family | ❌ Missing | Cohen's d, eta-squared, Cramér's V, rank-biserial r, epsilon-squared |
| Post-hoc tests (Tukey HSD) | ❌ Missing | For ANOVA / Kruskal when `posthoc=True` |
| Multiple testing correction | ❌ Missing | bonferroni / fdr_bh via `correction` param |
| `regression_analysis` | ❌ Missing | OLS + logistic; coefs, p-values, R², RMSE, VIF, diagnostics |
| `period_comparison` | ❌ Missing | MoM/QoQ/YoY with group_by support |
| `time_series_analysis` STL + ADF | ❌ Missing | STL decomposition, ACF/PACF, Augmented Dickey-Fuller |

### New T2 Tools

| Item | Status | Notes |
|---|---|---|
| `aggregate_dataset` mode=window | ❌ Missing | Window functions (row_number, rank, running totals) |
| `reshape_dataset` transpose | ❌ Missing | Flip rows and columns |
| `filter_dataset` extended ops | ❌ Missing | isin, not_isin, between, regex, date_range, quantile_between |

---

## Phase 3 — Project Layer

### T0 `data_project` — New Server (6 Tools)

| Tool | Status | Notes |
|---|---|---|
| `create_project` | ❌ Missing | Set up workspace: data/working/trial/report dirs + project.json |
| `open_project` | ❌ Missing | Load manifest, return aliases + pipeline history + active file |
| `register_file` | ❌ Missing | Add file with alias; stage: raw/working/trial/output |
| `list_project_files` | ❌ Missing | List aliases with stage, size, row count |
| `save_pipeline` | ❌ Missing | Save named pipeline template (list of op dicts) |
| `run_saved_pipeline` | ❌ Missing | Execute saved pipeline on alias, produce new alias |

### Shared Utilities

| Item | Status | Notes |
|---|---|---|
| `shared/project_utils.py` | ❌ Missing | Manifest I/O, alias → absolute path resolution, `project:name/alias` syntax |

### Alias System

| Item | Status | Notes |
|---|---|---|
| `project:name/alias` resolution in all tools | ❌ Missing | T1–T4 tools accept alias or absolute path |

---

## Phase 4 — Polish & Accessibility

| Item | Status | Notes |
|---|---|---|
| `customize_chart` (T4) | ❌ Missing | Read Plotly JSON from HTML, apply mods, re-render |
| `list_patch_ops()` (T1) | ❌ Missing | On-demand op catalog |

---

## Implementation Sequence

```
1. boxcox_transform + yeojohnson_transform + list_patch_ops  [T1 additions]
2. shared/project_utils.py                                   [foundation]
3. servers/data_project/                                     [T0]
4. servers/data_transform/                                   [T2]
5. servers/data_statistics/                                  [T3 — heaviest]
6. servers/data_visual/                                      [T4]
7. Format → Lint → Test → Commit → Push
```

---

## Token Budget (After Full Implementation)

| Tier | Server | Tools | Est. Schema Tokens |
|---|---|---|---|
| T0 | data_project | 6 | ~300 |
| T1 | data_basic | 8 + ops | ~400 |
| T2 | data_transform | 10 | ~550 |
| T3 | data_statistics | 11 | ~620 |
| T4 | data_visual | 12 | ~680 |

Max context cost at any time: **~680 tokens** (T4 loaded alone).
