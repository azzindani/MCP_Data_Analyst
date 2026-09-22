# Super-function: nested, reusable op chains

Design note, 2026-09-22. Nothing below is built yet. It records the idea, what
this repo already provides toward it, and what is missing.

## The idea

Most MCP servers are straight pipelines: one tool, one fixed function, and
only the model can chain calls, one round trip at a time. Scripts beat that on
novel work because a function can call a function. That lets the model wrap,
branch and retry instead of giving up. MCP beats scripts on routine work: it's
ready with no install, validated, snapshotted and consistent.

The goal is to keep the MCP properties while recovering most of the
composition a script has. The model is a toolbox and a workshop. Ready
pipelines cover the 90-95% of work that repeats. For the novel rest, existing
ops handle the known steps, only the new step is written, and once reused the
new chain is saved as a pipeline of its own.

In this server that means one executor that takes a chain of ops as data, plus
saved chains that can be called again with new values and can nest inside
each other.

## What already exists

| Piece | Where |
|---|---|
| Executor: ordered op chain, validated before the file is touched, `dry_run`, one snapshot, all-or-nothing write | `apply_patch`, `servers/data_basic/engine.py:849`, 52 ops |
| Same op handlers behind a second entry point | `run_cleaning_pipeline` (`OP_HANDLERS` in `servers/data_basic/_patch_ops.py`) |
| Save a chain under a name, validated at save time | `save_workspace_pipeline`, `servers/data_workspace/engine.py:366` |
| Run a saved chain on a new input | `run_workspace_pipeline`, `servers/data_workspace/engine.py:452` |
| Formula fields for logic no op names | `column_math`, `add_column` (math mode), `conditional_assign`, `feature_engineering(derive=[...])` |

A single MCP tool calling another is not a limit here. `unified_server.py`
runs all seven sub-servers in one process, and `run_workspace_pipeline`
already calls data_basic's `apply_patch` directly
(`servers/data_workspace/engine.py:523`).

## What is missing

1. **Nesting.** No `pipeline_ref` op, so a saved pipeline cannot include
   another. Pipelines are workspace-scoped and `apply_patch` is file-scoped,
   so a reference can only resolve inside `run_workspace_pipeline` until
   `apply_patch` knows about a workspace. Expand references at validation
   time, with a depth limit and cycle detection, so `dry_run` shows the
   flattened chain.
2. **Parameters.** A saved pipeline is fixed. `run_workspace_pipeline` has no
   `params`, so "same function, different values" today means only a
   different input file. Needs placeholders in the saved ops and a way to
   pass values (the repo's signature rules allow `list[dict]` for op arrays
   only, so decide the shape deliberately).
3. **Control ops.** No `for_each` (the same ops over several columns, cheap:
   expand at validation) and no dataset-level `if_then` ("if null share > 5%
   then ..."). Row-level branching already exists as `conditional_assign`.
4. **Reach of the vocabulary.** The 52 ops transform one table's rows and
   columns. `aggregate_dataset`, `pivot_table`, `merge_datasets`,
   `resample_timeseries` and `smart_impute` are separate tools, not ops, so one
   chain cannot run clean -> aggregate -> merge. Exposing them as ops is the
   largest part of the work.
5. **The formula parser returns wrong answers.** `_parse_expr`
   (`servers/data_basic/_patch_ops.py:161`) evaluates strictly left to right,
   so `a + b * 2` returns `(a + b) * 2`. With `a=1, b=10` it gives 22, not
   21, under `success: true`. It rejects parentheses (`(a + b) * 2`) and unary
   minus (`a * -1`). This is a live defect regardless of this design, and
   formulas are what the design leans on for novel logic.

## Deliberately left out

A sandboxed "run Python" escape hatch for the last few percent. This repo's
charter (`CLAUDE.md`, "What This Is (and Is Not)") says it is not a script
runner or notebook executor. The harness running the model can already run a
script for the rare case no chain covers. The result can then come back here
as a saved pipeline once it repeats.

## Build order

1. Fix `_parse_expr`: precedence, parentheses, unary minus. It's needed on its
   own.
2. `pipeline_ref` and parameters in workspace pipelines. After this, nesting
   and "same function, different values" both work.
3. `for_each` over columns.
4. Aggregate, pivot, merge, resample and impute as ops.
