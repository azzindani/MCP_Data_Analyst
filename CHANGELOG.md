# Changelog

All notable changes to this project will be documented in this file.

---

## [Unreleased]

### Fixed — what a tool says is what it does

- Every answer names its `op`: the wrapper every tool passes through
  (`shared/token_estimate.measure_responses`) fills it from the tool's name
  when the answer leaves it out. A sweep census found 25 tools answering
  without it, including `inspect_dataset`, `apply_patch` and
  `statistical_test` failures.
- `fill_nulls` takes `strategy: "value"` with `value`, the literal to fill
  with. `list_patch_ops` advertised it while `apply_patch` refused it.
- `run_cleaning_pipeline` names the ops it runs, and the closest one, when
  it refuses an unknown op, instead of pointing at `list_patch_ops` on
  another endpoint.
- `run_eda(mode="minimal", output_path=...)` says the page was not written
  and how to write it (`include={"html": True}`).
- `generate_multi_chart` no longer titles `multi_line` "Multi-Multi Line".

### Fixed — a quality score prices each fact once

- A duplicate-rows or missing-values alert no longer costs `validity`: the
  uniqueness and completeness components already price them
  (`shared/quality.py`, byte-identical with Machine_Learning's).

### Fixed — every tool sees the same dates

- `inspect_dataset`, `run_eda` and `generate_auto_profile` read a date column
  stored as text as a date, by the rule `generate_dashboard` uses
  (`shared/column_utils.read_dates`). They asked only the dtype, so
  Ad_Data.csv's `Date` was categorical to them and a date to the dashboard
  and `auto_detect_schema`, and a caller reading inspect concluded the file
  had no time axis.

### Fixed — an inf written is an inf reported

- A patch op that writes a numeric column (`apply_patch` and
  `run_cleaning_pipeline` alike) reports `non_finite_count` beside
  `null_count`, and a `warning` naming the column when it holds +/-inf, with
  the guard that writes a null instead (`a / b if b != 0 else None`).
  `spends/clicks` on Ad_Data.csv answered `null_count: 4104` with 426 rows of
  inf written and unmentioned.

### Fixed — a t-test names its groups

- `statistical_test` (t_test) and `statistical_tests` (ttest) answer with
  `groups` -- each group's name, n, mean and std -- and a `direction` naming
  the group with the higher mean and which group the statistic's sign
  follows. They answered `statistic: -33.2` with no names, so neither the
  sign nor the answer to "which is higher" could be read.
- Both run Welch's t-test (unequal variances) and say so. Student's t was run
  on groups whose spreads differed seven-fold, where it is wrong.
- `statistical_test` refuses a t-test on three or more groups, naming them,
  instead of comparing the first two the file listed. It answers with `op`,
  and its interpretation no longer reads "Reject H0: Reject H0".
- `statistical_tests` no longer sends every test result to apply_patch().

### Added — upload URLs, off by default

- With `MCP_UPLOAD_URLS=1` and `MCP_UPLOAD_BASE_URL`, a path from the caller's
  sandbox is refused with a single-use URL minted for that file, and one
  `curl -T` from the sandbox writes it to the inbox -- no bytes through the
  model. The token is HMAC-signed (`MCP_UPLOAD_SECRET`), fixes the file name,
  expires in 15 minutes, writes once, and is capped at `MCP_MAX_UPLOAD_MB`; a
  forged, re-signed, expired or spent one writes nothing. The route,
  `/upload/<token>`, is mounted at the root outside the API key and answers
  404 while uploads are off.

### Added — one endpoint, eight tools

- `/mcp` serves the whole surface as eight domain tools -- `data_inspect`,
  `data_edit`, `data_reshape`, `data_stats`, `data_chart`, `data_report`,
  `data_ingest`, `data_workspace` -- each an `action` (one of the 68 tier
  tools, by its own name) plus an `args` object whose properties say which
  actions take them, in the Pipeline server's shape. A model connected to every
  tier read 68 tool names on every turn; here it reads eight. Each action runs
  the tier tool itself, so validation, inline files, missing-file suggestions
  and answers are identical. An action asked of the wrong tool is pointed at
  the right one; an argument the action does not take is refused by name. The
  tier endpoints keep serving unchanged.

### Changed — four duplicate tools retired from the list

- `extended_stats` (served identically by the statistics server),
  `statistical_tests` (a subset of `statistical_test`), `filter_rows` (what
  `filter_dataset` was upgraded from) and `compute_aggregations`
  (`aggregate_dataset` in groupby mode) leave the medium tier's `tools/list`:
  68 tools listed instead of 72, and no name listed twice. They still answer
  exactly as before, and each answer carries `retired`, naming the tool to use.
  The first step of cutting the tool surface; see `shared/retired.py`.

### Added — a file too big for one call arrives in parts

- Each part is an inline file with `part=<i>/<n>;sha256=<of the whole file>`
  in its header. Parts wait in the inbox's hidden `.parts` folder, arrive in
  any order, and a part sent again replaces itself; until the last one lands
  the tool answers `tool_ran: false` with the parts still missing. The joined
  file must match its SHA-256 or nothing is kept. Capped at
  `MCP_MAX_UPLOAD_MB` (default 100); an upload left unfinished for an hour is
  dropped.

### Added — a file's bytes may go wherever a path goes

- `data:text/csv;name=sales.csv;base64,<bytes>` in place of a path is saved to
  `MCP_OUTPUT_DIR/inbox` under its name before the tool runs, and the tool
  reads that path. A wrapper on every tier does it, so no tool's schema changes
  and no tool echoes the bytes back. Capped at `MCP_MAX_INLINE_MB` (default 10)
  before decoding; a name cannot leave the inbox; the same bytes sent twice are
  one file, and a taken name is never overwritten. For a caller whose file is
  in its own sandbox -- a claude.ai upload -- with no link to give; the
  caller's-side refusal names this route first.

### Added — a file on the caller's side reaches the server, or the refusal says how

- A Google Drive, Docs/Sheets/Slides, Dropbox, GitHub or GitLab share link is
  rewritten to the address that serves the file (a Sheet to its CSV export,
  the tab in `gid` when named). As the browser shows them, each answered with
  a web page, which was saved as `data.csv` and parsed.
- A web page served where a file was asked for is refused, not written: a
  share link that is not public answers with a sign-in page even at the right
  address. A URL may serve a page only when its name says it is one.
- A path from the caller's side -- `/mnt/user-data/…` (a claude.ai upload),
  `/home/claude/…`, `/mnt/data/…`, a Windows drive or `/Users/…` on a server
  that is neither -- is refused as what it is, naming the routes in that work
  on this server, instead of "outside the folders this server can use".

### Fixed — one ordinary merge OOM-killed the whole server

- `merge_datasets(Ad_Data.csv, ad_clean.csv, how="inner", dry_run=True)` took
  down every tier and every session: auto-detect picked the first shared
  column, `Date`, the guard counted 1,804,677 rows against a 2,000,000-row cap,
  and the dry run performed the real merge -- about 750 MB of mostly-text rows
  in a 1 GB container. The guard now sizes the join in bytes as well as rows
  (`MCP_MAX_MERGE_MB`, default 256) from an exact count of the keys, and
  refuses before anything is built; a dry run answers from that count and never
  builds the table. A key is picked unasked only when it is unique on one side,
  matches something, and is neither a date nor a measurement -- otherwise the
  call says why each shared column was passed over. A key named on one side is
  used on both, and every answer reports `left_on`/`right_on`. Found by driving
  the deployed tools directly.

### Security — `extract_all_sheets` wrote its CSVs wherever it was pointed

- Every output here goes through `get_output_path`, which resolves and
  confines; `extract_all_sheets` built its folder with a bare `Path(output_dir)`,
  so on a confined server a relative folder landed beside the process and an
  absolute one was used as given. It now goes through `resolve_path` like the
  rest. Found by reading the output paths during a direct sweep of the fleet.
- A refused path is always an answer in the usual failure shape (`success:
  false`, `op`, `error`, `hint`). A resolver outside a tool's own `try` let the
  refusal escape as "Error executing tool" with no hint, as it did on the ML
  server's `anomaly_detection`; the per-tool wrapper now catches it wherever it
  is raised.

### Added — a missing file is answered with the files that exist

- "File not found: ad_data.csv", hint "Check file_path is absolute and the file
  exists", was the commonest failure a remote caller met, and both halves were
  wrong for it: a relative path is read from the data folder, and the caller
  cannot look to see what does exist. Every tool's missing-file failure now
  carries `did_you_mean` (the same name in other letters first, then the same
  stem with another extension, then close spellings, as the path to pass) and
  a hint built from it. With nothing close, a confined server's hint lists
  what the data folder holds. One choke point on the response
  (`shared/missing_file.py`), not sixty edits.
- The search is bounded (two folders deep, 2,000 entries, hidden files
  skipped) and, on a confined server, only looks inside the served folders:
  a suggestion never names a file the server would refuse to read.

### Fixed — a column formula means what it says

- **`column_math` and `add_column` ignored operator precedence.** The parser
  split the text on `+ - * /` and folded left to right, so `a + b * 2` wrote
  `(a + b) * 2` (22 where 21 was meant) under `success: true`, and it refused
  parentheses and unary minus, so the intended version could not be written.
  Formulas are now read by Python's grammar and evaluated by an allow-listed
  tree walk (`shared/expr.py`), never `eval()`: normal precedence, parentheses,
  unary minus, `** // %`, comparisons, `and`/`or`/`not`, `x if cond else y`,
  and `abs round sqrt log log10 exp floor ceil clip coalesce if_else isnull
  notnull`.
- Column names with spaces still work bare. A name that holds operator
  characters is written in backticks (`` `clicks-2` ``). A number that is also a
  column name (`2020` in a pivoted file) is refused until the caller writes
  `` `2020` `` for the column or `2020.0` for the number. The old parser read
  the column, and reading the number silently would have turned a year-over-year
  difference into `1.0`.

### Security — a deployed server reads and writes only inside the folders it serves

- Every tool resolved its path with `Path(file_path).resolve()`, so any
  authenticated caller of an HTTP deployment could read any file the container
  could. `inspect_dataset("/etc/hostname")` succeeded against the live
  endpoint, and `/proc/self/environ` (the API keys) was the same call. A
  workspace `base_dir` reached anywhere, a workspace name like `../../etc`
  walked out of the workspace root, and `concat_file` and the ingest tools'
  `output_path` bypassed path resolution altogether.
- With `MCP_CONFINE_PATHS` on, a path must lie inside `MCP_OUTPUT_DIR`, the
  workspace root, or a folder in `MCP_ALLOWED_ROOTS`, judged after symlinks
  are resolved. It is on in `docker-compose.yml` and set by
  `unified_server.py` when it serves, so every HTTP deployment is confined
  unless `MCP_CONFINE_PATHS=0`. A refusal names the reason and the folders. A
  relative path on a confined server is read from the data folder, not the
  container's working directory.
- A local stdio install is unchanged, except that `~` expands, `MCP_DATA_ROOT`
  sets where a relative path is read from, and a workspace name is always a
  plain name.
- `remote_smoke_test.sh` checks the refusal against the running container.

### Added — median, count and distinct count in the dashboard

- `agg_overrides` and a panel's `agg` accept `median`, `count` and
  `count_distinct` (aliases `nunique`, `distinct`, `unique`, `n`) alongside
  sum/mean/max/min. Every aggregator in the page (bar, KPI, time series,
  grouped bar, heatmap, choropleth) hands them to one reducer, `_agg`, which
  never counts or ranks a missing value. KPI labels read "Median units",
  "Count of orders", "Distinct customer_id". The KPI's first paint, computed in
  Python, agrees with the number the script computes.

### Fixed — a missing value is not a zero in the dashboard's numbers

- The dashboard recomputes every chart and KPI in the browser, reading each
  value with `+r['col']`. A missing cell is embedded as `null`, and `+null` is
  `0` in JavaScript, which passed the templates' `isNaN` guard. So a group
  holding [10, missing] averaged to 5, its minimum became 0, and the KPI mean
  sank once the script ran (the first paint, computed in Python, was right).
  Every read now goes through `_num`, which keeps missing values missing. A row
  with no value no longer satisfies a numeric range filter that has been set.
  Tested by running the generated JavaScript in node on rows with gaps.

### Fixed — the dashboard knows what a column is before choosing what to do with it

- **Identifiers are no longer quantities.** Every integer column was summed
  into a KPI and charted as a value: "Total customer_id", zip codes on a bar
  chart, the `Unnamed: 0` row counter a pandas export leaves behind. An
  identifier is now recognised by the last word of its name (`customer_id`,
  `zip_code`, `orderKey`, `sku`) or by being a row counter (unique integers
  rising by exactly one). It is never summed, averaged or charted as a value.
  An `agg_overrides` entry for it (`"zip:sum"`) says it is a quantity after
  all, and a spec may still name one explicitly.
- **CSV dates are dates.** The loader leaves `2024-01-05` as text, so no CSV
  ever got a time series, and a column of dates became a pie chart and a
  filter. A text column where at least 90% of values have a date's shape and
  parse is now read as dates. Version strings, IP addresses and codes are left
  alone.
- **`column_roles` in the response and the dry-run preview**:
  identifier / date / measure / dimension / text per column. A wrong guess is
  visible before anyone opens the page.

### Fixed — the aggregate is guessed from a column's words, not letters inside them

- `infer_agg` (the dashboard, pivot tables and lag correlation) matched its
  keywords as substrings, so `followers` became a minimum ("low"),
  `laptop_sales` a maximum ("top"), `attempts` a mean ("temp"),
  `database_size` a minimum ("base") and `problems_reported` a mean ("prob").
  All of those are counts to be summed. Keywords now match whole words, with
  camelCase split and plurals included. Only keywords of seven letters or more
  (`average`, `percent`, `density`, …) are still found inside a run-together
  name like `averageprice`.

### Security — a column name can no longer run as code in a dashboard

- Column names come from the CSV, and the dashboard wrote them raw into
  JavaScript string literals in every chart template, into inline `onchange`
  handlers in the filter bar, and into the numeric range's HTML label. A header
  holding `'` broke every chart on the page. A crafted header (`</script>…`,
  `x" onmouseover="…`, `<img src=x onerror=…>`) ran script in the browser of
  whoever opened the dashboard, and the dashboard is the file people send to a
  colleague. Names are now escaped for JavaScript at one choke point before the
  templates see them, escaped again for HTML inside attributes, and the
  correlation column list goes through `json_for_script`. Tested on the whole
  page against a benign twin: identical tags and attribute names, the same
  number of scripts, and every inline script passes `node --check`.
- The filter JavaScript looked up its dropdown with
  `querySelector('.ddw[data-col="'+col+'"]')`, so a name holding `"` threw and
  that filter stopped working. It uses `CSS.escape` now.

### Fixed — a dashboard spec reaches the page, not only the response

- **`generate_dashboard(spec=…)` echoed the spec and drew the detection.**
  `kpis`, `filters`, and each panel's `cols` and `agg` were validated and
  returned in `spec`, and then the page was built from auto-detection:
  `layout: [pie]` drew a pie plus a grouped bar, a box plot, a correlation
  matrix, a heatmap and a histogram per numeric column, while
  `charts_included` said `["pie"]`. A caller's layout now draws exactly one
  card per panel from that panel's columns (an empty panel is filled the way
  the detected page fills it), `kpis` are the KPI cards, and `filters` are the
  filter bar: text columns get a control, numeric ones a range. Tab slots
  address panels.
- **Refused by name instead of ignored:** an unknown panel key, a role a chart
  does not have, a text column in a numeric role, `agg` on a chart that does
  not group, a pie `agg` other than sum, a text KPI, and a filter that could
  not work (one value, or a text column with more than 100 values).
- A text column named as a panel's `date` is read as dates when at least 90%
  of it parses. The CSV loader leaves `2024-01-05` as text.
- The detected layout is written in the spec's own vocabulary (`choropleth`,
  not `geo_choropleth`), so a geo dashboard's spec can be handed back to
  `customize_dashboard`. It used to be refused. The resolved defaults now
  describe the page actually drawn: seven KPIs, not eight, and the filters
  the bar really offers.

### Fixed — an aggregate override is honoured or refused, never dropped

- **`generate_dashboard(agg_overrides=…)` discarded every entry it did not
  recognise.** `["units:count", "units:median", "revenue=mean",
  "nosuchcol:sum"]` parsed to `{"nosuchcol": "sum"}`: the caller asked for a
  count and got the detected sum, drawn on the page under `success: true`. Now
  an unknown aggregate, a missing `:`, a non-string entry or a column that is
  not numeric in the file is refused by name, every problem in one message, and
  nothing is written. `avg`/`average`/`total`/`maximum`/`minimum` and a `=`
  separator are understood.

## [0.3.0] — 2026-09-07

Source-only release: no wheel and no container image are published. Build the
image from the `Dockerfile` here, or install from the tag.

### Added — the schema now names its own legal values

- **Every dispatch parameter declares an `enum`.** `method`, `agg_func`,
  `chart_type`, `mode`, `normalize`, `test`, `model_type`, `period_unit` and
  the rest publish their legal values in `tools/list`, so a client validates
  before sending instead of learning the spelling by burning a call. Each enum
  renders from the table the runtime switches on, never a second copy, and a
  census test fails if a new tool arrives with a bare `mode: str`.
- **The enum advertises; it does not enforce.** `json_schema_extra={"enum": …}`
  emits the same JSON schema as `Literal` — measured, not assumed — but leaves
  the tool body to answer. That keeps every documented alias working
  (`zscore`→`std`, `average`→`mean`, `rows`→`index`, `MoM`→`M`) and keeps the
  refusals that name what a caller should have sent. A `Literal` would have
  replaced all of them with pydantic's generic `literal_error`.
- **`list_derive_ops`** — the `derive` grammar for `feature_engineering`, with
  each op's required and optional keys and a worked example. That grammar lives
  in a `list[dict]` the schema cannot describe, so it could previously only be
  learned by failing.

### Fixed

- **`check_outliers(method="zscore")` reported "no outliers" on a column
  holding 2,178 of them.** The method dispatch was two `if` statements with no
  `else`, so an unrecognised method skipped both branches and returned
  `success: true`. The guard now runs before the file is read, and `zscore` is
  an accepted alias rather than an error.
- **`resample_timeseries` advertised two aggregations it refuses.** It
  validates against a table of nine while its annotation named the eleven its
  siblings take, so `tools/list` offered `nunique` and `var` to a tool that
  answers "Invalid agg_func" to both. Narrowed to the nine it actually accepts.
- **`cross_tabulate`, `reshape_dataset`, `generate_multi_chart` and
  `aggregate_dataset`** passed `agg_func` / `normalize` straight to pandas, so a
  typo surfaced as `'DataFrameGroupBy' object has no attribute` under a hint
  naming the arguments that were fine. All four validate against the shared
  tables now.
- **`cross_tabulate` echoed the `normalize` it was sent, not the one it used**,
  and silently dropped an `agg_func` it could not apply. Both are reported.
- **`pivot_table`'s refusal blamed `file_path`** when `agg_func` was wrong.
- **Argument-type errors escaped the response envelope** on four servers,
  arriving as a raw pydantic dump complete with a link to pydantic.dev.

### Changed

- 2,981 tests, up from 2,894.

---

## [0.3.0] — 2026-09-07 · part two: the tool-user review

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

### Fixed — sweep rounds 24 and 25, "believe the description"

Two coverage rounds asked one question of all 243 tools in the fleet: is the
sentence an MCP client shows for this tool true? A client sees that one line
and nothing else — no README, no examples, no source — and in this repo it is
the docstring, capped at 80 characters by CI, which is exactly the pressure
that makes a description over-promise. Six answers here were no, and every one
is the same shape: **a vocabulary written down twice, where the copies
drifted.**

- **`dayfirst` documented three values and enforced none.** `parse_dates`
  accepted `"yes"`, `"1"`, `"no"`, `"0"` as silent aliases and let everything
  else fall through to auto-detect, so all of `auto true false yes banana`
  returned `success: true` with two different answers. Measured on
  `Ad_Data.csv`, whose `Date` column is unambiguous ISO, `dayfirst="yes"` moved
  the series from `2019-10-16..2020-07-07` to `2019-01-11..2020-12-06` — and
  those dates reach `trend`, `seasonality`, the rolling stats and the chart. A
  typo (`ture`, `flase`, `Yes`) silently chose a date interpretation and nothing
  in the response said so. Now exactly `auto` / `true` / `false`, case- and
  whitespace-insensitive, and anything else is refused by name. Office's
  `bold`/`italic` is this same tri-state string solved properly; this was the
  copy that drifted.
- **`statistical_test` listed six of its seventeen tests** as if that were the
  set, so eleven working tests — `levene`, `wilcoxon`, `ks`, `fisher`,
  `kendall`, `spearman`, `pearson`, `proportion_z`, `one_sample_t`,
  `paired_t_test`, `anderson` — were invisible to everyone reading the tool
  list. The vocabulary has outgrown 80 characters, so the description now says
  how many there are and points at the error hint that already enumerates them.
  (The README had listed all seventeen correctly the whole time.)
- **`resample_timeseries` documented `compute_aggregations`' vocabulary.** Its
  sentence said `agg: sum mean count min max`; it validates against
  `_VALID_AGGS`, which is nine, so `agg_func="median"` worked and was documented
  nowhere. Five words that were correct about a different tool.
- **`concat_datasets` documented `vertically`/`horizontally`** while the parser
  takes `rows`/`columns`, so reading the description and typing what it said
  earned a refusal. It now also states that column mode needs equal row counts —
  a constraint its refusal already named (`Got: [3, 1]`) and its description
  did not.
- **`feature_engineering` documented an `auto` feature type that does not
  exist**, and said nothing about `one_hot` being capped at 10 distinct values
  per column and 5 columns per call. The caps stay — one-hot on 16,834 distinct
  creative names is not what anyone means — and the response already carried
  `one_hot_skipped` with a reason per column; what changed is that the sentence
  says a cap exists before you call rather than after.

New tests read each description out of the source and compare it against the
vocabulary the code enforces, so the two cannot separate again silently. That
check is what nobody was running when six of seventeen became the documented
set.

One of these fixes had to be corrected after shipping: the first `dayfirst`
pass kept `"yes"`/`"1"` as aliases and passed its own tests, and a live check
against the deployed server caught that those aliases *were* the finding one
draft smaller. A fix verified only by its own tests is verified against the
author's reading of the contract.

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
