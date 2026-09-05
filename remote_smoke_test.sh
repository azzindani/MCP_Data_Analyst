#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# MCP_Data_Analyst — remote smoke test, all 70 tools across 7 sub-servers.
#
# NOT part of pytest / CI (see CLAUDE.md "Remote smoke tests"). Real auth
# enforcement + real handwritten-prompt-style tool calls on real generated
# datasets (a 200-row sales CSV, a small region-population CSV, a geo
# lat/lon CSV, and a real messy multi-sheet .xlsx with merged cells),
# chaining real outputs between calls, against the real public domain.
#
# Tools read/write files by server-side path, so this script docker-cp's
# generated fixtures into the running container first — only works run on
# the same host as the deployment (self-hosted, by design). docker cp
# preserves source ownership, which the non-root `app` user can't read —
# chown after copying.
#
# Usage:
#   ./remote_smoke_test.sh                      # reads DA_API_KEY from .env
#   DA_API_KEY=sk-... ./remote_smoke_test.sh     # or pass it directly
#   DOMAIN=http://localhost:8810 ./remote_smoke_test.sh   # test a different target
#   CONTAINER=mcp-data-analyst ./remote_smoke_test.sh      # override container name
# ─────────────────────────────────────────────────────────────────────────────
set -uo pipefail

DOMAIN="${DOMAIN:-https://data.casava.space}"
CONTAINER="${CONTAINER:-mcp-data-analyst}"
# Read the key out of .env without executing it. `source` runs every line of
# the file, so a line that is not a KEY=VALUE assignment is a command; that has
# already turned a stray summary line into a file named after a secret. A plain
# read of one assignment cannot do that.
if [ -z "${DA_API_KEY:-}" ] && [ -f .env ]; then
  DA_API_KEY=$(sed -n 's/^[[:space:]]*DA_API_KEY[[:space:]]*=[[:space:]]*//p' .env | tail -n1 | tr -d '\042\047\r')
fi
KEY="${DA_API_KEY:?Set DA_API_KEY (env var or .env file) before running}"
D=/tmp/remote-smoke-test
SALES="$D/sales.csv"
SALES2="$D/sales2.csv"
GEO="$D/geo.csv"
GEOJSON="$D/regions.geojson"
DAYFIRST="$D/dayfirst.csv"
XLSX="$D/workbook.xlsx"

FAILS=0
pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1"; FAILS=$((FAILS+1)); }
ok_json() { echo "$1" | grep -Eq '\\?"success\\?":[[:space:]]*true'; }

echo "Target: $DOMAIN"

# Tools called without an explicit output_path now default into
# MCP_OUTPUT_DIR, which on a real deployment is a directory the operator
# actually looks at. Remember what was there so the run can leave it exactly
# as it found it (see the cleanup at the very bottom).
SHARED_DIR=$(docker exec "$CONTAINER" printenv MCP_OUTPUT_DIR 2>/dev/null || true)
SHARED_BEFORE=$(mktemp)
[ -n "$SHARED_DIR" ] && docker exec "$CONTAINER" sh -c "ls -1A '$SHARED_DIR' 2>/dev/null" | sort > "$SHARED_BEFORE"

echo
echo "== seed real datasets + a real messy .xlsx into the container =="
docker exec "$CONTAINER" mkdir -p "$D"
TMP=$(mktemp -d)
python3 -c "
import random, datetime
random.seed(21)
lines = ['date,region,category,units,revenue']
start = datetime.date(2025,1,1)
regions = ['APAC','EMEA','AMER']
cats = ['Widgets','Gadgets','Gizmos']
for i in range(200):
    d = start + datetime.timedelta(days=i)
    r = random.choice(regions); c = random.choice(cats)
    units = random.randint(1,50)
    revenue = round(units * random.uniform(8,15) + random.gauss(0,5), 2)
    lines.append(f'{d.isoformat()},{r},{c},{units},{revenue}')
open('$TMP/sales.csv','w').write(chr(10).join(lines))
lines2 = ['region,population']
for r in regions: lines2.append(f'{r},{random.randint(1000000,9000000)}')
open('$TMP/sales2.csv','w').write(chr(10).join(lines2))
open('$TMP/geo.csv','w').write(chr(10).join(['region,lat,lon','APAC,13.7563,100.5018','EMEA,48.8566,2.3522','AMER,40.7128,-74.0060']))
import json
geojson = {
    'type': 'FeatureCollection',
    'features': [
        {'type': 'Feature', 'properties': {'region': 'APAC'}, 'geometry': {'type': 'Point', 'coordinates': [100.5018, 13.7563]}},
        {'type': 'Feature', 'properties': {'region': 'EMEA'}, 'geometry': {'type': 'Point', 'coordinates': [2.3522, 48.8566]}},
        {'type': 'Feature', 'properties': {'region': 'AMER'}, 'geometry': {'type': 'Point', 'coordinates': [-74.0060, 40.7128]}},
    ],
}
open('$TMP/regions.geojson','w').write(json.dumps(geojson))
# Month-start rows written DD-MM-YYYY, spanning three years. No field ever
# exceeds 12, so a parser that assumes month-first does not fail -- it reads
# every date transposed and reports success.
lines3 = ['period,value']
for y in (2020, 2021, 2022):
    for m in range(1, 13):
        lines3.append(f'01-{m:02d}-{y},{m * y}')
open('$TMP/dayfirst.csv','w').write(chr(10).join(lines3))
"
docker cp "$TMP/sales.csv" "$CONTAINER:$SALES"
docker cp "$TMP/sales2.csv" "$CONTAINER:$SALES2"
docker cp "$TMP/geo.csv" "$CONTAINER:$GEO"
docker cp "$TMP/regions.geojson" "$CONTAINER:$GEOJSON"
docker cp "$TMP/dayfirst.csv" "$CONTAINER:$DAYFIRST"
rm -rf "$TMP"
docker exec "$CONTAINER" python3 -c "
import openpyxl
wb = openpyxl.Workbook()
ws = wb.active; ws.title = 'Sheet1'
ws['B2'] = 'Quarterly Report'; ws.merge_cells('B2:D2')
ws.append([])
ws.append(['Region','Units','Revenue'])
ws.append(['APAC',120,1450.5]); ws.append(['EMEA',95,1120.2]); ws.append(['AMER',80,990.75])
ws2 = wb.create_sheet('Sheet2')
ws2.append(['Category','Count']); ws2.append(['Widgets',30]); ws2.append(['Gadgets',45])
wb.save('$XLSX')
"
docker exec -u root "$CONTAINER" chown -R app:app "$D"
pass "200-row sales.csv, sales2.csv, geo.csv, and a real messy workbook.xlsx seeded"

init_session() {
  curl -s -i -X POST "$DOMAIN/$1/mcp" \
    -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
    -H "Authorization: Bearer $KEY" \
    -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"smoke","version":"1"}}}' \
    | grep -i mcp-session-id | tr -d '\r' | awk '{print $2}'
}
init_notified() {
  curl -s -X POST "$DOMAIN/$1/mcp" -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
    -H "Authorization: Bearer $KEY" -H "mcp-session-id: $2" \
    -d '{"jsonrpc":"2.0","id":2,"method":"notifications/initialized"}' > /dev/null
}

echo
echo "== auth enforcement =="
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$DOMAIN/basic/mcp" \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"smoke","version":"1"}}}')
[ "$code" = "401" ] && pass "no token -> 401" || fail "no token -> expected 401, got $code"

declare -A SID
for tier in basic medium statistics transform visual workspace ingest; do
  SID[$tier]=$(init_session "$tier")
  init_notified "$tier" "${SID[$tier]}"
done
[ -n "${SID[basic]}" ] && pass "valid token -> sessions established on all 7 sub-servers" || fail "no session id"

call() {
  local tier="$1" id="$2" name="$3" args="$4"
  curl -s -X POST "$DOMAIN/$tier/mcp" -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
    -H "Authorization: Bearer $KEY" -H "mcp-session-id: ${SID[$tier]}" \
    -d "{\"jsonrpc\":\"2.0\",\"id\":$id,\"method\":\"tools/call\",\"params\":{\"name\":\"$name\",\"arguments\":$args}}"
}
extract() {
  # The key itself arrives escaped. A tool's document is delivered as the JSON
  # *string* result.content[0].text, so "output_path" reads \"output_path\" on
  # the wire, and a pattern anchored on a bare quote matches nothing. This
  # returned empty for every key, which read as "the tool gave no path":
  # generate_chart was reported as producing no chart_path and no public_url
  # while its response carried both. The Office copy of this helper already
  # allowed for the escaping and this one had never been updated to match.
  echo "$1" | grep -oE "\\\\?\"$2\\\\?\"[[:space:]]*:[[:space:]]*\\\\?\"[^\\\\\"]*" | head -1 | sed -E 's/.*"([^"]*)$/\1/'
}

N=10
run() {
  local tier="$1" name="$2" args="$3" prompt="$4" checker="${5:-ok_json}"
  echo "== prompt: \"$prompt\" -> $name =="
  N=$((N+1))
  R=$(call "$tier" "$N" "$name" "$args")
  if $checker "$R"; then pass "$name succeeded"; else fail "$name -> $R"; fi
}

echo
echo "===== data_basic (9 tools) ====="
run basic load_dataset "{\"file_path\":\"$SALES\"}" "load this sales dataset"
run basic load_geo_dataset "{\"file_path\":\"$GEOJSON\"}" "load this geo dataset"
run basic inspect_dataset "{\"file_path\":\"$SALES\"}" "inspect the sales dataset"
run basic read_column_stats "{\"file_path\":\"$SALES\",\"column\":\"revenue\"}" "what are the stats for revenue?"
run basic search_columns "{\"file_path\":\"$SALES\",\"dtype\":\"float64\"}" "which columns are float64?"
run basic apply_patch "{\"file_path\":\"$SALES\",\"ops\":[{\"op\":\"round_values\",\"column\":\"revenue\",\"decimals\":2}]}" "round revenue to 2 decimals"
run basic read_receipt "{\"file_path\":\"$SALES\"}" "show me the change history for sales.csv"
run basic restore_version "{\"file_path\":\"$SALES\"}" "list snapshots I could restore"
run basic list_patch_ops '{}' "what patch ops are available?"

echo
echo "===== data_medium (11 tools) ====="
run medium compute_aggregations "{\"file_path\":\"$SALES\",\"group_by\":[\"region\"],\"agg_column\":\"revenue\",\"agg_func\":\"sum\"}" "total revenue by region"
run medium cross_tabulate "{\"file_path\":\"$SALES\",\"row_column\":\"region\",\"col_column\":\"category\"}" "cross-tab region by category"
run medium pivot_table "{\"file_path\":\"$SALES\",\"index\":[\"region\"],\"columns\":[\"category\"],\"values\":[\"revenue\"]}" "pivot revenue by region and category"
run medium value_counts "{\"file_path\":\"$SALES\",\"columns\":[\"region\"]}" "count rows per region"
run medium filter_rows "{\"file_path\":\"$SALES\",\"conditions\":[{\"column\":\"region\",\"op\":\"equals\",\"value\":\"APAC\"}]}" "show only APAC rows"
run medium filter_rows "{\"file_path\":\"$SALES\",\"conditions\":[{\"column\":\"revenue\",\"op\":\"gt\",\"other_column\":\"units\"}]}" "rows where revenue is above units"
run medium sample_data "{\"file_path\":\"$SALES\",\"method\":\"random\",\"n\":10}" "give me a random sample of 10 rows"
run medium statistical_tests "{\"file_path\":\"$SALES\",\"test_type\":\"ttest\",\"column_a\":\"units\",\"column_b\":\"revenue\"}" "run a t-test between units and revenue"
run medium analyze_text_column "{\"file_path\":\"$SALES\",\"column\":\"region\"}" "analyze the region text column"
run medium detect_anomalies "{\"file_path\":\"$SALES\"}" "detect anomalies in this dataset"
run medium compare_datasets "{\"file_path_a\":\"$SALES\",\"file_path_b\":\"$SALES2\"}" "compare sales.csv and sales2.csv"
run medium extended_stats "{\"file_path\":\"$SALES\"}" "give me extended stats"

echo
echo "===== data_statistics (12 tools) ====="
run statistics validate_dataset "{\"file_path\":\"$SALES\"}" "validate this dataset"
run statistics auto_detect_schema "{\"file_path\":\"$SALES\"}" "auto-detect the schema"
run statistics check_outliers "{\"file_path\":\"$SALES\"}" "check for outliers"
run statistics scan_nulls_zeros "{\"file_path\":\"$SALES\"}" "scan for nulls and zeros"
run statistics correlation_analysis "{\"file_path\":\"$SALES\"}" "how correlated are units and revenue?"
run statistics statistical_test "{\"file_path\":\"$SALES\",\"test\":\"t_test\",\"column_a\":\"units\",\"column_b\":\"revenue\"}" "run a formal t-test"
run statistics regression_analysis "{\"file_path\":\"$SALES\",\"y_col\":\"revenue\",\"x_cols\":[\"units\"]}" "regress revenue on units"
run statistics time_series_analysis "{\"file_path\":\"$SALES\",\"date_column\":\"date\",\"value_columns\":[\"revenue\"]}" "analyze the revenue time series"
run statistics period_comparison "{\"file_path\":\"$SALES\",\"date_col\":\"date\",\"metrics\":[\"revenue\"],\"period_unit\":\"M\"}" "compare this month to last"

# The one assertion that has to read a value rather than trust success:true.
# Read month-first, 01-07-2020 becomes 7 January and the whole series collapses
# into three Januaries -- every row parses, nothing is dropped, and the response
# still says success. Only the range shows it.
run statistics time_series_analysis "{\"file_path\":\"$DAYFIRST\",\"date_column\":\"period\",\"value_columns\":[\"value\"]}" "analyze this day-first monthly series"
DF_START=$(extract "$R" start || true)
case "$DF_START" in
  2020-01-01*) pass "day-first dates parsed as DD-MM-YYYY (series starts $DF_START)" ;;
  "") fail "time_series_analysis returned no start date for the day-first file" ;;
  *) fail "day-first dates were transposed: series starts $DF_START, expected 2020-01-01" ;;
esac
run statistics time_series_analysis "{\"file_path\":\"$DAYFIRST\",\"date_column\":\"period\",\"value_columns\":[\"value\"],\"dayfirst\":\"false\"}" "force month-first on the same file"
run statistics cohort_analysis "{\"file_path\":\"$SALES\",\"cohort_column\":\"region\",\"date_column\":\"date\",\"value_column\":\"revenue\"}" "run a cohort analysis by region"
run statistics lag_correlation "{\"file_path\":\"$SALES\",\"date_column\":\"date\",\"x_column\":\"units\",\"y_column\":\"revenue\",\"max_lag\":5}" "does units lead revenue by a few days?"

echo
echo "===== data_transform (10 tools) ====="
FILTER_R=$(call transform 200 filter_dataset "{\"file_path\":\"$SALES\",\"output_path\":\"$D/sales_emea.csv\",\"conditions\":[{\"column\":\"region\",\"op\":\"equals\",\"value\":\"EMEA\"}]}")
if ok_json "$FILTER_R"; then pass "filter_dataset filtered to EMEA only"; else fail "filter_dataset -> $FILTER_R"; fi
# The lineage sidecar, checked on the live filesystem rather than in the
# response: the response naming a path it did not write is the failure mode.
LINEAGE_PATH=$(extract "$FILTER_R" lineage_path)
if [ -n "$LINEAGE_PATH" ] && docker exec "$CONTAINER" test -f "$LINEAGE_PATH"; then
  pass "filter_dataset wrote a lineage sidecar beside the derived file"
else
  fail "filter_dataset lineage sidecar missing — response said '$LINEAGE_PATH'"
fi
run transform reshape_dataset "{\"file_path\":\"$SALES\",\"mode\":\"pivot\",\"index\":[\"region\"],\"columns\":[\"category\"],\"values\":[\"revenue\"]}" "reshape sales into a pivot"
run transform aggregate_dataset "{\"file_path\":\"$SALES\",\"mode\":\"groupby\",\"group_by\":[\"region\"],\"agg\":{\"revenue\":\"sum\"}}" "aggregate sales by region"
run transform resample_timeseries "{\"file_path\":\"$SALES\",\"date_col\":\"date\",\"freq\":\"W\"}" "resample sales to weekly"
run transform merge_datasets "{\"file_path\":\"$SALES\",\"right_file_path\":\"$SALES2\",\"left_on\":\"region\",\"right_on\":\"region\"}" "merge sales with region population"
run transform concat_datasets "{\"file_paths\":[\"$SALES\",\"$SALES\"]}" "stack two copies of sales together"
run transform smart_impute "{\"file_path\":\"$SALES\"}" "smart-impute any missing values"
run transform run_cleaning_pipeline "{\"file_path\":\"$SALES\",\"ops\":[{\"op\":\"drop_duplicates\"}]}" "drop duplicate rows"
run transform feature_engineering "{\"file_path\":\"$SALES\",\"features\":[\"date_parts\",\"bins\"]}" "auto-engineer date and bin features"
run transform feature_engineering "{\"file_path\":\"$SALES\",\"output_path\":\"$D/derived.csv\",\"derive\":[{\"name\":\"year\",\"op\":\"date_part\",\"column\":\"date\",\"part\":\"year\"},{\"name\":\"rev_per_unit\",\"op\":\"arith\",\"column\":\"revenue\",\"how\":\"div\",\"other\":\"units\"}]}" "add a year column and revenue per unit"
run medium compute_aggregations "{\"file_path\":\"$D/derived.csv\",\"group_by\":[\"year\"],\"agg_column\":\"rev_per_unit\",\"agg_func\":\"mean\"}" "average revenue per unit by the year I just derived"
run transform enrich_with_geo "{\"file_path\":\"$SALES\",\"geo_file_path\":\"$GEOJSON\",\"join_column\":\"region\",\"geo_join_column\":\"region\"}" "enrich sales with lat/lon"

echo
echo "===== data_visual (12 tools) ====="
run visual run_eda "{\"file_path\":\"$SALES\"}" "run full EDA on sales"
run visual generate_auto_profile "{\"file_path\":\"$SALES\"}" "auto-profile this dataset"
run visual generate_distribution_plot "{\"file_path\":\"$SALES\",\"columns\":[\"revenue\"]}" "plot the revenue distribution"
run visual generate_correlation_heatmap "{\"file_path\":\"$SALES\"}" "show a correlation heatmap"
run visual generate_pairwise_plot "{\"file_path\":\"$SALES\",\"columns\":[\"units\",\"revenue\"]}" "plot units vs revenue pairwise"
run visual generate_multi_chart "{\"file_path\":\"$SALES\",\"chart_type\":\"bar\",\"value_columns\":[\"revenue\"],\"category_column\":\"region\"}" "bar chart revenue by region"
CHART_R=$(call visual 200 generate_chart "{\"file_path\":\"$SALES\",\"chart_type\":\"bar\",\"value_column\":\"revenue\",\"category_column\":\"region\"}")
if ok_json "$CHART_R"; then pass "generate_chart rendered a real bar chart"; else fail "generate_chart -> $CHART_R"; fi
CHART_PATH=$(extract "$CHART_R" output_path)
run visual generate_geo_map "{\"file_path\":\"$GEO\",\"lat_column\":\"lat\",\"lon_column\":\"lon\",\"location_column\":\"region\"}" "map these regions by lat/lon"
run visual generate_3d_chart "{\"file_path\":\"$SALES\",\"chart_type\":\"scatter_3d\",\"x_column\":\"units\",\"y_column\":\"revenue\",\"z_column\":\"units\"}" "3D scatter of units/revenue/units"
DASH_R=$(call visual 200 generate_dashboard "{\"file_path\":\"$SALES\"}")
if ok_json "$DASH_R"; then pass "generate_dashboard built a dashboard"; else fail "generate_dashboard -> $DASH_R"; fi
DASH_PATH=$(extract "$DASH_R" output_path)
run visual export_data "{\"file_path\":\"$SALES\",\"output_path\":\"$D/sales_export.xlsx\",\"format\":\"excel\"}" "export sales to xlsx"
# Wave 2 taught this the expensive way: a parameter can be added to the engine,
# pass every unit test, and be unreachable over the wire because the wrapper on
# the serving tier never forwarded it. These four drive the new arguments
# through the deployed HTTP surface, which is the only place that shows.
WB_R=$(call visual 200 export_data "{\"file_path\":\"$SALES\",\"output_path\":\"$D/sales_preview.xlsx\",\"format\":\"excel\",\"preview_rows\":5}")
if echo "$WB_R" | grep -q '"README"'; then pass "export_data wrote a README sheet"; else fail "export_data workbook -> $WB_R"; fi
if echo "$WB_R" | grep -q '"full_csv_path"'; then pass "a preview workbook wrote the full CSV beside it"; else fail "preview_rows did not write the full CSV -> $WB_R"; fi
run visual generate_distribution_plot "{\"file_path\":\"$SALES\",\"columns\":[\"revenue\"],\"max_points\":50}" "plot revenue from at most 50 points"
MULTI_R=$(call visual 200 generate_dashboard "{\"file_path\":\"$SALES\",\"output_path\":\"$D/multi_dash.html\",\"sources\":[\"$D/sales_emea.csv\"]}")
if echo "$MULTI_R" | grep -q 'sales_emea.csv'; then pass "generate_dashboard added a second source as a tab"; else fail "sources -> $MULTI_R"; fi
if [ -n "$CHART_PATH" ]; then
  run visual customize_chart "{\"chart_path\":\"$CHART_PATH\",\"title\":\"Revenue by Region (customized)\"}" "retitle that chart"
else
  fail "customize_chart skipped — no chart_path captured from generate_chart"
fi
# The round-trip the spec exists for: read back what the page was built from,
# change one field, regenerate. Against the live server, so it also proves the
# spec survived being written into the HTML and parsed out of it again.
if [ -n "$DASH_PATH" ]; then
  run visual customize_dashboard "{\"dashboard_path\":\"$DASH_PATH\",\"changes\":{\"title\":\"Sales (customized)\"}}" "rename that dashboard"
else
  fail "customize_dashboard skipped — no output_path captured from generate_dashboard"
fi

echo
echo "===== data_workspace (6 tools) ====="
WS="smoke-test-ws"
docker exec "$CONTAINER" rm -rf "$D/$WS"
run workspace create_workspace "{\"name\":\"$WS\",\"base_dir\":\"$D\"}" "create a workspace for this analysis"
run workspace open_workspace "{\"name\":\"$WS\",\"base_dir\":\"$D\"}" "open that workspace"
run workspace register_workspace_file "{\"workspace_name\":\"$WS\",\"file_path\":\"$SALES\",\"alias\":\"sales\",\"base_dir\":\"$D\"}" "register sales.csv in the workspace"
run workspace list_workspace_files "{\"workspace_name\":\"$WS\",\"base_dir\":\"$D\"}" "what files are in the workspace?"
run workspace save_workspace_pipeline "{\"workspace_name\":\"$WS\",\"pipeline_name\":\"clean\",\"ops\":[{\"op\":\"drop_duplicates\"}],\"base_dir\":\"$D\"}" "save a cleaning pipeline"
run workspace run_workspace_pipeline "{\"workspace_name\":\"$WS\",\"pipeline_name\":\"clean\",\"input_alias\":\"sales\",\"output_alias\":\"sales_clean\",\"base_dir\":\"$D\"}" "run the cleaning pipeline"

echo
echo "===== data_ingest (10 tools) ====="
run ingest list_sheets "{\"file_path\":\"$XLSX\"}" "what sheets does this workbook have?"
run ingest extract_sheet "{\"file_path\":\"$XLSX\",\"sheet\":\"Sheet2\",\"output_path\":\"$D/sheet2.csv\"}" "extract Sheet2 to CSV"
run ingest extract_all_sheets "{\"file_path\":\"$XLSX\",\"output_dir\":\"$D/sheets\"}" "extract every sheet to CSV"
run ingest detect_tables "{\"file_path\":\"$XLSX\",\"sheet\":\"Sheet1\"}" "find tables inside Sheet1"
run ingest extract_table "{\"file_path\":\"$XLSX\",\"sheet\":\"Sheet1\",\"table_index\":0,\"output_path\":\"$D/table0.csv\"}" "extract the first table from Sheet1"
run ingest normalize_headers "{\"file_path\":\"$D/sheet2.csv\"}" "normalize the headers in sheet2.csv"
run ingest trim_empty "{\"file_path\":\"$D/sheet2.csv\"}" "trim empty rows/cols from sheet2.csv"
run ingest promote_header "{\"file_path\":\"$D/table0.csv\",\"row_index\":0}" "promote row 0 to the header in table0.csv"
run ingest flatten_merged_cells "{\"file_path\":\"$XLSX\",\"sheet\":\"Sheet1\",\"output_path\":\"$D/flattened.csv\"}" "flatten the merged cells in Sheet1"
run ingest convert_file "{\"file_path\":\"$SALES\",\"output_format\":\"excel\"}" "convert sales.csv to xlsx"

echo
echo "===== hybrid file exchange (remote-only behaviour) ====="
# Only meaningful against a deployment that sets MCP_OUTPUT_DIR /
# MCP_PUBLIC_BASE_URL / MCP_FETCH_URLS — i.e. exactly what pytest cannot
# check, since pytest never spins up a server or touches the network.
SHARED_DIR=$(docker exec "$CONTAINER" printenv MCP_OUTPUT_DIR 2>/dev/null || true)
if [ -z "$SHARED_DIR" ]; then
  echo "  SKIP: MCP_OUTPUT_DIR is unset on $CONTAINER — nothing to verify"
else
  echo "== prompt: \"chart revenue by region\" -> generate_chart (default output path) =="
  N=$((N+1))
  EX_R=$(call visual "$N" generate_chart "{\"file_path\":\"$SALES\",\"chart_type\":\"bar\",\"value_column\":\"revenue\",\"category_column\":\"region\"}")
  EX_PATH=$(extract "$EX_R" output_path)
  EX_URL=$(extract "$EX_R" public_url)
  case "$EX_PATH" in
    "$SHARED_DIR"/*) pass "default output landed in the shared dir ($EX_PATH)" ;;
    *) fail "default output went to $EX_PATH, expected it under $SHARED_DIR" ;;
  esac
  [ -n "$EX_URL" ] && pass "response carried public_url ($EX_URL)" || fail "no public_url in response"
  if docker exec "$CONTAINER" test -s "$EX_PATH"; then
    pass "the chart is a real non-empty file on disk, not just a success message"
  else
    fail "no file at $EX_PATH inside the container"
  fi

  echo "== prompt: \"analyze the dataset at <link>\" -> load_dataset with a URL =="
  N=$((N+1))
  URL_R=$(call basic "$N" load_dataset "{\"file_path\":\"https://math.casava.space/health\"}")
  # /health is public on every sibling MCP endpoint, so this exercises a real
  # outbound fetch over the real domain without needing a credentialled URL.
  # It serves JSON, and load_dataset only accepts .csv — so the wrong-type
  # rejection IS the proof: the server could only know the extension by
  # downloading the URL first.
  #
  # Deliberately a *sibling* host, not $DOMAIN/health. Fetching its own public
  # URL deadlocks: the tool call occupies the worker that would have to serve
  # the request, and the fetch dies on the read timeout.
  # Skipped rather than failed when the server does not fetch URLs, which is
  # how the CI container runs it. The target above is a sibling's public
  # /health, so requiring this would make every build depend on an external
  # host being up -- and a suite that goes red because someone else's box
  # restarted stops being read. The deployed run has MCP_FETCH_URLS=1 and
  # covers these three assertions for real.
  FETCHES_URLS=1
  if echo "$URL_R" | grep -q "does not fetch URLs"; then
    FETCHES_URLS=0
    echo "  SKIP: MCP_FETCH_URLS is not enabled on $CONTAINER"
  elif echo "$URL_R" | grep -qE 'Expected \\?\.csv|inbox|"success\\?":[[:space:]]*true'; then
    pass "a URL was accepted as a file_path and fetched server-side"
  else
    fail "URL input -> $URL_R"
  fi

  if [ "$FETCHES_URLS" = 1 ]; then
    INBOX=$(docker exec "$CONTAINER" sh -c "ls -1 '$SHARED_DIR/inbox' 2>/dev/null | head -3" || true)
    [ -n "$INBOX" ] && pass "fetched file landed in the inbox ($INBOX)" || fail "inbox is empty after a URL fetch"

    echo "== SSRF guard: a private address must be refused =="
    N=$((N+1))
    SSRF_R=$(call basic "$N" load_dataset '{"file_path":"http://169.254.169.254/latest/meta-data/"}')
    if echo "$SSRF_R" | grep -q "non-public address"; then
      pass "link-local metadata address refused"
    else
      fail "SSRF guard did not fire -> $SSRF_R"
    fi
  else
    echo "  SKIP: inbox and SSRF guard need URL fetching enabled"
  fi
fi

if [ -n "$SHARED_DIR" ]; then
  echo
  echo "== leave the shared directory as we found it =="
  docker exec "$CONTAINER" sh -c "ls -1A '$SHARED_DIR' 2>/dev/null" | sort \
    | comm -13 "$SHARED_BEFORE" - \
    | while IFS= read -r leftover; do
        [ -n "$leftover" ] && docker exec "$CONTAINER" rm -rf "$SHARED_DIR/$leftover"
      done
  pass "removed everything this run added to $SHARED_DIR"
fi
rm -f "$SHARED_BEFORE"

echo
if [ "$FAILS" -eq 0 ]; then
  echo "ALL 70 TOOLS PASSED against $DOMAIN"
else
  echo "$FAILS TOOL(S) FAILED against $DOMAIN"
  exit 1
fi
