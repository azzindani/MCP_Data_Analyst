#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# MCP_Data_Analyst — remote smoke test.
#
# NOT part of pytest / CI (see CLAUDE.md "Remote smoke tests"). This script
# is the separate, manual/on-demand check that actually exercises the
# deployed HTTP endpoint: real auth enforcement + a real handwritten-prompt-
# style tool call on a real generated dataset, against the real public domain.
#
# Tools here read datasets by server-side file_path (not upload), so this
# script docker-cp's a small generated CSV into the running container first —
# only works run on the same host as the deployment (self-hosted, by design).
# docker cp preserves the source file's root ownership, which the
# container's non-root `app` user can't read — chown it after copying.
#
# Usage:
#   ./remote_smoke_test.sh                      # reads DA_API_KEY from .env
#   DA_API_KEY=sk-... ./remote_smoke_test.sh     # or pass it directly
#   DOMAIN=http://localhost:8810 ./remote_smoke_test.sh   # test a different target
#   CONTAINER=mcp-data-analyst ./remote_smoke_test.sh      # override container name
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

DOMAIN="${DOMAIN:-https://data.casava.space}"
CONTAINER="${CONTAINER:-mcp-data-analyst}"
if [ -f .env ]; then
  set -a; source .env; set +a
fi
KEY="${DA_API_KEY:?Set DA_API_KEY (env var or .env file) before running}"
DATASET_PATH="/tmp/remote-smoke-test/sales.csv"

pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1"; exit 1; }

echo "Target: $DOMAIN"
echo
echo "== seed a real dataset into the container =="
TMP_CSV=$(mktemp)
python3 -c "
import random
random.seed(7)
print('region,units,revenue')
regions = ['APAC', 'EMEA', 'AMER']
for _ in range(200):
    r = random.choice(regions)
    units = random.randint(1, 50)
    revenue = round(units * random.uniform(8, 15) + random.gauss(0, 5), 2)
    print(f'{r},{units},{revenue}')
" > "$TMP_CSV"
docker exec "$CONTAINER" mkdir -p /tmp/remote-smoke-test
docker cp "$TMP_CSV" "$CONTAINER:$DATASET_PATH"
rm -f "$TMP_CSV"
docker exec -u root "$CONTAINER" chown app:app "$DATASET_PATH"
pass "200-row synthetic sales dataset copied to $CONTAINER:$DATASET_PATH"

echo
echo "== auth enforcement =="

code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$DOMAIN/basic/mcp" \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"smoke","version":"1"}}}')
[ "$code" = "401" ] && pass "no token -> 401" || fail "no token -> expected 401, got $code"

SID=$(curl -s -i -X POST "$DOMAIN/statistics/mcp" \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -H "Authorization: Bearer $KEY" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"smoke","version":"1"}}}' \
  | grep -i mcp-session-id | tr -d '\r' | awk '{print $2}')
[ -n "$SID" ] && pass "valid token -> session established" || fail "valid token -> no session id returned"

curl -s -X POST "$DOMAIN/statistics/mcp" -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -H "Authorization: Bearer $KEY" -H "mcp-session-id: $SID" \
  -d '{"jsonrpc":"2.0","id":2,"method":"notifications/initialized"}' > /dev/null

echo
echo '== prompt: "how correlated are units and revenue in this dataset?" -> correlation_analysis =='
RESULT=$(curl -s -X POST "$DOMAIN/statistics/mcp" -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -H "Authorization: Bearer $KEY" -H "mcp-session-id: $SID" \
  -d "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\",\"params\":{\"name\":\"correlation_analysis\",\"arguments\":{\"file_path\":\"$DATASET_PATH\"}}}")
echo "$RESULT" | grep -q '"success":true' && pass "correlation_analysis computed real correlations from real data" || fail "unexpected result: $RESULT"

echo
echo "ALL CHECKS PASSED against $DOMAIN"
