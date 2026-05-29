#!/usr/bin/env bash
# check_metrics.sh
# Queries the Prometheus HTTP API and validates SLI/SLO thresholds.
# CI fails (exit 1) if any threshold is breached.
#
# Usage:
#   PROMETHEUS_URL=http://localhost:9090 bash scripts/check_metrics.sh

set -euo pipefail

PROMETHEUS_URL="${PROMETHEUS_URL:-http://localhost:9090}"
FAILURES=0

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# ── Helper ─────────────────────────────────────────────────────────────────────
query() {
  # Returns the scalar value of a PromQL instant query, or "0" if no data.
  local expr="$1"
  local encoded
  encoded=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$expr")
  python3 - "$PROMETHEUS_URL" "$encoded" << 'PYEOF'
import sys, urllib.request, json
url, expr = sys.argv[1], sys.argv[2]
try:
    with urllib.request.urlopen(f"{url}/api/v1/query?query={expr}", timeout=10) as r:
        data = json.load(r)
    results = data.get("data", {}).get("result", [])
    print(results[0]["value"][1] if results else "0")
except Exception as e:
    print("0", file=sys.stderr)
    print("0")
PYEOF
}

check() {
  local name="$1"
  local value="$2"
  local op="$3"       # lt | gt
  local threshold="$4"

  local pass
  if [ "$op" = "lt" ]; then
    pass=$(python3 -c "print('yes' if float('${value}') < float('${threshold}') else 'no')")
  else
    pass=$(python3 -c "print('yes' if float('${value}') > float('${threshold}') else 'no')")
  fi

  if [ "$pass" = "yes" ]; then
    echo -e "${GREEN}✅  PASS${NC}  ${name}: ${value} (threshold ${op} ${threshold})"
  else
    echo -e "${RED}❌  FAIL${NC}  ${name}: ${value} (threshold ${op} ${threshold})"
    FAILURES=$(( FAILURES + 1 ))
  fi
}

echo ""
echo "══════════════════════════════════════════"
echo "  Warehouse SLI / SLO Metric Validation"
echo "  Prometheus: ${PROMETHEUS_URL}"
echo "══════════════════════════════════════════"
echo ""

# ── SLI 1: API Availability — success rate > 99 % ──────────────────────────────
AVAIL=$(query 'sum(rate(wms_http_requests_total{status=~"2.."}[5m])) / (sum(rate(wms_http_requests_total[5m])) + 0.001)')
check "SLI-1 WMS availability (>99%)" "$AVAIL" "gt" "0.99"

# ── SLI 2: WMS p95 latency < 500 ms ───────────────────────────────────────────
P95=$(query 'histogram_quantile(0.95, sum(rate(wms_http_request_duration_seconds_bucket[5m])) by (le))')
check "SLI-2 WMS p95 latency (<0.5s)" "$P95" "lt" "0.5"

# ── SLI 3: Event processing delay p95 < 2 s ───────────────────────────────────
EP95=$(query 'histogram_quantile(0.95, sum(rate(event_processing_duration_seconds_bucket[5m])) by (le))')
check "SLI-3 Event processing p95 (<2s)" "$EP95" "lt" "2.0"

# ── Additional: Error rate < 1 % ───────────────────────────────────────────────
ERR=$(query 'sum(rate(wms_http_request_errors_total[5m])) / (sum(rate(wms_http_requests_total[5m])) + 0.001)')
check "Error rate (<1%)" "$ERR" "lt" "0.01"

# ── Additional: Consumer lag < 1000 ───────────────────────────────────────────
LAG=$(query 'max(consumer_lag)')
check "Consumer lag (<1000)" "$LAG" "lt" "1000"

echo ""
echo "══════════════════════════════════════════"
if [ "$FAILURES" -eq 0 ]; then
  echo -e "${GREEN}All ${#} checks passed.${NC}"
  exit 0
else
  echo -e "${RED}${FAILURES} check(s) failed — CI will fail.${NC}"
  exit 1
fi
