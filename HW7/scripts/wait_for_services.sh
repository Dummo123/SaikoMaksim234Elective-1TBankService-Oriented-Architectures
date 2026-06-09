#!/usr/bin/env bash
# wait_for_services.sh
# Polls each service until healthy or until MAX_WAIT seconds have elapsed.
# Exit code 0 = all services ready; 1 = timeout.

set -euo pipefail

MAX_WAIT=${MAX_WAIT:-300}   # seconds
INTERVAL=5

WMS_URL="${WMS_URL:-http://localhost:8000}"
CONSUMER_URL="${CONSUMER_URL:-http://localhost:8001}"

wait_for() {
  local name="$1"
  local url="$2"
  local elapsed=0

  echo "⏳  Waiting for ${name} at ${url} ..."
  until curl -sf "${url}" -o /dev/null 2>/dev/null; do
    if [ "${elapsed}" -ge "${MAX_WAIT}" ]; then
      echo "❌  Timeout waiting for ${name} after ${MAX_WAIT}s"
      exit 1
    fi
    sleep "${INTERVAL}"
    elapsed=$(( elapsed + INTERVAL ))
    echo "   ... still waiting for ${name} (${elapsed}s elapsed)"
  done
  echo "✅  ${name} is ready (${elapsed}s)"
}

# Wait for infrastructure via service health endpoints
wait_for "WMS Service"      "${WMS_URL}/health"
wait_for "Consumer Service" "${CONSUMER_URL}/health"

echo ""
echo "🚀  All services are ready."
