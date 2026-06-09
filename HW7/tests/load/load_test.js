/**
 * k6 load test for the Warehouse WMS service.
 *
 * Scenarios:
 *   - default : steady 10 VUs for 30 s (warm-up + sustained)
 *   - spike   : ramp to 30 VUs for 10 s then back down
 *
 * Thresholds (CI fails if any is breached):
 *   - p(95) request duration < 500 ms
 *   - error rate             < 1 %
 *   - health check always 200
 *
 * Run locally:
 *   k6 run tests/load/load_test.js
 *
 * Run in CI (results saved to file):
 *   k6 run --out json=tests/load/results/results.json tests/load/load_test.js
 */
import http from "k6/http";
import { check, sleep, group } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

// ── Custom metrics ────────────────────────────────────────────────────────────
const eventsSent   = new Counter("warehouse_events_sent");
const dlqEvents    = new Counter("warehouse_dlq_events");
const healthOk     = new Rate("warehouse_health_ok");
const eventLatency = new Trend("warehouse_event_latency", true);

// ── Options ───────────────────────────────────────────────────────────────────
export const options = {
  scenarios: {
    sustained_load: {
      executor: "constant-vus",
      vus: 10,
      duration: "30s",
      tags: { scenario: "sustained" },
    },
    spike: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "5s",  target: 30 },
        { duration: "10s", target: 30 },
        { duration: "5s",  target: 0  },
      ],
      startTime: "35s",   // starts after sustained_load finishes
      tags: { scenario: "spike" },
    },
  },
  thresholds: {
    // Overall p95 must stay under 500 ms
    http_req_duration:        ["p(95)<500"],
    // Error rate must stay under 1 %
    http_req_failed:          ["rate<0.01"],
    // All health checks must return 200
    warehouse_health_ok:      ["rate==1"],
    // Event endpoint p95 under 400 ms
    warehouse_event_latency:  ["p(95)<400"],
  },
};

const BASE_URL = __ENV.WMS_URL || "http://localhost:8000";

// ── Helpers ───────────────────────────────────────────────────────────────────
function uid() {
  return `${__VU}-${__ITER}-${Date.now()}`;
}

function sendEvent(eventType, extra = {}) {
  const payload = JSON.stringify({
    event_id:        `load-${uid()}`,
    event_type:      eventType,
    event_timestamp: Date.now(),
    product_id:      `SKU-LOAD-${__VU}`,
    quantity:        10,
    zone_id:         "ZONE-A",
    ...extra,
  });

  const start = Date.now();
  const res = http.post(`${BASE_URL}/events`, payload, {
    headers: { "Content-Type": "application/json" },
    tags:    { endpoint: "/events" },
  });
  eventLatency.add(Date.now() - start);

  const ok = check(res, {
    "event accepted (200)": (r) => r.status === 200,
    "response has event_id": (r) => {
      try { return JSON.parse(r.body).event_id !== undefined; } catch { return false; }
    },
  });
  if (ok) eventsSent.add(1);
  return res;
}

// ── Default function (one iteration per VU) ────────────────────────────────────
export default function () {
  // 1. Health check
  group("health", () => {
    const r = http.get(`${BASE_URL}/health`);
    const ok = check(r, { "health 200": (res) => res.status === 200 });
    healthOk.add(ok ? 1 : 0);
  });

  // 2. Receive product
  group("receive", () => {
    sendEvent("PRODUCT_RECEIVED", { quantity: 100 });
  });

  // 3. Reserve some stock
  group("reserve", () => {
    sendEvent("PRODUCT_RESERVED", { quantity: 5 });
  });

  // 4. Release the reservation
  group("release", () => {
    sendEvent("PRODUCT_RELEASED", { quantity: 5 });
  });

  // 5. Ship some stock
  group("ship", () => {
    sendEvent("PRODUCT_SHIPPED", { quantity: 10 });
  });

  // 6. Metrics scrape (lightweight)
  group("metrics", () => {
    const r = http.get(`${BASE_URL}/metrics`, { tags: { endpoint: "/metrics" } });
    check(r, { "metrics 200": (res) => res.status === 200 });
  });

  sleep(1);
}

// ── Summary hook — printed at end of run ─────────────────────────────────────
export function handleSummary(data) {
  const summary = {
    timestamp:   new Date().toISOString(),
    thresholds:  {},
    metrics: {
      p95_duration_ms: data.metrics.http_req_duration
        ? data.metrics.http_req_duration.values["p(95)"]
        : null,
      error_rate: data.metrics.http_req_failed
        ? data.metrics.http_req_failed.values.rate
        : null,
      events_sent: data.metrics.warehouse_events_sent
        ? data.metrics.warehouse_events_sent.values.count
        : 0,
    },
  };

  // Collect threshold pass/fail
  for (const [name, threshold] of Object.entries(data.thresholds || {})) {
    summary.thresholds[name] = threshold.ok ? "PASS" : "FAIL";
  }

  const output = JSON.stringify(summary, null, 2);
  console.log("=== Load Test Summary ===");
  console.log(output);

  // Return files to write
  return {
    "tests/load/results/summary.json": output,
    stdout: "\n" + output + "\n",
  };
}
