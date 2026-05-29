# Smart Warehouse — HW7: CI/CD, Testing & Observability

Builds on the event-driven warehouse system from HW6 by adding a full CI
pipeline, three tiers of automated tests, Prometheus/Grafana observability,
Alertmanager alert rules, k6 load testing, and SLI/SLO definitions.

---

## Table of Contents
1. [Architecture overview](#architecture-overview)
2. [Repository layout](#repository-layout)
3. [Quick start (Docker)](#quick-start-docker)
4. [Running tests locally](#running-tests-locally)
5. [CI pipeline](#ci-pipeline)
6. [Metrics & dashboards](#metrics--dashboards)
7. [Alert rules](#alert-rules)
8. [Load testing](#load-testing)
9. [SLI / SLO](#sli--slo)
10. [Schema evolution quick reference](#schema-evolution-quick-reference)
11. [Cassandra data-model rationale](#cassandra-data-model-rationale)
12. [PyCharm / local development](#pycharm--local-development)
13. [Troubleshooting](#troubleshooting)

---

## Architecture overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                         GitHub Actions CI                            │
│  unit-tests → build → integration-e2e → load-and-metrics            │
└──────────────┬───────────────────────────────────────────────────────┘
               │ docker compose up
               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  WMS Service :8000          Consumer Service :8001                  │
│  (FastAPI producer)  Kafka  (FastAPI + Kafka consumer)              │
│       │               │              │                              │
│       └───────────────┘              │                              │
│             /metrics                 │ /metrics                     │
│                                      ▼                              │
│                              Cassandra cluster                      │
│                              (3-node, RF=3)                         │
│                                                                     │
│  kafka-exporter :9308    cassandra-exporter :9500                   │
│          │                        │                                 │
│          └──────────┬─────────────┘                                 │
│                     ▼                                               │
│             Prometheus :9090  ──►  Alertmanager :9093               │
│                     │                                               │
│                     ▼                                               │
│             Grafana :3000                                           │
│                                                                     │
│             k6 load tests  →  scripts/check_metrics.sh             │
└─────────────────────────────────────────────────────────────────────┘
```

### Added components (HW7)

| Component | Purpose | HW7 point |
|---|---|---|
| `.github/workflows/ci.yml` | 4-job CI pipeline | 1 |
| `consumer_service/tests/` | Unit tests for all event handlers | 1, 2 |
| `wms_service/tests/` | Unit tests for Pydantic models | 1, 2 |
| `tests/integration/` | Cross-service tests (WMS→Kafka→Cassandra) | 2 |
| `tests/e2e/` | Full user-scenario tests | 3 |
| Prometheus middleware in both services | HTTP metrics | 4 |
| `grafana/dashboards/warehouse_services.json` | Service dashboard | 5 |
| `grafana/dashboards/infrastructure.json` | Infra dashboard | 6 |
| `kafka-exporter` | Kafka broker & topic metrics | 6 |
| `cassandra_exporter/` | Cassandra reachability & table metrics | 6 |
| `tests/load/load_test.js` | k6 load test with thresholds | 7 |
| `scripts/check_metrics.sh` | Post-load Prometheus validation | 8 |
| `prometheus/alerts.yml` | 8 alert rules | 9 |
| `alertmanager/alertmanager.yml` | Alert routing | 9 |
| `slo/sli_slo.yml` | SLI/SLO definitions with PromQL | 10 |

---

## Repository layout

```
warehouse-hw7/
├── .github/workflows/ci.yml           # GitHub Actions pipeline
├── cassandra/schema.cql               # Cassandra DDL (auto-applied at start)
├── schemas/
│   ├── warehouse_event_v1.avsc        # Avro schema v1
│   └── warehouse_event_v2.avsc        # Avro schema v2 (adds supplier_id)
│
├── wms_service/                       # Event producer (FastAPI :8000)
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py                        # + Prometheus middleware
│   └── tests/
│       └── test_unit_wms.py
│
├── consumer_service/                  # Kafka consumer (FastAPI :8001)
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py
│   ├── handlers.py                    # Business logic
│   ├── cassandra_client.py
│   ├── metrics.py                     # Kafka + HTTP metrics
│   └── tests/
│       └── test_unit_handlers.py      # 30+ unit tests
│
├── cassandra_exporter/                # Custom Prometheus exporter :9500
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
│
├── tests/
│   ├── integration/test_integration.py   # Requires running stack
│   ├── e2e/test_e2e.py                   # Full scenario tests
│   └── load/load_test.js                 # k6 load test
│
├── prometheus/
│   ├── prometheus.yml                 # Scrape config + alert rule reference
│   └── alerts.yml                     # 8 alert rules
│
├── alertmanager/alertmanager.yml
│
├── grafana/
│   ├── provisioning/                  # Auto-provisioned on startup
│   └── dashboards/
│       ├── warehouse_services.json    # 10-panel service dashboard
│       └── infrastructure.json       # 9-panel infrastructure dashboard
│
├── scripts/
│   ├── wait_for_services.sh           # Used by CI before tests
│   └── check_metrics.sh              # SLO validation against Prometheus
│
├── slo/sli_slo.yml                    # SLI/SLO definitions
├── docker-compose.yml                 # Full production-like stack
└── docker-compose.ci.yml             # CI override (single Cassandra node)
```

---

## Quick start (Docker)

**Prerequisites:** Docker Desktop (≥ 6 GB RAM), ports 2181, 3000, 8000, 8001,
8081, 9042, 9090, 9092, 9093, 9308, 9500 free.

```bash
# 1. Start everything
docker compose up --build

# 2. Verify health (wait ~3 min for Cassandra)
curl http://localhost:8001/health
# → {"status":"ok"}

# 3. Run the demo scenario
curl -X POST http://localhost:8000/scenario/basic-cycle
```

### Service URLs

| Service | URL |
|---|---|
| WMS API (Swagger) | http://localhost:8000/docs |
| Consumer health | http://localhost:8001/health |
| Consumer metrics | http://localhost:8001/metrics |
| WMS metrics | http://localhost:8000/metrics |
| Prometheus | http://localhost:9090 |
| Alertmanager | http://localhost:9093 |
| Grafana | http://localhost:3000 (admin / admin) |
| Schema Registry | http://localhost:8081/subjects |
| Kafka Exporter | http://localhost:9308/metrics |
| Cassandra Exporter | http://localhost:9500/metrics |

---

## Running tests locally

### Unit tests (no Docker needed)

```bash
# Consumer service
cd consumer_service
pip install -r requirements.txt -r tests/requirements.txt
pytest tests/test_unit_handlers.py -v

# WMS service
cd wms_service
pip install -r requirements.txt -r tests/requirements.txt
pytest tests/test_unit_wms.py -v
```

The consumer unit tests cover **30+ cases** across all 8 event types:
correct state transitions, staleness guard, negative-quantity rejection,
idempotency, unknown event type, order creation and completion.

### Integration tests (requires running stack)

```bash
docker compose up -d --build
bash scripts/wait_for_services.sh

pip install -r tests/integration/requirements.txt
CASSANDRA_HOSTS=localhost pytest tests/integration/test_integration.py -v
```

Integration tests verify the full cross-service path: WMS API → Kafka topic →
consumer → Cassandra. They cover:
- Health endpoint correctness
- Event reaches Cassandra after processing
- All three denormalized tables updated atomically
- Duplicate event not double-counted (idempotency)
- Bad event goes to DLQ; consumer stays alive
- Good event processed after DLQ event (no consumer stall)
- V1/V2 schema coexistence

### E2E tests (requires running stack)

```bash
pip install -r tests/e2e/requirements.txt
CASSANDRA_HOSTS=localhost pytest tests/e2e/test_e2e.py -v -s
```

E2E tests reproduce the HW6 showcase scenarios programmatically and assert
exact Cassandra state at every step:

1. **Full lifecycle** — RECEIVE → RESERVE → MOVE → SHIP → ORDER_CREATED → ORDER_COMPLETED
2. **Idempotency** — same event_id sent twice → available stays unchanged
3. **Out-of-order** — stale timestamp event silently dropped
4. **Cross-table consistency** — all three tables show the same value
5. **Schema evolution** — V1 stores `supplier_id=null`, V2 stores actual value

### Load tests

```bash
# Install k6: https://k6.io/docs/getting-started/installation/
docker compose up -d --build
bash scripts/wait_for_services.sh

k6 run --env WMS_URL=http://localhost:8000 tests/load/load_test.js
```

---

## CI pipeline

Defined in `.github/workflows/ci.yml`. Triggers on every push and PR.

```
unit-tests (no Docker)
      │
      ▼
    build (Docker images)
      │
      ▼
integration-e2e (docker compose CI profile)
      │
      ▼
load-and-metrics (docker compose + k6 + check_metrics.sh)
```

Each job uses `needs:` so a failure in any earlier job stops the pipeline
immediately. Every job uses `if: always()` teardown to ensure containers are
removed even on failure.

### CI-specific compose override

`docker-compose.ci.yml` overrides the full stack with:
- **Single Cassandra node** (512 MB heap) — GitHub Actions runners have 7 GB RAM;
  three full Cassandra nodes need ~6 GB leaving nothing for the JVM-heavy Kafka.
- Alertmanager and Grafana disabled (not needed for test correctness).
- Consumer points at `cassandra-1` only (driver discovers nothing else).

The production `docker-compose.yml` runs the full 3-node cluster and is used
for local development and the HW showcase.

### Artifacts saved by CI

| Artifact | Contents |
|---|---|
| `unit-test-results` | JUnit XML from pytest |
| `integration-e2e-results` | JUnit XML + service logs on failure |
| `load-test-results` | `summary.json` with threshold pass/fail |
| `service-logs-load` | Docker logs on load-job failure |

---

## Metrics & dashboards

### Metrics exposed per service

Both services expose `/metrics` in Prometheus text format.

#### WMS Service (`wms_*` prefix)

| Metric | Type | Labels | Description |
|---|---|---|---|
| `wms_http_requests_total` | Counter | method, endpoint, status | All HTTP requests |
| `wms_http_request_errors_total` | Counter | method, endpoint, error_type | 4xx/5xx responses |
| `wms_http_request_duration_seconds` | Histogram | method, endpoint | Response latency |
| `wms_events_produced_total` | Counter | event_type, version | Events sent to Kafka |

#### Consumer Service

| Metric | Type | Labels | Description |
|---|---|---|---|
| `http_requests_total` | Counter | method, endpoint, status | HTTP requests to /health and /metrics |
| `http_request_errors_total` | Counter | method, endpoint, error_type | HTTP errors |
| `http_request_duration_seconds` | Histogram | method, endpoint | HTTP latency |
| `events_processed_total` | Counter | event_type | Successfully committed events |
| `events_dlq_total` | Counter | — | Events routed to DLQ |
| `event_processing_duration_seconds` | Histogram | event_type | Kafka→Cassandra latency |
| `cassandra_write_errors_total` | Counter | — | Cassandra batch failures |
| `consumer_lag` | Gauge | partition | Messages behind HEAD |

#### Cassandra Exporter (`cassandra_*`)

| Metric | Type | Description |
|---|---|---|
| `cassandra_up` | Gauge | 1 if cluster reachable |
| `cassandra_cluster_nodes_total` | Gauge | Visible node count |
| `cassandra_read_latency_ms` | Gauge | Round-trip to system.local |
| `cassandra_inventory_records_total` | Gauge | Rows in inventory_by_product_zone |
| `cassandra_processed_events_stored` | Gauge | Rows in processed_events (TTL) |
| `cassandra_orders_total` | Gauge | Rows in orders |

#### Kafka Exporter (`kafka_*`)

Provided by `danielqsj/kafka-exporter`. Key metrics:
- `kafka_brokers` — number of available brokers
- `kafka_topic_partition_current_offset` — current offset per partition
- `kafka_consumergroup_current_offset` — committed offset per consumer group
- `kafka_consumergroup_lag` — lag per consumer group / partition

### Grafana dashboards

Two dashboards are provisioned automatically at startup (no manual import needed):

**Warehouse — Services** (`grafana/dashboards/warehouse_services.json`)
10 panels: WMS throughput, WMS latency percentiles (p50/p95/p99), WMS error rate,
consumer events/s, event processing latency (p50/p95/p99), consumer lag by partition,
stat panels for total events/DLQ/Cassandra errors/WMS p95.

**Warehouse — Infrastructure** (`grafana/dashboards/infrastructure.json`)
9 panels: Kafka consumer lag, events produced rate, broker count, partition offsets,
Cassandra read latency, Cassandra table record counts, Cassandra up/node-count stats,
Cassandra write error rate.

Open Grafana at http://localhost:3000 (admin / admin) → Dashboards → Warehouse.

---

## Alert rules

Defined in `prometheus/alerts.yml`, loaded by Prometheus automatically.
View firing alerts at http://localhost:9090/alerts or http://localhost:9093
(Alertmanager UI).

| Alert | Condition | For | Severity |
|---|---|---|---|
| `HighErrorRate` | WMS error rate > 5 % | 5m | critical |
| `ConsumerHighErrorRate` | Consumer error rate > 5 % | 5m | critical |
| `HighLatencyWMS` | WMS p95 > 1 s | 5m | warning |
| `HighEventProcessingLatency` | Event p95 > 2 s | 5m | warning |
| `HighConsumerLag` | Any partition lag > 1000 | 5m | warning |
| `CriticalConsumerLag` | Any partition lag > 5000 | 2m | critical |
| `ServiceDown` | Prometheus target unreachable | 1m | critical |
| `CassandraWriteErrors` | Write error rate > 0.1/s | 3m | critical |
| `DLQSpike` | DLQ rate > 0.5/s | 2m | warning |
| `CassandraDown` | `cassandra_up == 0` | 1m | critical |
| `KafkaBrokerDown` | `kafka_brokers < 1` | 1m | critical |

### Triggering an alert manually (demo)

```bash
# 1. Stop the consumer to build lag
docker compose stop consumer-service

# 2. Send events — lag grows
for i in $(seq 1 20); do
  curl -s -X POST http://localhost:8000/events \
    -H 'Content-Type: application/json' \
    -d "{\"event_id\":\"lag-$i\",\"event_type\":\"PRODUCT_RECEIVED\",
         \"event_timestamp\":$(date +%s000),\"product_id\":\"SKU-LAG\",
         \"quantity\":1,\"zone_id\":\"ZONE-A\"}" > /dev/null
done

# 3. Wait ~1 min — HighConsumerLag fires in Prometheus/Alertmanager UI

# 4. Restart consumer — lag drains, alert resolves
docker compose start consumer-service
```

---

## Load testing

`tests/load/load_test.js` uses k6 with two scenarios:

| Scenario | VUs | Duration | Start |
|---|---|---|---|
| `sustained_load` | 10 | 30 s | 0 s |
| `spike` | 0→30→0 | 20 s total | 35 s |

### Thresholds (CI fails if breached)

| Threshold | Value |
|---|---|
| `http_req_duration` p95 | < 500 ms |
| `http_req_failed` rate | < 1 % |
| `warehouse_health_ok` rate | = 100 % |
| `warehouse_event_latency` p95 | < 400 ms |

Each VU runs: health-check → PRODUCT_RECEIVED → PRODUCT_RESERVED →
PRODUCT_RELEASED → PRODUCT_SHIPPED → metrics scrape, then sleeps 1 s.

Results are written to `tests/load/results/summary.json` and saved as a
CI artifact.

---

## SLI / SLO

Full definitions with PromQL and rationale are in `slo/sli_slo.yml`.

| SLI | PromQL (abbreviated) | SLO target | Failure threshold |
|---|---|---|---|
| API Availability | `rate(wms_http_requests_total{status=~"2.."}[5m]) / rate(total[5m])` | > 99.5 % | < 95 % |
| WMS p95 Latency | `histogram_quantile(0.95, rate(wms_..._bucket[5m]))` | < 500 ms | > 1 s |
| Event Processing p95 | `histogram_quantile(0.95, rate(event_processing_..._bucket[5m]))` | < 2 s | > 5 s |

`scripts/check_metrics.sh` queries Prometheus after the load test and exits 1
if any failure threshold is breached, failing the `load-and-metrics` CI job.

---

## Schema evolution quick reference

Both schemas are registered at startup under `warehouse-events-value`.

### Adding V3 (new field)

```bash
# 1. Copy v2, add field with null default
cp schemas/warehouse_event_v2.avsc schemas/warehouse_event_v3.avsc
# Add: {"name": "destination_country", "type": ["null", "string"], "default": null}

# 2. Test backward compatibility
curl -X POST http://localhost:8081/compatibility/subjects/warehouse-events-value/versions/latest \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d "{\"schema\": $(cat schemas/warehouse_event_v3.avsc | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')}"
# → {"is_compatible":true}

# 3. Register
curl -X POST http://localhost:8081/subjects/warehouse-events-value/versions \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d "{\"schema\": $(cat schemas/warehouse_event_v3.avsc | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')}"

# 4. Add column to Cassandra
docker exec -it cassandra-1 cqlsh -e \
  "ALTER TABLE warehouse.inventory_by_product_zone ADD destination_country TEXT;"

# 5. Read in handler: event.get("destination_country")  # → None for V1/V2
```

### Compatibility strategy
`BACKWARD` — new consumers can read old messages (V2 consumer reads V1 messages
because supplier_id has a null default). Old consumers cannot read new messages,
but since we only ever run the latest consumer, this is acceptable.

---

## Cassandra data-model rationale

Tables are designed **query-first**, not normalized:

| Table | Partition key | Clustering key | Query |
|---|---|---|---|
| `inventory_by_product_zone` | `product_id` | `zone_id` | Stock of product in a specific zone |
| `inventory_by_product` | `product_id` | — | Total stock across all zones |
| `inventory_by_zone` | `zone_id` | `product_id` | All products in a zone |
| `event_history` | `product_id` | `event_timestamp DESC` | Audit trail, newest first |
| `processed_events` | `event_id` | — | Idempotency check (TTL 7 days) |
| `orders` | `order_id` | — | Order status lookup |

**Why denormalize?** Cassandra has no JOINs. All three inventory tables are
updated atomically in a LOGGED BATCH — either all succeed or none do,
preventing partial state.

**Consistency levels:**
- Writes: `QUORUM` — majority of replicas acknowledge; tolerates 1 node failure.
- Idempotency reads: `QUORUM` — must not miss a recent write that landed on a replica we don't read.
- Regular reads: `ONE` — lowest latency; acceptable because the consumer is the single writer, so stale reads from a lagging replica don't cause correctness issues (the timestamp guard prevents double-application).

---

## PyCharm / local development

Run Kafka, Cassandra, Schema Registry, Prometheus in Docker; run the two Python
services on the host with breakpoints.

```bash
# Start infrastructure only
docker compose up zookeeper kafka schema-registry kafka-setup \
  cassandra-1 cassandra-2 cassandra-3 cassandra-init \
  kafka-exporter cassandra-exporter prometheus grafana
```

**Consumer service run config:**

| Field | Value |
|---|---|
| Module | `uvicorn` |
| Parameters | `main:app --host 0.0.0.0 --port 8001 --reload` |
| Working dir | `<project>/consumer_service` |
| Env vars | `KAFKA_BOOTSTRAP=localhost:29092 SCHEMA_REGISTRY_URL=http://localhost:8081 CASSANDRA_CONTACT_POINTS=localhost` |

**WMS service run config:**

| Field | Value |
|---|---|
| Module | `uvicorn` |
| Parameters | `main:app --host 0.0.0.0 --port 8000 --reload` |
| Working dir | `<project>/wms_service` |
| Env vars | `KAFKA_BOOTSTRAP=localhost:29092 SCHEMA_REGISTRY_URL=http://localhost:8081` |

> **Note:** Kafka is accessible from the host on port `29092`
> (`PLAINTEXT_HOST` listener). Cassandra port `9042` is exposed on `cassandra-1`.
> The driver discovers the other two nodes automatically via gossip.

---

## Troubleshooting

### Cassandra takes too long to start
Normal. Each node needs 60–90 s. The `cassandra-init` container waits for all
three to be healthy before applying the schema. Total cold-start time: ~5 min.

```bash
# Watch healthcheck status
watch 'docker ps --format "table {{.Names}}\t{{.Status}}"'
```

### consumer-service crashes at startup
Usually means Cassandra schema hasn't been applied yet. Check:
```bash
docker logs cassandra-init
# Should end with: "Schema applied successfully"
```
If it failed, re-run: `docker compose restart cassandra-init`

### Unit tests fail with import errors
Make sure you're running pytest from the correct working directory and that
`sys.path` includes the service root (the test files do this automatically
with `sys.path.insert(0, ...)`).

### k6 thresholds fail locally
Usually means the service is still warm. Wait 30 s after startup, then
re-run the load test. The `sustained_load` scenario is designed for a
warmed-up WMS + consumer; Schema Registry and Avro serialization are slow
on first use.

### Alert not firing in Prometheus
Check `http://localhost:9090/rules` — if the rule shows `error` the PromQL
expression returned no data (e.g. no traffic yet). Send some events first:
```bash
curl -X POST http://localhost:8000/scenario/basic-cycle
```
Then wait for the `for:` duration to elapse.

### DLQ messages
```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic warehouse-events-dlq \
  --from-beginning --max-messages 10
```
