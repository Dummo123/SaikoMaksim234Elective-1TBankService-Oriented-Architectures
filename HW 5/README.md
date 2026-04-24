# Online Cinema: Event Streaming + Analytics Pipeline

**Complete pipeline** for online cinema analytics using **Kafka (2 brokers, replication), Schema Registry (Avro), ClickHouse, PostgreSQL, Grafana, MinIO (S3)**.
Generates synthetic user events, ingests via Kafka, stores raw data in ClickHouse, computes business metrics (DAU, retention, conversion, top movies, avg watch time), pushes results to PostgreSQL, and exports daily aggregates to MinIO. Everything runs with `docker compose up`.

---

## Quick Start (one command)

```bash
# Start entire stack
docker compose up -d --build

# Wait ~30-60 seconds for healthchecks, then open:
# Grafana: http://localhost:3000 (admin/admin)
# Producer API docs: http://localhost:8000/docs
# Aggregation API docs: http://localhost:8001/docs
# Export API docs: http://localhost:8002/docs
# MinIO console: http://localhost:9002 (minioadmin/minioadmin)

# Manually trigger aggregation for today (if you want data immediately)
curl -X POST "http://localhost:8001/run?date=$(date -u +%F)"

# Run integration test (publishes one event and verifies it appears in ClickHouse)
docker compose run --rm test_runner
```

---

## Architecture Overview

```
Producer (HTTP API + Generator) → Kafka (2 brokers, topic raw-movie-events, 3 partitions, rep=2)
                              ↓ (Avro, Schema Registry)
ClickHouse (Kafka Engine → MergeTree → AggregatingMergeTree for DAU, views, avg time)
                              ↓
Aggregation Service (cron + manual) → PostgreSQL (metrics_daily, retention_stats, top_movies_daily)
                              ↓
Export Service (daily cron + manual) → MinIO (S3) daily/YYYY-MM-DD/aggregates.{csv|json|parquet}
```

---

## Services, Ports & Credentials

| Service | URL / Endpoint | Login |
|---|---|---|
| Producer (Swagger) | http://localhost:8000/docs | – |
| Aggregation API | http://localhost:8001/docs | – |
| Export API | http://localhost:8002/docs | – |
| Grafana | http://localhost:3000 | admin / admin |
| ClickHouse HTTP | http://localhost:8123 | default / (no password) |
| Schema Registry | http://localhost:8081 | – |
| MinIO S3 API | http://localhost:9001 | minioadmin / minioadmin |
| MinIO Console | http://localhost:9002 | minioadmin / minioadmin |
| PostgreSQL | localhost:5432 | metrics_user / secret_pass |

---

## How This Maps to the Assignment

### Block 1–4 points (basic infrastructure)

| # | Requirement | Implementation |
|---|---|---|
| 1 | **Kafka topic + Avro schema** | `producer/schemas/movie_event.avsc` – full Avro schema. Auto-registered in Schema Registry (`auto.register.schemas=true`). Topic `raw-movie-events` created with **3 partitions**, replication factor 2, `min.insync.replicas=1`. Partition key = `user_id` (preserves order per user session). |
| 2 | **Producer (HTTP + generator)** | FastAPI service: `POST /events` validates JSON (Pydantic + Avro) and sends to Kafka. Background generator simulates realistic sessions: `VIEW_STARTED → PAUSE/RESUME → FINISHED`, plus `LIKED`, `SEARCHED`. Producer uses `acks=all`, `enable.idempotence=true`, retries with exponential backoff, logs each publication. |
| 3 | **ClickHouse ingestion** | Kafka engine table `kafka_events_stream` reads Avro via Schema Registry. Materialized view moves data to `raw_events` (MergeTree, partitioned by date, sorted by `(event_date, event_type, user_id, event_time)`). Additionally, three `AggregatingMergeTree` tables + MV for DAU, movie views, avg watch time. |
| 4 | **Integration test** | `tests/test_pipeline.py` – publishes a unique event via producer, polls ClickHouse until it appears, validates all fields. Also tests invalid event rejection and session sequence. Run with `docker compose run --rm test_runner`. |

---

### Block 5–7 points (aggregation – all 3 points)

| Feature | Where / How |
|---|---|
| Separate container | `agg_service` in docker-compose, independent of producer. |
| Reads raw events from ClickHouse (not Kafka) | `aggregation/metrics.py` uses `clickhouse-connect` directly. |
| Cron scheduler (configurable via env) | `SCHEDULE_CRON="*/5 * * * *"` (default every 5 minutes) with `APScheduler`. |
| Manual HTTP trigger | `POST /run?date=YYYY-MM-DD` |
| Logging of cycles | Logs records processed and elapsed time. |
| DAU | `uniqMerge(uniq_users)` from `daily_dau_agg`. |
| Average watch time | `avgMerge(avg_sec)` from `avg_progress_agg` (only `VIEW_FINISHED`). |
| Top movies | `countMerge(view_cnt)` from `movie_views_agg`. |
| View conversion | `countIf(VIEW_FINISHED) / countIf(VIEW_STARTED)` from `raw_events`. |
| Retention D1 and D7 | Cohort analysis: first activity date → count users returning on day offset 1 and 7. Implemented with CTE, `dateDiff`, `countIf`. |
| Aggregate / window functions | Uses `uniqState/uniqMerge`, `countState/countMerge`, `avgState/avgMerge`, `countIf`, `dateDiff`. No plain `count(*)` for metrics. |
| Materialized views in ClickHouse | Three `AggregatingMergeTree` tables + corresponding materialized views. |
| Idempotent UPSERT in PostgreSQL | `metrics_daily` has `UNIQUE (metric_date, metric_name, extra)` → `ON CONFLICT DO UPDATE`. `retention_stats` uses primary key upsert. `top_movies_daily` uses `DELETE + INSERT` in a transaction. |
| Error handling & retry | `tenacity` decorator with `stop_after_attempt(3)` and exponential backoff on all PG writes. |

---

### Block 8–10 points (visualization, export, fault tolerance)

| # | Requirement | Implementation |
|---|---|---|
| 6 | **Grafana dashboard** | Auto-provisioned datasource (`ch_datasource`) and dashboard `movie_analytics.json`. Panels: **Retention Cohort Heatmap** (mandatory, with color thresholds), DAU (timeseries), View Conversion (stat), Top 10 Movies (barchart), Device Distribution (piechart), Average Watch Time (timeseries). Uses both raw and aggregated tables. |
| 7 | **Export to S3 (MinIO)** | `export_service` container: daily cron (01:00 UTC) + manual `POST /run?date=...&format=...`. Supports CSV, JSON, Parquet (configurable). Files stored as `s3://movie-analytics/daily/YYYY-MM-DD/aggregates.{format}`. Overwrites existing (idempotent). Retries on failures. |
| 8 | **Fault-tolerant Kafka** | Two Kafka brokers in compose. Topic created with `--replication-factor 2` and `min.insync.replicas=1`. Schema Registry uses `SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR=2`. Healthchecks defined for all components: `kafka-broker-api-versions`, `curl /subjects`, `pg_isready`, `wget /ping`, `curl /health` on custom services. |

---

## Useful Commands (Manual Demo)

```bash
# Check events in ClickHouse
docker exec clickhouse_db clickhouse-client --query "SELECT event_date, event_type, count() FROM movie_analytics.raw_events GROUP BY event_date, event_type ORDER BY event_date DESC LIMIT 20"

# Force aggregation for a specific date (e.g., today)
curl -X POST "http://localhost:8001/run?date=$(date -u +%F)"

# See aggregated metrics in PostgreSQL
docker exec postgres_analytics psql -U metrics_user -d metrics_store -c "SELECT metric_date, metric_name, metric_value FROM metrics_daily ORDER BY metric_date DESC, metric_name LIMIT 30"

# Export today's aggregates to MinIO (Parquet)
curl -X POST "http://localhost:8002/run?date=$(date -u +%F)&format=parquet"

# Send a custom test event manually
curl -X POST http://localhost:8000/events -H "Content-Type: application/json" -d '{"user_id":"alice","movie_id":"inception","event_type":"VIEW_STARTED","device_type":"DESKTOP","session_id":"sess123","progress_seconds":0}'

# Stream producer logs (see synthetic events)
docker compose logs -f event_producer
```

---

## Project Structure

```
task5/
├── docker-compose.yml
├── README.md                          (this file)
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py
│   ├── kafka_producer.py
│   ├── generator.py
│   └── schemas/movie_event.avsc
├── clickhouse/init/01_schema.sql
├── postgres/init/01_schema.sql
├── aggregation/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py
│   ├── metrics.py
│   └── db.py
├── export_service/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
├── tests/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── test_pipeline.py
└── grafana/
    ├── provisioning/
    │   ├── datasources/clickhouse.yml
    │   └── dashboards/dashboards.yml
    └── dashboards/movie_analytics.json
```

---

## Troubleshooting

- **Grafana doesn't see ClickHouse** – the plugin installs on first start. If not, restart `grafana_ui`: `docker compose restart grafana_ui`.
- **ClickHouse Kafka engine not consuming** – check `SELECT * FROM system.kafka_consumers`. Sometimes you need to `DETACH TABLE kafka_events_stream; ATTACH TABLE kafka_events_stream;`.
- **Aggregation returns no data** – by default it computes for *yesterday* UTC. Use the manual `/run` endpoint with today's date.
- **Integration test timeout** – wait ~60 seconds for all healthchecks to pass, then run `docker compose run --rm test_runner`.

---