# Cinema Analytics Pipeline (Kafka + ClickHouse + PG + Grafana)

This is a full event streaming pipeline for an online cinema – built for a homework assignment.  
It generates synthetic user actions, pushes them through Kafka (Avro + Schema Registry), stores raw events in ClickHouse, computes business metrics (DAU, retention, conversion, top movies, avg watch time) and pushes them into PostgreSQL, then exports aggregated data to MinIO (S3) daily. Everything runs via `docker compose up`.

## Quick Start

```bash
# Spin up the whole stack (background)
docker compose up -d --build

# Wait ~30 seconds for healthchecks, then check Grafana:
open http://localhost:3000  (admin/admin)

# Manually trigger aggregation for today (if you want to see numbers immediately)
make aggregate

# Run integration test (publishes one event and waits for ClickHouse)
make test

# See exported file in MinIO console: http://localhost:9002  (minioadmin/minioadmin)