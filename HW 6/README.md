# Smart Warehouse — Event-Driven State Management with Kafka & Cassandra

Event-driven warehouse inventory system. A WMS producer publishes typed Avro events to Kafka; a stateful consumer reads them, applies business logic, and keeps a denormalized Cassandra cluster in sync. The whole stack spins up with a single `docker compose up`.

---

## Table of Contents

- [Architecture](#architecture)
- [Project Layout](#project-layout)
- [Quick Start](#quick-start)
- [Data Model](#data-model)
- [Event Semantics](#event-semantics)
- [Reliability Guarantees](#reliability-guarantees)
- [Schema Evolution](#schema-evolution)
- [Monitoring](#monitoring)
- [Consistency Level Decisions](#consistency-level-decisions)
- [Scenarios](#scenarios)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  WMS Service  (FastAPI, port 8000)                              │
│  POST /events      → Avro V1                                    │
│  POST /events/v2   → Avro V2  (+ supplier_id)                  │
│  POST /scenario/{name}  → fires canned demo sequences          │
└───────────────────────┬─────────────────────────────────────────┘
                        │  Avro over Schema Registry
                        ▼
             ┌──────────────────┐
             │  Kafka           │
             │  warehouse-events│  3 partitions
             │  warehouse-events│
             │      -dlq        │  1 partition (dead letters)
             └────────┬─────────┘
                      │  consumer group: warehouse-state-consumer
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│  Consumer Service  (FastAPI, port 8001)                         │
│  ├── Avro deserializer (Schema Registry)                        │
│  ├── Idempotency check (processed_events, QUORUM read)          │
│  ├── EventHandler — per-type business logic                     │
│  │    ├── staleness guard (out-of-order drop)                   │
│  │    └── LOGGED BATCH → all three inventory tables at once     │
│  ├── DLQ producer — bad events routed, consumer never blocks    │
│  ├── GET /health                                                │
│  └── GET /metrics  (Prometheus)                                 │
└───────────────────────┬─────────────────────────────────────────┘
                        │  QUORUM writes / ONE reads
                        ▼
             ┌──────────────────────────────┐
             │  Cassandra cluster (3 nodes) │
             │  NetworkTopologyStrategy RF=3│
             │  ──────────────────────────  │
             │  inventory_by_product_zone   │
             │  inventory_by_product        │
             │  inventory_by_zone           │
             │  processed_events (TTL 7d)   │
             │  orders                      │
             │  event_history   (TTL 30d)   │
             └──────────────────────────────┘
                        │
             ┌──────────┴──────────┐
             │  Prometheus  9090   │
             │  Grafana     3000   │
             └─────────────────────┘
```

---

## Project Layout

```
HW6/
├── cassandra/
│   └── schema.cql               CQL DDL, runs once via cassandra-init container
├── consumer_service/
│   ├── Dockerfile
│   ├── main.py                  FastAPI app, Kafka consume loop, DLQ, /health, /metrics
│   ├── cassandra_client.py      Thin driver wrapper — prepared statements, batch helpers
│   ├── handlers.py              One method per event type, out-of-order guard
│   ├── metrics.py               Prometheus counters / histograms / gauges
│   └── requirements.txt
├── wms_service/
│   ├── Dockerfile
│   ├── main.py                  FastAPI producer, schema registration, scenario endpoints
│   └── requirements.txt
├── schemas/
│   ├── warehouse_event_v1.avsc
│   └── warehouse_event_v2.avsc  Adds supplier_id (nullable, backward-compatible)
├── grafana/
│   ├── dashboards/warehouse.json
│   └── provisioning/
│       ├── dashboard/dashboards.yml
│       └── datasources/prometheus.yml
├── prometheus.yml
└── docker-compose.yml
```

---

## Quick Start

```bash
docker compose up --build
```

All services start in dependency order. Cassandra takes the longest (~90 s for all three nodes to reach UN status). The `cassandra-init` container waits for all three nodes to pass their healthcheck before running `schema.cql`, so the consumer won't connect to a missing keyspace.

**Ports:**

| Service          | URL                          |
|------------------|------------------------------|
| WMS producer API | http://localhost:8000/docs   |
| Consumer metrics | http://localhost:8001/metrics|
| Consumer health  | http://localhost:8001/health |
| Prometheus       | http://localhost:9090        |
| Grafana          | http://localhost:3000 (admin/admin) |

**Run a built-in scenario:**

```bash
# Full warehouse lifecycle
curl -X POST http://localhost:8000/scenario/basic-cycle

# Duplicate event (idempotency check)
curl -X POST http://localhost:8000/scenario/idempotency

# Events arriving out of chronological order
curl -X POST http://localhost:8000/scenario/out-of-order

# Send a bad event → watch it land in the DLQ
curl -X POST http://localhost:8000/scenario/dlq-test
```

**Query Cassandra directly:**

```bash
docker exec -it cassandra-1 cqlsh

USE warehouse;

-- Stock of SKU-001 across all zones
SELECT * FROM inventory_by_product_zone WHERE product_id = 'SKU-001';

-- Totals
SELECT * FROM inventory_by_product WHERE product_id = 'SKU-001';

-- Everything in ZONE-A
SELECT * FROM inventory_by_zone WHERE zone_id = 'ZONE-A';

-- Audit trail for SKU-001
SELECT * FROM event_history WHERE product_id = 'SKU-001' LIMIT 20;
```

---

## Data Model

The tables are designed around query patterns, not around the relational shape of the data. There are no joins in Cassandra, so every read access pattern gets its own table.

### `inventory_by_product_zone`

```
PRIMARY KEY (product_id, zone_id)
```

Answers: *"How much SKU-001 is in ZONE-A?"* and *"List every zone that holds SKU-001."*

`product_id` is the partition key — all zones for a product land on the same node, so a full product scan is a single partition read. `zone_id` is the clustering key and keeps rows sorted, which also makes point lookups fast.

`last_event_timestamp` is stored here and acts as the out-of-order fence: before applying any event, the handler compares the event's timestamp against this value. If the event is older, it is dropped.

`supplier_id` was added in V2 of the Avro schema. V1 events get `null` here.

### `inventory_by_product`

```
PRIMARY KEY (product_id)
```

Answers: *"What are the total available and reserved quantities for SKU-001 across the entire warehouse?"*

One row per product. Maintained as a running total: each event that touches a zone also adjusts this row by the same delta. It's denormalized on purpose — computing the sum by reading all zones would require a full partition scan every time.

### `inventory_by_zone`

```
PRIMARY KEY (zone_id, product_id)
```

Answers: *"What products does ZONE-A contain?"*

Mirror of `inventory_by_product_zone` with the partition key flipped. Same data, different access direction. Cassandra's model requires this duplication.

### `processed_events`

```
PRIMARY KEY (event_id)
TTL 604800 (7 days)
```

Idempotency store. Before processing any event, the consumer does a QUORUM read against this table. If the `event_id` is already there, the event is skipped. Kafka's at-least-once delivery guarantees that duplicates will arrive; this is what stops them from corrupting state.

TTL is 7 days. Events older than that won't realistically be redelivered under any failure mode we care about.

### `orders`

```
PRIMARY KEY (order_id)
```

Tracks order lifecycle. `status` goes `CREATED → COMPLETED`. `items` is stored as a JSON string (list of `{product_id, zone_id, quantity}`) to avoid needing a separate item table.

### `event_history`

```
PRIMARY KEY (product_id, event_timestamp DESC, event_id)
TTL 2592000 (30 days)
```

Append-only audit log. Partitioned by `product_id` so the full history of a product is a single partition read. Clustered by timestamp descending so the most recent events come back first. `event_id` is included as a secondary clustering key to avoid a collision if two events arrive with the same millisecond timestamp. 30-day TTL keeps it from growing forever.

---

## Event Semantics

All event types flow through `EventHandler.handle()` which dispatches to a per-type method. Every method:

1. Checks the idempotency store (in `main.py`, before `handle()` is called).
2. Checks for out-of-order timestamps (where applicable).
3. Reads current state.
4. Computes the new state.
5. Writes everything atomically in a single Cassandra LOGGED BATCH.

| Event               | Inventory effect                                                                                  |
|---------------------|---------------------------------------------------------------------------------------------------|
| `PRODUCT_RECEIVED`  | `available += qty` in zone, product total, zone view                                              |
| `PRODUCT_SHIPPED`   | `available -= qty` in zone, product total, zone view. Fails if insufficient stock.                |
| `PRODUCT_MOVED`     | `available -= qty` from source zone, `+= qty` in destination zone. Product total unchanged.       |
| `PRODUCT_RESERVED`  | `available -= qty`, `reserved += qty`. Fails if insufficient available stock.                     |
| `PRODUCT_RELEASED`  | `reserved -= qty`, `available += qty`. Fails if insufficient reserved stock.                      |
| `INVENTORY_COUNTED` | Sets `available = counted_qty` regardless of current value (physical count overrides the system). |
| `ORDER_CREATED`     | Creates order record, then fires `PRODUCT_RESERVED` for each line item.                           |
| `ORDER_COMPLETED`   | For each line item: `reserved -= qty` (available was already decremented at reservation time).    |

---

## Reliability Guarantees

### At-least-once delivery + idempotency

The consumer uses manual offset commit (`enable.auto.commit=false`). Offsets are committed only after the event has been written to Cassandra successfully, or after it has been sent to the DLQ. On restart, processing resumes from the last committed offset — the consumer may replay the most recent event, which is safe because of the idempotency check.

The idempotency check uses `ConsistencyLevel.QUORUM` to avoid reading stale data from a lagging replica. A QUORUM read on a 3-node cluster requires responses from 2 nodes, which is enough to observe any write that was committed at QUORUM.

### Atomic multi-table updates (Cassandra LOGGED BATCH)

Cassandra's LOGGED BATCH guarantees that either all statements in the batch are applied or none are. A single event updates `inventory_by_product_zone`, `inventory_by_product`, `inventory_by_zone`, `processed_events`, and `event_history` together. There's no window where one table reflects the new state and another still has the old.

The batch is not a performance tool here — it's a consistency tool. Cassandra will log the batch to a system table before applying it, so a node crash mid-apply will be recovered automatically.

### Out-of-order events

Kafka partitions preserve order within a partition, but events for the same product-zone pair may arrive from different partitions, or be replayed. The staleness check compares the incoming event's `event_timestamp` against `last_event_timestamp` stored in `inventory_by_product_zone`. If the incoming timestamp is not strictly greater, the event is dropped and only marked as processed.

This means: if events arrive in a sequence with timestamps T1=12:00, T2=12:05, T3=12:02 (the third one is a late arrival), the state after T2 is preserved and T3 is silently discarded. The last committed offset advances past T3 so it won't be reprocessed.

### Dead Letter Queue

Any exception during `event_handler.handle()` sends the event to `warehouse-events-dlq`. The consumer then commits the offset and continues. The DLQ message includes the full original event, the exception message, and the Kafka partition/offset so it can be tracked back to the source.

Deserialization failures (malformed Avro) are also caught before the handler is even called and routed to the DLQ the same way.

---

## Schema Evolution

Two schema versions are registered in Confluent Schema Registry under the subject `warehouse-events-value`.

**V1** (`warehouse_event_v1.avsc`) — base schema, 10 fields.

**V2** (`warehouse_event_v2.avsc`) — adds `supplier_id` as a nullable union `["null", "string"]` with `"default": null`.

Schema compatibility mode is set to `BACKWARD` on the subject before registration. Under BACKWARD compatibility, consumers running V2 can read V1-serialized messages — the missing `supplier_id` field resolves to its default value of `null`. This is how the two versions coexist in the same topic simultaneously without any consumer coordination.

**How to add a new version:**

1. Copy the current schema file and add the new field as a nullable union with a default:
   ```json
   {"name": "my_new_field", "type": ["null", "string"], "default": null}
   ```
   Fields with non-null types or without defaults are not backward-compatible and will be rejected.

2. Verify compatibility before registering:
   ```bash
   curl -X POST \
     http://localhost:8081/compatibility/subjects/warehouse-events-value/versions/latest \
     -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     -d '{"schema": "<escaped-json-schema>"}'
   ```

3. Register the new version:
   ```bash
   curl -X POST \
     http://localhost:8081/subjects/warehouse-events-value/versions \
     -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     -d '{"schema": "<escaped-json-schema>"}'
   ```

4. If the new field needs to be persisted, add the column to Cassandra with a default:
   ```cql
   ALTER TABLE inventory_by_product_zone ADD my_new_field TEXT;
   ```

5. Read the field in the handler with `.get("my_new_field")` — returns `None` for V1 events, the actual value for V3+ events.

The WMS service exposes `/events` (V1 serializer) and `/events/v2` (V2 serializer). Both write to the same topic. The consumer's Avro deserializer resolves the schema from the message's embedded schema ID, so it handles both transparently.

---

## Monitoring

The consumer exposes a Prometheus-compatible `/metrics` endpoint scraped every 15 seconds.

| Metric                              | Type      | Labels          | What it measures                                                  |
|-------------------------------------|-----------|-----------------|-------------------------------------------------------------------|
| `events_processed_total`            | Counter   | `event_type`    | Successfully processed events, broken down by type               |
| `events_dlq_total`                  | Counter   | —               | Events routed to DLQ                                              |
| `cassandra_write_errors_total`      | Counter   | —               | Cassandra write failures                                          |
| `event_processing_duration_seconds` | Histogram | —               | End-to-end latency from Kafka receive to Cassandra commit         |
| `consumer_lag`                      | Gauge     | `partition`     | Difference between latest Kafka offset and last committed offset  |

Consumer lag is updated every 15 seconds in a background asyncio task. It's calculated per partition so you can see if a specific partition is falling behind while others are fine.

**Grafana** is auto-provisioned with a datasource pointing at Prometheus and a dashboard at `grafana/dashboards/warehouse.json` with three panels: consumer lag by partition, events-per-second by type, and Cassandra write errors.

**Health endpoint** (`GET /health`) checks both Cassandra (`SELECT now() FROM system.local`) and Kafka (`list_topics`) and returns 503 if either is unreachable.

---

## Consistency Level Decisions

**Writes: QUORUM**

With replication factor 3, QUORUM requires acknowledgement from 2 nodes before the write is confirmed. This means the data survives one node going down without any writes being lost. It's the minimum consistency level that gives us durable writes in a 3-node cluster.

**Reads: ONE (default profile) — with a deliberate exception**

Most reads fetch current inventory to compute deltas before writing. Using ONE here means we might read a value from a slightly lagging replica — but since the write that follows is a QUORUM write, the next read of that data will see the new value. The small window of staleness is acceptable because the consumer is the only writer; there is no concurrent modification to race against.

The idempotency check (`processed_events`) is the exception. It uses QUORUM explicitly because its entire purpose is to detect a write that may have just happened on a different replica. A stale ONE read here could cause a duplicate event to slip through. QUORUM closes that window: if a write was committed at QUORUM, a QUORUM read is guaranteed to see it.

This means `CL=QUORUM + CL=QUORUM` satisfies the strong consistency requirement (`W + R > RF`, i.e. `2 + 2 > 3`) where it matters, and the less critical reads use `CL=ONE` for lower latency.

**Node failure behavior**

With RF=3 and QUORUM writes, the cluster continues accepting reads and writes when one node is stopped. `nodetool status` will show the remaining two nodes as `UN` (Up/Normal) and the stopped node as `DN`. The consumer keeps processing without error.

```bash
# Demonstrate fault tolerance
docker stop cassandra-2

# Consumer keeps running — verify with a new event
curl -X POST http://localhost:8000/events -H "Content-Type: application/json" \
  -d '{"event_id":"ft-1","event_type":"PRODUCT_RECEIVED","timestamp":1700000000000,
       "product_id":"SKU-099","quantity":10,"zone_id":"ZONE-A"}'

# Bring the node back
docker start cassandra-2

# Wait for it to rejoin, then check
docker exec cassandra-1 nodetool status
```

---

## Scenarios

These match the E2E scenarios from the assignment. Each curl call below uses the WMS service's scenario shortcuts.

### Basic cycle

```bash
curl -X POST http://localhost:8000/scenario/basic-cycle
```

Fires in sequence: receive 100 → reserve 30 → move 20 to ZONE-B → ship 10 → create order for 15 → complete order.

Expected final state for SKU-001:
- ZONE-A: available=25, reserved=0
- ZONE-B: available=20, reserved=0
- Total: available=45

### Idempotency

```bash
curl -X POST http://localhost:8000/scenario/idempotency
```

Sends the same `event_id` twice. The second message is ignored — `available` stays at 50, not 100.

### Out-of-order

```bash
curl -X POST http://localhost:8000/scenario/out-of-order
```

Sends: receive@T0 → ship@T+5min → receive@T+2min (late). The third event is discarded because its timestamp is behind the last processed timestamp for that product-zone. `available` stays at 80.

### DLQ

```bash
curl -X POST http://localhost:8000/scenario/dlq-test
```

Sends `PRODUCT_SHIPPED` with `quantity=-5` (no stock exists). This fails the business logic check, gets routed to the DLQ, and the consumer moves on. The next valid event in the scenario processes cleanly.

Check the DLQ:

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic warehouse-events-dlq \
  --from-beginning \
  --max-messages 10
```

### Schema evolution

```bash
# V1 event — no supplier_id
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{"event_id":"v1-1","event_type":"PRODUCT_RECEIVED","timestamp":1700000000000,
       "product_id":"SKU-010","quantity":50,"zone_id":"ZONE-A"}'

# V2 event — with supplier_id
curl -X POST http://localhost:8000/events/v2 \
  -H "Content-Type: application/json" \
  -d '{"event_id":"v2-1","event_type":"PRODUCT_RECEIVED","timestamp":1700000100000,
       "product_id":"SKU-010","quantity":50,"zone_id":"ZONE-A","supplier_id":"SUP-001"}'
```

Check Cassandra:

```cql
SELECT product_id, zone_id, available_quantity, supplier_id
FROM inventory_by_product_zone
WHERE product_id = 'SKU-010';

-- v1-1 row: supplier_id = null
-- v2-1 row: supplier_id = 'SUP-001'
```

View registered schema versions in Schema Registry:

```bash
# List versions
curl http://localhost:8081/subjects/warehouse-events-value/versions

# Inspect V1
curl http://localhost:8081/subjects/warehouse-events-value/versions/1

# Inspect V2
curl http://localhost:8081/subjects/warehouse-events-value/versions/2
```
