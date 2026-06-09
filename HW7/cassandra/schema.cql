-- Warehouse keyspace with 3-node replication across datacenter1
CREATE KEYSPACE IF NOT EXISTS warehouse
WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'datacenter1': 3
}
AND durable_writes = true;

USE warehouse;

-- ─────────────────────────────────────────────────────────────
-- TABLE 1: inventory_by_product_zone
-- Primary access pattern: get stock of a specific product in a specific zone.
-- Partition key = product_id  →  all zones for one product live on the same node,
-- making "give me all zones for SKU-001" a single partition read.
-- Clustering key = zone_id   →  rows within the partition are sorted by zone.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS inventory_by_product_zone (
    product_id          TEXT,
    zone_id             TEXT,
    available_quantity  INT,
    reserved_quantity   INT,
    last_event_id       TEXT,
    last_event_timestamp BIGINT,
    updated_at          TIMESTAMP,
    supplier_id         TEXT,
    PRIMARY KEY (product_id, zone_id)
) WITH CLUSTERING ORDER BY (zone_id ASC)
  AND comment = 'Point lookup: stock of product X in zone Y';

-- ─────────────────────────────────────────────────────────────
-- TABLE 2: inventory_by_product
-- Aggregated view: total available and reserved across ALL zones for a product.
-- Partition key = product_id  →  single row per product, instant total lookup.
-- No clustering key — one row per partition.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS inventory_by_product (
    product_id          TEXT PRIMARY KEY,
    total_available     INT,
    total_reserved      INT,
    last_updated        TIMESTAMP
);

-- ─────────────────────────────────────────────────────────────
-- TABLE 3: inventory_by_zone
-- Access pattern: "what products are in ZONE-A and how many?"
-- Partition key = zone_id    →  all products in a zone on one node.
-- Clustering key = product_id →  rows sorted for range scans.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS inventory_by_zone (
    zone_id             TEXT,
    product_id          TEXT,
    available_quantity  INT,
    reserved_quantity   INT,
    last_updated        TIMESTAMP,
    PRIMARY KEY (zone_id, product_id)
) WITH CLUSTERING ORDER BY (product_id ASC)
  AND comment = 'List all products in a zone';

-- ─────────────────────────────────────────────────────────────
-- TABLE 4: processed_events
-- Idempotency store. event_id is the primary key for O(1) dedup check.
-- TTL = 7 days — events older than a week won't realistically be redelivered.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS processed_events (
    event_id        TEXT PRIMARY KEY,
    event_type      TEXT,
    processed_at    TIMESTAMP
) WITH default_time_to_live = 604800
  AND comment = 'Deduplication store — 7-day TTL';

-- ─────────────────────────────────────────────────────────────
-- TABLE 5: orders
-- Tracks order lifecycle. Partition key = order_id (natural for point lookups).
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS orders (
    order_id        TEXT PRIMARY KEY,
    status          TEXT,
    items           TEXT,
    created_at      TIMESTAMP,
    completed_at    TIMESTAMP
);

-- ─────────────────────────────────────────────────────────────
-- TABLE 6: event_history  (audit log)
-- Partition key = product_id  →  all events for one product together.
-- Clustering key = (event_timestamp DESC, event_id)  →  most recent first.
-- TTL = 30 days to limit unbounded growth.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS event_history (
    product_id          TEXT,
    event_timestamp     BIGINT,
    event_id            TEXT,
    event_type          TEXT,
    zone_id             TEXT,
    quantity            INT,
    PRIMARY KEY (product_id, event_timestamp, event_id)
) WITH CLUSTERING ORDER BY (event_timestamp DESC, event_id ASC)
  AND default_time_to_live = 2592000
  AND comment = 'Audit log — 30-day TTL';
