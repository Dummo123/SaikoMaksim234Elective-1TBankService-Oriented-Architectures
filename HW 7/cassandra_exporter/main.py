"""
Prometheus exporter for Cassandra cluster health.

Exposes metrics by querying Cassandra system keyspace tables and
measuring round-trip read/write latency with lightweight operations.
This sidesteps the need for JMX access inside Docker.

Metrics exposed on :9500/metrics:
  cassandra_up                       — 1 if reachable, 0 otherwise
  cassandra_cluster_nodes_total      — number of peers seen by the driver
  cassandra_read_latency_ms          — system.local SELECT round-trip (ms)
  cassandra_inventory_records_total  — rows in inventory_by_product_zone
  cassandra_processed_events_stored  — rows in processed_events (sampled)
  cassandra_orders_total             — rows in orders table
"""
import os
import time
import logging
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.policies import DCAwareRoundRobinPolicy
from prometheus_client import start_http_server, Gauge, Info

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("cassandra_exporter")

CASSANDRA_HOSTS  = os.getenv("CASSANDRA_CONTACT_POINTS", "cassandra-1").split(",")
KEYSPACE         = os.getenv("CASSANDRA_KEYSPACE", "warehouse")
PORT             = int(os.getenv("METRICS_PORT", "9500"))
SCRAPE_INTERVAL  = int(os.getenv("SCRAPE_INTERVAL", "15"))

# ── Metrics ───────────────────────────────────────────────────────────────────

cassandra_up = Gauge(
    "cassandra_up",
    "Whether the Cassandra cluster is reachable (1=up, 0=down)",
)
cassandra_cluster_nodes = Gauge(
    "cassandra_cluster_nodes_total",
    "Number of Cassandra nodes visible to the driver",
)
cassandra_read_latency_ms = Gauge(
    "cassandra_read_latency_ms",
    "Round-trip latency for a lightweight SELECT on system.local (ms)",
)
cassandra_inventory_records = Gauge(
    "cassandra_inventory_records_total",
    "Total rows in inventory_by_product_zone",
)
cassandra_processed_events = Gauge(
    "cassandra_processed_events_stored",
    "Total rows currently in processed_events (TTL-bounded)",
)
cassandra_orders = Gauge(
    "cassandra_orders_total",
    "Total rows in orders table",
)


# ── Collection loop ───────────────────────────────────────────────────────────

def collect():
    try:
        cluster = Cluster(
            CASSANDRA_HOSTS,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
            protocol_version=4,
            connect_timeout=10,
        )
        session = cluster.connect()

        cassandra_up.set(1)
        cassandra_cluster_nodes.set(len(cluster.metadata.all_hosts()))

        # Read latency ping
        t0 = time.perf_counter()
        session.execute("SELECT now() FROM system.local")
        cassandra_read_latency_ms.set((time.perf_counter() - t0) * 1000)

        # Application table stats
        session.set_keyspace(KEYSPACE)

        row = session.execute("SELECT COUNT(*) AS cnt FROM inventory_by_product_zone").one()
        cassandra_inventory_records.set(row.cnt if row else 0)

        row = session.execute("SELECT COUNT(*) AS cnt FROM processed_events").one()
        cassandra_processed_events.set(row.cnt if row else 0)

        row = session.execute("SELECT COUNT(*) AS cnt FROM orders").one()
        cassandra_orders.set(row.cnt if row else 0)

        cluster.shutdown()
        log.info("Metrics collected successfully")

    except NoHostAvailable:
        log.warning("Cassandra not reachable — setting cassandra_up=0")
        cassandra_up.set(0)
    except Exception as exc:
        log.error("Unexpected error during collection: %s", exc)
        cassandra_up.set(0)


if __name__ == "__main__":
    log.info("Starting Cassandra exporter on port %d", PORT)
    start_http_server(PORT)
    while True:
        collect()
        time.sleep(SCRAPE_INTERVAL)
