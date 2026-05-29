"""
End-to-End tests — full user scenarios tested against the running system.

Each test exercises a complete business workflow from the WMS API through
Kafka and the Consumer all the way to Cassandra state verification.

Run after `docker compose up -d` and the wait script:
    pytest tests/e2e/test_e2e.py -v -s

Env vars:
    WMS_URL            http://localhost:8000
    CONSUMER_URL       http://localhost:8001
    CASSANDRA_HOSTS    localhost
    PROCESS_WAIT       4   (seconds to allow consumer to commit state)
"""
import os
import time
import uuid
import json
import pytest
import requests
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy

WMS_URL = os.getenv("WMS_URL", "http://localhost:8000")
CONSUMER_URL = os.getenv("CONSUMER_URL", "http://localhost:8001")
CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "warehouse")
PROCESS_WAIT = float(os.getenv("PROCESS_WAIT", "4"))


# ── Session fixtures ──────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def cass():
    cluster = Cluster(
        CASSANDRA_HOSTS,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
        protocol_version=4,
        connect_timeout=30,
    )
    session = cluster.connect(CASSANDRA_KEYSPACE)
    yield session
    cluster.shutdown()


def _uid():
    return str(uuid.uuid4())[:8]


def _post(path: str, body: dict) -> requests.Response:
    r = requests.post(f"{WMS_URL}{path}", json=body, timeout=10)
    assert r.status_code == 200, f"POST {path} failed: {r.status_code} {r.text}"
    return r


def _wait(extra: float = 0):
    time.sleep(PROCESS_WAIT + extra)


# ── E2E Scenario 1: Full warehouse cycle ─────────────────────────────────────

class TestBasicWarehouseCycle:
    """
    Reproduces the complete showcase Scenario 1 programmatically:
    RECEIVE → RESERVE → MOVE → SHIP → ORDER_CREATED → ORDER_COMPLETED
    and verifies every step in Cassandra.
    """

    def test_full_cycle(self, cass):
        pid = f"E2E-{_uid()}"
        base_ts = int(time.time() * 1000)

        # Step 1: receive 100 units
        _post("/events", {
            "event_id": f"{pid}-recv",
            "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": base_ts,
            "product_id": pid,
            "quantity": 100,
            "zone_id": "ZONE-A",
        })
        _wait()

        row = cass.execute(
            "SELECT available_quantity, reserved_quantity FROM inventory_by_product_zone "
            "WHERE product_id=%s AND zone_id=%s", (pid, "ZONE-A")
        ).one()
        assert row.available_quantity == 100
        assert row.reserved_quantity == 0

        # Step 2: reserve 30
        _post("/events", {
            "event_id": f"{pid}-res",
            "event_type": "PRODUCT_RESERVED",
            "event_timestamp": base_ts + 100,
            "product_id": pid,
            "quantity": 30,
            "zone_id": "ZONE-A",
        })
        _wait()

        row = cass.execute(
            "SELECT available_quantity, reserved_quantity FROM inventory_by_product_zone "
            "WHERE product_id=%s AND zone_id=%s", (pid, "ZONE-A")
        ).one()
        assert row.available_quantity == 70
        assert row.reserved_quantity == 30

        # Step 3: move 20 from ZONE-A to ZONE-B
        _post("/events", {
            "event_id": f"{pid}-move",
            "event_type": "PRODUCT_MOVED",
            "event_timestamp": base_ts + 200,
            "product_id": pid,
            "quantity": 20,
            "from_zone_id": "ZONE-A",
            "to_zone_id": "ZONE-B",
        })
        _wait()

        row_a = cass.execute(
            "SELECT available_quantity FROM inventory_by_product_zone "
            "WHERE product_id=%s AND zone_id=%s", (pid, "ZONE-A")
        ).one()
        row_b = cass.execute(
            "SELECT available_quantity FROM inventory_by_product_zone "
            "WHERE product_id=%s AND zone_id=%s", (pid, "ZONE-B")
        ).one()
        assert row_a.available_quantity == 50
        assert row_b.available_quantity == 20

        # Step 4: ship 10 from ZONE-A
        _post("/events", {
            "event_id": f"{pid}-ship",
            "event_type": "PRODUCT_SHIPPED",
            "event_timestamp": base_ts + 300,
            "product_id": pid,
            "quantity": 10,
            "zone_id": "ZONE-A",
        })
        _wait()

        row = cass.execute(
            "SELECT available_quantity FROM inventory_by_product_zone "
            "WHERE product_id=%s AND zone_id=%s", (pid, "ZONE-A")
        ).one()
        assert row.available_quantity == 40

        # Step 5: create order for 15 units
        order_id = f"ORD-{_uid()}"
        order_items = json.dumps([{"product_id": pid, "zone_id": "ZONE-A", "quantity": 15}])
        _post("/events", {
            "event_id": f"{pid}-order",
            "event_type": "ORDER_CREATED",
            "event_timestamp": base_ts + 400,
            "product_id": order_id,
            "order_id": order_id,
            "order_items": order_items,
        })
        _wait()

        row = cass.execute(
            "SELECT available_quantity, reserved_quantity FROM inventory_by_product_zone "
            "WHERE product_id=%s AND zone_id=%s", (pid, "ZONE-A")
        ).one()
        # available was 40, now reserve 15 more → 25 available, 45 reserved
        assert row.available_quantity == 25
        assert row.reserved_quantity == 45

        order_row = cass.execute(
            "SELECT status FROM orders WHERE order_id=%s", (order_id,)
        ).one()
        assert order_row.status == "CREATED"

        # Step 6: complete order
        _post("/events", {
            "event_id": f"{pid}-complete",
            "event_type": "ORDER_COMPLETED",
            "event_timestamp": base_ts + 500,
            "product_id": order_id,
            "order_id": order_id,
        })
        _wait()

        row = cass.execute(
            "SELECT reserved_quantity FROM inventory_by_product_zone "
            "WHERE product_id=%s AND zone_id=%s", (pid, "ZONE-A")
        ).one()
        assert row.reserved_quantity == 30  # 45 - 15 = 30 (original 30 still reserved)

        order_row = cass.execute(
            "SELECT status FROM orders WHERE order_id=%s", (order_id,)
        ).one()
        assert order_row.status == "COMPLETED"


# ── E2E Scenario 2: Idempotency ───────────────────────────────────────────────

class TestIdempotencyE2E:
    def test_duplicate_not_applied(self, cass):
        pid = f"E2E-DUP-{_uid()}"
        eid = f"dup-e2e-{_uid()}"
        ev = {
            "event_id": eid,
            "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": int(time.time() * 1000),
            "product_id": pid,
            "quantity": 50,
            "zone_id": "ZONE-A",
        }
        _post("/events", ev)
        _wait()
        _post("/events", ev)   # exact duplicate
        _wait()

        row = cass.execute(
            "SELECT available_quantity FROM inventory_by_product_zone "
            "WHERE product_id=%s AND zone_id=%s", (pid, "ZONE-A")
        ).one()
        assert row.available_quantity == 50, \
            f"Idempotency broken: expected 50 but got {row.available_quantity}"


# ── E2E Scenario 3: Out-of-order events ──────────────────────────────────────

class TestOutOfOrderE2E:
    def test_stale_event_ignored(self, cass):
        pid = f"E2E-OOO-{_uid()}"
        now = int(time.time() * 1000)

        _post("/events", {
            "event_id": f"{pid}-t1",
            "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": now,
            "product_id": pid,
            "quantity": 100,
            "zone_id": "ZONE-A",
        })
        _wait()

        _post("/events", {
            "event_id": f"{pid}-t2",
            "event_type": "PRODUCT_SHIPPED",
            "event_timestamp": now + 300_000,   # +5 min
            "product_id": pid,
            "quantity": 20,
            "zone_id": "ZONE-A",
        })
        _wait()

        # Stale event: timestamp is BEFORE the shipped event
        _post("/events", {
            "event_id": f"{pid}-t3",
            "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": now + 120_000,   # +2 min (stale vs +5 min)
            "product_id": pid,
            "quantity": 50,
            "zone_id": "ZONE-A",
        })
        _wait()

        row = cass.execute(
            "SELECT available_quantity FROM inventory_by_product_zone "
            "WHERE product_id=%s AND zone_id=%s", (pid, "ZONE-A")
        ).one()
        assert row.available_quantity == 80, \
            f"Stale event was applied: expected 80 but got {row.available_quantity}"


# ── E2E Scenario 4: Consistency across tables ─────────────────────────────────

class TestConsistencyE2E:
    def test_all_tables_agree(self, cass):
        pid = f"E2E-CONS-{_uid()}"
        _post("/events", {
            "event_id": f"{pid}-recv",
            "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": int(time.time() * 1000),
            "product_id": pid,
            "quantity": 123,
            "zone_id": "ZONE-C",
        })
        _wait()

        pz = cass.execute(
            "SELECT available_quantity FROM inventory_by_product_zone "
            "WHERE product_id=%s AND zone_id=%s", (pid, "ZONE-C")
        ).one()
        prod = cass.execute(
            "SELECT total_available FROM inventory_by_product WHERE product_id=%s", (pid,)
        ).one()
        zone = cass.execute(
            "SELECT available_quantity FROM inventory_by_zone "
            "WHERE zone_id=%s AND product_id=%s", ("ZONE-C", pid)
        ).one()

        assert pz is not None and pz.available_quantity == 123
        assert prod is not None and prod.total_available == 123
        assert zone is not None and zone.available_quantity == 123


# ── E2E Scenario 5: Schema evolution ─────────────────────────────────────────

class TestSchemaEvolutionE2E:
    def test_v1_and_v2_coexist(self, cass):
        pid = f"E2E-SCHEMA-{_uid()}"
        ts = int(time.time() * 1000)

        # V1 (no supplier_id)
        _post("/events", {
            "event_id": f"{pid}-v1",
            "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": ts,
            "product_id": pid,
            "quantity": 10,
            "zone_id": "ZONE-A",
        })
        _wait()

        row_v1 = cass.execute(
            "SELECT supplier_id FROM inventory_by_product_zone "
            "WHERE product_id=%s AND zone_id=%s", (pid, "ZONE-A")
        ).one()
        assert row_v1 is not None
        assert row_v1.supplier_id is None

        # V2 (with supplier_id) — different product to avoid staleness guard
        pid2 = f"E2E-V2-{_uid()}"
        requests.post(f"{WMS_URL}/events/v2", json={
            "event_id": f"{pid2}-v2",
            "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": ts,
            "product_id": pid2,
            "quantity": 20,
            "zone_id": "ZONE-A",
            "supplier_id": "SUP-E2E",
        }, timeout=10)
        _wait()

        row_v2 = cass.execute(
            "SELECT supplier_id FROM inventory_by_product_zone "
            "WHERE product_id=%s AND zone_id=%s", (pid2, "ZONE-A")
        ).one()
        assert row_v2 is not None
        assert row_v2.supplier_id == "SUP-E2E"
