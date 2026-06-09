"""
Integration tests — require the full docker-compose stack to be running.

These tests verify cross-service interaction:
  WMS API  →  Kafka  →  Consumer  →  Cassandra

Run after `docker compose up -d` and the wait script:
    pytest tests/integration/test_integration.py -v

Environment variables (with defaults matching docker-compose):
    WMS_URL            http://localhost:8000
    CONSUMER_URL       http://localhost:8001
    CASSANDRA_HOSTS    localhost
    CASSANDRA_KEYSPACE warehouse
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
PROCESS_WAIT = float(os.getenv("PROCESS_WAIT", "4"))   # seconds to wait for consumer


# ── Fixtures ─────────────────────────────────────────────────────────────────

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


def _uid() -> str:
    return str(uuid.uuid4())[:8]


def _send(event: dict, version: str = "v1") -> requests.Response:
    endpoint = f"{WMS_URL}/events" if version == "v1" else f"{WMS_URL}/events/v2"
    return requests.post(endpoint, json=event, timeout=10)


def _wait():
    time.sleep(PROCESS_WAIT)


def _cleanup(cass, product_id: str, zone_id: str = None):
    """Remove test rows so tests don't bleed into each other."""
    cass.execute("DELETE FROM inventory_by_product WHERE product_id=%s", (product_id,))
    cass.execute("DELETE FROM inventory_by_product_zone WHERE product_id=%s", (product_id,))
    if zone_id:
        cass.execute(
            "DELETE FROM inventory_by_zone WHERE zone_id=%s AND product_id=%s",
            (zone_id, product_id),
        )


# ── Health checks ─────────────────────────────────────────────────────────────

class TestHealthEndpoints:
    def test_wms_health(self):
        r = requests.get(f"{WMS_URL}/health", timeout=5)
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_consumer_health(self):
        r = requests.get(f"{CONSUMER_URL}/health", timeout=5)
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_consumer_metrics_endpoint(self):
        r = requests.get(f"{CONSUMER_URL}/metrics", timeout=5)
        assert r.status_code == 200
        assert "events_processed_total" in r.text

    def test_wms_metrics_endpoint(self):
        r = requests.get(f"{WMS_URL}/metrics", timeout=5)
        assert r.status_code == 200
        assert "wms_http_requests_total" in r.text


# ── Cross-service flow ────────────────────────────────────────────────────────

class TestProductReceivedFlow:
    def test_event_reaches_cassandra(self, cass):
        pid = f"INT-{_uid()}"
        ev = {
            "event_id": f"int-recv-{_uid()}",
            "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": int(time.time() * 1000),
            "product_id": pid,
            "quantity": 75,
            "zone_id": "ZONE-A",
        }
        r = _send(ev)
        assert r.status_code == 200, r.text
        _wait()

        row = cass.execute(
            "SELECT available_quantity FROM inventory_by_product_zone "
            "WHERE product_id=%s AND zone_id=%s",
            (pid, "ZONE-A"),
        ).one()
        assert row is not None, "Row not found in inventory_by_product_zone"
        assert row.available_quantity == 75

        _cleanup(cass, pid, "ZONE-A")

    def test_all_three_tables_updated(self, cass):
        """Logged BATCH must keep all tables in sync (HW6 point 5)."""
        pid = f"INT-CONS-{_uid()}"
        ev = {
            "event_id": f"int-cons-{_uid()}",
            "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": int(time.time() * 1000),
            "product_id": pid,
            "quantity": 100,
            "zone_id": "ZONE-B",
        }
        r = _send(ev)
        assert r.status_code == 200
        _wait()

        r1 = cass.execute(
            "SELECT available_quantity FROM inventory_by_product_zone "
            "WHERE product_id=%s AND zone_id=%s",
            (pid, "ZONE-B"),
        ).one()
        r2 = cass.execute(
            "SELECT total_available FROM inventory_by_product WHERE product_id=%s", (pid,)
        ).one()
        r3 = cass.execute(
            "SELECT available_quantity FROM inventory_by_zone "
            "WHERE zone_id=%s AND product_id=%s",
            ("ZONE-B", pid),
        ).one()

        assert r1 is not None and r1.available_quantity == 100
        assert r2 is not None and r2.total_available == 100
        assert r3 is not None and r3.available_quantity == 100

        _cleanup(cass, pid, "ZONE-B")


class TestIdempotency:
    def test_duplicate_event_not_double_counted(self, cass):
        pid = f"INT-DUP-{_uid()}"
        eid = f"dup-int-{_uid()}"
        ev = {
            "event_id": eid,
            "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": int(time.time() * 1000),
            "product_id": pid,
            "quantity": 50,
            "zone_id": "ZONE-A",
        }
        _send(ev)
        _wait()
        _send(ev)   # same event_id — should be skipped
        _wait()

        row = cass.execute(
            "SELECT available_quantity FROM inventory_by_product_zone "
            "WHERE product_id=%s AND zone_id=%s",
            (pid, "ZONE-A"),
        ).one()
        assert row is not None
        assert row.available_quantity == 50, f"Expected 50, got {row.available_quantity}"

        _cleanup(cass, pid, "ZONE-A")


class TestDLQ:
    def test_bad_event_does_not_crash_consumer(self, cass):
        """Ship from zero stock → goes to DLQ; consumer stays alive."""
        pid = f"INT-DLQ-{_uid()}"
        _send({
            "event_id": f"dlq-bad-{_uid()}",
            "event_type": "PRODUCT_SHIPPED",
            "event_timestamp": int(time.time() * 1000),
            "product_id": pid,
            "quantity": 99,
            "zone_id": "ZONE-A",
        })
        _wait()
        # Consumer must still respond
        r = requests.get(f"{CONSUMER_URL}/health", timeout=5)
        assert r.status_code == 200

    def test_good_event_after_bad_is_processed(self, cass):
        pid = f"INT-RECOVER-{_uid()}"
        # bad
        _send({
            "event_id": f"dlq-bad2-{_uid()}",
            "event_type": "PRODUCT_SHIPPED",
            "event_timestamp": int(time.time() * 1000),
            "product_id": pid,
            "quantity": 50,
            "zone_id": "ZONE-A",
        })
        # good
        eid = f"dlq-good-{_uid()}"
        _send({
            "event_id": eid,
            "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": int(time.time() * 1000) + 1,
            "product_id": pid,
            "quantity": 20,
            "zone_id": "ZONE-A",
        })
        _wait()
        row = cass.execute(
            "SELECT available_quantity FROM inventory_by_product_zone "
            "WHERE product_id=%s AND zone_id=%s",
            (pid, "ZONE-A"),
        ).one()
        assert row is not None and row.available_quantity == 20

        _cleanup(cass, pid, "ZONE-A")


class TestSchemaEvolution:
    def test_v1_event_supplier_id_null(self, cass):
        pid = f"INT-V1-{_uid()}"
        r = _send({
            "event_id": f"v1-{_uid()}",
            "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": int(time.time() * 1000),
            "product_id": pid,
            "quantity": 10,
            "zone_id": "ZONE-A",
        }, version="v1")
        assert r.status_code == 200
        _wait()
        row = cass.execute(
            "SELECT supplier_id FROM inventory_by_product_zone "
            "WHERE product_id=%s AND zone_id=%s",
            (pid, "ZONE-A"),
        ).one()
        assert row is not None
        assert row.supplier_id is None

        _cleanup(cass, pid, "ZONE-A")

    def test_v2_event_supplier_id_stored(self, cass):
        pid = f"INT-V2-{_uid()}"
        r = _send({
            "event_id": f"v2-{_uid()}",
            "event_type": "PRODUCT_RECEIVED",
            "event_timestamp": int(time.time() * 1000),
            "product_id": pid,
            "quantity": 10,
            "zone_id": "ZONE-A",
            "supplier_id": "SUP-INTEGRATION",
        }, version="v2")
        assert r.status_code == 200
        _wait()
        row = cass.execute(
            "SELECT supplier_id FROM inventory_by_product_zone "
            "WHERE product_id=%s AND zone_id=%s",
            (pid, "ZONE-A"),
        ).one()
        assert row is not None
        assert row.supplier_id == "SUP-INTEGRATION"

        _cleanup(cass, pid, "ZONE-A")
