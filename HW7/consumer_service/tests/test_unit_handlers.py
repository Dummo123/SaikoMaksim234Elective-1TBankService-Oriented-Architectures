"""
Unit tests for handlers.py.
All Cassandra I/O is mocked — no running infrastructure required.
Run:  pytest consumer_service/tests/test_unit_handlers.py -v
"""
import json
import pytest
from unittest.mock import MagicMock, call, patch
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from handlers import EventHandler


# ─── Helper ───────────────────────────────────────────────────────────────────

def _db(available=0, reserved=0, last_ts=0, total_avail=None, total_res=None):
    """Return a fully mocked CassandraClient with controllable inventory."""
    db = MagicMock()
    db.get_zone_inventory.return_value = {
        "available": available,
        "reserved": reserved,
        "last_ts": last_ts,
    }
    db.get_product_inventory.return_value = {
        "total_available": total_avail if total_avail is not None else available,
        "total_reserved": total_res if total_res is not None else reserved,
    }
    db.get_zone_product_inventory.return_value = {
        "available": available,
        "reserved": reserved,
    }
    return db


def _ev(event_type, qty=10, ts=1000, **extra):
    base = {
        "event_id": f"test-{event_type.lower()}",
        "event_type": event_type,
        "event_timestamp": ts,
        "product_id": "SKU-TEST",
        "zone_id": "ZONE-A",
        "quantity": qty,
    }
    base.update(extra)
    return base


# ─── PRODUCT_RECEIVED ─────────────────────────────────────────────────────────

class TestProductReceived:
    def test_adds_available_quantity(self):
        db = _db(available=100)
        EventHandler(db).handle(_ev("PRODUCT_RECEIVED", qty=50))
        kw = db.execute_inventory_update.call_args[1]
        assert kw["new_avail"] == 150
        assert kw["new_reserved"] == 0

    def test_sets_prod_total_available(self):
        db = _db(available=100, total_avail=200)
        EventHandler(db).handle(_ev("PRODUCT_RECEIVED", qty=50))
        kw = db.execute_inventory_update.call_args[1]
        assert kw["prod_total_avail"] == 250

    def test_passes_supplier_id_v2(self):
        db = _db()
        EventHandler(db).handle(_ev("PRODUCT_RECEIVED", qty=10, supplier_id="SUP-001"))
        kw = db.execute_inventory_update.call_args[1]
        assert kw["supplier_id"] == "SUP-001"

    def test_supplier_id_none_for_v1(self):
        db = _db()
        EventHandler(db).handle(_ev("PRODUCT_RECEIVED", qty=10))
        kw = db.execute_inventory_update.call_args[1]
        assert kw["supplier_id"] is None

    def test_stale_event_is_skipped(self):
        db = _db(available=100, last_ts=2000)
        EventHandler(db).handle(_ev("PRODUCT_RECEIVED", qty=50, ts=1000))
        db.mark_processed_only.assert_called_once()
        db.execute_inventory_update.assert_not_called()

    def test_negative_qty_raises(self):
        db = _db()
        with pytest.raises(ValueError, match="positive"):
            EventHandler(db).handle(_ev("PRODUCT_RECEIVED", qty=-1))

    def test_zero_qty_raises(self):
        db = _db()
        with pytest.raises(ValueError, match="positive"):
            EventHandler(db).handle(_ev("PRODUCT_RECEIVED", qty=0))


# ─── PRODUCT_SHIPPED ──────────────────────────────────────────────────────────

class TestProductShipped:
    def test_deducts_available(self):
        db = _db(available=100)
        EventHandler(db).handle(_ev("PRODUCT_SHIPPED", qty=30))
        kw = db.execute_inventory_update.call_args[1]
        assert kw["new_avail"] == 70

    def test_insufficient_stock_raises(self):
        db = _db(available=5)
        with pytest.raises(ValueError, match="Insufficient stock"):
            EventHandler(db).handle(_ev("PRODUCT_SHIPPED", qty=10))

    def test_negative_qty_raises(self):
        db = _db(available=100)
        with pytest.raises(ValueError, match="positive"):
            EventHandler(db).handle(_ev("PRODUCT_SHIPPED", qty=-5))

    def test_stale_skipped(self):
        db = _db(available=100, last_ts=5000)
        EventHandler(db).handle(_ev("PRODUCT_SHIPPED", qty=10, ts=3000))
        db.mark_processed_only.assert_called_once()


# ─── PRODUCT_MOVED ────────────────────────────────────────────────────────────

class TestProductMoved:
    def _move_ev(self, qty=20, ts=1000, from_last_ts=0):
        db = MagicMock()
        # get_zone_inventory is called twice: from_zone then to_zone
        db.get_zone_inventory.side_effect = [
            {"available": 100, "reserved": 0, "last_ts": from_last_ts},
            {"available": 0,   "reserved": 0, "last_ts": 0},
        ]
        db.get_zone_product_inventory.return_value = {"available": 50, "reserved": 0}
        ev = {
            "event_id": "mv-1", "event_type": "PRODUCT_MOVED",
            "product_id": "SKU-MV", "event_timestamp": ts,
            "from_zone_id": "ZONE-A", "to_zone_id": "ZONE-B", "quantity": qty,
        }
        return db, ev

    def test_moves_quantity(self):
        db, ev = self._move_ev(qty=20)
        EventHandler(db).handle(ev)
        kw = db.execute_move_update.call_args[1]
        assert kw["from_avail"] == 80
        assert kw["to_avail"] == 20

    def test_insufficient_stock_raises(self):
        db, ev = self._move_ev(qty=200)
        with pytest.raises(ValueError, match="Cannot move"):
            EventHandler(db).handle(ev)

    def test_stale_from_zone_skipped(self):
        db, ev = self._move_ev(qty=20, ts=500, from_last_ts=1000)
        EventHandler(db).handle(ev)
        db.mark_processed_only.assert_called_once()
        db.execute_move_update.assert_not_called()


# ─── PRODUCT_RESERVED ────────────────────────────────────────────────────────

class TestProductReserved:
    def test_moves_available_to_reserved(self):
        db = _db(available=100, reserved=10)
        EventHandler(db).handle(_ev("PRODUCT_RESERVED", qty=30))
        kw = db.execute_inventory_update.call_args[1]
        assert kw["new_avail"] == 70
        assert kw["new_reserved"] == 40

    def test_insufficient_available_raises(self):
        db = _db(available=5)
        with pytest.raises(ValueError, match="Cannot reserve"):
            EventHandler(db).handle(_ev("PRODUCT_RESERVED", qty=10))


# ─── PRODUCT_RELEASED ────────────────────────────────────────────────────────

class TestProductReleased:
    def test_releases_reserved_back_to_available(self):
        db = _db(available=70, reserved=30)
        EventHandler(db).handle(_ev("PRODUCT_RELEASED", qty=30))
        kw = db.execute_inventory_update.call_args[1]
        assert kw["new_avail"] == 100
        assert kw["new_reserved"] == 0

    def test_insufficient_reserved_raises(self):
        db = _db(available=100, reserved=5)
        with pytest.raises(ValueError, match="Cannot release"):
            EventHandler(db).handle(_ev("PRODUCT_RELEASED", qty=10))


# ─── INVENTORY_COUNTED ───────────────────────────────────────────────────────

class TestInventoryCounted:
    def test_overrides_available(self):
        db = _db(available=50)
        EventHandler(db).handle(_ev("INVENTORY_COUNTED", qty=200))
        kw = db.execute_inventory_update.call_args[1]
        assert kw["new_avail"] == 200

    def test_zero_count_is_valid(self):
        db = _db(available=50)
        EventHandler(db).handle(_ev("INVENTORY_COUNTED", qty=0))
        kw = db.execute_inventory_update.call_args[1]
        assert kw["new_avail"] == 0

    def test_negative_count_raises(self):
        db = _db()
        with pytest.raises(ValueError):
            EventHandler(db).handle(_ev("INVENTORY_COUNTED", qty=-1))

    def test_delta_applied_to_product_total(self):
        db = _db(available=50, total_avail=150)
        EventHandler(db).handle(_ev("INVENTORY_COUNTED", qty=80))
        kw = db.execute_inventory_update.call_args[1]
        # delta = 80-50=30, total was 150 → 180
        assert kw["prod_total_avail"] == 180


# ─── ORDER_CREATED ───────────────────────────────────────────────────────────

class TestOrderCreated:
    def test_creates_order_and_reserves_items(self):
        db = _db(available=100)
        items = json.dumps([
            {"product_id": "SKU-TEST", "zone_id": "ZONE-A", "quantity": 15}
        ])
        ev = {
            "event_id": "ord-1", "event_type": "ORDER_CREATED",
            "order_id": "ORD-001", "event_timestamp": 1000,
            "order_items": items, "product_id": "ORD-001",
        }
        EventHandler(db).handle(ev)
        db.create_order.assert_called_once_with("ORD-001", items, "ord-1", "ORDER_CREATED")
        # reserve call: available-15=85
        kw = db.execute_inventory_update.call_args[1]
        assert kw["new_avail"] == 85
        assert kw["new_reserved"] == 15

    def test_empty_items_still_creates_order(self):
        db = _db()
        ev = {
            "event_id": "ord-2", "event_type": "ORDER_CREATED",
            "order_id": "ORD-002", "event_timestamp": 1000,
            "order_items": "[]", "product_id": "ORD-002",
        }
        EventHandler(db).handle(ev)
        db.create_order.assert_called_once()
        db.execute_inventory_update.assert_not_called()


# ─── ORDER_COMPLETED ─────────────────────────────────────────────────────────

class TestOrderCompleted:
    def test_completes_and_releases_reserved(self):
        db = _db(available=70, reserved=30)
        items = json.dumps([
            {"product_id": "SKU-TEST", "zone_id": "ZONE-A", "quantity": 30}
        ])
        order_mock = MagicMock()
        order_mock.status = "CREATED"
        order_mock.items = items
        db.get_order.return_value = order_mock

        ev = {
            "event_id": "oc-1", "event_type": "ORDER_COMPLETED",
            "order_id": "ORD-001", "event_timestamp": 2000,
        }
        EventHandler(db).handle(ev)
        db.complete_order.assert_called_once_with("ORD-001", "oc-1", "ORDER_COMPLETED")
        kw = db.execute_inventory_update.call_args[1]
        assert kw["new_reserved"] == 0       # 30-30=0

    def test_already_completed_is_skipped(self):
        db = _db()
        order_mock = MagicMock()
        order_mock.status = "COMPLETED"
        db.get_order.return_value = order_mock
        ev = {
            "event_id": "oc-2", "event_type": "ORDER_COMPLETED",
            "order_id": "ORD-001", "event_timestamp": 2000,
        }
        EventHandler(db).handle(ev)
        db.mark_processed_only.assert_called_once()
        db.complete_order.assert_not_called()

    def test_unknown_order_raises(self):
        db = _db()
        db.get_order.return_value = None
        ev = {
            "event_id": "oc-3", "event_type": "ORDER_COMPLETED",
            "order_id": "MISSING", "event_timestamp": 2000,
        }
        with pytest.raises(ValueError, match="Order not found"):
            EventHandler(db).handle(ev)


# ─── Dispatcher ──────────────────────────────────────────────────────────────

class TestDispatcher:
    def test_unknown_event_type_raises(self):
        db = _db()
        with pytest.raises(ValueError, match="Unknown event_type"):
            EventHandler(db).handle(_ev("UNKNOWN_TYPE"))
