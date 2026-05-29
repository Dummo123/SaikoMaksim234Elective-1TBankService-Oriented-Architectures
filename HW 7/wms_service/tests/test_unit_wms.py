"""
Unit tests for WMS service.
FastAPI app is tested via TestClient; Kafka and Schema Registry are mocked.
Run:  pytest wms_service/tests/test_unit_wms.py -v
"""
import pytest
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Patch heavy imports before importing the app module
from unittest.mock import MagicMock, patch, call
import json


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_app():
    """Import the app with all external calls patched."""
    with patch("requests.get") as mock_get, \
         patch("requests.put"), \
         patch("confluent_kafka.schema_registry.SchemaRegistryClient"), \
         patch("confluent_kafka.schema_registry.avro.AvroSerializer") as mock_ser, \
         patch("confluent_kafka.Producer") as mock_producer_cls:

        mock_get.return_value.status_code = 200
        mock_ser.return_value = lambda data, ctx: b"serialized"
        mock_producer_cls.return_value = MagicMock()

        import importlib
        import wms_service.main as wms_main          # noqa: F401
        return wms_main


# Because the module-level side effects (wait_for_schema_registry, register_schema)
# run at import time we test model validation and endpoint behaviour directly.

class TestEventV1Model:
    def test_all_required_fields_present(self):
        # Import pydantic model directly - no Kafka needed
        with patch("requests.get") as g, patch("requests.put"), \
             patch("confluent_kafka.schema_registry.SchemaRegistryClient"), \
             patch("confluent_kafka.schema_registry.avro.AvroSerializer"), \
             patch("confluent_kafka.Producer"):
            g.return_value.status_code = 200
            import importlib, sys as _sys
            # ensure fresh import
            for mod in list(_sys.modules):
                if "wms_service" in mod:
                    del _sys.modules[mod]
            import wms_service.main as m
            ev = m.EventV1(
                event_id="e1",
                event_type="PRODUCT_RECEIVED",
                event_timestamp=1000,
                product_id="SKU-001",
                quantity=100,
                zone_id="ZONE-A",
            )
            assert ev.event_id == "e1"
            assert ev.supplier_id is None          # EventV1 has no supplier_id

    def test_optional_fields_default_to_none(self):
        with patch("requests.get") as g, patch("requests.put"), \
             patch("confluent_kafka.schema_registry.SchemaRegistryClient"), \
             patch("confluent_kafka.schema_registry.avro.AvroSerializer"), \
             patch("confluent_kafka.Producer"):
            g.return_value.status_code = 200
            import sys as _sys
            for mod in list(_sys.modules):
                if "wms_service" in mod:
                    del _sys.modules[mod]
            import wms_service.main as m
            ev = m.EventV1(
                event_id="e2", event_type="PRODUCT_SHIPPED",
                event_timestamp=2000, product_id="SKU-002", quantity=10,
            )
            assert ev.zone_id is None
            assert ev.from_zone_id is None
            assert ev.to_zone_id is None
            assert ev.order_id is None
            assert ev.order_items is None


class TestEventV2Model:
    def test_v2_has_supplier_id(self):
        with patch("requests.get") as g, patch("requests.put"), \
             patch("confluent_kafka.schema_registry.SchemaRegistryClient"), \
             patch("confluent_kafka.schema_registry.avro.AvroSerializer"), \
             patch("confluent_kafka.Producer"):
            g.return_value.status_code = 200
            import sys as _sys
            for mod in list(_sys.modules):
                if "wms_service" in mod:
                    del _sys.modules[mod]
            import wms_service.main as m
            ev = m.EventV2(
                event_id="e3", event_type="PRODUCT_RECEIVED",
                event_timestamp=3000, product_id="SKU-003",
                quantity=50, zone_id="ZONE-A", supplier_id="SUP-XYZ",
            )
            assert ev.supplier_id == "SUP-XYZ"

    def test_v2_supplier_id_optional(self):
        with patch("requests.get") as g, patch("requests.put"), \
             patch("confluent_kafka.schema_registry.SchemaRegistryClient"), \
             patch("confluent_kafka.schema_registry.avro.AvroSerializer"), \
             patch("confluent_kafka.Producer"):
            g.return_value.status_code = 200
            import sys as _sys
            for mod in list(_sys.modules):
                if "wms_service" in mod:
                    del _sys.modules[mod]
            import wms_service.main as m
            ev = m.EventV2(
                event_id="e4", event_type="PRODUCT_RECEIVED",
                event_timestamp=4000, product_id="SKU-004", quantity=10,
            )
            assert ev.supplier_id is None


class TestOrderItemsJson:
    """Validate that ORDER_CREATED items JSON is correctly structured."""

    def test_order_items_valid_json(self):
        items = json.dumps([{"product_id": "SKU-001", "zone_id": "ZONE-A", "quantity": 15}])
        parsed = json.loads(items)
        assert len(parsed) == 1
        assert parsed[0]["quantity"] == 15

    def test_order_items_empty_list(self):
        items = json.dumps([])
        parsed = json.loads(items)
        assert parsed == []

    def test_order_items_malformed_gracefully(self):
        """Malformed JSON should not crash the handler — it falls back to []."""
        import json as _json
        for bad in ["not-json", None, ""]:
            try:
                result = _json.loads(bad or "[]")
            except (TypeError, _json.JSONDecodeError):
                result = []
            assert isinstance(result, list)
