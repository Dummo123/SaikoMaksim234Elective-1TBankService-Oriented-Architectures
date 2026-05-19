import os
import json
import logging
import time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
from confluent_kafka import Producer
from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="WMS Service")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC = "warehouse-events"

def load_schema(filename: str) -> str:
    with open(f"/app/schemas/{filename}", "r") as f:
        return f.read()

SCHEMA_V1 = load_schema("warehouse_event_v1.avsc")
SCHEMA_V2 = load_schema("warehouse_event_v2.avsc")

def wait_for_schema_registry(url, max_retries=30, delay=2):
    for i in range(max_retries):
        try:
            resp = requests.get(f"{url}/subjects", timeout=2)
            if resp.status_code == 200:
                logger.info("Schema Registry is ready")
                return
        except Exception:
            pass
        logger.info(f"Waiting for Schema Registry... ({i+1}/{max_retries})")
        time.sleep(delay)
    raise RuntimeError("Schema Registry not available after maximum retries")

wait_for_schema_registry(SCHEMA_REGISTRY_URL)

schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

def register_schema_with_backward_compatibility(subject: str, schema_str: str):
    try:
        compat_url = f"{SCHEMA_REGISTRY_URL}/config/{subject}"
        requests.put(compat_url, json={"compatibility": "BACKWARD"})
    except Exception as e:
        logger.warning(f"Could not set compatibility: {e}")
    schema = Schema(schema_str, schema_type="AVRO")
    schema_id = schema_registry_client.register_schema(subject, schema)
    logger.info(f"Registered {subject} with id {schema_id}")
    return schema_id

subject = f"{TOPIC}-value"
register_schema_with_backward_compatibility(subject, SCHEMA_V1)
register_schema_with_backward_compatibility(subject, SCHEMA_V2)

avro_serializer_v1 = AvroSerializer(schema_registry_client, SCHEMA_V1)
avro_serializer_v2 = AvroSerializer(schema_registry_client, SCHEMA_V2)

producer_conf = {"bootstrap.servers": KAFKA_BOOTSTRAP}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.info(f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

@app.get("/health")
async def health():
    return {"status": "ok"}

class EventV1(BaseModel):
    event_id: str
    event_type: str
    event_timestamp: int
    product_id: str
    quantity: int
    zone_id: Optional[str] = None
    from_zone: Optional[str] = None
    to_zone: Optional[str] = None
    order_id: Optional[str] = None
    order_items: Optional[str] = None

class EventV2(EventV1):
    supplier_id: Optional[str] = None

@app.post("/events")
async def send_event_v1(event: EventV1):
    try:
        data = event.dict()
        serialized = avro_serializer_v1(data, SerializationContext(TOPIC, MessageField.VALUE))
        producer.produce(TOPIC, value=serialized, callback=delivery_report)
        producer.flush()
        return {"status": "sent", "event_id": event.event_id, "version": "v1"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/events/v2")
async def send_event_v2(event: EventV2):
    try:
        data = event.dict()
        serialized = avro_serializer_v2(data, SerializationContext(TOPIC, MessageField.VALUE))
        producer.produce(TOPIC, value=serialized, callback=delivery_report)
        producer.flush()
        return {"status": "sent", "event_id": event.event_id, "version": "v2"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def _produce(event_dict: dict, version: str = "v1"):
    """Напрямую отправляет событие в Kafka без HTTP-вызова."""
    if version == "v1":
        serialized = avro_serializer_v1(event_dict, SerializationContext(TOPIC, MessageField.VALUE))
    else:
        serialized = avro_serializer_v2(event_dict, SerializationContext(TOPIC, MessageField.VALUE))
    producer.produce(TOPIC, value=serialized, callback=delivery_report)
    producer.flush()

@app.post("/scenario/{name}")
async def run_scenario(name: str):
    base_ts = int(time.time() * 1000)
    if name == "basic-cycle":
        _produce({"event_id": "recv-1", "event_type": "PRODUCT_RECEIVED", "event_timestamp": base_ts,
                  "product_id": "SKU-001", "quantity": 100, "zone_id": "ZONE-A"})
        _produce({"event_id": "res-1", "event_type": "PRODUCT_RESERVED", "event_timestamp": base_ts + 300000,
                  "product_id": "SKU-001", "quantity": 30, "zone_id": "ZONE-A"})
        _produce({"event_id": "move-1", "event_type": "PRODUCT_MOVED", "event_timestamp": base_ts + 600000,
                  "product_id": "SKU-001", "quantity": 20, "from_zone": "ZONE-A", "to_zone": "ZONE-B"})
        _produce({"event_id": "ship-1", "event_type": "PRODUCT_SHIPPED", "event_timestamp": base_ts + 900000,
                  "product_id": "SKU-001", "quantity": 10, "zone_id": "ZONE-A"})
        order_items = json.dumps([{"product_id": "SKU-001", "zone_id": "ZONE-A", "quantity": 15}])
        _produce({"event_id": "order-1", "event_type": "ORDER_CREATED", "event_timestamp": base_ts + 1200000,
                  "order_id": "ORD-001", "order_items": order_items})
        _produce({"event_id": "order-complete-1", "event_type": "ORDER_COMPLETED", "event_timestamp": base_ts + 1500000,
                  "order_id": "ORD-001"})
        return {"scenario": "basic-cycle", "status": "executed"}
    elif name == "idempotency":
        _produce({"event_id": "dup-1", "event_type": "PRODUCT_RECEIVED", "event_timestamp": base_ts,
                  "product_id": "SKU-002", "quantity": 50, "zone_id": "ZONE-A"})
        _produce({"event_id": "dup-1", "event_type": "PRODUCT_RECEIVED", "event_timestamp": base_ts,
                  "product_id": "SKU-002", "quantity": 50, "zone_id": "ZONE-A"})
        return {"scenario": "idempotency", "status": "executed"}
    elif name == "out-of-order":
        _produce({"event_id": "time-1", "event_type": "PRODUCT_RECEIVED", "event_timestamp": base_ts,
                  "product_id": "SKU-004", "quantity": 100, "zone_id": "ZONE-A"})
        _produce({"event_id": "time-2", "event_type": "PRODUCT_SHIPPED", "event_timestamp": base_ts + 300000,
                  "product_id": "SKU-004", "quantity": 20, "zone_id": "ZONE-A"})
        _produce({"event_id": "time-3", "event_type": "PRODUCT_RECEIVED", "event_timestamp": base_ts + 120000,
                  "product_id": "SKU-004", "quantity": 50, "zone_id": "ZONE-A"})
        return {"scenario": "out-of-order", "status": "executed"}
    elif name == "dlq-test":
        _produce({"event_id": "bad-1", "event_type": "PRODUCT_SHIPPED", "event_timestamp": base_ts,
                  "product_id": "SKU-005", "quantity": -5, "zone_id": "ZONE-A"})
        _produce({"event_id": "good-1", "event_type": "PRODUCT_RECEIVED", "event_timestamp": base_ts + 300000,
                  "product_id": "SKU-005", "quantity": 10, "zone_id": "ZONE-A"})
        return {"scenario": "dlq-test", "status": "executed"}
    else:
        raise HTTPException(status_code=404, detail="Scenario not found")