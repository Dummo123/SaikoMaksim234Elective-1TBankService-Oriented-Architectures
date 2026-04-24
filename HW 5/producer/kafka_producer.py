import json
import logging
import time
from pathlib import Path
from threading import Lock

import fastavro
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer

logger = logging.getLogger("kafka_producer")

class AvroEventPublisher:
    def __init__(self, bootstrap, reg_url, topic_name, schema_file):
        self.topic = topic_name
        with open(schema_file, "r") as f:
            self.schema_raw = f.read()
        self.parsed = fastavro.parse_schema(json.loads(self.schema_raw))

        self.reg_client = SchemaRegistryClient({"url": reg_url})
        self.avro_ser = AvroSerializer(
            schema_registry_client=self.reg_client,
            schema_str=self.schema_raw,
            conf={"auto.register.schemas": True},
        )
        self.key_ser = StringSerializer("utf_8")

        self.p = Producer({
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 10,
            "retry.backoff.ms": 500,
            "max.in.flight.requests.per.connection": 5,
            "compression.type": "snappy",
            "linger.ms": 20,
            "client.id": "cinema-producer",
        })
        self.counter = 0
        self._lock = Lock()

    def send_event(self, ev_dict):
        fastavro.validate(ev_dict, self.parsed, raise_errors=True)

        ctx = SerializationContext(self.topic, MessageField.VALUE)
        val_bytes = self.avro_ser(ev_dict, ctx)
        key_bytes = self.key_ser(ev_dict["user_id"])

        self.p.produce(
            topic=self.topic,
            key=key_bytes,
            value=val_bytes,
            on_delivery=self._delivery_callback,
            headers={
                "event_type": ev_dict["event_type"].encode("utf-8"),
                "event_id": ev_dict["event_id"].encode("utf-8"),
            },
        )
        self.p.poll(0)

        with self._lock:
            self.counter += 1

        logger.info("sent event_id=%s type=%s user=%s", ev_dict["event_id"], ev_dict["event_type"], ev_dict["user_id"])

    def _delivery_callback(self, err, msg):
        if err:
            logger.error("kafka delivery fail: %s", err)

    def flush(self, timeout=10.0):
        return self.p.flush(timeout)

    def get_stats(self):
        with self._lock:
            return {"total_sent": self.counter}