import os
import asyncio
import logging
import json
import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from cassandra_client import CassandraClient
from handlers import EventHandler
from metrics import (
    events_processed_total as EVENTS_PROCESSED,
    event_processing_duration_seconds as EVENT_PROCESSING_DURATION,
    cassandra_write_errors_total as CASSANDRA_WRITE_ERRORS,
    consumer_lag,
    events_dlq_total as EVENTS_DLQ,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC = "warehouse-events"
DLQ_TOPIC = "warehouse-events-dlq"
GROUP_ID = "warehouse-state-consumer"
CASSANDRA_CONTACT_POINTS = os.getenv("CASSANDRA_CONTACT_POINTS", "cassandra-1,cassandra-2,cassandra-3").split(",")

schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
avro_deserializer = AvroDeserializer(schema_registry_client)

cassandra_client = None
event_handler = None
consumer = None
producer = None

def delivery_report(err, msg):
    if err:
        logger.error(f"DLQ delivery failed: {err}")

def send_to_dlq(original_event, error_reason, partition, offset):
    global producer
    if producer is None:
        producer_conf = {"bootstrap.servers": KAFKA_BOOTSTRAP}
        producer = Producer(producer_conf)
    dlq_msg = {
        "original_event": original_event,
        "error_reason": error_reason,
        "error_code": "PROCESSING_ERROR",
        "failed_at": datetime.datetime.utcnow().isoformat() + "Z",
        "kafka_metadata": {"partition": partition, "offset": offset}
    }
    producer.produce(DLQ_TOPIC, value=json.dumps(dlq_msg).encode("utf-8"), callback=delivery_report)
    producer.poll(0)
    EVENTS_DLQ.inc()

def process_message(msg):
    try:
        event = avro_deserializer(msg.value(), None)
    except Exception as e:
        logger.exception(f"Deserialization error, sending to DLQ")
        send_to_dlq({"raw": msg.value().decode(errors='ignore')}, str(e), msg.partition(), msg.offset())
        consumer.commit(message=msg)
        return

    event_id = event.get("event_id")
    event_type = event.get("event_type")
    logger.info(f"Processing {event_type} id={event_id} partition={msg.partition()} offset={msg.offset()}")

    if cassandra_client.is_processed(event_id):
        logger.info(f"Duplicate event {event_id}, skipping")
        consumer.commit(message=msg)
        return

    with EVENT_PROCESSING_DURATION.labels(event_type=event_type).time():
        try:
            event_handler.handle(event)
            EVENTS_PROCESSED.labels(event_type=event_type).inc()
            consumer.commit(message=msg)
        except Exception as e:
            logger.exception(f"Error handling event {event_id}")
            CASSANDRA_WRITE_ERRORS.inc()
            send_to_dlq(event, str(e), msg.partition(), msg.offset())
            consumer.commit(message=msg)

async def update_consumer_lag():
    while True:
        if consumer is None:
            await asyncio.sleep(5)
            continue
        try:
            assignment = consumer.assignment()
            if not assignment:
                await asyncio.sleep(5)
                continue
            for tp in assignment:
                low, high = consumer.get_watermark_offsets(tp)
                committed = consumer.committed([tp])[0].offset
                lag = high - committed if committed >= 0 else 0
                consumer_lag.labels(partition=tp.partition).set(lag)
        except Exception as e:
            logger.warning(f"Failed to update consumer lag: {e}")
        await asyncio.sleep(15)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global cassandra_client, event_handler, consumer
    logger.info("Lifespan starting...")
    logger.info("Connecting to Cassandra...")
    cassandra_client = CassandraClient(hosts=CASSANDRA_CONTACT_POINTS, keyspace="warehouse")
    cassandra_client.connect()
    logger.info("Cassandra connected")

    event_handler = EventHandler(cassandra_client)

    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 600000,
    }
    logger.info(f"Creating Kafka consumer with config: {consumer_conf}")
    consumer = Consumer(consumer_conf)
    logger.info(f"Subscribing to topic {TOPIC}")
    consumer.subscribe([TOPIC])
    logger.info("Subscription done")

    lag_task = asyncio.create_task(update_consumer_lag())
    logger.info("Lag updater task created")

    async def consume_loop():
        logger.info("Consume loop started")
        loop = asyncio.get_event_loop()
        while True:
            try:
                msg = await loop.run_in_executor(None, consumer.poll, 1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        raise KafkaException(msg.error())
                process_message(msg)
                await asyncio.sleep(0)
            except Exception as e:
                logger.exception(f"Unhandled exception in consume_loop: {e}")
                # Не завершаем цикл, а ждём и продолжаем
                await asyncio.sleep(5)

    task = asyncio.create_task(consume_loop())
    logger.info("Consume loop task created")
    yield
    logger.info("Lifespan shutting down...")
    task.cancel()
    lag_task.cancel()
    consumer.close()
    cassandra_client.close()

app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health():
    if cassandra_client is None or consumer is None:
        return JSONResponse({"status": "unavailable"}, status_code=503)
    try:
        cassandra_client.session.execute("SELECT now() FROM system.local")
    except Exception:
        return JSONResponse({"status": "cassandra unreachable"}, status_code=503)
    try:
        consumer.list_topics(timeout=5)
    except Exception:
        return JSONResponse({"status": "kafka unreachable"}, status_code=503)
    return {"status": "ok"}

@app.get("/metrics")
async def metrics():
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    return generate_latest(), {"Content-Type": CONTENT_TYPE_LATEST}