import os
import asyncio
import logging
import json
import time
import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
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
    events_dlq_total as EVENTS_DLQ,
    consumer_lag,
    http_requests_total,
    http_request_errors_total,
    http_request_duration_seconds,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC = "warehouse-events"
DLQ_TOPIC = "warehouse-events-dlq"
GROUP_ID = "warehouse-state-consumer"
CASSANDRA_CONTACT_POINTS = os.getenv(
    "CASSANDRA_CONTACT_POINTS", "cassandra-1,cassandra-2,cassandra-3"
).split(",")

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
        producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    dlq_msg = {
        "original_event": original_event,
        "error_reason": error_reason,
        "error_code": "PROCESSING_ERROR",
        "failed_at": datetime.datetime.utcnow().isoformat() + "Z",
        "kafka_metadata": {"partition": partition, "offset": offset},
    }
    producer.produce(
        DLQ_TOPIC,
        value=json.dumps(dlq_msg).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)
    EVENTS_DLQ.inc()


def process_message(msg):
    try:
        event = avro_deserializer(msg.value(), None)
    except Exception as e:
        logger.exception("Deserialization error, sending to DLQ")
        send_to_dlq(
            {"raw": msg.value().decode(errors="ignore")},
            str(e),
            msg.partition(),
            msg.offset(),
        )
        consumer.commit(message=msg)
        return

    event_id = event.get("event_id")
    event_type = event.get("event_type")
    logger.info(
        f"Processing {event_type} id={event_id} "
        f"partition={msg.partition()} offset={msg.offset()}"
    )

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
    cassandra_client = CassandraClient(
        hosts=CASSANDRA_CONTACT_POINTS, keyspace="warehouse"
    )
    cassandra_client.connect()
    event_handler = EventHandler(cassandra_client)
    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 600000,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC])
    lag_task = asyncio.create_task(update_consumer_lag())

    async def consume_loop():
        loop = asyncio.get_event_loop()
        while True:
            msg = await loop.run_in_executor(None, consumer.poll, 1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            process_message(msg)
            await asyncio.sleep(0)

    task = asyncio.create_task(consume_loop())
    yield
    task.cancel()
    lag_task.cancel()
    consumer.close()
    cassandra_client.close()


app = FastAPI(lifespan=lifespan)


@app.middleware("http")
async def prometheus_middleware(request: Request, call_next):
    start = time.time()
    method = request.method
    endpoint = request.url.path
    try:
        response = await call_next(request)
        duration = time.time() - start
        status = response.status_code
        http_requests_total.labels(method=method, endpoint=endpoint, status=status).inc()
        http_request_duration_seconds.labels(method=method, endpoint=endpoint).observe(duration)
        if status >= 400:
            http_request_errors_total.labels(
                method=method, endpoint=endpoint, error_type=str(status)
            ).inc()
        return response
    except Exception as exc:
        duration = time.time() - start
        http_request_errors_total.labels(
            method=method, endpoint=endpoint, error_type=type(exc).__name__
        ).inc()
        http_requests_total.labels(method=method, endpoint=endpoint, status=500).inc()
        raise


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
    from fastapi.responses import Response
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
