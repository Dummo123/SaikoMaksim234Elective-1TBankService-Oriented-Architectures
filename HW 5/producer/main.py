import asyncio
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator

from kafka_producer import AvroEventPublisher
from generator import SessionSimulator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer_api")

# env
KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker-a:29092,kafka-broker-b:29093")
SCHEMA_REG_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema_registry_svc:8081")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "raw-movie-events")
GEN_ENABLED = os.getenv("GENERATOR_ENABLED", "true").lower() == "true"
GEN_EPS = int(os.getenv("GENERATOR_EPS", "5"))

publisher: Optional[AvroEventPublisher] = None
gen_task: Optional[asyncio.Task] = None

class IncomingEvent(BaseModel):
    event_id: Optional[str] = None
    user_id: str = Field(..., min_length=1)
    movie_id: str = Field(..., min_length=1)
    event_type: str  # will be validated via custom validator
    timestamp: Optional[int] = None
    device_type: str
    session_id: str = Field(..., min_length=1)
    progress_seconds: Optional[int] = Field(None, ge=0)

    @field_validator("event_type")
    def check_event_type(cls, v):
        allowed = ["VIEW_STARTED", "VIEW_FINISHED", "VIEW_PAUSED", "VIEW_RESUMED", "LIKED", "SEARCHED"]
        if v not in allowed:
            raise ValueError(f"event_type must be one of {allowed}")
        return v

    @field_validator("device_type")
    def check_device(cls, v):
        allowed = ["MOBILE", "DESKTOP", "TV", "TABLET"]
        if v not in allowed:
            raise ValueError(f"device_type must be one of {allowed}")
        return v

@asynccontextmanager
async def lifespan(app: FastAPI):
    global publisher, gen_task
    log.info("starting producer, brokers=%s", KAFKA_BROKERS)
    publisher = AvroEventPublisher(
        bootstrap=KAFKA_BROKERS,
        reg_url=SCHEMA_REG_URL,
        topic_name=TOPIC_NAME,
        schema_file="schemas/movie_event.avsc",
    )
    if GEN_ENABLED:
        sim = SessionSimulator(publisher, eps=GEN_EPS)
        gen_task = asyncio.create_task(sim.run_loop())
        log.info("background generator enabled (EPS=%d)", GEN_EPS)
    yield
    log.info("shutting down")
    if gen_task:
        gen_task.cancel()
        try:
            await gen_task
        except asyncio.CancelledError:
            pass
    if publisher:
        publisher.flush()

app = FastAPI(title="Event Producer", lifespan=lifespan)

@app.get("/health")
def health():
    return {"status": "alive"}

@app.post("/events")
def publish_event(ev: IncomingEvent):
    eid = ev.event_id or str(uuid.uuid4())
    ts = ev.timestamp or int(datetime.now(timezone.utc).timestamp() * 1000)
    payload = {
        "event_id": eid,
        "user_id": ev.user_id,
        "movie_id": ev.movie_id,
        "event_type": ev.event_type,
        "timestamp": ts,
        "device_type": ev.device_type,
        "session_id": ev.session_id,
        "progress_seconds": ev.progress_seconds,
    }
    try:
        publisher.send_event(payload)
    except ValueError as e:
        raise HTTPException(400, detail=str(e))
    except Exception as e:
        log.exception("kafka send error")
        raise HTTPException(500, detail="internal kafka error")
    return {"event_id": eid, "status": "accepted"}

@app.get("/stats")
def stats():
    return publisher.get_stats() if publisher else {}