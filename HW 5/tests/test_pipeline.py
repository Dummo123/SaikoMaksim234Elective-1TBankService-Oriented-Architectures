import os
import time
import uuid
from datetime import datetime, timezone

import clickhouse_connect
import pytest
import requests

PROD_URL = os.getenv("PRODUCER_URL", "http://event_producer:8000")
CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse_srv")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_DB = os.getenv("CLICKHOUSE_DB", "movie_analytics")
MAX_WAIT = 60

@pytest.fixture(scope="session")
def ch():
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            c = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, database=CH_DB)
            c.query("SELECT 1")
            yield c
            c.close()
            return
        except Exception as e:
            time.sleep(1)
    pytest.fail("clickhouse not ready")

@pytest.fixture(scope="session", autouse=True)
def prod_ready():
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            r = requests.get(f"{PROD_URL}/health", timeout=2)
            if r.status_code == 200:
                return
        except:
            pass
        time.sleep(1)
    pytest.fail("producer not ready")

def wait_for_event(ch, eid):
    deadline = time.time() + MAX_WAIT
    while time.time() < deadline:
        rows = ch.query("SELECT event_id, user_id, movie_id, event_type, device_type, session_id, progress_seconds FROM raw_events WHERE event_id = toUUID(%(e)s)", parameters={"e": eid}).result_rows
        if rows:
            return rows[0]
        time.sleep(2)
    return None

def test_single_event_e2e(ch):
    eid = str(uuid.uuid4())
    sid = str(uuid.uuid4())
    payload = {
        "event_id": eid,
        "user_id": "tester_007",
        "movie_id": "test_film",
        "event_type": "VIEW_STARTED",
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        "device_type": "DESKTOP",
        "session_id": sid,
        "progress_seconds": 0,
    }
    r = requests.post(f"{PROD_URL}/events", json=payload, timeout=5)
    assert r.status_code == 200, r.text
    assert r.json()["event_id"] == eid

    row = wait_for_event(ch, eid)
    assert row is not None, f"event {eid} missing"
    ch_eid, user, movie, etype, device, sess, prog = row
    assert str(ch_eid) == eid
    assert user == "tester_007"
    assert movie == "test_film"
    assert etype == "VIEW_STARTED"
    assert device == "DESKTOP"
    assert sess == sid
    assert prog == 0

def test_invalid_rejected():
    bad = {"user_id": "x", "movie_id": "y", "event_type": "NOT_EXIST", "device_type": "MOBILE", "session_id": "s"}
    r = requests.post(f"{PROD_URL}/events", json=bad, timeout=5)
    assert r.status_code == 422

def test_session_sequence(ch):
    sid = str(uuid.uuid4())
    uid = f"seq_{uuid.uuid4().hex[:6]}"
    evs = [("VIEW_STARTED", 0), ("VIEW_PAUSED", 15), ("VIEW_RESUMED", 15), ("VIEW_FINISHED", 90)]
    ids = []
    for i, (typ, prog) in enumerate(evs):
        eid = str(uuid.uuid4())
        ids.append(eid)
        r = requests.post(f"{PROD_URL}/events", json={
            "event_id": eid,
            "user_id": uid,
            "movie_id": "seq_movie",
            "event_type": typ,
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000) + i * 1000,
            "device_type": "MOBILE",
            "session_id": sid,
            "progress_seconds": prog,
        }, timeout=5)
        assert r.status_code == 200
    for eid in ids:
        assert wait_for_event(ch, eid) is not None
    cnt = ch.query("SELECT count() FROM raw_events WHERE session_id = %(s)s", parameters={"s": sid}).result_rows[0][0]
    assert cnt == 4