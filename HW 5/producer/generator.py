import asyncio
import logging
import random
import uuid
from datetime import datetime, timezone

from kafka_producer import AvroEventPublisher

log = logging.getLogger("generator")

USER_POOL = [f"user_{i:04d}" for i in range(1, 201)]
MOVIE_POOL = [f"movie_{i:03d}" for i in range(1, 51)]
DEVICES = ["MOBILE", "DESKTOP", "TV", "TABLET"]

class SessionSimulator:
    def __init__(self, publisher: AvroEventPublisher, eps=5):
        self.pub = publisher
        self.rate = max(1, eps)
        self.active_sessions = []

    def _now_ms(self):
        return int(datetime.now(timezone.utc).timestamp() * 1000)

    def _publish(self, sess, ev_type, progress_val=None):
        data = {
            "event_id": str(uuid.uuid4()),
            "user_id": sess["user_id"],
            "movie_id": sess["movie_id"],
            "event_type": ev_type,
            "timestamp": self._now_ms(),
            "device_type": sess["device_type"],
            "session_id": sess["session_id"],
            "progress_seconds": progress_val,
        }
        self.pub.send_event(data)

    def _start_new_session(self):
        u = random.choice(USER_POOL)
        m = random.choice(MOVIE_POOL)
        sess = {
            "session_id": str(uuid.uuid4()),
            "user_id": u,
            "movie_id": m,
            "device_type": random.choice(DEVICES),
            "progress": 0,
        }
        self._publish(sess, "VIEW_STARTED", 0)
        self.active_sessions.append(sess)

    def _step_session(self, sess):
        sess["progress"] += random.randint(10, 30)
        r = random.random()
        if r < 0.15:
            self._publish(sess, "VIEW_PAUSED", sess["progress"])
        elif r < 0.3:
            self._publish(sess, "VIEW_RESUMED", sess["progress"])
        elif r < 0.4:
            self._publish(sess, "LIKED", None)
        elif r < 0.55 and sess["progress"] > 60:
            self._publish(sess, "VIEW_FINISHED", sess["progress"])
            self.active_sessions.remove(sess)
        elif r < 0.6:
            self.active_sessions.remove(sess)

    def _search_event(self):
        u = random.choice(USER_POOL)
        sess = {
            "session_id": str(uuid.uuid4()),
            "user_id": u,
            "movie_id": random.choice(MOVIE_POOL),
            "device_type": random.choice(DEVICES),
        }
        self._publish(sess, "SEARCHED", None)

    async def run_loop(self):
        log.info("generator started, target eps=%d", self.rate)
        interval = 1.0 / self.rate
        while True:
            try:
                roll = random.random()
                if not self.active_sessions or roll < 0.2:
                    self._start_new_session()
                elif roll < 0.25:
                    self._search_event()
                else:
                    chosen = random.choice(self.active_sessions)
                    self._step_session(chosen)
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                log.info("generator loop cancelled")
                break
            except Exception as e:
                log.exception("unexpected error in generator: %s", e)
                await asyncio.sleep(1)