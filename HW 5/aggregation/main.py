import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException, Query

from db import get_ch_client, PgUpserter
from metrics import compute_all

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s :: %(message)s")
log = logging.getLogger("agg_main")

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse_srv")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_DB = os.getenv("CLICKHOUSE_DB", "movie_analytics")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres_db")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "metrics_store")
PG_USER = os.getenv("POSTGRES_USER", "metrics_user")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "secret_pass")
CRON_EXPR = os.getenv("SCHEDULE_CRON", "*/5 * * * *")

scheduler = None
pg_writer = None

def run_job(target_date: Optional[date] = None):
    if target_date is None:
        target_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    start = time.time()
    log.info("=== aggregation cycle start date=%s ===", target_date)

    ch = get_ch_client(CH_HOST, CH_PORT, CH_DB)
    try:
        res = compute_all(ch, target_date)
        pg_writer.write_metrics(res["metrics"])
        pg_writer.replace_top_movies(target_date, res["top_movies"])
        pg_writer.write_retention(res["retention"])
        elapsed = time.time() - start
        total_records = len(res["metrics"]) + len(res["top_movies"]) + len(res["retention"])
        log.info("=== cycle done date=%s records=%d time=%.2fs ===", target_date, total_records, elapsed)
        return {"status": "ok", "date": str(target_date), "records": total_records, "elapsed": round(elapsed,2)}
    finally:
        ch.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    global scheduler, pg_writer
    pg_writer = PgUpserter(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS)
    scheduler = BackgroundScheduler(timezone="UTC")
    trigger = CronTrigger.from_crontab(CRON_EXPR)
    scheduler.add_job(run_job, trigger=trigger, id="agg_cron", misfire_grace_time=300)
    scheduler.start()
    log.info("scheduler started with cron '%s'", CRON_EXPR)
    # initial run (if there is any data)
    try:
        run_job()
    except Exception as e:
        log.warning("initial run failed (maybe empty clickhouse): %s", e)
    yield
    if scheduler:
        scheduler.shutdown(wait=False)

app = FastAPI(title="Aggregation Service", lifespan=lifespan)

@app.get("/health")
def health():
    return {"alive": True}

@app.post("/run")
def manual_run(target_date: Optional[str] = Query(None, alias="date")):
    d = None
    if target_date:
        try:
            d = datetime.strptime(target_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(400, "use YYYY-MM-DD")
    try:
        return run_job(d)
    except Exception as e:
        log.exception("manual run error")
        raise HTTPException(500, str(e))

@app.get("/metrics/{target_date}")
def get_metrics(target_date: str):
    try:
        d = datetime.strptime(target_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, "YYYY-MM-DD required")
    with pg_writer._conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT metric_name, metric_value, extra, updated_at FROM metrics_daily WHERE metric_date = %s", (d,))
        metrics = cur.fetchall()
        cur.execute("SELECT movie_id, rank_pos, view_count FROM top_movies_daily WHERE metric_date = %s ORDER BY rank_pos", (d,))
        top = cur.fetchall()
        cur.execute("SELECT day_offset, cohort_total, returned, retention_pct FROM retention_stats WHERE cohort_date = %s ORDER BY day_offset", (d,))
        ret = cur.fetchall()
    return {"date": target_date, "metrics": metrics, "top_movies": top, "retention": ret}