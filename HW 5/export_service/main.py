import io
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import boto3
import pandas as pd
import psycopg
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from botocore.client import Config
from fastapi import FastAPI, HTTPException, Query
from psycopg.rows import dict_row
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s :: %(message)s")
log = logging.getLogger("export")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres_db")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "metrics_store")
PG_USER = os.getenv("POSTGRES_USER", "metrics_user")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "secret_pass")

S3_EP = os.getenv("S3_ENDPOINT", "http://minio_storage:9000")
S3_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "movie-analytics")
EXPORT_FMT = os.getenv("EXPORT_FORMAT", "parquet").lower()
CRON_SCHED = os.getenv("SCHEDULE_CRON", "0 1 * * *")

scheduler = None

def pg_dsn():
    return f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASS}"

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_EP,
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30))
def export_day(target_date: date, fmt: str = None):
    fmt = (fmt or EXPORT_FMT).lower()
    if fmt not in ("csv", "json", "parquet"):
        raise ValueError("format must be csv/json/parquet")

    log.info("=== export start date=%s fmt=%s ===", target_date, fmt)

    with psycopg.connect(pg_dsn(), row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT metric_date, metric_name, metric_value, extra, updated_at FROM metrics_daily WHERE metric_date = %s", (target_date,))
            metrics = cur.fetchall()
            cur.execute("SELECT metric_date, movie_id, rank_pos, view_count, computed_at FROM top_movies_daily WHERE metric_date = %s ORDER BY rank_pos", (target_date,))
            top = cur.fetchall()
            cur.execute("SELECT cohort_date, day_offset, cohort_total, returned, retention_pct, computed_at FROM retention_stats WHERE cohort_date = %s ORDER BY day_offset", (target_date,))
            ret = cur.fetchall()

    total = len(metrics) + len(top) + len(ret)
    if total == 0:
        log.warning("no data for %s", target_date)

    payload = {
        "export_date": str(target_date),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "metrics": [{**m, "metric_date": str(m["metric_date"]), "updated_at": m["updated_at"].isoformat()} for m in metrics],
        "top_movies": [{**t, "metric_date": str(t["metric_date"]), "computed_at": t["computed_at"].isoformat()} for t in top],
        "retention": [{**r, "cohort_date": str(r["cohort_date"]), "computed_at": r["computed_at"].isoformat()} for r in ret],
    }

    buffer = io.BytesIO()
    if fmt == "json":
        buffer.write(json.dumps(payload, default=str, indent=2).encode("utf-8"))
        content_type = "application/json"
    elif fmt == "csv":
        rows = []
        for m in payload["metrics"]:
            rows.append({"table": "metrics", **m})
        for t in payload["top_movies"]:
            rows.append({"table": "top_movies", **t})
        for r in payload["retention"]:
            rows.append({"table": "retention", **r})
        df = pd.DataFrame(rows)
        df.to_csv(buffer, index=False)
        content_type = "text/csv"
    else:  # parquet
        rows = []
        for m in payload["metrics"]:
            rows.append({"table": "metrics", **m})
        for t in payload["top_movies"]:
            rows.append({"table": "top_movies", **t})
        for r in payload["retention"]:
            rows.append({"table": "retention", **r})
        df = pd.DataFrame(rows)
        df.to_parquet(buffer, index=False, engine="pyarrow")
        content_type = "application/octet-stream"

    buffer.seek(0)
    key = f"daily/{target_date.isoformat()}/aggregates.{fmt}"
    s3 = get_s3_client()
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buffer.getvalue(), ContentType=content_type)

    log.info("export done s3://%s/%s size=%d records=%d", S3_BUCKET, key, buffer.getbuffer().nbytes, total)
    return {"status": "ok", "bucket": S3_BUCKET, "key": key, "records": total, "size_bytes": buffer.getbuffer().nbytes, "format": fmt}

@asynccontextmanager
async def lifespan(app: FastAPI):
    global scheduler
    scheduler = BackgroundScheduler(timezone="UTC")
    trigger = CronTrigger.from_crontab(CRON_SCHED)
    scheduler.add_job(
        lambda: export_day((datetime.now(timezone.utc) - timedelta(days=1)).date()),
        trigger=trigger,
        id="export_daily",
        misfire_grace_time=3600,
    )
    scheduler.start()
    log.info("export scheduler started, cron='%s'", CRON_SCHED)
    yield
    if scheduler:
        scheduler.shutdown(wait=False)

app = FastAPI(title="Export Service", lifespan=lifespan)

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/run")
def run_manual(target_date: Optional[str] = Query(None, alias="date"), fmt: Optional[str] = Query(None, alias="format")):
    try:
        d = datetime.strptime(target_date, "%Y-%m-%d").date() if target_date else (datetime.now(timezone.utc) - timedelta(days=1)).date()
    except ValueError:
        raise HTTPException(400, "date must be YYYY-MM-DD")
    try:
        return export_day(d, fmt)
    except Exception as e:
        log.exception("export failed")
        raise HTTPException(500, str(e))