import logging
from contextlib import contextmanager

import clickhouse_connect
import psycopg
from psycopg.types.json import Jsonb
from tenacity import retry, stop_after_attempt, wait_exponential

log = logging.getLogger("agg_db")

def get_ch_client(host, port, db):
    return clickhouse_connect.get_client(host=host, port=port, database=db, username="default")

class PgUpserter:
    def __init__(self, host, port, db, user, pw):
        self.dsn = f"host={host} port={port} dbname={db} user={user} password={pw}"

    @contextmanager
    def _conn(self):
        conn = psycopg.connect(self.dsn)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    def write_metrics(self, metric_list):
        sql = """
        INSERT INTO metrics_daily (metric_date, metric_name, metric_value, extra, updated_at)
        VALUES (%s, %s, %s, %s::jsonb, now())
        ON CONFLICT (metric_date, metric_name, extra) DO UPDATE
        SET metric_value = EXCLUDED.metric_value, updated_at = EXCLUDED.updated_at
        """
        with self._conn() as conn, conn.cursor() as cur:
            for m in metric_list:
                cur.execute(sql, (m["date"], m["name"], m["value"], Jsonb(m.get("dimensions", {}))))
        log.info("wrote %d metric rows", len(metric_list))

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    def replace_top_movies(self, target_date, rows):
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM top_movies_daily WHERE metric_date = %s", (target_date,))
            for r in rows:
                cur.execute(
                    "INSERT INTO top_movies_daily (metric_date, movie_id, rank_pos, view_count, computed_at) VALUES (%s, %s, %s, %s, now())",
                    (target_date, r["movie_id"], r["rank"], r["views"])
                )
        log.info("top movies upserted for %s", target_date)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    def write_retention(self, rows):
        sql = """
        INSERT INTO retention_stats (cohort_date, day_offset, cohort_total, returned, retention_pct, computed_at)
        VALUES (%s, %s, %s, %s, %s, now())
        ON CONFLICT (cohort_date, day_offset) DO UPDATE
        SET cohort_total = EXCLUDED.cohort_total, returned = EXCLUDED.returned,
            retention_pct = EXCLUDED.retention_pct, computed_at = EXCLUDED.computed_at
        """
        with self._conn() as conn, conn.cursor() as cur:
            for r in rows:
                cur.execute(sql, (r["cohort_date"], r["day_offset"], r["cohort_size"], r["returned"], r["retention_pct"]))
        log.info("wrote %d retention rows", len(rows))