import logging
from datetime import date, timedelta

log = logging.getLogger("agg_metrics")

def fetch_dau(ch, target_date):
    q = "SELECT uniqMerge(uniq_users) FROM daily_dau_agg WHERE event_date = %(d)s"
    res = ch.query(q, parameters={"d": target_date})
    val = res.result_rows[0][0] if res.result_rows else 0
    return {"date": target_date, "name": "dau", "value": float(val), "dimensions": {}}

def fetch_avg_watch(ch, target_date):
    q = "SELECT avgMerge(avg_sec) FROM avg_progress_agg WHERE event_date = %(d)s"
    res = ch.query(q, parameters={"d": target_date})
    val = res.result_rows[0][0] if res.result_rows else 0.0
    return {"date": target_date, "name": "avg_watch_seconds", "value": float(val), "dimensions": {}}

def fetch_conversion(ch, target_date):
    q = """
    SELECT countIf(event_type='VIEW_STARTED'), countIf(event_type='VIEW_FINISHED')
    FROM raw_events WHERE event_date = %(d)s
    """
    res = ch.query(q, parameters={"d": target_date})
    started, finished = res.result_rows[0] if res.result_rows else (0, 0)
    conv = finished / started if started > 0 else 0.0
    return {"date": target_date, "name": "view_conversion", "value": conv, "dimensions": {"starts": started, "ends": finished}}

def top_movies(ch, target_date, limit=10):
    q = """
    SELECT movie_id, countMerge(view_cnt) AS v
    FROM movie_views_agg
    WHERE event_date = %(d)s
    GROUP BY movie_id
    ORDER BY v DESC LIMIT %(lim)s
    """
    res = ch.query(q, parameters={"d": target_date, "lim": limit})
    out = []
    for rank, (mid, cnt) in enumerate(res.result_rows, 1):
        out.append({"movie_id": mid, "rank": rank, "views": cnt})
    return out

def retention_cohort(ch, cohort_date, max_off=7):
    q = """
    WITH cohort AS (
        SELECT user_id FROM raw_events
        GROUP BY user_id
        HAVING toDate(min(event_time)) = %(d)s
    ),
    activity AS (
        SELECT e.user_id, dateDiff('day', toDate(%(d)s), e.event_date) AS off
        FROM raw_events e INNER JOIN cohort USING (user_id)
        WHERE e.event_date BETWEEN %(d)s AND addDays(%(d)s, %(max)s)
        GROUP BY e.user_id, off
    )
    SELECT off, (SELECT count() FROM cohort) AS tot, count(DISTINCT user_id) AS ret
    FROM activity GROUP BY off ORDER BY off
    """
    res = ch.query(q, parameters={"d": cohort_date, "max": max_off})
    rows = []
    for off, tot, ret in res.result_rows:
        rows.append({
            "cohort_date": cohort_date,
            "day_offset": int(off),
            "cohort_size": int(tot),
            "returned": int(ret),
            "retention_pct": float(ret / tot) if tot > 0 else 0.0,
        })
    return rows

def compute_all(ch, target_date):
    log.info("computing for %s", target_date)
    metrics = [
        fetch_dau(ch, target_date),
        fetch_avg_watch(ch, target_date),
        fetch_conversion(ch, target_date),
    ]
    top = top_movies(ch, target_date, limit=10)
    retention_rows = []
    for offset in [1, 7]:
        cohort = target_date - timedelta(days=offset)
        retention_rows.extend(retention_cohort(ch, cohort, max_off=7))
    return {"metrics": metrics, "top_movies": top, "retention": retention_rows}