CREATE DATABASE IF NOT EXISTS movie_analytics;
USE movie_analytics;

-- Kafka engine table (reads Avro with schema registry)
CREATE TABLE IF NOT EXISTS kafka_events_stream
(
    event_id         String,
    user_id          String,
    movie_id         String,
    event_type       LowCardinality(String),
    timestamp        Int64,
    device_type      LowCardinality(String),
    session_id       String,
    progress_seconds Nullable(Int32)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker-a:29092,kafka-broker-b:29093',
    kafka_topic_list = 'raw-movie-events',
    kafka_group_name = 'clickhouse_consumer_group',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 2,
    kafka_max_block_size = 1048576,
    format_avro_schema_registry_url = 'http://schema_registry_svc:8081';

-- Permanent storage (MergeTree)
CREATE TABLE IF NOT EXISTS raw_events
(
    event_id         UUID,
    user_id          String,
    movie_id         String,
    event_type       LowCardinality(String),
    event_time       DateTime64(3, 'UTC'),
    event_date       Date MATERIALIZED toDate(event_time),
    device_type      LowCardinality(String),
    session_id       String,
    progress_seconds Nullable(Int32),
    ingested_at      DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY event_date
ORDER BY (event_date, event_type, user_id, event_time)
TTL event_date + INTERVAL 90 DAY;

-- Materialized view: Kafka -> raw_events
CREATE MATERIALIZED VIEW IF NOT EXISTS kafka_to_raw_mv
TO raw_events AS
SELECT
    toUUID(event_id) AS event_id,
    user_id,
    movie_id,
    event_type,
    fromUnixTimestamp64Milli(timestamp, 'UTC') AS event_time,
    device_type,
    session_id,
    progress_seconds
FROM kafka_events_stream;

-- Aggregating tables for performance
CREATE TABLE IF NOT EXISTS daily_dau_agg
(
    event_date  Date,
    uniq_users  AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY event_date;

CREATE MATERIALIZED VIEW IF NOT EXISTS dau_agg_mv
TO daily_dau_agg AS
SELECT event_date, uniqState(user_id) AS uniq_users
FROM raw_events
GROUP BY event_date;

CREATE TABLE IF NOT EXISTS movie_views_agg
(
    event_date Date,
    movie_id   String,
    view_cnt   AggregateFunction(count)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, movie_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS movie_views_mv
TO movie_views_agg AS
SELECT event_date, movie_id, countState() AS view_cnt
FROM raw_events
WHERE event_type = 'VIEW_STARTED'
GROUP BY event_date, movie_id;

CREATE TABLE IF NOT EXISTS avg_progress_agg
(
    event_date Date,
    avg_sec    AggregateFunction(avg, Int32)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY event_date;

CREATE MATERIALIZED VIEW IF NOT EXISTS avg_progress_mv
TO avg_progress_agg AS
SELECT event_date, avgState(assumeNotNull(progress_seconds)) AS avg_sec
FROM raw_events
WHERE event_type = 'VIEW_FINISHED' AND progress_seconds IS NOT NULL
GROUP BY event_date;