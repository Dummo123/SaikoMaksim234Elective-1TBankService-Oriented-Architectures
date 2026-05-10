from prometheus_client import Counter, Gauge, Histogram

events_processed_total = Counter(
    "events_processed_total",
    "Total number of warehouse events successfully processed",
    ["event_type"],
)

events_dlq_total = Counter(
    "events_dlq_total",
    "Total number of events routed to the Dead Letter Queue",
)

cassandra_write_errors_total = Counter(
    "cassandra_write_errors_total",
    "Total number of Cassandra write failures",
)

event_processing_duration_seconds = Histogram(
    "event_processing_duration_seconds",
    "End-to-end processing time per event (Kafka receive → Cassandra commit)",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

consumer_lag = Gauge(
    "consumer_lag",
    "Current consumer lag per partition", ["partition"]
)
