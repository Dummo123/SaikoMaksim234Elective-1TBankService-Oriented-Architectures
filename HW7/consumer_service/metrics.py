from prometheus_client import Counter, Gauge, Histogram

# ── Kafka / event-processing metrics ────────────────────────────────────────

events_processed_total = Counter(
    "events_processed_total",
    "Total warehouse events successfully processed",
    ["event_type"],
)

events_dlq_total = Counter(
    "events_dlq_total",
    "Total events routed to the Dead Letter Queue",
)

cassandra_write_errors_total = Counter(
    "cassandra_write_errors_total",
    "Total Cassandra write failures",
)

event_processing_duration_seconds = Histogram(
    "event_processing_duration_seconds",
    "End-to-end processing time per event (Kafka receive → Cassandra commit)",
    ["event_type"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

consumer_lag = Gauge(
    "consumer_lag",
    "Current consumer lag per partition",
    ["partition"],
)

# ── HTTP metrics (HW7 point 4) ───────────────────────────────────────────────

http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP requests handled by the consumer service",
    ["method", "endpoint", "status"],
)

http_request_errors_total = Counter(
    "http_request_errors_total",
    "Total HTTP requests that resulted in an error",
    ["method", "endpoint", "error_type"],
)

http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request processing duration",
    ["method", "endpoint"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

EVENTS_PROCESSED = events_processed_total
EVENT_PROCESSING_DURATION = event_processing_duration_seconds
CASSANDRA_WRITE_ERRORS = cassandra_write_errors_total
