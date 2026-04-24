CREATE TABLE IF NOT EXISTS metrics_daily (
    id              BIGSERIAL PRIMARY KEY,
    metric_date     DATE        NOT NULL,
    metric_name     TEXT        NOT NULL,
    metric_value    DOUBLE PRECISION NOT NULL,
    extra           JSONB       NOT NULL DEFAULT '{}',
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT unique_metric UNIQUE (metric_date, metric_name, extra)
);

CREATE TABLE IF NOT EXISTS retention_stats (
    cohort_date  DATE    NOT NULL,
    day_offset   INT     NOT NULL,
    cohort_total INT     NOT NULL,
    returned     INT     NOT NULL,
    retention_pct DOUBLE PRECISION NOT NULL,
    computed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (cohort_date, day_offset)
);

CREATE TABLE IF NOT EXISTS top_movies_daily (
    metric_date DATE    NOT NULL,
    movie_id    TEXT    NOT NULL,
    rank_pos    INT     NOT NULL,
    view_count  BIGINT  NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (metric_date, movie_id)
);

CREATE INDEX idx_metrics_date ON metrics_daily (metric_date);
CREATE INDEX idx_top_date_rank ON top_movies_daily (metric_date, rank_pos);