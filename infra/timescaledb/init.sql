CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS metrics (
    time        TIMESTAMPTZ     NOT NULL,
    service_id  TEXT            NOT NULL,
    metric_name TEXT            NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    span_id     TEXT,
    anomaly_score DOUBLE PRECISION,
    is_anomaly  BOOLEAN         DEFAULT FALSE
);

-- README: 1-day chunks (Timescale's default is 7 days).
SELECT create_hypertable('metrics', 'time',
                         chunk_time_interval => INTERVAL '1 day',
                         if_not_exists => TRUE);

-- Flink's side effects are at-least-once (a task restarted from a checkpoint re-fires its
-- windows), so the window writer must be idempotent: this unique index makes a replayed
-- window's INSERT ... ON CONFLICT DO NOTHING a no-op. (Includes `time`, as hypertables require.)
CREATE UNIQUE INDEX IF NOT EXISTS metrics_time_service_metric_uq
    ON metrics (time, service_id, metric_name);

CREATE TABLE IF NOT EXISTS anomaly_events (
    id                    UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    detected_at           TIMESTAMPTZ     NOT NULL,
    service_id            TEXT            NOT NULL,
    anomaly_type          TEXT            NOT NULL,
    severity              INTEGER         CHECK (severity BETWEEN 1 AND 5),
    model_used            TEXT            NOT NULL,
    reconstruction_error  DOUBLE PRECISION,
    correlated_incident_id UUID,
    resolved              BOOLEAN         DEFAULT FALSE
);
CREATE TABLE IF NOT EXISTS incidents (
    id                     UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    opened_at              TIMESTAMPTZ     NOT NULL,
    closed_at              TIMESTAMPTZ,
    root_cause_service_id  TEXT,
    member_service_ids     TEXT[]          NOT NULL,
    anomaly_count          INTEGER         NOT NULL,
    severity               INTEGER         CHECK (severity BETWEEN 1 AND 5),
    causality_method       TEXT            NOT NULL DEFAULT 'granger',
    resolved               BOOLEAN         DEFAULT FALSE
);