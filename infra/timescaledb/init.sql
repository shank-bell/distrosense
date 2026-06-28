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

SELECT create_hypertable('metrics', 'time', if_not_exists => TRUE);

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