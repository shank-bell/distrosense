"""
One-off migration for the `incidents` table. A fresh docker-compose
volume already gets this table from infra/timescaledb/init.sql — this
script exists for a TimescaleDB instance that's already running with
data, so it doesn't need a volume wipe to pick up the new table.
"""
import psycopg2
from correlation.config import TIMESCALE_HOST, TIMESCALE_PORT, TIMESCALE_DB, TIMESCALE_USER, TIMESCALE_PASSWORD

conn = psycopg2.connect(
    host=TIMESCALE_HOST, port=TIMESCALE_PORT, dbname=TIMESCALE_DB,
    user=TIMESCALE_USER, password=TIMESCALE_PASSWORD,
)
conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS incidents (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            opened_at TIMESTAMPTZ NOT NULL,
            closed_at TIMESTAMPTZ,
            root_cause_service_id TEXT,
            member_service_ids TEXT[] NOT NULL,
            anomaly_count INTEGER NOT NULL,
            severity INTEGER CHECK (severity BETWEEN 1 AND 5),
            causality_method TEXT NOT NULL DEFAULT 'granger',
            resolved BOOLEAN DEFAULT FALSE
        )
    """)
conn.close()
print("[Migration] incidents table ready.")