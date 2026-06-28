import psycopg2
from psycopg2.extras import execute_values
from flink_jobs.config import (
    TIMESCALE_HOST, TIMESCALE_PORT,
    TIMESCALE_DB, TIMESCALE_USER, TIMESCALE_PASSWORD
)


class TimescaleDBSink:
    """
    LLD: Flink → SQL INSERT metrics + anomaly_events into TimescaleDB
    """
    def __init__(self):
        self._conn = None

    def open(self):
        self._conn = psycopg2.connect(
            host     = TIMESCALE_HOST,
            port     = TIMESCALE_PORT,
            dbname   = TIMESCALE_DB,
            user     = TIMESCALE_USER,
            password = TIMESCALE_PASSWORD,
        )
        self._conn.autocommit = False
        print("[TimescaleDBSink] Connected")

    def write_metric(self, event: dict):
        sql = """
            INSERT INTO metrics
                (time, service_id, metric_name, value, span_id, anomaly_score, is_anomaly)
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        rows = []
        for metric_name in ["cpu_percent", "latency_p99", "error_rate", "request_rate"]:
            rows.append((
                event["timestamp_iso"],
                event["service_id"],
                metric_name,
                event.get(metric_name, 0.0),
                event.get("span_id"),
                event.get("anomaly_score"),
                event.get("is_anomaly", False),
            ))
        with self._conn.cursor() as cur:
            execute_values(cur, sql, rows)
        self._conn.commit()

    def write_anomaly(self, anomaly: dict):
        sql = """
            INSERT INTO anomaly_events
                (detected_at, service_id, anomaly_type, severity,
                 model_used, reconstruction_error, resolved)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (
                anomaly["detected_at_iso"],
                anomaly["service_id"],
                anomaly["anomaly_type"],
                anomaly["severity"],
                anomaly["model_used"],
                anomaly.get("reconstruction_error"),
                False,
            ))
        self._conn.commit()

    def close(self):
        if self._conn:
            self._conn.close()
            print("[TimescaleDBSink] Disconnected")