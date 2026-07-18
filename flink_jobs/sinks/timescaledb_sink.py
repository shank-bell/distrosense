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

    def write_metric(self, aggregate: dict):
        """
        Takes the FULL aggregate dict from MetricAggregationJob._flush_window
        (one call per window, not four) and maps its keys to the metrics
        table's (metric_name, value) rows. The old version received a
        pre-subset dict per metric with mismatched key names and silently
        wrote mostly-zero rows.
        """
        sql = """
            INSERT INTO metrics
                (time, service_id, metric_name, value, span_id, anomaly_score, is_anomaly)
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        metric_map = {
            "cpu_percent":  aggregate.get("cpu_mean"),
            "latency_p99":  aggregate.get("latency_p99"),
            "error_rate":   aggregate.get("error_rate_mean"),
            "request_rate": aggregate.get("request_rate_mean"),
        }
        rows = [
            (aggregate["timestamp_iso"], aggregate["service_id"], metric_name, value, None, None, False)
            for metric_name, value in metric_map.items()
            if value is not None
        ]
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