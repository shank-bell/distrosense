import datetime
import time

import psycopg2
from psycopg2.extras import execute_values

from flink_jobs.config import (
    TIMESCALE_HOST, TIMESCALE_PORT,
    TIMESCALE_DB, TIMESCALE_USER, TIMESCALE_PASSWORD
)

_METRICS_UNIQUE_INDEX = "metrics_time_service_metric_uq"


def iso_utc(epoch_ms: int) -> str:
    """Unambiguous (timezone-aware) ISO timestamp for a TIMESTAMPTZ column."""
    return datetime.datetime.fromtimestamp(epoch_ms / 1000, tz=datetime.timezone.utc).isoformat()


class TimescaleDBSink:
    """
    LLD: Flink → SQL INSERT metrics + anomaly_events into TimescaleDB.

    Used from inside Flink window functions. Flink's guarantee is at-least-once
    for side effects (a task restarted from a checkpoint re-fires windows), so
    every write is idempotent: anomaly_events by primary key, metrics by a unique
    index on (time, service_id, metric_name) that open() creates if it is missing
    (infra/timescaledb/init.sql creates it on a fresh database).

    A failed write is retried (reconnecting) a few times and then RAISED: the
    task fails and Flink restarts it from the last checkpoint instead of silently
    losing rows.
    """
    ATTEMPTS = 3

    def __init__(self):
        self._conn = None
        self._idempotent_metrics = False

    def open(self):
        self._conn = psycopg2.connect(
            host     = TIMESCALE_HOST,
            port     = TIMESCALE_PORT,
            dbname   = TIMESCALE_DB,
            user     = TIMESCALE_USER,
            password = TIMESCALE_PASSWORD,
            connect_timeout = 10,
        )
        self._conn.autocommit = False
        self._ensure_unique_index()
        print("[TimescaleDBSink] Connected")

    def _ensure_unique_index(self):
        try:
            with self._conn.cursor() as cur:
                cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {_METRICS_UNIQUE_INDEX} "
                            "ON metrics (time, service_id, metric_name)")
            self._conn.commit()
        except Exception as e:
            # Existing duplicate rows, or another parallel instance creating it at the same moment.
            self._conn.rollback()
            print(f"[TimescaleDBSink] could not create unique index on metrics ({type(e).__name__}); checking")
        with self._conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_indexes WHERE indexname = %s", (_METRICS_UNIQUE_INDEX,))
            self._idempotent_metrics = cur.fetchone() is not None
        self._conn.commit()
        if not self._idempotent_metrics:
            print("[TimescaleDBSink] WARNING: metrics has no unique index — a replayed window "
                  "would insert duplicate rows")

    def _ensure_conn(self):
        """Reconnect if the server dropped the connection (restart, idle timeout...)."""
        if self._conn is None or self._conn.closed:
            self.open()

    def reset(self):
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            pass
        self._conn = None

    def _run(self, operation):
        last = None
        for attempt in range(1, self.ATTEMPTS + 1):
            try:
                self._ensure_conn()
                operation()
                self._conn.commit()
                return
            except Exception as e:
                last = e
                try:
                    if self._conn is not None and not self._conn.closed:
                        self._conn.rollback()   # else the aborted transaction poisons every later write
                except Exception:
                    self.reset()
                if attempt < self.ATTEMPTS:
                    self.reset()
                    time.sleep(0.5 * attempt)
        raise last

    def write_metric(self, aggregate: dict):
        """
        Takes the FULL aggregate dict of one tumbling window (one call per window,
        not four) and maps its keys to the metrics table's (metric_name, value) rows.
        """
        target = "(time, service_id, metric_name) " if self._idempotent_metrics else ""
        sql = f"""
            INSERT INTO metrics
                (time, service_id, metric_name, value, span_id, anomaly_score, is_anomaly)
            VALUES %s
            ON CONFLICT {target}DO NOTHING
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

        def op():
            with self._conn.cursor() as cur:
                execute_values(cur, sql, rows)
        self._run(op)

    def write_anomaly(self, anomaly: dict):
        sql = """
            INSERT INTO anomaly_events
                (id, detected_at, service_id, anomaly_type, severity,
                 model_used, reconstruction_error, resolved)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """
        params = (
            anomaly["id"],
            anomaly["detected_at_iso"],
            anomaly["service_id"],
            anomaly["anomaly_type"],
            anomaly["severity"],
            anomaly["model_used"],
            anomaly.get("reconstruction_error"),
            False,
        )

        def op():
            with self._conn.cursor() as cur:
                cur.execute(sql, params)
        self._run(op)

    def close(self):
        if self._conn:
            self._conn.close()
            print("[TimescaleDBSink] Disconnected")
