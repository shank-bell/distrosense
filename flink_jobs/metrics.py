"""
Prometheus metrics for the Flink job.

PyFlink runs Python operators in separate worker processes (one per task, per
parallel instance), so a plain in-process registry would only ever see the
driver. runtime.prepare_metrics_dir() therefore sets PROMETHEUS_MULTIPROC_DIR
before anything imports prometheus_client: every worker then writes its
counters to a shared directory, and the driver serves the SUM of all of them on
one port (see serve_metrics). Metric names are unchanged from the pre-PyFlink
version, so the Grafana dashboard keeps working.

Counters only: in multiprocess mode they accumulate across worker restarts.
"""
import os

from prometheus_client import Counter

EVENTS_PROCESSED = Counter("distrosense_flink_events_processed_total", "Events that passed dedup + watermark", ["job"])
DEDUP_DROPPED    = Counter("distrosense_flink_dedup_dropped_total",    "Events dropped as duplicates", ["job"])
LATE_DROPPED     = Counter("distrosense_flink_late_dropped_total",     "Events dropped by the watermark as late", ["job"])
WINDOWS_FLUSHED  = Counter("distrosense_flink_windows_flushed_total",  "Tumbling windows aggregated and written")
ANOMALIES        = Counter("distrosense_flink_anomalies_detected_total", "Anomalies emitted to anomaly-events", ["anomaly_type"])
PROCESS_ERRORS   = Counter("distrosense_flink_processing_errors_total", "Messages whose processing raised", ["job"])
DECODE_FAILURES  = Counter("distrosense_flink_decode_failures_total",  "Avro payloads that could not be decoded")
JOB_RESTARTS     = Counter("distrosense_flink_job_restarts_total",     "Task restarts after a failure (Flink restart strategy)", ["job"])
# The Bloom pre-check is only worth having if it actually avoids RocksDB reads.
BLOOM_SKIPPED_LOOKUPS = Counter("distrosense_flink_dedup_bloom_skipped_lookups_total",
                                "Dedup state reads avoided because the Bloom filter proved the span new")
STATE_LOOKUPS    = Counter("distrosense_flink_dedup_state_lookups_total",
                           "Dedup RocksDB ValueState reads actually performed")


def serve_metrics(port: int, service: str = "flink-jobs") -> bool:
    """
    Serve /metrics. With PROMETHEUS_MULTIPROC_DIR set this aggregates every
    Python worker's counters; without it (unit tests, ad-hoc runs) it falls
    back to this process's own registry. Never fatal.
    """
    from prometheus_client import CollectorRegistry, multiprocess, start_http_server
    try:
        if os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
            registry = CollectorRegistry()
            multiprocess.MultiProcessCollector(registry)
            start_http_server(port, registry=registry)
        else:
            start_http_server(port)
        print(f"[metrics] {service}: Prometheus metrics on :{port}/metrics")
        return True
    except OSError as e:
        print(f"[metrics] {service}: could not bind :{port} ({e}) — metrics disabled")
        return False
