import json
from typing import Iterable, List

import numpy as np
from pyflink.datastream.functions import ProcessWindowFunction, RuntimeContext

from flink_jobs import metrics as M
from flink_jobs.sinks.timescaledb_sink import TimescaleDBSink, iso_utc


def aggregate_events(service_id: str, window_start_ms: int, window_end_ms: int, events: List) -> dict:
    """LLD AggregateFunction: mean, p99, max per service over one tumbling window."""
    cpu = [e.cpu_percent for e in events]
    lat = [e.latency_p99 for e in events]
    err = [e.error_rate for e in events]
    req = [e.request_rate for e in events]
    return {
        "service_id":        service_id,
        "window_start_ms":   window_start_ms,
        "window_end_ms":     window_end_ms,
        "cpu_mean":          float(np.mean(cpu)),
        "cpu_max":           float(np.max(cpu)),
        "latency_p99":       float(np.percentile(lat, 99)),
        "latency_mean":      float(np.mean(lat)),
        "error_rate_mean":   float(np.mean(err)),
        "request_rate_mean": float(np.mean(req)),
        "span_count":        len(events),
    }


class MetricAggregationWindow(ProcessWindowFunction):
    """
    LLD: TumblingEventTimeWindows(60s) -> ProcessWindowFunction.
    Emits the aggregate as JSON for the exactly-once Kafka sink and writes the
    same row to TimescaleDB (a unique index on (time, service_id, metric_name)
    makes a replayed window a no-op).
    """

    def __init__(self):
        self._tsdb = None

    def open(self, runtime_context: RuntimeContext):
        self._tsdb = TimescaleDBSink()
        self._tsdb.open()

    def close(self):
        if self._tsdb:
            self._tsdb.close()

    def process(self, key: str, context: ProcessWindowFunction.Context, elements: Iterable) -> Iterable[str]:
        window = context.window()
        events = list(elements)
        if not events:
            return
        aggregate = aggregate_events(key, window.start, window.end, events)
        self._tsdb.write_metric({**aggregate, "timestamp_iso": iso_utc(window.end)})
        M.WINDOWS_FLUSHED.inc()
        print(f"[TumblingWindow] {key} [{window.start}..{window.end}) {len(events)} spans")
        yield json.dumps(aggregate)

    def clear(self, context: ProcessWindowFunction.Context):
        pass
