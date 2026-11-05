import json
import uuid
from typing import Iterable, List, Optional

import numpy as np
from pyflink.datastream.functions import ProcessWindowFunction, RuntimeContext

from flink_jobs import metrics as M
from flink_jobs.config import MIN_WINDOW_EVENTS, SLIDE_INTERVAL_SECONDS, ZSCORE_THRESHOLD
from flink_jobs.models.anomaly_event import AnomalyEvent
from flink_jobs.sinks.redis_sink import RedisTopKSink
from flink_jobs.sinks.timescaledb_sink import TimescaleDBSink, iso_utc

METRIC_FIELDS = ("cpu_percent", "latency_p99", "error_rate", "request_rate")
ANOMALY_TYPES = {
    "cpu_percent":  "CPU_SPIKE",
    "latency_p99":  "LATENCY_BURST",
    "error_rate":   "ERROR_RATE_SPIKE",
    "request_rate": "REQUEST_DROP",
}
# Which way each metric has to move to be an anomaly (README: "z > 3.0"). CPU, latency and
# error rate matter when they go UP; request rate when it goes DOWN. An error rate that
# fell by 3 sigma is not a "spike" (the pre-PyFlink code flagged both directions).
DIRECTION = {
    "cpu_percent":  +1.0,
    "latency_p99":  +1.0,
    "error_rate":   +1.0,
    "request_rate": -1.0,
}


class ZScoreTrigger:
    """
    LLD: z = (value - mean) / (std + ε), anomaly when |z| > 3.0.

    Pure maths, no Flink: evaluate() gets every event of one sliding window and
    scores the LATEST event against the distribution of the whole 5 minutes —
    four tests (one per metric) per window, as the LLD describes.

    Do not widen this to "every event in the window": with a fixed z > 3 cut-off
    the false-positive rate scales with the number of tests. Gaussian noise gives
    ~0.3% per test, so scoring ~120 samples per window (30 events x 4 metrics)
    flagged ~30% of all windows on pure noise in the first live run.

    The latest event must also be NEW in this window (inside the last `slide`
    seconds). If the service sent nothing in that slide the same latest event
    would be judged again in the next window and emitted twice.
    """

    def __init__(self, threshold: float = ZSCORE_THRESHOLD, min_events: int = MIN_WINDOW_EVENTS):
        self._threshold = threshold
        self._min_events = min_events

    def evaluate(self, events: List, new_from_ms: int) -> Optional[dict]:
        """Returns the anomaly for the latest event if its strongest metric exceeds the threshold, else None."""
        if len(events) < self._min_events:
            return None
        row = max(range(len(events)), key=lambda i: events[i].timestamp)
        if events[row].timestamp < new_from_ms:
            return None
        values = np.array([[getattr(e, f) for f in METRIC_FIELDS] for e in events], dtype=float)
        z = (values[row] - values.mean(axis=0)) / (values.std(axis=0) + 1e-9)
        toward_anomaly = z * np.array([DIRECTION[f] for f in METRIC_FIELDS])
        col = int(np.argmax(toward_anomaly))
        z_value = float(z[col])                       # signed: negative for REQUEST_DROP
        if toward_anomaly[col] <= self._threshold:
            return None
        event = events[row]
        field = METRIC_FIELDS[col]
        return {
            "anomaly_type": ANOMALY_TYPES[field],
            "z_score": round(z_value, 4),
            "event": event,
        }


def build_anomaly(service_id: str, candidate: dict) -> AnomalyEvent:
    event = candidate["event"]
    z_value = candidate["z_score"]
    anomaly_score = min(1.0, abs(z_value) / 10.0)
    # Deterministic id + event-time detected_at: if Flink replays a window after a
    # failure, the same anomaly gets the same id, so the TimescaleDB insert
    # (ON CONFLICT DO NOTHING) and the Redis ZADD are idempotent.
    anomaly_id = str(uuid.uuid5(uuid.NAMESPACE_URL,
                                f"distrosense/anomaly/{service_id}/{event.timestamp}/{candidate['anomaly_type']}"))
    return AnomalyEvent(
        id            = anomaly_id,
        service_id    = service_id,
        anomaly_type  = candidate["anomaly_type"],
        severity      = AnomalyEvent.compute_severity(anomaly_score),
        anomaly_score = anomaly_score,
        model_used    = "ZSCORE",
        detected_at   = event.timestamp,
        z_score       = z_value,
        trace_id      = event.trace_id,
        span_id       = event.span_id,
    )


class ZScoreWindowFunction(ProcessWindowFunction):
    """
    LLD: SlidingEventTimeWindows(5min, slide=30s) -> ProcessWindowFunction.
    Emits the anomaly as a JSON string for the exactly-once Kafka sink and, as a
    side effect, records it in TimescaleDB and the Redis top-k (idempotent).
    """

    def __init__(self):
        self._trigger = None
        self._tsdb = None
        self._topk = None

    def open(self, runtime_context: RuntimeContext):
        self._trigger = ZScoreTrigger()
        self._tsdb = TimescaleDBSink()
        self._tsdb.open()
        self._topk = RedisTopKSink()

    def close(self):
        if self._tsdb:
            self._tsdb.close()

    def process(self, key: str, context: ProcessWindowFunction.Context, elements: Iterable) -> Iterable[str]:
        window = context.window()
        candidate = self._trigger.evaluate(list(elements), window.end - SLIDE_INTERVAL_SECONDS * 1000)
        if candidate is None:
            return
        anomaly = build_anomaly(key, candidate)
        record = anomaly.to_dict()
        print(f"[SlidingWindow] ANOMALY {key} type={anomaly.anomaly_type} "
              f"z={anomaly.z_score:.2f} severity={anomaly.severity}")
        M.ANOMALIES.labels(anomaly.anomaly_type).inc()

        # A failure here fails the task: Flink restarts it from the last checkpoint and
        # re-fires this window; the deterministic id makes the second insert a no-op.
        self._tsdb.write_anomaly({
            "id":                   anomaly.id,
            "detected_at_iso":      iso_utc(anomaly.detected_at),
            "service_id":           key,
            "anomaly_type":         anomaly.anomaly_type,
            "severity":             anomaly.severity,
            "model_used":           anomaly.model_used,
            "reconstruction_error": None,
        })
        self._topk.record_anomaly(record)          # best-effort, never raises
        yield json.dumps(record)

    def clear(self, context: ProcessWindowFunction.Context):
        pass
