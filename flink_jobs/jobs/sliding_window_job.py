import json
import time
import datetime
from kafka import KafkaConsumer
from flink_jobs.config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID,
    TOPIC_RAW_TELEMETRY, SLIDE_INTERVAL_SECONDS,
)
from flink_jobs.models.metric_event import MetricEvent
from flink_jobs.models.anomaly_event import AnomalyEvent
from flink_jobs.operators.zscore_trigger import ZScoreTrigger
from flink_jobs.operators.deduplication_filter import DeduplicationFilter
from flink_jobs.operators.watermark_assigner import BoundedOutOfOrdernessWatermark
from flink_jobs.sinks.kafka_sink import KafkaSink
from flink_jobs.sinks.timescaledb_sink import TimescaleDBSink


class SlidingWindowJob:
    """
    LLD: SlidingEventTimeWindows(5min, 30s)
    z = (value - mean) / (std + ε)
    if z > 3.0: emit AnomalyCandidate to anomaly-events
    """
    def __init__(self):
        self._zscore     = ZScoreTrigger()
        self._dedup      = DeduplicationFilter()
        self._watermark  = BoundedOutOfOrdernessWatermark()
        self._kafka_sink = KafkaSink()
        self._tsdb_sink  = TimescaleDBSink()
        self._last_slide: dict[str, int] = {}

    def run(self):
        consumer = KafkaConsumer(
            TOPIC_RAW_TELEMETRY,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID + "-sliding",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
        )

        self._kafka_sink.open()
        self._tsdb_sink.open()
        print("[SlidingWindowJob] Running sliding window z-score detection...")

        try:
            for msg in consumer:
                event_dict = msg.value
                span_id    = event_dict.get("span_id", "")
                ts_ms      = event_dict.get("timestamp", int(time.time() * 1000))

                # Dedup
                if self._dedup.is_duplicate(span_id):
                    continue

                # Watermark
                self._watermark.update(ts_ms)
                if self._watermark.is_late(ts_ms):
                    continue

                event      = MetricEvent.from_dict(event_dict)
                service_id = event.service_id
                metrics    = {
                    "cpu_percent":  event.cpu_percent,
                    "latency_p99":  event.latency_p99,
                    "error_rate":   event.error_rate,
                    "request_rate": event.request_rate,
                }

                # Add to sliding window
                self._zscore.add_event(service_id, ts_ms, metrics)

                # Check slide interval (every 30s per service)
                last = self._last_slide.get(service_id, 0)
                if ts_ms - last < SLIDE_INTERVAL_SECONDS * 1000:
                    continue

                self._last_slide[service_id] = ts_ms

                # Compute z-scores
                z_scores = self._zscore.compute_zscores(service_id, metrics)
                if not z_scores:
                    continue

                is_anomaly, anomaly_type, max_z = self._zscore.is_anomalous(z_scores)
                if not is_anomaly:
                    continue

                # Build anomaly event
                anomaly_score = min(1.0, abs(max_z) / 10.0)
                severity      = AnomalyEvent.compute_severity(anomaly_score)

                anomaly = AnomalyEvent(
                    service_id    = service_id,
                    anomaly_type  = anomaly_type,
                    severity      = severity,
                    anomaly_score = anomaly_score,
                    model_used    = "ZSCORE",
                    z_score       = max_z,
                    trace_id      = event.trace_id,
                    span_id       = event.span_id,
                )

                anomaly_dict = anomaly.to_dict()
                print(f"[SlidingWindowJob] ANOMALY detected: {service_id} "
                      f"type={anomaly_type} z={max_z:.2f} severity={severity}")

                # Write to anomaly-events Kafka topic
                self._kafka_sink.write_anomaly(anomaly_dict)

                # Write to TimescaleDB
                ts_iso = datetime.datetime.utcfromtimestamp(ts_ms / 1000).isoformat()
                self._tsdb_sink.write_anomaly({
                    "detected_at_iso":     ts_iso,
                    "service_id":          service_id,
                    "anomaly_type":        anomaly_type,
                    "severity":            severity,
                    "model_used":          "ZSCORE",
                    "reconstruction_error": None,
                })

                self._dedup.evict_expired()

        finally:
            self._kafka_sink.close()
            self._tsdb_sink.close()