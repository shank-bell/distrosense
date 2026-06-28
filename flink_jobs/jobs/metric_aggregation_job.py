import json
import time
import datetime
import numpy as np
from collections import defaultdict, deque
from kafka import KafkaConsumer
from flink_jobs.config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID,
    TOPIC_RAW_TELEMETRY, TUMBLING_WINDOW_SECONDS,
)
from flink_jobs.models.metric_event import MetricEvent
from flink_jobs.operators.deduplication_filter import DeduplicationFilter
from flink_jobs.operators.watermark_assigner import BoundedOutOfOrdernessWatermark
from flink_jobs.sinks.timescaledb_sink import TimescaleDBSink
from flink_jobs.sinks.kafka_sink import KafkaSink


class MetricAggregationJob:
    """
    LLD: TumblingEventTimeWindows(60s)
    AggregateFunction: mean, p99, max
    Output: MetricAggregate → TimescaleDB + aggregated-metrics topic
    """
    def __init__(self):
        self._dedup      = DeduplicationFilter()
        self._watermark  = BoundedOutOfOrdernessWatermark()
        self._tsdb_sink  = TimescaleDBSink()
        self._kafka_sink = KafkaSink()
        # Per service: deque of events in current tumbling window
        self._windows: dict[str, list] = defaultdict(list)
        self._window_start: dict[str, int] = {}

    def run(self):
        consumer = KafkaConsumer(
            TOPIC_RAW_TELEMETRY,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID + "-aggregation",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
        )

        self._tsdb_sink.open()
        self._kafka_sink.open()
        print("[MetricAggregationJob] Running tumbling window aggregation...")

        try:
            for msg in consumer:
                event_dict = msg.value
                span_id    = event_dict.get("span_id", "")
                ts_ms      = event_dict.get("timestamp", int(time.time() * 1000))

                # Step 1: Dedup check (Bloom filter + dict)
                if self._dedup.is_duplicate(span_id):
                    continue

                # Step 2: Watermark check — drop late events
                self._watermark.update(ts_ms)
                if self._watermark.is_late(ts_ms):
                    continue

                event      = MetricEvent.from_dict(event_dict)
                service_id = event.service_id

                # Step 3: keyBy(service_id) — assign to window bucket
                if service_id not in self._window_start:
                    self._window_start[service_id] = ts_ms

                self._windows[service_id].append(event)

                # Step 4: Check if tumbling window closed (60s)
                window_age_ms = ts_ms - self._window_start[service_id]
                if window_age_ms >= TUMBLING_WINDOW_SECONDS * 1000:
                    self._flush_window(service_id, ts_ms)

                # Periodic TTL eviction
                self._dedup.evict_expired()

        finally:
            self._tsdb_sink.close()
            self._kafka_sink.close()

    def _flush_window(self, service_id: str, ts_ms: int):
        events = self._windows[service_id]
        if not events:
            return

        cpu_vals    = [e.cpu_percent  for e in events]
        lat_vals    = [e.latency_p99  for e in events]
        err_vals    = [e.error_rate   for e in events]
        req_vals    = [e.request_rate for e in events]

        aggregate = {
            "service_id":        service_id,
            "window_start_ms":   self._window_start[service_id],
            "window_end_ms":     ts_ms,
            "cpu_mean":          float(np.mean(cpu_vals)),
            "cpu_max":           float(np.max(cpu_vals)),
            "latency_p99":       float(np.percentile(lat_vals, 99)),
            "latency_mean":      float(np.mean(lat_vals)),
            "error_rate_mean":   float(np.mean(err_vals)),
            "request_rate_mean": float(np.mean(req_vals)),
            "span_count":        len(events),
        }

        # Write to TimescaleDB
        ts_iso = datetime.datetime.utcfromtimestamp(ts_ms / 1000).isoformat()
        for metric_name in ["cpu_mean", "latency_p99", "error_rate_mean", "request_rate_mean"]:
            self._tsdb_sink.write_metric({
                "timestamp_iso": ts_iso,
                "service_id":    service_id,
                metric_name:     aggregate[metric_name],
                "span_id":       None,
                "anomaly_score": None,
                "is_anomaly":    False,
            })

        # Write to aggregated-metrics topic
        self._kafka_sink.write_aggregate(aggregate)

        # Reset window
        self._windows[service_id] = []
        self._window_start[service_id] = ts_ms
        print(f"[MetricAggregationJob] Flushed window for {service_id} — {len(events)} events")