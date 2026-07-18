from dataclasses import dataclass
from typing import Optional


@dataclass
class MetricEvent:
    trace_id:     str
    span_id:      str
    service_id:   str
    timestamp:    int
    cpu_percent:  float
    latency_p99:  float
    error_rate:   float
    request_rate: float
    anomaly_type: Optional[str] = None
    is_anomaly:   bool = False

    @staticmethod
    def from_dict(d: dict) -> "MetricEvent":
        # The simulator's spans nest all four readings under "metrics"
        # (see trace_generator.py's generate_span) — this used to look
        # for cpu_percent etc. at the top level and always got 0.0.
        metrics = d.get("metrics", {})
        return MetricEvent(
            trace_id     = d.get("trace_id", ""),
            span_id      = d.get("span_id", ""),
            service_id   = d.get("service_id", ""),
            timestamp    = d.get("timestamp", 0),
            cpu_percent  = metrics.get("cpu_percent", 0.0),
            latency_p99  = metrics.get("latency_p99", 0.0),
            error_rate   = metrics.get("error_rate", 0.0),
            request_rate = metrics.get("request_rate", 0.0),
            anomaly_type = d.get("anomaly_type"),
            is_anomaly   = d.get("is_anomaly", False),
        )

    def to_dict(self) -> dict:
        return {
            "trace_id":     self.trace_id,
            "span_id":      self.span_id,
            "service_id":   self.service_id,
            "timestamp":    self.timestamp,
            "cpu_percent":  self.cpu_percent,
            "latency_p99":  self.latency_p99,
            "error_rate":   self.error_rate,
            "request_rate": self.request_rate,
            "anomaly_type": self.anomaly_type,
            "is_anomaly":   self.is_anomaly,
        }

    def feature_vector(self) -> list:
        return [
            self.cpu_percent,
            self.latency_p99,
            self.error_rate,
            self.request_rate,
        ]