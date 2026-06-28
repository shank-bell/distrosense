import uuid
import time


def generate_trace_id() -> str:
    return str(uuid.uuid4()).replace("-", "")


def generate_span_id() -> str:
    return str(uuid.uuid4()).replace("-", "")[:16]


def generate_span(service_id: str, metrics: dict, anomaly_type: str | None) -> dict:
    """
    Builds an OTEL-like span dict for a single service emission.
    This is what gets serialised and sent to Kafka.
    """
    return {
        "trace_id": generate_trace_id(),
        "span_id": generate_span_id(),
        "service_id": service_id,
        "timestamp": int(time.time() * 1000),
        "metrics": metrics,
        "anomaly_type": anomaly_type,
        "is_anomaly": anomaly_type is not None,
    }