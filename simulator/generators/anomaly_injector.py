import random
import numpy as np
from simulator.services.base_service import BaseService

ANOMALY_TYPES = [
    "CPU_SPIKE",
    "LATENCY_BURST",
    "ERROR_RATE_SPIKE",
    "REQUEST_DROP",
]


def inject_anomaly(metrics: dict, anomaly_type: str) -> dict:
    """
    Takes normal metrics and distorts them based on anomaly type.
    """
    m = metrics.copy()

    if anomaly_type == "CPU_SPIKE":
        m["cpu_percent"] = min(100.0, m["cpu_percent"] * random.uniform(2.5, 4.0))

    elif anomaly_type == "LATENCY_BURST":
        m["latency_p99"] = m["latency_p99"] * random.uniform(5.0, 15.0)

    elif anomaly_type == "ERROR_RATE_SPIKE":
        m["error_rate"] = min(1.0, m["error_rate"] * random.uniform(10.0, 50.0))

    elif anomaly_type == "REQUEST_DROP":
        m["request_rate"] = m["request_rate"] * random.uniform(0.05, 0.2)

    return m


def maybe_inject(metrics: dict, probability: float = 0.05) -> tuple[dict, str | None]:
    """
    Randomly decides whether to inject an anomaly.
    Returns (metrics, anomaly_type) where anomaly_type is None if normal.
    """
    if random.random() < probability:
        anomaly_type = random.choice(ANOMALY_TYPES)
        return inject_anomaly(metrics, anomaly_type), anomaly_type
    return metrics, None