import numpy as np
from simulator.services.base_service import BaseService


def generate_metrics(service: BaseService) -> dict:
    """
    Generate realistic metrics for a service using gaussian noise.
    Returns a dict of metric_name -> value.
    """
    cfg = service.config

    cpu = max(0.0, min(100.0,
        cfg.base_cpu + np.random.normal(0, cfg.base_cpu * 0.1)
    ))

    latency = max(0.0,
        cfg.base_latency + np.random.normal(0, cfg.base_latency * 0.15)
    )

    error_rate = max(0.0, min(1.0,
        cfg.base_error_rate + np.random.normal(0, cfg.base_error_rate * 0.2)
    ))

    request_rate = max(0.0,
        cfg.base_request_rate + np.random.normal(0, cfg.base_request_rate * 0.1)
    )

    return {
        "cpu_percent": round(cpu, 4),
        "latency_p99": round(latency, 4),
        "error_rate": round(error_rate, 6),
        "request_rate": round(request_rate, 4),
    }