from prometheus_client import start_http_server


def start_metrics_server(port: int, service: str) -> bool:
    """
    Expose this process's Prometheus metrics at http://0.0.0.0:<port>/metrics.
    Never fatal: a port clash (e.g. a second copy of the service) just disables
    metrics for that process instead of taking the service down.
    """
    try:
        start_http_server(port)
        print(f"[metrics] {service}: Prometheus metrics on :{port}/metrics")
        return True
    except OSError as e:
        print(f"[metrics] {service}: could not bind :{port} ({e}) — metrics disabled")
        return False
