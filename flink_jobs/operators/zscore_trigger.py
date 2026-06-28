import numpy as np
from collections import defaultdict, deque
from flink_jobs.config import ZSCORE_THRESHOLD, SLIDING_WINDOW_SECONDS


class ZScoreTrigger:
    """
    LLD: SlidingEventTimeWindows(5min, 30s)
    ProcessWindowFunction: z-score calc
    z = (value - mean) / (std + ε)
    if z > 3.0: emit AnomalyCandidate
    """
    def __init__(self):
        # Per service_id: deque of (timestamp_ms, metrics_dict)
        self._windows: dict[str, deque] = defaultdict(deque)
        self._window_ms = SLIDING_WINDOW_SECONDS * 1000

    def add_event(self, service_id: str, timestamp_ms: int, metrics: dict):
        window = self._windows[service_id]
        window.append((timestamp_ms, metrics))
        self._evict_old(service_id, timestamp_ms)

    def _evict_old(self, service_id: str, current_ts_ms: int):
        window = self._windows[service_id]
        cutoff = current_ts_ms - self._window_ms
        while window and window[0][0] < cutoff:
            window.popleft()

    def compute_zscores(self, service_id: str, current_metrics: dict) -> dict:
        window = self._windows[service_id]
        if len(window) < 10:
            return {}

        history = [m for _, m in window]
        z_scores = {}

        for metric in ["cpu_percent", "latency_p99", "error_rate", "request_rate"]:
            values = np.array([h.get(metric, 0.0) for h in history])
            mean = np.mean(values)
            std  = np.std(values)
            current_val = current_metrics.get(metric, 0.0)
            z = (current_val - mean) / (std + 1e-9)
            z_scores[metric] = round(float(z), 4)

        return z_scores

    def is_anomalous(self, z_scores: dict) -> tuple[bool, str, float]:
        """
        Returns (is_anomaly, anomaly_type, max_z_score)
        LLD threshold: z > 3.0
        """
        max_z = 0.0
        anomaly_type = None

        mapping = {
            "cpu_percent":  "CPU_SPIKE",
            "latency_p99":  "LATENCY_BURST",
            "error_rate":   "ERROR_RATE_SPIKE",
            "request_rate": "REQUEST_DROP",
        }

        for metric, z in z_scores.items():
            if abs(z) > ZSCORE_THRESHOLD and abs(z) > abs(max_z):
                max_z = z
                anomaly_type = mapping.get(metric, "UNKNOWN")

        return (anomaly_type is not None), anomaly_type, max_z