import time
import redis
from flink_jobs.config import REDIS_HOST, REDIS_PORT, ANOMALY_TOPK_KEY, ANOMALY_TOPK_SIZE


class RedisTopKSink:
    """
    README: `ZADD anomaly_scores` (top-k). Every detected anomaly is added to a
    sorted set scored by its anomaly_score; the set is trimmed to the K highest,
    so ZREVRANGE gives the dashboard the worst anomalies without a DB query.
    Redis is a nice-to-have here: failures are logged (throttled) and swallowed,
    never allowed to stall anomaly detection.
    """

    def __init__(self, host: str = REDIS_HOST, port: int = REDIS_PORT,
                 key: str = ANOMALY_TOPK_KEY, size: int = ANOMALY_TOPK_SIZE):
        self._r = redis.Redis(host=host, port=port, socket_timeout=0.5, socket_connect_timeout=1)
        self._key = key
        self._size = size
        self._last_error_log = 0.0

    def record_anomaly(self, anomaly: dict) -> bool:
        member = f'{anomaly["service_id"]}|{anomaly["anomaly_type"]}|{anomaly["detected_at"]}'
        try:
            pipe = self._r.pipeline(transaction=False)
            pipe.zadd(self._key, {member: float(anomaly["anomaly_score"])})
            pipe.zremrangebyrank(self._key, 0, -(self._size + 1))     # keep only the top `size`
            pipe.execute()
            return True
        except redis.RedisError as e:
            now = time.time()
            if now - self._last_error_log > 30:
                self._last_error_log = now
                print(f"[RedisTopKSink] write failed ({type(e).__name__}: {e}) — top-k not updated")
            return False
