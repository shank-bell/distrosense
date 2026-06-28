import time
from flink_jobs.config import WATERMARK_LAG_SECONDS


class BoundedOutOfOrdernessWatermark:
    """
    LLD: BoundedOutOfOrderness(5s)
    Allows events up to 5 seconds late before closing a window.
    Events older than (max_seen_ts - lag) are considered late and dropped.
    """
    def __init__(self):
        self._max_seen_ts = 0
        self._lag_ms = WATERMARK_LAG_SECONDS * 1000

    def update(self, event_timestamp_ms: int):
        if event_timestamp_ms > self._max_seen_ts:
            self._max_seen_ts = event_timestamp_ms

    def current_watermark(self) -> int:
        return self._max_seen_ts - self._lag_ms

    def is_late(self, event_timestamp_ms: int) -> bool:
        return event_timestamp_ms < self.current_watermark()