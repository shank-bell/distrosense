import time
from pybloom_live import ScalableBloomFilter
from flink_jobs.config import DEDUP_TTL_MINUTES


class DeduplicationFilter:
    """
    LLD: DeduplicateOperator
    - Bloom filter pre-check (O(1), ~70% reduction in dict lookups)
    - Dict acts as RocksDB ValueState<Long> simulation (span_id -> last_seen_ts)
    - TTL = 10 minutes auto-expire
    """
    def __init__(self):
        self._bloom = ScalableBloomFilter(
            initial_capacity=100000,
            error_rate=0.001,
        )
        self._seen: dict[str, int] = {}
        self._ttl_ms = DEDUP_TTL_MINUTES * 60 * 1000

    def is_duplicate(self, span_id: str) -> bool:
        # Step 1: Bloom filter pre-check — O(1)
        # If not in bloom, definitely not a duplicate
        if span_id not in self._bloom:
            self._bloom.add(span_id)
            self._seen[span_id] = int(time.time() * 1000)
            return False

        # Step 2: Bloom said maybe — check dict (RocksDB ValueState simulation)
        if span_id in self._seen:
            return True

        # Bloom false positive — not actually seen
        self._seen[span_id] = int(time.time() * 1000)
        return False

    def evict_expired(self):
        """
        TTL eviction — remove span_ids older than 10 minutes.
        In real Flink this is handled by RocksDB TTL config.
        """
        now_ms = int(time.time() * 1000)
        expired = [
            sid for sid, ts in self._seen.items()
            if now_ms - ts > self._ttl_ms
        ]
        for sid in expired:
            del self._seen[sid]