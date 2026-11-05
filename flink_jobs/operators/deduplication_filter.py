import time

from pybloom_live import BloomFilter
from pyflink.common import Time, Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

from flink_jobs import metrics as M
from flink_jobs.config import DEDUP_BLOOM_CAPACITY, DEDUP_BLOOM_ERROR, DEDUP_TTL_MINUTES


class RotatingBloom:
    """
    A Bloom filter that forgets. Two generations are kept and swapped every TTL:
    at any moment the union holds every key added during the last `ttl_ms`
    (it actually spans between one and two TTLs), so it can stand in front of a
    state that expires entries after `ttl_ms` without growing forever or
    drifting into a high false-positive rate.

    The filter lives in the worker's memory only, so it is EMPTY after a task
    restart while the restored RocksDB state is not. `is_warm()` says whether a
    full TTL has passed since this instance started; before that a Bloom MISS
    proves nothing and the caller must consult the state.
    """

    def __init__(self, ttl_ms: int, capacity: int, error_rate: float, clock=time.time):
        self._ttl_ms = ttl_ms
        self._capacity = capacity
        self._error = error_rate
        self._clock = clock
        self._started_ms = self._now_ms()
        self._rotated_ms = self._started_ms
        self._cur = BloomFilter(capacity=capacity, error_rate=error_rate)
        self._prev = None

    def _now_ms(self) -> int:
        return int(self._clock() * 1000)

    def _rotate_if_due(self):
        now = self._now_ms()
        if now - self._rotated_ms >= self._ttl_ms:
            self._prev, self._cur = self._cur, BloomFilter(capacity=self._capacity, error_rate=self._error)
            self._rotated_ms = now

    def is_warm(self) -> bool:
        return self._now_ms() - self._started_ms >= self._ttl_ms

    def maybe_contains(self, key: str) -> bool:
        self._rotate_if_due()
        return key in self._cur or (self._prev is not None and key in self._prev)

    def add(self, key: str):
        self._rotate_if_due()
        # BloomFilter raises IndexError once `capacity` items were added; the
        # generation is replaced early (a burst) and the key goes in the new one.
        try:
            self._cur.add(key)
        except IndexError:
            self._prev, self._cur = self._cur, BloomFilter(capacity=self._capacity, error_rate=self._error)
            self._rotated_ms = self._now_ms()
            self._cur.add(key)


class DeduplicationFilter(KeyedProcessFunction):
    """
    LLD: DeduplicateOperator, keyed by span_id.

    - state: ValueState<Long> (first-seen time), TTL 10 minutes, on the RocksDB
      state backend (expired entries are dropped by a RocksDB compaction filter)
    - Bloom filter pre-check in front of it: a span the filter has never seen is
      provably new, so the state READ is skipped (~one remote state call per
      event in PyFlink); only the write remains. A Bloom hit ("maybe seen") is
      confirmed against the state, which also absorbs false positives.
    """

    def __init__(self):
        self._seen = None
        self._bloom = None

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor("span-first-seen-ms", Types.LONG())
        ttl = (StateTtlConfig.new_builder(Time.minutes(DEDUP_TTL_MINUTES))
               .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
               .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
               .cleanup_in_rocksdb_compact_filter(1000)
               .build())
        descriptor.enable_time_to_live(ttl)
        self._seen = runtime_context.get_state(descriptor)
        self._bloom = RotatingBloom(DEDUP_TTL_MINUTES * 60 * 1000, DEDUP_BLOOM_CAPACITY, DEDUP_BLOOM_ERROR)

    def process_element(self, event, ctx: KeyedProcessFunction.Context):
        span_id = event.span_id

        if self._bloom.is_warm() and not self._bloom.maybe_contains(span_id):
            M.BLOOM_SKIPPED_LOOKUPS.inc()                  # provably new: no state read
        else:
            M.STATE_LOOKUPS.inc()
            if self._seen.value() is not None:
                M.DEDUP_DROPPED.labels("dedup").inc()
                self._bloom.add(span_id)                   # keep the pre-check complete
                return
        self._seen.update(int(time.time() * 1000))
        self._bloom.add(span_id)
        M.EVENTS_PROCESSED.labels("dedup").inc()
        yield event
