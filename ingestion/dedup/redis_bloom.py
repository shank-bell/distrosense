import hashlib
import math
import time


class RedisBloomFilter:
    """
    Bloom filter backed by Redis bitmaps (SETBIT/GETBIT), so span_id dedup is
    shared across ingestion restarts and instances. Plain redis:7-alpine has
    no RedisBloom module, hence bitmaps.

    A Bloom filter only ever fills up, so the filter is time-bucketed: one
    bitmap per `bucket_seconds`, with the previous bucket still consulted and
    each bucket expiring after two periods. `capacity` is therefore spans per
    bucket, not forever. False positives (~error_rate) mean ~0.1% of fresh
    spans are wrongly treated as duplicates — the documented trade-off.
    """

    def __init__(self, client, prefix: str = "bloom:span", capacity: int = 500_000,
                 error_rate: float = 0.001, bucket_seconds: int = 3600, clock=time.time):
        self._r = client
        self._prefix = prefix
        self._bucket_seconds = bucket_seconds
        self._clock = clock
        self.num_bits = int(math.ceil(-capacity * math.log(error_rate) / (math.log(2) ** 2)))
        self.num_hashes = max(1, int(round(self.num_bits / capacity * math.log(2))))

    def _positions(self, item: str):
        # Kirsch-Mitzenmacher double hashing: k positions from one 128-bit digest.
        digest = hashlib.blake2b(item.encode("utf-8"), digest_size=16).digest()
        h1 = int.from_bytes(digest[:8], "big")
        h2 = int.from_bytes(digest[8:], "big") | 1
        return [(h1 + i * h2) % self.num_bits for i in range(self.num_hashes)]

    def _key(self, bucket: int) -> str:
        return f"{self._prefix}:{bucket}"

    def seen_or_add(self, item: str) -> bool:
        """True if `item` was (probably) seen before; otherwise records it and returns False."""
        positions = self._positions(item)
        bucket = int(self._clock() // self._bucket_seconds)
        current, previous = self._key(bucket), self._key(bucket - 1)

        pipe = self._r.pipeline(transaction=False)
        for p in positions:
            pipe.getbit(current, p)
        for p in positions:
            pipe.getbit(previous, p)
        bits = pipe.execute()
        k = len(positions)
        if all(bits[:k]) or all(bits[k:]):
            return True

        pipe = self._r.pipeline(transaction=False)
        for p in positions:
            pipe.setbit(current, p, 1)
        pipe.expire(current, 2 * self._bucket_seconds + 60)
        pipe.execute()
        return False
