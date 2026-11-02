from prometheus_client import Counter, Histogram

SPANS_SENT      = Counter("distrosense_simulator_spans_sent_total",      "Spans handed to the publisher's transport")
SPANS_ACCEPTED  = Counter("distrosense_simulator_spans_accepted_total",  "Spans the ingestion service reported accepting")
SPANS_DROPPED   = Counter("distrosense_simulator_spans_dropped_total",   "Spans dropped after ingestion stayed unreachable")
SPANS_DUPLICATE = Counter("distrosense_simulator_spans_duplicate_total", "Spans skipped by the simulator's own Bloom filter")
BATCHES_SENT    = Counter("distrosense_simulator_batches_sent_total",    "gRPC SpanBatch calls that succeeded")
BATCH_SECONDS   = Histogram("distrosense_simulator_batch_seconds",       "gRPC IngestSpanBatch round-trip time",
                            buckets=(.002, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5))
