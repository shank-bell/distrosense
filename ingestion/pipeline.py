import time
from pydantic import ValidationError
from ingestion import metrics as M
from ingestion.models import SpanPayload


class IngestPipeline:
    """
    The single path every span takes, whether it arrived over gRPC or REST:
    validate (Pydantic) -> dedup (Redis Bloom filter) -> Avro/Kafka -> log index.
    ingest() returns one of: accepted | rejected | duplicate | failed.
    """

    def __init__(self, producer, dedup=None, log_indexer=None):
        self._producer = producer
        self._dedup = dedup
        self._log_indexer = log_indexer
        self._last_reject_log = 0.0

    def ingest(self, raw: dict) -> str:
        M.SPANS_RECEIVED.inc()
        try:
            span = SpanPayload(**raw)
        except (ValidationError, TypeError) as e:
            M.SPANS_REJECTED.inc()
            self._log_reject(raw, e)
            return "rejected"

        if self._dedup is not None:
            try:
                if self._dedup.seen_or_add(span.span_id):
                    M.SPANS_DUPLICATE.inc()
                    return "duplicate"
            except Exception:
                M.DEDUP_ERRORS.inc()          # Redis down: fail open, never block ingestion

        if not self._producer.publish(span.model_dump(), span.service_id):
            return "failed"
        M.SPANS_ACCEPTED.inc()
        if self._log_indexer is not None:
            self._log_indexer.add(span)
        return "accepted"

    def _log_reject(self, raw, err):
        now = time.time()
        if now - self._last_reject_log > 10:   # a bad producer must not flood the log
            self._last_reject_log = now
            first = err.errors()[0] if isinstance(err, ValidationError) else err
            print(f"[Ingest] rejected span {raw.get('span_id')!r}: {first}")
