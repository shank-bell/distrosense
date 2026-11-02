from prometheus_client import Counter, Histogram

SPANS_RECEIVED  = Counter("distrosense_ingestion_spans_received_total",  "Spans received over gRPC + REST")
SPANS_ACCEPTED  = Counter("distrosense_ingestion_spans_accepted_total",  "Spans validated, de-duplicated and handed to Kafka")
SPANS_REJECTED  = Counter("distrosense_ingestion_spans_rejected_total",  "Spans that failed validation")
SPANS_DUPLICATE = Counter("distrosense_ingestion_spans_duplicate_total", "Spans dropped by the Redis Bloom-filter dedup")
SPANS_PUBLISHED = Counter("distrosense_ingestion_spans_published_total", "Kafka deliveries acknowledged by the broker")
SPANS_FAILED    = Counter("distrosense_ingestion_spans_failed_total",    "Spans that could not be serialised or delivered")
DLQ_MESSAGES    = Counter("distrosense_ingestion_dlq_messages_total",    "Messages routed to the dead-letter queue")
DEDUP_ERRORS    = Counter("distrosense_ingestion_dedup_errors_total",    "Redis dedup failures (ingestion fails open)")
ES_INDEXED      = Counter("distrosense_ingestion_es_docs_indexed_total", "Log documents indexed into Elasticsearch")
ES_FAILED       = Counter("distrosense_ingestion_es_docs_failed_total",  "Log documents Elasticsearch rejected or that could not be sent")
ES_DROPPED      = Counter("distrosense_ingestion_es_docs_dropped_total", "Log documents dropped because the indexer queue was full")
BATCH_SECONDS   = Histogram("distrosense_ingestion_batch_seconds", "Time to process one gRPC SpanBatch",
                            buckets=(.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5))
