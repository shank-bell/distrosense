import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("KAFKA_SCHEMA_REGISTRY_URL", "http://localhost:8081")

TOPIC_RAW_TELEMETRY = "raw-telemetry"

GRPC_HOST = os.getenv("GRPC_HOST", "0.0.0.0")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))

REST_HOST = os.getenv("REST_HOST", "0.0.0.0")
REST_PORT = int(os.getenv("REST_PORT", "8000"))

# Redis Bloom-filter span_id dedup / Elasticsearch log index (README: Storage)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
DEDUP_ENABLED = os.getenv("INGEST_DEDUP_ENABLED", "true").lower() == "true"
ES_LOGS_ENABLED = os.getenv("INGEST_ES_LOGS_ENABLED", "true").lower() == "true"

KAFKA_PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",
    "enable.idempotence": True,
    # gzip, not snappy: every consumer in this repo is kafka-python, which only ships
    # gzip. With snappy those consumers died with UnsupportedCodecError (they need the
    # optional python-snappy native extension), crash-looping the Flink jobs.
    "compression.type": "gzip",
    "linger.ms": 5,
    "batch.size": 65536,
    "message.timeout.ms": 30000,   # surface undeliverable messages (-> DLQ) instead of retrying for 5 min
}
