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

KAFKA_PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",
    "enable.idempotence": True,
    "compression.type": "snappy",
    "linger.ms": 5,
    "batch.size": 65536,
}