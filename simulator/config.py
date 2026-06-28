import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("KAFKA_SCHEMA_REGISTRY_URL", "http://localhost:8081")

TOPIC_RAW_TELEMETRY = "raw-telemetry"

NUM_SERVICES = 100
EMIT_INTERVAL_SECONDS = 1.0
ANOMALY_PROBABILITY = 0.05
CASCADE_PROBABILITY = 0.3

METRIC_NAMES = [
    "cpu_percent",
    "latency_p99",
    "error_rate",
    "request_rate",
]

SERVICE_TIERS = ["frontend", "backend", "database", "cache", "gateway"]
SERVICE_TEAMS = ["payments", "orders", "inventory", "auth", "search", "infra"]