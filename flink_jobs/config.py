import os
from dotenv import load_dotenv

load_dotenv()

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = "flink-anomaly-detector"

# Topics
TOPIC_RAW_TELEMETRY     = "raw-telemetry"
TOPIC_AGGREGATED        = "aggregated-metrics"
TOPIC_ANOMALY_EVENTS    = "anomaly-events"
TOPIC_INCIDENT_ALERTS   = "incident-alerts"

# TimescaleDB
TIMESCALE_HOST     = os.getenv("TIMESCALE_HOST", "localhost")
TIMESCALE_PORT     = int(os.getenv("TIMESCALE_PORT", "5432"))
TIMESCALE_DB       = os.getenv("TIMESCALE_DB", "distrosense")
TIMESCALE_USER     = os.getenv("TIMESCALE_USER", "postgres")
TIMESCALE_PASSWORD = os.getenv("TIMESCALE_PASSWORD", "postgres")

# Flink windows — exactly as per LLD
TUMBLING_WINDOW_SECONDS = 60
SLIDING_WINDOW_SECONDS  = 300   # 5 minutes
SLIDE_INTERVAL_SECONDS  = 30
WATERMARK_LAG_SECONDS   = 5     # BoundedOutOfOrderness
CHECKPOINT_INTERVAL_MS  = 30000 # 30s
DEDUP_TTL_MINUTES       = 10

# Z-score threshold — LLD: z > 3.0
ZSCORE_THRESHOLD = 3.0

# ML service
ML_SERVICE_HOST = os.getenv("ML_SERVICE_HOST", "localhost")
ML_SERVICE_PORT = int(os.getenv("ML_SERVICE_PORT", "50051"))