import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

_REPO_ROOT = Path(__file__).resolve().parent.parent

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = "flink-anomaly-detector"

# Topics
TOPIC_RAW_TELEMETRY     = "raw-telemetry"
TOPIC_AGGREGATED        = "aggregated-metrics"
TOPIC_ANOMALY_EVENTS    = "anomaly-events"
TOPIC_INCIDENT_ALERTS   = "incident-alerts"

# raw-telemetry is Avro (Confluent wire format) when it arrives via the ingestion service
SCHEMA_REGISTRY_URL = os.getenv("KAFKA_SCHEMA_REGISTRY_URL", "http://localhost:8081")

# Where a consumer group with NO committed offset starts. A group that already
# has offsets always resumes from them, whatever this says — so restarts never
# lose data. "latest" is the default for raw-telemetry because a brand-new group
# on "earliest" would replay up to a week of telemetry and write every historic
# window into `metrics` a second time. Set FLINK_AUTO_OFFSET_RESET=earliest for
# a deliberate backfill.
AUTO_OFFSET_RESET = os.getenv("FLINK_AUTO_OFFSET_RESET", "latest")

# Redis: top-k anomaly scores (README: ZADD anomaly_scores)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
ANOMALY_TOPK_KEY  = "anomaly_scores"
ANOMALY_TOPK_SIZE = 100

METRICS_PORT = int(os.getenv("FLINK_METRICS_PORT", "9101"))

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
CHECKPOINT_INTERVAL_MS  = int(os.getenv("FLINK_CHECKPOINT_INTERVAL_MS", "30000"))  # 30s
DEDUP_TTL_MINUTES       = 10

# Z-score threshold — LLD: z > 3.0
ZSCORE_THRESHOLD = 3.0
# A window with fewer events than this has no meaningful distribution to score against.
MIN_WINDOW_EVENTS = 10

# ---- Flink runtime -------------------------------------------------------------
# Task parallelism. Every Python operator that sits behind a keyBy runs in its own
# Python worker process per parallel instance, so this multiplies memory.
PARALLELISM = int(os.getenv("FLINK_PARALLELISM", "1"))

# A source subtask that sees no data for this long stops holding the watermark back
# (raw-telemetry has 16 partitions; some subtasks can be idle).
SOURCE_IDLENESS_SECONDS = int(os.getenv("FLINK_SOURCE_IDLENESS_SECONDS", "30"))

# RocksDB working files + checkpoints live under this directory (a volume in Docker).
STATE_DIR = Path(os.getenv("FLINK_STATE_DIR", str(_REPO_ROOT / ".flink-state")))
CHECKPOINT_DIR = os.getenv("FLINK_CHECKPOINT_DIR", (STATE_DIR / "checkpoints").as_uri())
ROCKSDB_DIR    = os.getenv("FLINK_ROCKSDB_DIR", str(STATE_DIR / "rocksdb"))

# Exactly-once Kafka sinks use transactions. The timeout must stay below the broker's
# transaction.max.timeout.ms (15 min by default) and above checkpoint interval + restart time.
KAFKA_TRANSACTION_TIMEOUT_MS = int(os.getenv("FLINK_KAFKA_TXN_TIMEOUT_MS", "600000"))
# Stable per sink: a restarted job aborts the previous run's lingering transactions by prefix.
TXN_PREFIX_AGGREGATED = "distrosense-flink-aggregated"
TXN_PREFIX_ANOMALIES  = "distrosense-flink-anomalies"

RESTART_ATTEMPTS = int(os.getenv("FLINK_RESTART_ATTEMPTS", "1000"))
RESTART_DELAY_MS = int(os.getenv("FLINK_RESTART_DELAY_MS", "10000"))

# Dedup Bloom pre-check sizing (per parallel instance, per generation)
DEDUP_BLOOM_CAPACITY = int(os.getenv("FLINK_DEDUP_BLOOM_CAPACITY", "200000"))
DEDUP_BLOOM_ERROR    = 0.001

# Flink Web UI of the local mini cluster (0 = off)
WEB_UI_PORT = int(os.getenv("FLINK_WEB_UI_PORT", "8082"))

# Where fetch_jars.py puts the Kafka connector
JARS_DIR = Path(os.getenv("FLINK_JARS_DIR", str(_REPO_ROOT / "flink_jobs" / "lib")))

# ML service
ML_SERVICE_HOST = os.getenv("ML_SERVICE_HOST", "localhost")
ML_SERVICE_PORT = int(os.getenv("ML_SERVICE_PORT", "50051"))
