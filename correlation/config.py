import os
from dotenv import load_dotenv

load_dotenv()

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = "correlation-engine"

TOPIC_ANOMALY_EVENTS  = "anomaly-events"
TOPIC_INCIDENT_ALERTS = "incident-alerts"

# Neo4j — LLD: (:Service)-[:CALLS {avg_latency, volume}]->(:Service)
NEO4J_URI      = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "distrosense")

# TimescaleDB — reads `metrics` for Granger causality, writes `incidents`,
# updates `anomaly_events.correlated_incident_id`
TIMESCALE_HOST     = os.getenv("TIMESCALE_HOST", "localhost")
TIMESCALE_PORT     = int(os.getenv("TIMESCALE_PORT", "5432"))
TIMESCALE_DB       = os.getenv("TIMESCALE_DB", "distrosense")
TIMESCALE_USER     = os.getenv("TIMESCALE_USER", "postgres")
TIMESCALE_PASSWORD = os.getenv("TIMESCALE_PASSWORD", "postgres")

# LLD: 2-hop Cypher traversal to find affected services
CORRELATION_HOPS = 2

# How long a window of anomalies can be clustered into one incident.
# Matches flink_jobs.config.SLIDING_WINDOW_SECONDS (5 min): anomalies
# themselves are detected over a rolling 5-minute metric window, so a
# downstream service's anomaly can legitimately take up to that long to
# cross its own z-score threshold after an upstream cascade starts. A
# shorter correlation window risks closing (and losing) a cluster before
# a slow-forming downstream anomaly ever gets the chance to fire.
CORRELATION_WINDOW_SECONDS = 300

# LLD: Granger causality (maxlag=5, p<0.05) for root cause identification
GRANGER_MAXLAG = 5
GRANGER_PVALUE_THRESHOLD = 0.05

# Hard cap on how long a cluster can stay open regardless of activity. On a
# dense graph, a cluster's 2-hop neighborhood can keep overlapping with
# incoming anomalies indefinitely, continually refreshing its idle timer
# and never going idle long enough to finalize on busy traffic. This is
# what actually bounds a cluster's lifetime in practice — the window above
# only matters for a cluster that goes idle well before this cap.
CORRELATION_MAX_CLUSTER_AGE_SECONDS = 90

# Minimum aligned history points before attempting a Granger test — below
# this, results aren't meaningful (or statsmodels errors on too little data)
GRANGER_MIN_POINTS = 15

# How far back to pull metric history per service for causality testing
GRANGER_LOOKBACK_SECONDS = 1800  # 30 minutes

# Anomaly type -> metric name, used to pick each service's Granger input series
ANOMALY_TYPE_TO_METRIC = {
    "CPU_SPIKE":        "cpu_percent",
    "LATENCY_BURST":    "latency_p99",
    "ERROR_RATE_SPIKE": "error_rate",
    "REQUEST_DROP":     "request_rate",
}