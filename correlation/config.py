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
# Not in the original LLD's one-line spec — chosen because it comfortably
# spans a few SlidingWindowJob slide intervals (30s) so a cascade has time
# to show up downstream before we close the cluster.
CORRELATION_WINDOW_SECONDS = 120

# LLD: Granger causality (maxlag=5, p<0.05) for root cause identification
GRANGER_MAXLAG = 5
GRANGER_PVALUE_THRESHOLD = 0.05
CORRELATION_MAX_CLUSTER_AGE_SECONDS = 90