import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
LIVE_TOPICS = {"anomaly-events": "anomaly", "incident-alerts": "incident"}   # topic -> websocket frame type

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
ANOMALY_TOPK_KEY = "anomaly_scores"

ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")

TIMESCALE_HOST     = os.getenv("TIMESCALE_HOST", "localhost")
TIMESCALE_PORT     = int(os.getenv("TIMESCALE_PORT", "5432"))
TIMESCALE_DB       = os.getenv("TIMESCALE_DB", "distrosense")
TIMESCALE_USER     = os.getenv("TIMESCALE_USER", "postgres")
TIMESCALE_PASSWORD = os.getenv("TIMESCALE_PASSWORD", "postgres")

NEO4J_URI      = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "distrosense")

DASHBOARD_HOST = os.getenv("DASHBOARD_HOST", "0.0.0.0")
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8090"))
# Vite dev server origin (the built UI is served by this same process, so no CORS then)
CORS_ORIGINS = os.getenv("DASHBOARD_CORS_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173").split(",")
WS_BACKFILL = 50   # events replayed to a newly connected browser so the feed isn't empty
