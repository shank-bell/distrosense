import os
from dotenv import load_dotenv

load_dotenv()

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = "response-orchestrator"
TOPIC_INCIDENT_ALERTS = "incident-alerts"

# Kubernetes — namespace the demo Rollouts/Services/HPAs are deployed into
K8S_NAMESPACE = os.getenv("K8S_NAMESPACE", "default")

# kubeconfig context to act on. Unset = whatever `kubectl config current-context`
# is, which is easy to get wrong when several clusters (minikube, docker-desktop,
# ...) share one kubeconfig. Set to "docker-desktop" to pin the orchestrator to
# the cluster that actually has the demo Rollouts + Argo Rollouts controller.
K8S_CONTEXT = os.getenv("K8S_CONTEXT") or None

# HPA scale-up target used by the trigger_hpa action
HPA_SCALE_UP_MIN_REPLICAS = 4
# ...and how long the raised minReplicas is kept before being put back. Without
# this a one-off incident would pin the service at 4+ replicas forever.
HPA_REVERT_AFTER_SECONDS = int(os.getenv("HPA_REVERT_AFTER_SECONDS", "900"))
K8S_REQUEST_TIMEOUT_SECONDS = 10   # the consumer loop is blocking: never wait forever on the API server

METRICS_PORT = int(os.getenv("ORCH_METRICS_PORT", "9103"))

# Alert hygiene (see orchestrator/incident_filter.py)
# Same root-cause service inside this window is suppressed unless its severity escalates.
ORCH_COOLDOWN_SECONDS = int(os.getenv("ORCH_COOLDOWN_SECONDS", "300"))
# Incidents older than this are not acted on (replay after downtime / first start with `earliest`).
ORCH_MAX_INCIDENT_AGE_SECONDS = int(os.getenv("ORCH_MAX_INCIDENT_AGE_SECONDS", "3600"))

# Slack — LLD 8.4. If SLACK_BOT_TOKEN isn't set, the Slack client logs and
# skips instead of failing, so the rest of the orchestrator still works
# before a real workspace/bot token exists.
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL", "#incidents")

# LLM runbook suggester — LLD 8.3. Defaults to Gemini; the placeholder model id
# in the original .env.example was not a real model id.
LLM_API_KEY = os.getenv("LLM_API_KEY", "")
# "gemini" or "xai" (Grok). Unset = inferred from the key: xAI keys start "xai-".
LLM_PROVIDER = (
    os.getenv("LLM_PROVIDER") or ("xai" if LLM_API_KEY.startswith("xai-") else "gemini")
).lower()
# "gemini-flash-lite-latest" is Google's rolling alias: pinned ids like
# gemini-2.0-flash / 2.5-flash get retired (they already return 404 for new
# users), the alias keeps working. Flash-lite is plenty for a 5-step runbook
# and was the fast, reliable one when tested (the full flash tier timed out).
_DEFAULT_MODELS = {"gemini": "gemini-flash-lite-latest", "xai": "grok-4"}
LLM_MODEL = os.getenv("LLM_MODEL") or _DEFAULT_MODELS.get(LLM_PROVIDER, "gemini-flash-lite-latest")

# TimescaleDB — kept for symmetry/future use
TIMESCALE_HOST     = os.getenv("TIMESCALE_HOST", "localhost")
TIMESCALE_PORT     = int(os.getenv("TIMESCALE_PORT", "5432"))
TIMESCALE_DB       = os.getenv("TIMESCALE_DB", "distrosense")
TIMESCALE_USER     = os.getenv("TIMESCALE_USER", "postgres")
TIMESCALE_PASSWORD = os.getenv("TIMESCALE_PASSWORD", "postgres")

# Neo4j — for pulling the relevant subgraph into the LLM prompt (LLD 8.3)
NEO4J_URI      = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "distrosense")