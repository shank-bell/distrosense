import json
from kafka import KafkaConsumer
from orchestrator.config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID, TOPIC_INCIDENT_ALERTS,
    K8S_NAMESPACE, K8S_CONTEXT, HPA_SCALE_UP_MIN_REPLICAS,
    HPA_REVERT_AFTER_SECONDS, K8S_REQUEST_TIMEOUT_SECONDS,
    ORCH_COOLDOWN_SECONDS, ORCH_MAX_INCIDENT_AGE_SECONDS, METRICS_PORT,
    NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD,
)
from orchestrator import metrics as M
from observability.server import start_metrics_server
from orchestrator.rule_engine import RuleEngine
from orchestrator.incident_filter import IncidentFilter
from orchestrator.integrations.k8s_client import K8sClient, DisabledK8sClient
from orchestrator.integrations.slack_client import SlackClient
from orchestrator.integrations.llm_runbook import RunbookSuggester
from correlation.graph.neo4j_client import Neo4jClient


def _safe(label, fn, *args, **kwargs):
    """
    Run one action. A failure (k8s ApiException, Slack network error, LLM
    error...) is logged and swallowed so it can neither skip the remaining
    actions — e.g. Slack must still fire if k8s is down — nor kill the
    consumer loop.
    """
    try:
        result = fn(*args, **kwargs)
        M.ACTIONS.labels(label, "ok").inc()
        return result
    except Exception as e:
        M.ACTIONS.labels(label, "error").inc()
        print(f"[Orchestrator] {label} failed: {type(e).__name__}: {e}")
        return None


def _deserialize(raw: bytes):
    """A malformed message becomes None (skipped) instead of killing the consumer."""
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None


def _handle_incident(incident, rule_engine, k8s, slack, runbook_suggester, neo4j):
    """
    LLD 8: matches the incident against the rule engine, then dispatches
    whichever actions the matched rule specifies. Each action is
    independent — a k8s action being skipped (no real deployment for that
    service_id) doesn't block Slack/runbook from still happening.
    """
    rule = rule_engine.evaluate(incident)
    if rule is None:
        print(f"[Orchestrator] No rule matched incident {incident['incident_id'][:8]} — skipping")
        return

    M.RULE_MATCHES.labels(rule["name"]).inc()
    actions = rule["actions"]
    root_cause = incident["root_cause_service"]
    print(f"[Orchestrator] Incident {incident['incident_id'][:8]} matched rule "
          f"'{rule['name']}': {actions}")

    if "trigger_hpa" in actions:
        _safe("trigger_hpa", k8s.trigger_hpa, root_cause, HPA_SCALE_UP_MIN_REPLICAS)

    if "rollback_canary" in actions:
        _safe("rollback_canary", k8s.rollback_canary, root_cause)

    runbook = None
    if "generate_runbook" in actions:
        related_services = _safe("neo4j subgraph", neo4j.get_related_services, root_cause, hops=2) or []
        runbook = _safe("generate_runbook", runbook_suggester.generate,
                        incident, related_services=related_services)

    if "post_slack" in actions:
        _safe("post_slack", slack.post_incident, incident, runbook=runbook)


def _dispatch(incident, incident_filter, rule_engine, k8s, slack, runbook_suggester, neo4j):
    if not isinstance(incident, dict):
        M.INCIDENTS.labels("undecodable").inc()
        print("[Orchestrator] Skipping undecodable message")
        return
    try:
        should_act, reason = incident_filter.check(incident)
        if not should_act:
            M.INCIDENTS.labels(reason.split(":")[0].split(" ")[0]).inc()   # duplicate | stale | cooldown
            print(f"[Orchestrator] Skipping incident {str(incident.get('incident_id'))[:8]}: {reason}")
            return
        _handle_incident(incident, rule_engine, k8s, slack, runbook_suggester, neo4j)
        M.INCIDENTS.labels("acted").inc()
    except Exception as e:  # malformed payload etc. — skip it, keep consuming
        M.INCIDENTS.labels("error").inc()
        print(f"[Orchestrator] Skipping incident that could not be handled: "
              f"{type(e).__name__}: {e}")


def run():
    start_metrics_server(METRICS_PORT, "orchestrator")
    rule_engine = RuleEngine()
    try:
        k8s = K8sClient(
            namespace=K8S_NAMESPACE, context=K8S_CONTEXT,
            revert_after_seconds=HPA_REVERT_AFTER_SECONDS, request_timeout=K8S_REQUEST_TIMEOUT_SECONDS,
        )
    except Exception as e:
        # No kubeconfig / unknown context: run Slack-only rather than refusing to start.
        print(f"[Orchestrator] Kubernetes unavailable ({type(e).__name__}: {e}) — "
              f"running without HPA/canary actions")
        k8s = DisabledK8sClient(f"{type(e).__name__}")
    slack = SlackClient()
    runbook_suggester = RunbookSuggester()
    neo4j = Neo4jClient(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    incident_filter = IncidentFilter(ORCH_COOLDOWN_SECONDS, ORCH_MAX_INCIDENT_AGE_SECONDS)

    # At-least-once: start from `earliest` (so incidents published while the
    # orchestrator was down are not lost — stale ones are filtered out) and
    # commit the offset only AFTER an incident has been handled. A crash
    # mid-handling means redelivery, which IncidentFilter de-duplicates.
    consumer = KafkaConsumer(
        TOPIC_INCIDENT_ALERTS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=_deserialize,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )

    print("[Orchestrator] Listening on incident-alerts...")
    try:
        while True:
            # poll() (not `for msg in consumer`) so the HPA revert timer still
            # runs during quiet periods. max_records=1 keeps commit() exact.
            records = consumer.poll(timeout_ms=1000, max_records=1)
            _safe("hpa revert", k8s.revert_expired)
            for messages in records.values():
                for msg in messages:
                    _dispatch(msg.value, incident_filter, rule_engine, k8s, slack,
                              runbook_suggester, neo4j)
                    consumer.commit()
    except KeyboardInterrupt:
        print("[Orchestrator] Shutting down...")
    finally:
        consumer.close()
        neo4j.close()


if __name__ == "__main__":
    run()
