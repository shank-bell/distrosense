from common.kafka_json import JsonProducer
from correlation.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_INCIDENT_ALERTS


class IncidentPublisher:
    """
    LLD 7.4 step 5: writes the grouped incident to incident-alerts with a
    root_cause_service field. Idempotent confluent-kafka producer; publish()
    waits for the broker ack and raises on failure, so the caller can report
    "saved but NOT published" accurately.
    """

    def __init__(self):
        self._producer = None

    def open(self):
        self._producer = JsonProducer(KAFKA_BOOTSTRAP_SERVERS, "IncidentPublisher")

    def publish(self, incident_id: str, cluster, root_cause_result: dict):
        # severity/anomaly_types: the response orchestrator's rule engine
        # (LLD 8.2) matches on these, so they need to travel with the
        # incident rather than requiring a separate lookup.
        # `or` (not a .get default) so an explicit None can't crash max()/sorted().
        severity = max((a.get("severity") or 1) for a in cluster.anomalies)
        anomaly_types = sorted({a.get("anomaly_type") or "UNKNOWN" for a in cluster.anomalies})

        payload = {
            "incident_id": incident_id,
            "root_cause_service": root_cause_result["root_cause_service_id"],
            "causality_method": root_cause_result["method"],
            "member_service_ids": sorted(cluster.service_ids),
            "anomaly_count": len(cluster.anomalies),
            "severity": severity,
            "anomaly_types": anomaly_types,
            "opened_at": cluster.created_at,
            "closed_at": cluster.last_updated,
        }
        self._producer.send(
            TOPIC_INCIDENT_ALERTS,
            root_cause_result["root_cause_service_id"],
            payload,
            wait=True,
        )

    def close(self):
        if self._producer:
            self._producer.close()
