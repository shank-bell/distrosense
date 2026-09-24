import json
from kafka import KafkaProducer
from correlation.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_INCIDENT_ALERTS


class IncidentPublisher:
    """
    LLD 7.4 step 5: writes the grouped incident to incident-alerts with a
    root_cause_service field.
    """

    def __init__(self):
        self._producer = None

    def open(self):
        self._producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            compression_type="gzip",
        )

    def publish(self, incident_id: str, cluster, root_cause_result: dict):
        payload = {
            "incident_id": incident_id,
            "root_cause_service": root_cause_result["root_cause_service_id"],
            "causality_method": root_cause_result["method"],
            "member_service_ids": sorted(cluster.service_ids),
            "anomaly_count": len(cluster.anomalies),
            "opened_at": cluster.created_at,
            "closed_at": cluster.last_updated,
        }
        self._producer.send(
            TOPIC_INCIDENT_ALERTS,
            key=root_cause_result["root_cause_service_id"],
            value=payload,
        )
        self._producer.flush()

    def close(self):
        if self._producer:
            self._producer.close()