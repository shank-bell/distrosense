import json
from kafka import KafkaProducer
from flink_jobs.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_ANOMALY_EVENTS, TOPIC_AGGREGATED


class KafkaSink:
    """
    LLD: Flink writes to anomaly-events + aggregated-metrics Kafka topics
    """
    def __init__(self):
        self._producer = None

    def open(self):
        self._producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            enable_idempotence=True,
            compression_type="gzip",
        )
        print("[KafkaSink] Connected")

    def write_anomaly(self, anomaly: dict):
        self._producer.send(
            TOPIC_ANOMALY_EVENTS,
            key=anomaly.get("service_id"),
            value=anomaly,
        )
        self._producer.flush()

    def write_aggregate(self, aggregate: dict):
        self._producer.send(
            TOPIC_AGGREGATED,
            key=aggregate.get("service_id"),
            value=aggregate,
        )
        self._producer.flush()

    def close(self):
        if self._producer:
            self._producer.close()
            print("[KafkaSink] Disconnected")