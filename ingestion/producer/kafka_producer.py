from confluent_kafka import Producer
from ingestion.config import KAFKA_PRODUCER_CONFIG, TOPIC_RAW_TELEMETRY
from ingestion.serialisation.avro_serialiser import serialise

TOPIC_DLQ = "dead-letter-queue"


class KafkaSpanProducer:
    def __init__(self):
        self._producer = Producer(KAFKA_PRODUCER_CONFIG)

    def publish(self, record: dict, service_id: str):
        payload = serialise(record)
        self._producer.produce(
            topic=TOPIC_RAW_TELEMETRY,
            key=service_id.encode("utf-8"),
            value=payload,
            on_delivery=self._delivery_report,
        )
        self._producer.poll(0)

    def flush(self):
        self._producer.flush()

    def _delivery_report(self, err, msg):
        if err:
            print(f"[KafkaProducer] Delivery failed: {err} — routing to DLQ")
            self._producer.produce(
                topic=TOPIC_DLQ,
                value=str(err).encode("utf-8"),
            )
        else:
            print(f"[KafkaProducer] Delivered to {msg.topic()} partition {msg.partition()}")