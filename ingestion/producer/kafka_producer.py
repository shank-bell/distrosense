import json
import threading
from confluent_kafka import Producer
from ingestion import metrics as M
from ingestion.config import KAFKA_PRODUCER_CONFIG, TOPIC_RAW_TELEMETRY, SCHEMA_REGISTRY_URL
from ingestion.serialisation.avro_serialiser import SpanAvroSerializer

TOPIC_DLQ = "dead-letter-queue"


class KafkaSpanProducer:
    """
    Idempotent, snappy-compressed (see KAFKA_PRODUCER_CONFIG) Avro producer for
    raw-telemetry. Anything that can't be serialised or delivered goes to the
    dead-letter queue with the failure reason in a header, instead of vanishing.
    """

    def __init__(self):
        self._producer = Producer(KAFKA_PRODUCER_CONFIG)
        self._serialize = SpanAvroSerializer(SCHEMA_REGISTRY_URL, TOPIC_RAW_TELEMETRY)
        self._stop = threading.Event()
        # Delivery callbacks only fire inside poll(); a dedicated thread keeps
        # them (and the delivered/failed metrics) moving even when traffic pauses.
        self._poller = threading.Thread(target=self._poll_loop, name="kafka-poller", daemon=True)
        self._poller.start()

    def publish(self, record: dict, service_id: str) -> bool:
        """Queue one span. Returns False if it had to be dead-lettered instead."""
        try:
            payload = self._serialize(record)
        except Exception as e:
            M.SPANS_FAILED.inc()
            self._dead_letter(json.dumps(record).encode(), service_id.encode(), f"serialization failed: {e}")
            return False
        try:
            self._produce(payload, service_id.encode())
        except BufferError:                      # local queue full: let the poller drain, retry once
            self._producer.poll(0.5)
            self._produce(payload, service_id.encode())
        return True

    def _produce(self, payload: bytes, key: bytes):
        self._producer.produce(topic=TOPIC_RAW_TELEMETRY, key=key, value=payload,
                               on_delivery=self._on_delivery)
        self._producer.poll(0)

    def _on_delivery(self, err, msg):
        if err is None:
            M.SPANS_PUBLISHED.inc()
            return
        M.SPANS_FAILED.inc()
        print(f"[KafkaProducer] Delivery failed: {err} — routing to DLQ")
        self._dead_letter(msg.value(), msg.key(), str(err))

    def _dead_letter(self, value: bytes, key, reason: str):
        M.DLQ_MESSAGES.inc()
        try:
            self._producer.produce(topic=TOPIC_DLQ, key=key, value=value,
                                   headers=[("error", reason[:300].encode()),
                                            ("source_topic", TOPIC_RAW_TELEMETRY.encode())])
        except Exception as e:
            print(f"[KafkaProducer] Could not even write to the DLQ: {e}")

    def _poll_loop(self):
        while not self._stop.is_set():
            self._producer.poll(0.5)

    def flush(self, timeout: float = 10.0):
        return self._producer.flush(timeout)

    def close(self):
        self._stop.set()
        self.flush()
