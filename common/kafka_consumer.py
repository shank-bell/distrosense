import json
import time
from collections import namedtuple

from confluent_kafka import Consumer, KafkaException

ConsumedRecord = namedtuple("ConsumedRecord", ["topic", "key", "value"])


class JsonConsumer:
    """
    JSON consumer on confluent-kafka (librdkafka), reading with
    isolation.level=read_committed.

    Flink's anomaly-events and aggregated-metrics sinks are exactly-once: they write
    inside Kafka transactions that commit when a checkpoint completes. A
    read_committed consumer sees only committed records (never one from an aborted
    attempt, never a duplicate after a restart) and the transaction marker records
    are skipped for it. kafka-python cannot do this — it has no read_committed mode
    and hands the marker records to the application as ordinary messages — so every
    consumer of a transactional topic goes through this class. It is also fine on
    non-transactional topics (incident-alerts).

    poll() returns a ConsumedRecord, or None when nothing arrived within the
    timeout or the record was not valid JSON (skipped, never raised: one bad
    payload must not kill a consumer loop).
    """

    def __init__(self, topics, bootstrap_servers: str, group_id: str,
                 auto_offset_reset: str = "latest", enable_auto_commit: bool = True,
                 name: str = "JsonConsumer"):
        self._name = name
        self._last_error_log = 0.0
        self._consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": enable_auto_commit,
            "isolation.level": "read_committed",
        })
        self._consumer.subscribe(list(topics))

    def poll(self, timeout: float = 1.0):
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            if msg.error().fatal():
                raise KafkaException(msg.error())
            self._log(f"consumer error: {msg.error()}")      # transient (e.g. topic not yet visible)
            return None
        try:
            value = json.loads(msg.value().decode("utf-8"))
        except (AttributeError, UnicodeDecodeError, ValueError):
            self._log(f"skipping an undecodable record from {msg.topic()}[{msg.partition()}]@{msg.offset()}")
            return None
        return ConsumedRecord(msg.topic(), msg.key(), value)

    def close(self):
        self._consumer.close()

    def _log(self, text: str):
        now = time.time()
        if now - self._last_error_log > 30:
            self._last_error_log = now
            print(f"[{self._name}] {text}")
