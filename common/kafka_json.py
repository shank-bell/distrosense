import json
import time
from confluent_kafka import Producer


class JsonProducer:
    """
    JSON producer on confluent-kafka (librdkafka), which — unlike kafka-python —
    supports the README's producer settings: acks=all, enable.idempotence,
    linger.ms=5, batch.size=64KB (compression is gzip, see below). Used for anomaly-events,
    aggregated-metrics and incident-alerts.

    send(wait=False) is fire-and-forget (delivery failures are logged, throttled);
    send(wait=True) blocks until the broker acks and RAISES if the message
    could not be delivered, so callers can't mistake a lost message for a sent one.
    """

    def __init__(self, bootstrap_servers: str, name: str):
        self._name = name
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            # gzip: the consumers (kafka-python) can't decode snappy without the
            # optional python-snappy extension — see ingestion/config.py.
            "compression.type": "gzip",
            "linger.ms": 5,
            "batch.size": 65536,
            "message.timeout.ms": 30000,
        })
        self._last_error_log = 0.0

    def send(self, topic: str, key, value: dict, wait: bool = False):
        data = json.dumps(value).encode("utf-8")
        k = key.encode("utf-8") if isinstance(key, str) else key
        errors = []

        def on_delivery(err, msg):
            if err is not None:
                errors.append(err)
                self._log(f"delivery to {topic} failed: {err}")

        try:
            self._producer.produce(topic, key=k, value=data, on_delivery=on_delivery)
        except BufferError:                      # local queue full — let it drain, retry once
            self._producer.poll(0.5)
            self._producer.produce(topic, key=k, value=data, on_delivery=on_delivery)
        self._producer.poll(0)

        if wait:
            remaining = self._producer.flush(10)
            if errors:
                raise RuntimeError(f"{self._name}: delivery to {topic} failed: {errors[0]}")
            if remaining:
                raise RuntimeError(f"{self._name}: {remaining} message(s) to {topic} not acknowledged within 10s")

    def close(self):
        self._producer.flush(10)

    def _log(self, msg: str):
        now = time.time()
        if now - self._last_error_log > 30:
            self._last_error_log = now
            print(f"[{self._name}] {msg}")
