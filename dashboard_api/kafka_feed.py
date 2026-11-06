import asyncio
import threading
import time
import uuid
from common.kafka_consumer import JsonConsumer
from dashboard_api.config import KAFKA_BOOTSTRAP_SERVERS, LIVE_TOPICS


class KafkaFeed:
    """
    Tails anomaly-events and incident-alerts from "now" and pushes every message
    to the Hub. The consumer blocks, so it runs in its own thread and hands
    messages to the asyncio loop with run_coroutine_threadsafe. A unique group id
    per process means the dashboard never steals partitions from (or commits
    offsets on behalf of) the real pipeline consumers.

    Reads with read_committed (common/kafka_consumer.py): anomaly-events is
    written by Flink's exactly-once sink, so a record becomes visible here when
    the checkpoint that contains it completes (up to ~30s after detection).
    """

    def __init__(self, loop: asyncio.AbstractEventLoop, hub):
        self._loop = loop
        self._hub = hub
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="dashboard-kafka-feed", daemon=True)
        self.connected = False

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _run(self):
        while not self._stop.is_set():
            try:
                consumer = JsonConsumer(
                    list(LIVE_TOPICS.keys()),
                    KAFKA_BOOTSTRAP_SERVERS,
                    f"dashboard-{uuid.uuid4().hex[:8]}",
                    auto_offset_reset="latest",
                    enable_auto_commit=False,
                    name="Dashboard",
                )
                self.connected = True
                print("[Dashboard] Kafka feed connected")
                try:
                    while not self._stop.is_set():
                        record = consumer.poll(1.0)
                        if record is None:
                            continue
                        frame_type = LIVE_TOPICS[record.topic]
                        asyncio.run_coroutine_threadsafe(self._hub.broadcast(frame_type, record.value), self._loop)
                finally:
                    consumer.close()
            except Exception as e:
                self.connected = False
                print(f"[Dashboard] Kafka feed error: {type(e).__name__}: {e} — retrying in 5s")
                time.sleep(5)
