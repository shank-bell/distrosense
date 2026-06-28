import json
import asyncio
from aiokafka import AIOKafkaProducer
from pybloom_live import ScalableBloomFilter
from simulator.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_RAW_TELEMETRY


class KafkaPublisher:
    def __init__(self):
        self._producer = None
        self._bloom = ScalableBloomFilter(
            initial_capacity=10000,
            error_rate=0.001,
        )

    async def start(self):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            compression_type="snappy",
            acks="all",
            enable_idempotence=True,
            linger_ms=5,
            max_batch_size=65536,
        )
        await self._producer.start()
        print(f"[KafkaPublisher] Connected to {KAFKA_BOOTSTRAP_SERVERS}")

    async def stop(self):
        if self._producer:
            await self._producer.stop()
            print("[KafkaPublisher] Disconnected")

    async def publish(self, span: dict, service_id: str):
        if not self._producer:
            raise RuntimeError("Publisher not started. Call start() first.")

        span_id = span.get("span_id")
        if span_id and span_id in self._bloom:
            print(f"[KafkaPublisher] Duplicate span_id {span_id} caught by Bloom filter — skipped")
            return

        if span_id:
            self._bloom.add(span_id)

        await self._producer.send(
            TOPIC_RAW_TELEMETRY,
            value=span,
            key=service_id.encode("utf-8"),
        )