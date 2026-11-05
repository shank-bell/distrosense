from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee, KafkaRecordSerializationSchema, KafkaSink,
)

from flink_jobs.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TRANSACTION_TIMEOUT_MS


def build_kafka_sink(topic: str, transactional_id_prefix: str) -> KafkaSink:
    """
    LLD: Flink writes anomaly-events + aggregated-metrics with exactly-once
    semantics — Flink's two-phase-commit Kafka sink. Records are written inside
    a Kafka transaction that is committed only when a checkpoint completes (every
    30s), so a consumer must read with isolation.level=read_committed to see them
    (and never sees a record twice, or one from a failed attempt).

    - the transactional id prefix must be stable across runs: a restarted job
      aborts the previous run's lingering transactions by prefix
    - transaction.timeout.ms stays below the broker's transaction.max.timeout.ms
    - gzip, because the downstream kafka-python consumers cannot decode snappy
    - values are JSON strings; records are written without a key
    """
    return (KafkaSink.builder()
            .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build())
            .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .set_transactional_id_prefix(transactional_id_prefix)
            .set_property("transaction.timeout.ms", str(KAFKA_TRANSACTION_TIMEOUT_MS))
            .set_property("compression.type", "gzip")
            .set_property("linger.ms", "5")
            .build())
